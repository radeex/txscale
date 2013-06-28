"""
Redis request/response support.

TODO! Use RPOPLPUSH to handle crashing endpoints! http://redis.io/commands/rpoplpush

Pros:
 - you can deploy it on Redis

Cons:


Design -- Responder:

The responder makes two connections to Redis. One is the "listening" connection and the other is
the "responding" connection.

The "listening" connection calls BLPOP on a list named after the service. When an item is returned,
the responder handles the message asynchronously and calls BLPOP again, immediately (with
a configurable maximum concurrency!).

The list that the responder calls BLPOP on is shared between multiple responders potentially
running on multiple hosts.

When the asynchronous handling of the message is complete, the "responding" connection is told
to PUSH the result onto a list named by the requester (that only that requester is listening on).


Design -- Requester:

The requester makes two connections to Redis. One is the "requesting" connection and the other
is the "response receiving" connection.

The "requesting" connection is used to PUSH a request onto a list named after the service. This is
done on demand, whenever the requester is told to send a request.

The "response receiving" connection is used to call BLPOP on a list named uniquely for the
requester. Whenever an item is returned, it is dispatched to the Deferred associated with the
request that it matches. It must then immediately re-issue a BLPOP call to fetch any other messages
that may come in.
"""

from uuid import uuid4

from zope.interface import implementer

from twisted.python import log
from twisted.internet.protocol import ReconnectingClientFactory
from twisted.internet.defer import Deferred, gatherResults

from txredis.client import Redis

from .interfaces import IResponder, IRequester
from .messages import splitRequest, splitResponse, generateRequest, generateResponse


class TimeOutError(Exception):
    """Timed out."""

    def __init__(self, timeout_type, timeout_value, request):
        self.request = request
        self.timeout_type = timeout_type
        self.timeout_value = timeout_value
        super(TimeOutError, self).__init__(
            "Request reached %s of %s: %r" % (timeout_type, timeout_value, request))


class _WatchedConnection(object):
    """
    Ensures that a protocol remains connected to an endpoint.

    Calls the the passed-in callback when a new connection is made.
    """
    def __init__(self, endpoint, protocol_class, connection_callback=None):
        self.connection = None

        self._protocol_class = protocol_class
        self._connection_callback = connection_callback

        sf = ReconnectingClientFactory()
        sf.protocol = lambda: self._buildProtocol()
        endpoint.connect(sf)

    def _buildProtocol(self):
        protocol = self._protocol_class()
        protocol.connectionMade = lambda: self._saveConnection(protocol)
        protocol.connectionLost = lambda reason: self._removeConnection(protocol)
        return protocol

    def _saveConnection(self, protocol):
        log.msg("connection-made-to-something")
        self.connection = protocol
        if self._connection_callback is not None:
            self._connection_callback()

    def _removeConnection(self, protocol):
        self.connection = None


@implementer(IResponder)
class RedisResponder(object):
    """The Redis responder."""

    def __init__(self, redis_endpoint, _redis=Redis):
        """
        @param redis_endpoint: A L{IStreamClientEndpoint} pointing at a Redis server.
        """
        self.redis_endpoint = redis_endpoint
        self._response_queue = []
        self.uuid = uuid4().hex
        self._waiting_results = set()  # deferreds that we're still waiting to fire
        self._redis = _redis

    def listen(self, name, handler):
        """
        """
        # TODO:
        # - support multiple calls to listen() with different names/handlers
        #   (and only use one pair of connections for multiple listeners)
        # - error reporting?
        # - kick off the listener XXX
        self.name = name
        self.handler = handler
        self.running = True
        self._responding_connection = _WatchedConnection(
            self.redis_endpoint,
            self._redis,
            connection_callback=self._sendQueuedResponses)
        self._listening_connection = _WatchedConnection(
            self.redis_endpoint,
            self._redis,
            connection_callback=self._listenLoop)

    def _listenLoop(self):
        if self.running:
            if self._listening_connection.connection is None:
                log.msg("request-listener-lost-connection")
                return
            r = self._listening_connection.connection.bpop(["txscale." + self.name], timeout=0)
            r.addCallback(self._messageReceived)

    def _sendQueuedResponses(self):
        # XXX log
        for d, response_list_name, data in self._response_queue:
            d.chainDeferred(self._responding_connection.connection.push(response_list_name, data))

    def _messageReceived(self, bpop_result):
        data = bpop_result[1]
        try:
            request = splitRequest(data)
        except:
            print "ERROR PARSING REQUEST", repr(data)
            raise
        d = self.handler.handle(request.data)  # XXX maybeDeferred?
        d.addCallback(self._sendResponse, request.message_id, request.response_channel)
        self._waiting_results.add(d)
        d.addErrback(log.err) # XXX yessss
        d.addCallback(lambda ignored: self._waiting_results.remove(d))
        self._listenLoop()

    def _sendResponse(self, payload, message_id, response_list_name):
        data = generateResponse(message_id, payload)
        if self._responding_connection.connection is not None:
            self._responding_connection.connection.push(response_list_name, data)
        else:
            return self._queueResponse(response_list_name, data)

    def _queueResponse(self, response_list_name, data):
        d = Deferred()
        self._response_queue.append((d, response_list_name, data))
        return d

    def stop(self):
        self.running = False # XXX Do something with this.
        print "waiting for all results to be sent", self._waiting_results
        d = gatherResults(self._waiting_results)
        d.addErrback(log.err)
        return d


@implementer(IRequester)
class RedisRequester(object):

    def __init__(self, service_name, redis_endpoint, clock, total_timeout=3.0,
                 after_request_timeout=1.0,
                 _redis=Redis):
        """
        @type clock: L{IReactorTime}
        @param clock: Typically, C{twisted.internet.reactor}.
        @param service_name: The name of the service to which we will connect.
        @param total_timeout: The number of seconds to wait after L{request} is invoked to trigger
            a timeout.
        @param after_request_timeout: The number of seconds to wait after the request has actually
            been published to trigger a timeout.
        @param redis_endpoint: An endpoint pointing at a Redis server.
        @type redis_endpoint: L{IStreamClientEndpoint}.
        """
        self._redis = _redis
        self.clock = clock
        self.redis_endpoint = redis_endpoint
        self.service_name = service_name
        self.request_list_name = "txscale.%s" % (service_name,)
        self.response_list_name = "txscale-client.%s.%s" % (service_name, uuid4().hex)
        self._request_queue = []
        self._outstanding_requests = {}  # msg-id -> _ClientRequest
        self.total_timeout = total_timeout
        self.after_request_timeout = after_request_timeout

        self._connecting = False

    def _ensureConnection(self):
        if self._connecting:
            return
        self._connecting = True
        self._request_connection = _WatchedConnection(
            self.redis_endpoint,
            self._redis,
            connection_callback=self._sendQueuedRequests)
        self._response_connection = _WatchedConnection(
            self.redis_endpoint,
            self._redis,
            connection_callback=self._listenLoop)

    def _listenLoop(self):
        if self._response_connection.connection is None:
            log.msg("response-listener-lost-connection")
            return
        r = self._response_connection.connection.bpop(
            [self.response_list_name],
            timeout=0)
        r.addCallback(self._messageReceived)

    def request(self, data):
        """
        Send a request.
        """
        self._ensureConnection()
        message_id, message = generateRequest(self.response_list_name, data)
        request = _ClientRequest(self.clock, self.service_name, message_id, message,
                                 self.total_timeout)
        self._outstanding_requests[message_id] = request
        if self._request_connection.connection is not None:
            self._sendRequest(request)
        else:
            self._queueRequest(request)

        def _cleanUpRequest(result):
            del self._outstanding_requests[message_id]
            return result
        request.result_deferred.addBoth(_cleanUpRequest)
        return request.result_deferred

    def _sendRequest(self, request):
        """
        Publish the message to the connected protocol, and start the C{after_request_timeout}
        ticking.
        """
        self._request_connection.connection.push(
            self.request_list_name, request.message, tail=True)
        request.startTimeOut(self.after_request_timeout)

    def _queueRequest(self, request):
        """Save the request to be sent when we have a connection to the Redis server."""
        self._request_queue.append(request)

    def _sendQueuedRequests(self):
        for request in self._request_queue:
            self._sendRequest(request)

    def _messageReceived(self, bpop_result):
        data = bpop_result[1]
        message_id, message = splitResponse(data)
        if message_id not in self._outstanding_requests:
            log.msg(
                "Got unexpected response to message-id %r. Maybe the request timed out?",
                message_id=message_id)
        else:
            self._outstanding_requests[message_id].succeed(message)

        self._listenLoop()


class _ClientRequest(object):
    def __init__(self, clock, service_name, message_id, message, total_timeout):
        self.clock = clock
        self.message_id = message_id
        self.result_deferred = Deferred()
        self.service_name = None
        self.message = message
        self._after_request_timeout_call = None
        self.total_timeout = total_timeout
        self._total_timeout_call = self.clock.callLater(
            self.total_timeout, self._timedOut, "total_timeout", total_timeout)

    def startTimeOut(self, timeout):
        self._after_request_timeout_call = self.clock.callLater(
            timeout, self._timedOut, "after_request_timeout", timeout)

    def _timedOut(self, timeout_type, timeout_value):
        self.result_deferred.errback(TimeOutError(timeout_type, timeout_value, self))

    def _cancel_timeouts(self):
        for call in (self._total_timeout_call, self._after_request_timeout_call):
            if call is not None and call.active():
                call.cancel()

    def succeed(self, result):
        self._cancel_timeouts()
        self.result_deferred.callback(result)

    def fail(self, failure):
        self._cancel_timeouts()
        self.result_deferred.errback(failure)

    def __repr__(self):
        return "<_ClientRequest message_id=%r service=%s>" % (self.message_id, self.service_name)

    def __str__(self):
        return repr(self)
