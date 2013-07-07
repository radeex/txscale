"""
Redis request/response support.


TODO:
 - Use RPOPLPUSH to handle crashing endpoints! http://redis.io/commands/rpoplpush
 - Make sure we don't push into "dead" queues! maybe RPOPLPUSH is good enough for that?
 - The server has graceful shutdown, should the client as well?
   (refuse to send more requests but handle the results to the existing ones?)
   Maybe that should just be done at the application level.
 - Cleanup of client response lists on client shutdown.
   - Use RPUSHX instead of RPUSH for responses sent by the server so that we won't try to send
     messages to clients that definitely aren't listening.


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
from twisted.internet.defer import Deferred, gatherResults
from twisted.internet.task import cooperate

from txredis.client import Redis

from .interfaces import IResponder, IRequester
from .messages import splitRequest, splitResponse, generateRequest, generateResponse
from ..lib.queuedconnection import QueuedConnection
from ..lib.watchedconnection import WatchedConnection


class TimeOutError(Exception):
    """Timed out."""

    def __init__(self, timeout_type, timeout_value, request):
        self.request = request
        self.timeout_type = timeout_type
        self.timeout_value = timeout_value
        super(TimeOutError, self).__init__(
            "Request reached %s of %s: %r" % (timeout_type, timeout_value, request))


class RedisListener(object):
    """
    blpops items off a list and dispatches to a handler, allowing concurrency up to a configured
    maximum.
    """
    def __init__(self, endpoint, protocol_class, list_name, handler, concurrency):
        self.list_name = list_name
        self.handler = handler
        self.concurrency = concurrency
        self.connection = WatchedConnection(
            endpoint, protocol_class, connection_callback=self._listen)

    def stop(self):
        """
        Stop listening and disconnect.
        """
        self.connection.stop()

    def _listen(self):
        """
        Concurrently run L{_listenLoop} a number of times as specified in C{self.concurrency}.
        """
        for i in range(self.concurrency):
            x = self._listenLoop()
            cooperate(x)

    def _listenLoop(self):
        """
        Pull messages off the queue and dispatch them to a handler.
        """
        # This is a *little* bit weird, in some sense. we run bpop here, concurrently with bpops
        # running in other calls to _listenLoop, even though the bpops aren't really concurrent
        # on the redis connection (one bpop call blocks before another one can be sent).
        # It doesn't really matter, though. Whoever bpops first will get the first message, and if
        # there are no messages yet, they'll all effectively block.
        while self.connection.connection is not None:
            result = self.connection.connection.bpop([self.list_name], timeout=0)
            result.addCallback(self.handler)
            result.addErrback(log.err, "redis-listener-error")
            yield result
        log.msg("redis-listener-lost-connection")


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
        # - only use one pair of connections for multiple listeners
        #   actually I don't think that's possible with bpop. Only one bpop per connection.
        self._responding_connection = QueuedConnection(
            self.redis_endpoint,
            self._redis)
        request_callback = lambda bpop_result: self._messageReceived(bpop_result, handler)
        self._listener = RedisListener(
            self.redis_endpoint, self._redis, "txscale." + name, request_callback, 10)

    def _messageReceived(self, bpop_result, handler):
        data = bpop_result[1]
        try:
            request = splitRequest(data)
        except:
            print "ERROR PARSING REQUEST", repr(data)
            raise
        d = handler.handle(request.data)  # XXX maybeDeferred?
        d.addCallback(self._sendResponse, request.message_id, request.response_channel)
        self._waiting_results.add(d)
        d.addErrback(log.err) # XXX yessss
        d.addCallback(lambda ignored: self._waiting_results.remove(d))
        return d

    def _sendResponse(self, payload, message_id, response_list_name):
        data = generateResponse(message_id, payload)
        self._responding_connection.do("push", response_list_name, data)

    def stop(self):
        """
        Immediately stop listening for new connections, and return a Deferred that will fire when
        all outstanding requests have had their responses sent.
        """
        self._listener.stop()
        print "waiting for all results to be sent", self._waiting_results
        d = gatherResults(self._waiting_results)
        d.addErrback(log.err)
        return d


@implementer(IRequester)
class RedisRequester(object):

    def __init__(self, service_name, redis_endpoint, clock, total_timeout=3.0,
                 _redis=Redis):
        """
        @type clock: L{IReactorTime}
        @param clock: Typically, C{twisted.internet.reactor}.
        @param service_name: The name of the service to which we will connect.
        @param total_timeout: The number of seconds to wait after L{request} is invoked to trigger
            a timeout.
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

        # Don't forget, it's okay if the request connection gets established (and we even send
        # requests) before the response listener is hooked up, because the response listener will
        # just pull messages off the list whenever it's ready.
        self._request_connection = QueuedConnection(
            self.redis_endpoint,
            self._redis)
        self._response_listener = RedisListener(
            self.redis_endpoint, self._redis, self.response_list_name, self._messageReceived, 1)

    def request(self, data):
        """
        Send a request.
        """
        message_id, message = generateRequest(self.response_list_name, data)
        request = _ClientRequest(self.clock, self.service_name, message_id, message,
                                 self.total_timeout)
        self._outstanding_requests[message_id] = request
        self._sendRequest(request)

        def _cleanUpRequest(result):
            del self._outstanding_requests[message_id]
            return result
        request.result_deferred.addBoth(_cleanUpRequest)
        return request.result_deferred

    def _sendRequest(self, request):
        """
        Publish the message to the connected protocol.
        """
        self._request_connection.do("push",  self.request_list_name, request.message, tail=True)

    def _messageReceived(self, bpop_result):
        data = bpop_result[1]
        message_id, message = splitResponse(data)
        if message_id not in self._outstanding_requests:
            log.msg("Got unexpected response. Maybe the request timed out?",
                    message_id=message_id)
        else:
            self._outstanding_requests[message_id].succeed(message)


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
