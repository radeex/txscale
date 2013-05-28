"""
Redis request/response support.

Pros:
 - you can deploy it on Redis

Cons:
 - Unreliable:
   - If a client gets temporarily disconnected after it sends a request, it might miss the
     response.
   - Servers might pull a message off the stack and then not respond to it, even in graceful
     shutdown. But at least the client will know that it wasn't processed: see
     L{ServiceGracefullyDisappearedError}.

Redis pubsub really really simple, and good at what it does, but message routing needs more than
pubsub.  This means we have to add a significant amount of bookkeeping on top of the basic pubsub
functionality in order to get a reliable, scalable, dynamic message routing system.

First, Redis can't do the routing for us.  Pubsub is purely fan-out, of course, but we want only
one service instance to get any given message.  So we have a system of multiple channels, one for
each service instance, and the client must choose a random one for each request so as to distribute
load evenly.

There's not even a way, in Redis, to list channels currently subscribed, so we have to create
another mechanism to allow clients to discover which channels for a given service instance are
available.  So we make the servers maintain a SET in Redis, name "txscale.<name>", containing the
names of each channel a service instance is listening on.

To make the client's job easier, we also have the services publish a message to a special control
channel named "txscale.<name>.service-management" every time they are started or stopped, so that
the client needn't continually poll the "txscale.<name>" set to discover when services instances
are started or stopped.

Every service process maintains two connections to the redis server, one for subscribing to
requests and another for publishing responses.

Clients also must maintain two connections to the redis server, one for publishing requests and
another for subscribing to responses.

Clients subscribe on channels named txscale-client.<name>.<id>.  When a client sends a request to a
server, it includes its client-specific name and a random message-ID in the request, and the server
will publish the result on that response-channel with the original message-ID.

The service instances implement a graceful shutdown by waiting for all outstanding requests to be
completed and then shutting down.

Extra consideration must be given in the management of the txscale.<name> set of request channels,
specifically for handling the case of a service instance dying without graceful shutdown.  We
mustn't allow the clients to continue attempting to send messages to the unresponsive service.  So
in the event that a client simply receives no response from the server in the alloted time, it
will...  XXX send a "ping" message with a 10-second timeout?  and if that times out, remove the
service's channel from the set and broadcast a removal message?
"""

import random

from uuid import uuid4

from zope.interface import implementer

from twisted.python import log
from twisted.python.failure import Failure
from twisted.internet.protocol import ReconnectingClientFactory
from twisted.internet.defer import Deferred, gatherResults

from txredis.protocol import Redis, RedisSubscriber

from .interfaces import IResponder, IRequester
from .messages import splitRequest, splitResponse, generateRequest, generateResponse


class NoServiceError(Exception):
    """No service instances are listening."""
    def __init__(self, service_name):
        self.service_name = service_name
        super(NoServiceError, self).__init__(
            "No services are listening on txscale.%s." % (service_name,))


class TimeOutError(Exception):
    """Timed out."""

    def __init__(self, timeout_type, timeout_value, request):
        self.request = request
        self.timeout_type = timeout_type
        self.timeout_value = timeout_value
        super(TimeOutError, self).__init__(
            "Request reached %s of %s: %r" % (timeout_type, timeout_value, request))


class ServiceGracefullyDisappearedError(Exception):
    """
    A service (instance) disappeared while waiting for a response.

    This is raised when a service has gracefully shut down and sent the client a message indicating
    that it has done so.  Given that services must complete processing of all received messages
    before broadcasting the message indicating it has shut down, that means the service must not
    have received the request in the first place.  Thus, retrying a request in the case of this
    exception is reasonable.
    """
    def __init__(self, request):
        self.request = request
        super(ServiceGracefullyDisappearedError, self).__init__(
            "Service disappeared while waiting for response to request %r" % (self.request))


def watchProtocol(protocol, connectionMade, connectionLost):
    """
    Hook into a Protocol's connectionLost and connectionMade to invoke callbacks.
    """
    protocol.connectionMade = lambda: connectionMade(protocol)
    protocol.connectionLost = lambda reason: connectionLost(protocol)
    return protocol


def unregisterServiceInstance(publishing_protocol, service_name, channel):
    print "unregistering service channel %s" % (channel,)
    management_channel = "txscale.%s.service-management" % (service_name,)
    d1 = publishing_protocol.publish(management_channel, "GONE " + channel)
    d2 = publishing_protocol.srem("txscale." + service_name, channel)
    return gatherResults([d1, d2])


@implementer(IResponder)
class RedisResponder(object):
    """The Redis responder."""

    def __init__(self, redis_endpoint,
                 _redis_subscriber=RedisSubscriber,
                 _redis=Redis):
        """
        @param redis_endpoint: A L{IStreamClientEndpoint} pointing at a Redis server.
        """
        self.redis_endpoint = redis_endpoint
        self._response_queue = []
        self._subscription_protocol = None
        self._response_protocol = None
        self.uuid = uuid4().hex
        self._waiting_results = set()  # deferreds that we're still waiting to fire
        self._redis_subscriber = _redis_subscriber
        self._redis = _redis

    def listen(self, name, handler):
        """
        Allocate a unique ID, subscribe to channel txscale.<name>.<id>, and dispatch
        messages received to the handler.
        """
        # XXX must support multiple calls to listen() with different names
        self.name = name
        self.channel = "txscale." + self.name + "." + self.uuid
        self.service_management_channel = "txscale." + name + ".service-management"
        # XXX error reporting
        sf = ReconnectingClientFactory()
        sf.protocol = lambda: watchProtocol(self._redis_subscriber(),
                                            self._saveSubscriptionConnection,
                                            self._removeSubscriptionConnection)
        self.redis_endpoint.connect(sf)
        pf = ReconnectingClientFactory()
        pf.protocol = lambda: watchProtocol(self._redis(),
                                            self._savePublishingConnection,
                                            self._removePublishingConnection)
        self.redis_endpoint.connect(pf)
        self.handler = handler

    def _saveSubscriptionConnection(self, protocol):
        # XXX log
        print "subscribing to", self.channel
        protocol.messageReceived = self._messageReceived
        protocol.subscribe(self.channel)
        self._subscription_protocol = protocol

    def _savePublishingConnection(self, protocol):
        # XXX log
        self._response_protocol = protocol
        self._registerServer()
        protocol.publish(self.service_management_channel, "NEW " + self.channel)

        for d, response_channel, data in self._response_queue:
            d.chainDeferred(self._response_protocol.publish(response_channel, data))

    def _removeSubscriptionConnection(self, protocol):
        # XXX log
        self._subscription_protocol = None

    def _removePublishingConnection(self, protocol):
        # XXX log
        self._response_protocol = None

    def _registerServer(self):
        """
        Save this server's UUID in the txscale.<name> set, so clients know what listeners they can
        talk to.
        """
        self._response_protocol.sadd("txscale." + self.name, self.channel)

    def _messageReceived(self, channel, data):
        if data.startswith("ping "):
            response_channel = data.split(" ", 1)[1]
            self._response_protocol.publish(response_channel, "pong %s" % (self.channel,))
            return
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

    def _sendResponse(self, payload, message_id, response_channel):
        data = generateResponse(message_id, payload)
        if self._response_protocol is not None:
            return self._response_protocol.publish(response_channel, data)
        else:
            return self._queueResponse(response_channel, data)

    def _queueResponse(self, response_channel, data):
        d = Deferred()
        self._response_queue.append((d, response_channel, data))
        return d

    def stop(self):
        print "waiting for all results to be sent", self._waiting_results
        d = gatherResults(self._waiting_results)
        d.addErrback(log.err)
        d.addCallback(lambda ignored: unregisterServiceInstance(self._response_protocol,
                                                                self.name, self.channel))
        return d


"""
WTF, how come this happens sometimes?

2013-05-15 07:01:26+0000 [_Publisher,client] Stopping factory <twisted.internet.protocol.ReconnectingClientFactory instance at 0x300ea70>
2013-05-15 07:01:26+0000 [_Publisher,client] Unhandled Error
    Traceback (most recent call last):
      File "/home/radix/Projects/Twisted/trunk/twisted/python/log.py", line 88, in callWithLogger
        return callWithContext({"system": lp}, func, *args, **kw)
      File "/home/radix/Projects/Twisted/trunk/twisted/python/log.py", line 73, in callWithContext
        return context.call({ILogContext: newCtx}, func, *args, **kw)
      File "/home/radix/Projects/Twisted/trunk/twisted/python/context.py", line 118, in callWithContext
        return self.currentContext().callWithContext(ctx, func, *args, **kw)
      File "/home/radix/Projects/Twisted/trunk/twisted/python/context.py", line 81, in callWithContext
        return func(*args,**kw)
    --- <exception caught here> ---
      File "/home/radix/Projects/Twisted/trunk/twisted/internet/posixbase.py", line 619, in _doReadOrWrite
        why = selectable.doWrite()
      File "/home/radix/Projects/Twisted/trunk/twisted/internet/abstract.py", line 251, in doWrite
        l = self.writeSomeData(self.dataBuffer)
      File "/home/radix/Projects/Twisted/trunk/twisted/internet/tcp.py", line 247, in writeSomeData
        return untilConcludes(self.socket.send, limitedData)
    exceptions.AttributeError: 'Client' object has no attribute 'socket'
"""


@implementer(IRequester)
class RedisRequester(object):

    def __init__(self, service_name, redis_endpoint, clock,  total_timeout=3.0,
                 after_request_timeout=1.0, service_disappeared_timeout=10,
                 _redis_subscriber=RedisSubscriber,
                 _redis=Redis):
        """
        @type clock: L{IReactorTime}
        @param clock: Typically, C{twisted.internet.reactor}.
        @param service_name: The name of the service to which we will connect.
        # @param total_timeout: The number of seconds to wait after L{request} is invoked to trigger
        #     a timeout.
        @param after_request_timeout: The number of seconds to wait after the request has actually
            been published to trigger a timeout.
        @param redis_endpoint: An endpoint pointing at a Redis server.
        @type redis_endpoint: L{IStreamClientEndpoint}.
        """
        self._redis_subscriber = _redis_subscriber
        self._redis = _redis
        self.clock = clock
        self.service_management_channel = "txscale." + service_name + ".service-management"
        self.service_channels = set()
        self.redis_endpoint = redis_endpoint
        self.service_name = service_name
        self.client_channel = "txscale-client.%s.%s" % (service_name, uuid4().hex)
        self._request_queue = []
        self._request_protocol = None
        self._response_protocol = None
        self._outstanding_requests = {}  # msg-id -> _ClientRequest
        self.total_timeout = total_timeout
        self.after_request_timeout = after_request_timeout
        self.service_disappeared_timeout = service_disappeared_timeout
        self.timeout_blacklist = set()
        self._service_timeout_delayed_calls = {}

        self._connecting = False

    def _ensureConnection(self):
        if self._connecting:
            return
        self._connecting = True
        # XXX error reporting
        sf = ReconnectingClientFactory()
        sf.protocol = lambda: watchProtocol(self._redis(),
                                            self._savePublishingConnection,
                                            self._removePublishingConnection)
        self.redis_endpoint.connect(sf)
        pf = ReconnectingClientFactory()
        pf.protocol = lambda: watchProtocol(self._redis_subscriber(),
                                            self._saveSubscriptionConnection,
                                            self._removeSubscriptionConnection)
        self.redis_endpoint.connect(pf)

    def _getServiceChannel(self):
        # XXX handle the case for no channels
        # XXX blacklist shouldn't be a strict blacklist if there's nothing else to choose
        # XXX queue messages if there's nothing available
        channels = self.service_channels - self.timeout_blacklist
        if not channels:
            raise NoServiceError(self.service_name)
        return random.sample(channels, 1)[0]

    def request(self, data):
        """
        Send a request.

        There's a big problem, though.  Sometimes a server will disappear without responding.
        Ideally, we'll get a control message on the service_management_channel that tells us the
        server was gone, so if we get one of those about for any channels that we know we're
        waiting for a response on, we should just call that one a loss and errback that Deferred.

        Sometimes, though, we won't even get one of those (the server's cable was unplugged).
        """
        self._ensureConnection()
        message_id, message = generateRequest(self.client_channel, data)
        request = _ClientRequest(self.clock, message_id, message, self.total_timeout)
        self._outstanding_requests[message_id] = request
        if self._request_protocol is not None:
            self._publishConnected(request)
        else:
            self._queueRequest(request)

        def _cleanUpRequest(result):
            del self._outstanding_requests[message_id]
            return result
        request.result_deferred.addBoth(_cleanUpRequest)

        def _checkTimeOut(failure):
            failure.trap(TimeOutError)
            self._checkService(request.channel)
            return failure
        request.result_deferred.addErrback(_checkTimeOut)
        return request.result_deferred

    def _checkService(self, channel):
        """Make sure the service instance at the given channel is still there."""
        # hmm, you know it'd be cool if we also sent a "lagging" command over the service-
        # management channel to allow other clients and servers know that a service instance is
        # timing out on the application level, allowing them to weight it lower to give it a chance
        # to recover.
        if channel in self._service_timeout_delayed_calls:
            print "already sent ping"
            return
        if self._request_protocol is not None:
            print "pinging service at channel %s to determine if it's still there" % (channel,)
            self._request_protocol.publish(channel, "ping %s" % (self.client_channel,))
            delayed_call = self.clock.callLater(10, self._unregisterServiceInstance, channel)
            self._service_timeout_delayed_calls[channel] = delayed_call
            self.timeout_blacklist.add(channel)

    def _unregisterServiceInstance(self, channel):
        unregisterServiceInstance(self._request_protocol, self.service_name, channel)

    def _publishConnected(self, request):
        """
        Publish the message to the connected protocol, and start the C{after_request_timeout}
        ticking.
        """
        try:
            channel = self._getServiceChannel()
        except:
            request.fail(Failure())
            return
        request.setChannel(channel)
        # XXX this should handle the case of _request_protocol being None
        self._request_protocol.publish(channel, request.message)
        request.startTimeOut(self.after_request_timeout)

    def _queueRequest(self, request):
        """Save the request to be sent when we have a connection to the Redis server."""
        self._request_queue.append(request)

    def _savePublishingConnection(self, protocol):
        # XXX We shouldn't consider this really ready for sending messages until the subscription
        # connection is ALSO made (and subscribed). We may end up making a request before we can
        # process its response and miss it.
        self._request_protocol = protocol
        d = self._request_protocol.smembers("txscale." + self.service_name)
        d.addCallback(self._gotAllServiceChannels)

    def _gotAllServiceChannels(self, channels):
        self.service_channels = channels
        for request in self._request_queue:
            self._publishConnected(request)

    def _saveSubscriptionConnection(self, protocol):
        def subscribed(r):
            self._response_protocol = protocol
        protocol.messageReceived = self._messageReceived
        protocol.subscribe(self.client_channel, self.service_management_channel)
        protocol.getResponse().addCallback(subscribed)  # XXX oh god testing

    def _removePublishingConnection(self, protocol):
        self._request_protocol = None

    def _removeSubscriptionConnection(self, protocol):
        # XXX log
        self._response_protocol = None

    def _messageReceived(self, channel, data):
        if channel == self.service_management_channel:
            print "control message!", data
            command, channel = data.split()

            if command == "NEW":
                self.service_channels.add(channel)
            elif command == "GONE":
                self.service_channels.discard(channel)  # XXX test case when we don't even have that channel
                print "checking for requests to discard in", self._outstanding_requests
                for request in self._outstanding_requests.values():
                    if request.channel == channel:
                        print "found a request on that channel", channel
                        request.fail(
                            ServiceGracefullyDisappearedError(request))
        elif channel == self.client_channel:
            if data.startswith("pong "):
                channel = data.split(" ", 1)[1]
                self.timeout_blacklist.discard(channel)
                if channel in self._service_timeout_delayed_calls:
                    # XXX WE NEED TO REMOVE THIS CALL FROM THE DICT
                    call = self._service_timeout_delayed_calls[channel]
                    if call.active():
                        call.cancel()
                return
            message_id, message = splitResponse(data)
            if message_id not in self._outstanding_requests:
                log.msg(
                    "Got unexpected response to message-id %r. Maybe the request timed out?",
                    message_id=message_id)
            else:
                self._outstanding_requests[message_id].succeed(message)


class _ClientRequest(object):
    def __init__(self, clock, message_id, message, total_timeout):
        self.clock = clock
        self.message_id = message_id
        self.result_deferred = Deferred()
        self.channel = None
        self.message = message
        self._after_request_timeout_call = None
        self.total_timeout = total_timeout
        self._total_timeout_call = self.clock.callLater(
            self.total_timeout, self._timedOut, "total_timeout", total_timeout)

    def setChannel(self, channel):
        """Record the channel on which this request is being sent."""
        self.channel = channel

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
        return "<_ClientRequest message_id=%r channel=%s>" % (self.message_id, self.channel)

    def __str__(self):
        return repr(self)
