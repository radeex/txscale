"""
Interfaces for the request/response layer.

Say you want to use txScale with JSON-RPC over Redis pubsub. Here's the stack:

JSONRequestHandler implements IRequestHandler.
    This parses and dispatches JSON-RPC requests from and sends responses to an IResponder.
    It dispatches to an implementation-defined handler where your application code lives.
RedisServiceEndpoint implements IResponder.
    This handles requests with raw string payloads coming from Redis pubsub.

JSONRPCClient is just a class with an implementation-defined interface.
    Application code calls this to send JSON-RPC requests to and receives responses from an
    IRequester.
RedisClientEndpoint implements IRequester.
    This sends requests with raw string payloads to Redis pubsub.
"""

from zope.interface import Interface, Attribute

# Big TODO:

# Tubes or producers/consumers should be used to deal with buffering of messages. The interesting
# thing here is that we shouldn't be limited to buffering for a single or already-extant
# connection  -- requests should be able to be buffered even before a connection is made, or when
# a connection has temporarily been lost.

# On top of that, I want to make sure that unchecked sending of requests is eventually shut down
# with a "reasonable" (and configurable) limit. I've had production systems fall over when too many
# messages were buffered, and that sucks.


class IRequestHandler(Interface):
    """
    An object that can handle request messages and provide response messages.

    This interface works with raw strings of bytes, stripped of their routing information.

    Examples of implementations would parse and generate json or protobuf strings to expose a
    higher-level interface.  Alternatively an application developer may use this interface directly
    to implement a communication layer with very little overhead.
    """

    def handle(self, data):
        """
        Handle a request.

        @param data: The request data.
        @type data: C{str}

        @return: The result that should be sent back to the client.  If None, then no response will be sent.  To send an acknowledgement of receipt with no content, return an empty
            string.
        @rtype: C{str} or C{None}
        """


class IResponder(Interface):
    """
    The server side of a request/response transport.

    This receives messages over things like AMQP or Redis pubsub and dispatches to an RPC parser 
    like JSON-RPC.
    """

    def listen(self, name, handler):
        """
        Listen for requests on the named channel and dispatch them to the given handler.

        This method can be called multiple times.

        @type handler: L{IRequestHandler} implementor.
        @return: None
        """

    def stop(self):
        """
        Stop listening for requests.
        """


class IRequester(Interface):
    """
    The client side of a request/response transport.

    Note that this does not provide a "connect"-like method.  See L{request}.

    Examples of implementations would use Redis pubsub or AMQP to receive and transmit messages.
    """

    def request(self, data):
        """
        Send a request.  If necessary, the request will be buffered locally until any underlying
        connections are made and the request can be sent.

        @return: A L{Deferred} that will fire when the response is available.
        """
