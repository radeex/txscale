"""
Interfaces for the request/response layer.
"""


from zope.interface import Interface, Attribute


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

        @return: The result that should be sent back to the client.  If None, then no response will
            be sent.  To send an acknowledgement of receipt with no content, return an empty
            string.
        @rtype: C{str} or C{None}
        """


class IServiceEndpoint(Interface):
    """
    A representation of a potentially listening service that knows how to dispatch requests to a
    handler.

    Examples of implementations would use Redis pubsub or AMQP to receive and transmit messages.
    """

    def listen(self, name, handler):
        """
        Listen for requests on the named channel and dispatch them to the given handler.

        This method can be called multiple times.

        @type handler: L{IHandler} implementor.
        @return: None
        """

    def stop(self):
        """
        Stop listening for requests.
        """


class IClientEndpoint(Interface):
    """
    A client that can send requests to a service.

    Note that this does not provide a "connect"-like method.  See L{request}.

    Examples of implementations would use Redis pubsub or AMQP to receive and transmit messages.
    """

    max_outstanding_requests = Attribute("The maximum number of outstanding requests allowed.")

    def request(self, data):
        """
        Send a request.  If necessary, the request will be buffered locally until any underlying
        connections are made and the request can be sent.

        If the number of outstanding requests exceeds the value of L{max_outstanding_requests} then
        L{RequestBufferExceeded} will be raised.

        @return: A L{Deferred} that will fire when the response is available.
        """


class RequestBufferExceeded(Exception):
    """
    The request buffer is full.
    """
