from collections import deque, namedtuple

from twisted.internet.defer import Deferred

from .watchedconnection import WatchedConnection


QueueItem = namedtuple("QueueItem", ("method", "args", "kwargs", "deferred"))


class QueuedConnection(object):
    """
    A queue for invoking methods on a protocol instance.

    This class allows invoking methods on a protocol object that can temporarily disappear in the
    case of a reconnection -- any methods that are invoked on the protocol (with L{do}) will be
    queued if the protocol isn't currently connected.

    I generally hate metaprogramming of even the slightest nature, and this is certainly
    metaprogramming, but I think the convenience outweighs the unclarity in the code. There is
    a certain class of Protocol implementations for Twisted that have the general interface of
    providing methods that perform requests and return Deferreds that result in the responses for
    those requests, and this class can work with any of them.
    """

    def __init__(self, endpoint, protocol_class):
        self.watched_connection = WatchedConnection(endpoint, protocol_class, self._flush)
        self._queue = deque()

    def do(self, method, *args, **kwargs):
        """
        Attempt to call the named method with specified arguments on the underlying connection.

        If the underlying connection is not available, queue the method call and return a Deferred
        that will ultimately fire with the result of the underlying method.
        """
        if self.watched_connection.connection is not None:
            return self._do(method, args, kwargs)
        else:
            deferred = Deferred()
            item = QueueItem(method, args, kwargs, deferred)
            self._queue.append(item)
            return deferred

    def _do(self, method, args, kwargs):
        """
        Dispatch the method call to the connected protocol.
        """
        return getattr(self.watched_connection.connection, method)(*args, **kwargs)

    def _flush(self):
        """
        Call all the method invocations that have been queued up.
        """
        while self._queue:
            item = self._queue.popleft()
            result = self._do(item.method, item.args, item.kwargs)
            result.chainDeferred(item.deferred)
