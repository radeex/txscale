from twisted.internet import reactor
from twisted.internet.task import deferLater
from twisted.internet.protocol import ClientFactory
from twisted.python import log


class WatchedConnection(object):
    """
    Ensures that a protocol remains connected to an endpoint.

    This is kind of a dumb object, but it makes interacting with a reconnecting protocol
    significantly more convenient. The basic pattern is this:

    - create a WatchedConnection
    - every time you want to interact with the protocol, ensure that
      watchedConnection.connection is not None (and if it is None, optionally "queue up" requests
      to make with it)
    - specify a C{connection_callback} that sends the "queued up" requests.

    Note that if a protocol implements connectionLost, the original implementation won't be called
    if this class is used.
    """
    def __init__(self, endpoint, protocol_class, connection_callback=None):
        self.connection = None
        self.stopped = False

        self.endpoint = endpoint
        self._protocol_class = protocol_class
        self._connection_callback = connection_callback

        self.factory = ClientFactory()
        self.factory.protocol = self._buildProtocol

    def _connect(self):
        """
        Keep trying to connect until a connection is made.
        """
        result = self.endpoint.connect(self.factory)
        result.addErrback(lambda failure: deferLater(reactor, 0.5, self._connect))
        return result

    def ensureWatchedConnection(self):
        """
        Connect and save the resulting protocol instance as C{self.connection}.
        """
        result = self._connect()
        result.addCallback(self._saveConnection)
        return result

    def _buildProtocol(self):
        protocol = self._protocol_class()
        protocol.connectionLost = lambda reason: self._removeConnection(protocol)
        return protocol

    def _saveConnection(self, protocol):
        log.msg("connection-made-to-something")
        self.connection = protocol
        if self._connection_callback is not None:
            self._connection_callback()

    def _removeConnection(self, protocol):
        self.connection = None
        self.ensureWatchedConnection()

    def stop(self):
        """
        Disconnect and don't try to reconnect.
        """
        self.stopped = True
        if self.connection is not None:
            self.connection.transport.loseConnection()
