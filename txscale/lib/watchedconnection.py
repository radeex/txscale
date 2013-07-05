from twisted.internet.protocol import ReconnectingClientFactory
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
