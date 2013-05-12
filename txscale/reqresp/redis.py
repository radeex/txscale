"""
Redis request/response support.

Servers subscribe on channels named txscale-servers.<name>.<id>.  "name" is based on configuration
and "id" is a random string.

txscale-servers.<name> is a SET containing all IDs of servers.

Every service process maintains two connections to the redis server, one for subscribing to
requests and another for publishing responses.  Before a server starts subscribing to requests, it
must add a random string to txscale-servers.<name> set, and then it will subscribe to txscale-
servers.<name>.

Clients subscribe on channels named txscale-clients.<name>.<id>.  When a client sends a request to
a server, it includes its client-specific name and a random message-ID in the request, and the
server will PUBLISH the result on that response-channel with the original message-ID.

Clients also must maintain two connections to the redis server, one for publishing requests and
another for subscribing to responses.
"""

from zope.interface import implementer
from .interfaces import IResponder


@implementer(IResponder)
class RedisServiceEndpoint(object):

    def __init__(self, client_endpoint):
        self.client_endpoint = client_endpoint

    def listen(self, name, handler):
        """
        Allocate a unique ID, subscribe to channel txscale-servers.<name>.<id>, and dispatch
        messages received to the handler.
        """
        self.client_endpoint.connect(...)
        self.handler = handler


