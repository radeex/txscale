"""
Testing utilities for the txScale request/response layer.
"""

from zope.interface import implementer

from twisted.internet.defer import Deferred

from .interfaces import IRequester


class Request(object):
    def __init__(self, data, response_deferred):
        self.data = data
        self.response_deferred = response_deferred


@implementer(IRequester)
class RequestClient(object):

    def __init__(self):
        self.requests = []

    def request(self, data):
        """
        Stash away the data to be inspected by the unit test, and allow response with
        L{simulate_response}.
        """
        d = Deferred()
        self.requests.append(Request(data, d))
        return d
