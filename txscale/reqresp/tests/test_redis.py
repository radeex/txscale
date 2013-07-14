from collections import defaultdict, namedtuple

from twisted.trial.unittest import TestCase
from twisted.internet.task import Clock
from twisted.internet.defer import succeed

from ..redis import RedisResponder, RedisRequester, TimeOutError
from ..messages import splitRequest


class FakeRedisModel(object):
    """
    A model that simulates the storage and pubsub functionality of the Redis server.

    Multiple L{FakeRedis} and L{FakeRedisSubscriber} instances can be hooked up to this.
    """
    def __init__(self):
        self.sets = defaultdict(set)
        self.lists = defaultdict(list)


class FakeRedis(object):
    """A fake Redis protocol."""

    def __init__(self, model):
        self.model = model

    def connectionMade(self):
        pass

    def bpop(self, list_names, timeout):
        return succeed(None)

    def push(self, key, value, tail=False):
        assert tail is True
        self.model.lists[key].append(value)
        return succeed(None)

    def sadd(self, set_key, member):
        self.model.sets[set_key].add(member)
        return succeed(None)

    def srem(self, set_key, member):
        self.model.sets[set_key].remove(member)
        return succeed(None)


class FakeEndpoint(object):
    """A fake L{IStreamClientEndpoint} that keeps track of connected factories."""

    def __init__(self):
        self.protos = {}

    def connect(self, factory):
        self.protocol = factory.buildProtocol(None)
        self.protocol.connectionMade()
        return succeed(self.protocol)


class RedisRequesterTests(TestCase):
    """Tests for L{RedisRequester}."""

    def setUp(self):
        self.redis_model = FakeRedisModel()
        self.clock = Clock()
        self.total_timeout = 5.0

    def connect(self):
        endpoint = FakeEndpoint()
        redis_factory = lambda: FakeRedis(self.redis_model)
        requester = RedisRequester(
            "test-service", endpoint, self.clock,
            total_timeout=self.total_timeout,
            _redis=redis_factory)
        return requester

    def _getPublishedRequests(self, list_name="txscale.test-service"):
        return map(splitRequest, self.redis_model.lists[list_name])

    def test_registration(self):
        """
        Clients get registered in the txscale.clients set.
        """
        requester = self.connect()
        self.assertEqual(
            self.redis_model.sets["txscale.clients"],
            set([requester.response_list_name]))

    def test_deregistration(self):
        """
        When a requester is stopped, it is removed from the txscale.clients set.
        """
        requester = self.connect()
        requester.stop()
        self.assertEqual(self.redis_model.sets["txscale.clients"], set())

    def test_request_pushes_request(self):
        """
        Requests are encoded and pushed onto a list representing the server's request queue.
        """
        requester = self.connect()
        requester.request("request-data")
        [request] = self._getPublishedRequests()
        self.assertEqual(request.data, "request-data")
        self.assertEqual(request.response_channel, requester.response_list_name)

    # def test_request_response(self):
    #     """
    #     The requester will fire the returned Deferred with the response sent from the responder.
    #     """
    #     requester = self.connect()
    #     result = requester.request("request-data")

    #     [request] = self._getPublishedRequests()
