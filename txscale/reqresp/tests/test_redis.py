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
        assert tail
        self.model.lists[key].append(value)
        return succeed(None)

    def sadd(self, set_key, member):
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

    def test_request_pushes_request(self):
        requester = self.connect()
        requester.request("request-data")
        [request_data] = self.redis_model.lists["txscale.test-service"]
        request = splitRequest(request_data)
        self.assertEqual(request.data, "request-data")
        self.assertEqual(request.response_channel, requester.response_list_name)
