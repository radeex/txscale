from collections import defaultdict

from twisted.trial.unittest import TestCase
from twisted.internet.task import Clock
from twisted.internet.defer import succeed

from ..redis import RedisResponder, RedisRequester
from ..messages import splitRequest

# TODO:
# restart redis while running craps and crapc


class FakeRedisModel(object):
    """
    A model that simulates the storage and pubsub functionality of the Redis server.

    Multiple L{FakeRedis} and L{FakeRedisSubscriber} instances can be hooked up to this.
    """
    def __init__(self):
        self.sets = defaultdict(set)
        self.channels = defaultdict(list)

    def publish(self, channel, message):
        self.channels[channel].append(message)


class FakeRedis(object):
    """A fake Redis protocol."""

    name = "command"

    def __init__(self, model):
        self.model = model

    def publish(self, channel, message):
        self.model.publish(channel, message)

    def srem(self, set_name, element):
        self.model.sets[set_name].discard(element)
        return succeed(None)

    def sadd(self, set_name, element):
        self.model.sets[set_name].add(element)
        return succeed(None)

    def smembers(self, set_name):
        return succeed(self.model.sets[set_name])


class FakeRedisSubscriber(object):
    """A fake Redis subscriber protocol."""

    name = "subscription"

    def __init__(self, model):
        self.model = model

    def subscribe(self, *channels):
        pass

    def getResponse(self):
        return succeed(None)


class FakeEndpoint(object):
    """A fake L{IStreamClientEndpoint} that keeps track of connected factories."""

    def __init__(self):
        self.protos = {}

    def connect(self, factory):
        protocol = factory.buildProtocol(None)
        self.protos[protocol.name] = protocol


class RedisRequesterTests(TestCase):
    """Tests for L{RedisRequester}."""

    def setUp(self):
        self.redis_model = FakeRedisModel()
        self.redis_model.sets["txscale.test-service"] = {"txscale.test-service.chan1"}
        self.clock = Clock()
        self.endpoint = FakeEndpoint()
        redis_factory = lambda: FakeRedis(self.redis_model)
        redis_subscriber_factory = lambda: FakeRedisSubscriber(self.redis_model)
        self.requester = RedisRequester(
            "test-service", self.endpoint, self.clock,
            _redis=redis_factory, _redis_subscriber=redis_subscriber_factory)
        self.endpoint.protos["subscription"].connectionMade()
        self.endpoint.protos["command"].connectionMade()

    def test_request(self):
        """
        The C{request} method, when invoked after connections have been established, immediately
        publishes a valid request message to one of the queues in the txscale.<name> set.
        """
        self.requester.request("foo")
        [request] = self.redis_model.channels["txscale.test-service.chan1"]
        message_id, response_channel, data = splitRequest(request)
        self.assertEqual(data, "foo")
