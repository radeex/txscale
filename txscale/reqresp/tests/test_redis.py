from collections import defaultdict, namedtuple

from twisted.trial.unittest import TestCase
from twisted.internet.task import Clock
from twisted.internet.defer import succeed

from ..redis import RedisResponder, RedisRequester, NoServiceError, TimeOutError
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

    def __init__(self, auto_connect=True):
        self.protos = {}
        self.auto_connect = auto_connect

    def connect(self, factory):
        protocol = factory.buildProtocol(None)
        self.protos[protocol.name] = protocol
        if self.auto_connect:
            protocol.connectionMade()


class RedisRequesterTests(TestCase):
    """Tests for L{RedisRequester}."""

    def setUp(self):
        self.redis_model = FakeRedisModel()
        self.redis_model.sets["txscale.test-service"] = {"txscale.test-service.chan1"}
        self.clock = Clock()
        self.total_timeout = 5
        self.after_request_timeout = 1

    def connect(self, auto_connect=True):
        redis_client_stuff = namedtuple("RedisClientStuff", ["requester", "endpoint"])
        endpoint = FakeEndpoint(auto_connect=auto_connect)
        redis_factory = lambda: FakeRedis(self.redis_model)
        redis_subscriber_factory = lambda: FakeRedisSubscriber(self.redis_model)
        requester = RedisRequester(
            "test-service", endpoint, self.clock,
            total_timeout=self.total_timeout,
            after_request_timeout=self.after_request_timeout,
            _redis=redis_factory, _redis_subscriber=redis_subscriber_factory)

        return redis_client_stuff(requester=requester, endpoint=endpoint)

    def _respond(self, client_stuff, request, response):
        client_stuff.endpoint.protos["subscription"].messageReceived(
            request.response_channel, request.message_id + response)

    def _getPublishedRequests(self, channel="txscale.test-service.chan1"):
        return map(splitRequest, self.redis_model.channels[channel])

    def test_request(self):
        """
        The C{request} method, when invoked after connections have been established, immediately
        publishes a valid request message to one of the queues in the txscale.<name> set.
        """
        client = self.connect()
        client.requester.request("foo")
        [request] = self.redis_model.channels["txscale.test-service.chan1"]
        message_id, response_channel, data = splitRequest(request)
        self.assertEqual(data, "foo")

    def test_response(self):
        """The C{request} method returns the result as returned by the service."""
        client = self.connect()
        deferred = client.requester.request("foo")
        [request] = self._getPublishedRequests()
        self._respond(client, request, "RESULT")
        self.assertEqual(self.successResultOf(deferred), "RESULT")

    def test_response_matched_to_message_id(self):
        """
        Responses can come in a different order than the requests went out, and they will fire the
        result Deferred of the requests as they come in. They are matched by message-ID.
        """
        client = self.connect()
        deferred1 = client.requester.request("foo")
        deferred2 = client.requester.request("foo")
        [request1, request2] = self._getPublishedRequests()
        self._respond(client, request2, "RESULT2")
        self.assertEqual(self.successResultOf(deferred2), "RESULT2")
        self._respond(client, request1, "RESULT1")
        self.assertEqual(self.successResultOf(deferred1), "RESULT1")

    def test_request_no_server_available(self):
        """
        When no service is listed in the txscale.<name> set, the C{request} method will, by
        default, raise a L{NoServiceError}.
        """
        client = self.connect()
        self.redis_model.sets["txscale.test-service"] = set()
        d = client.requester.request("foo")
        failure = self.failureResultOf(d)
        failure.trap(NoServiceError)
        self.assertEqual(failure.value.service_name, "test-service")

    def test_clients_use_different_response_channels(self):
        """Clients use unique response channels."""
        client1 = self.connect()
        client2 = self.connect()
        client1.requester.request("foo")
        client2.requester.request("bar")
        self.assertNotEqual(client1.requester.client_channel, client2.requester.client_channel)
        [request1, request2] = self._getPublishedRequests()
        self.assertEqual(request1.response_channel, client1.requester.client_channel)
        self.assertEqual(request2.response_channel, client2.requester.client_channel)

    def test_after_request_timeout(self):
        """
        When an amount of time equivalent to the C{after_request_timeout} has passed after the
        request was delivered, a L{TimeOutError} will be raised.
        """
        client = self.connect()
        deferred = client.requester.request("foo")
        self.clock.advance(self.after_request_timeout)
        failure = self.failureResultOf(deferred)
        failure.trap(TimeOutError)
        self.assertEqual(failure.value.timeout_type, "after_request_timeout")
        self.assertEqual(failure.value.timeout_value, self.after_request_timeout)

    def test_after_request_timeout_ignored_response(self):
        """
        After the L{TimeOutError} for the C{after_request_timeout} has been raised, if the request
        gets a real response, it will be ignored.
        """
        client = self.connect()
        deferred = client.requester.request("foo")
        [request] = self._getPublishedRequests()
        self.clock.advance(self.after_request_timeout)
        failure = self.failureResultOf(deferred)
        failure.trap(TimeOutError)
        self._respond(client, request, "result")
        # XXX I don't have a good way to test for log output.
        # I'll figure it out when I have a structured logging system in place.

    def test_after_request_timeout_call_canceled(self):
        """
        The after-request timeout does not cause a L{TimeOutError} if the result has been received.
        """
        client = self.connect()
        deferred = client.requester.request("foo")
        [request] = self._getPublishedRequests()
        self._respond(client, request, "RESULT")
        self.assertEqual(self.successResultOf(deferred), "RESULT")
        # The following line would raise AlreadyCalledError if the timeout were still active.
        self.clock.advance(self.after_request_timeout)

    def test_requests_queued_while_disconnected(self):
        """
        Requests made while the client isn't currently connected to the command protocol will be
        sent when the command protocol's connection is established.
        """
        client = self.connect(auto_connect=False)
        client.requester.request("foo")
        self.assertEqual(len(self._getPublishedRequests()), 0)
        # XXX also wait for the subscription protocol to be established!
        client.endpoint.protos["command"].connectionMade()
        [request] = self._getPublishedRequests()
        self.assertEqual(request.data, "foo")

    def test_total_request_timeout(self):
        """
        A request will time out while queued if the C{total_request_timeout} is passed.
        """
        client = self.connect(auto_connect=False)
        deferred = client.requester.request("foo")
        self.assertEqual(len(self._getPublishedRequests()), 0)
        self.clock.advance(self.total_timeout)
        failure = self.failureResultOf(deferred)
        failure.trap(TimeOutError)

    # - option to turn NoServiceError into a queue?
    # - race conditions with one connection made and the other pending
    # - and swapped
    # - multiple clients don't interfere with each other
    # - received a response with an unexpected message ID
