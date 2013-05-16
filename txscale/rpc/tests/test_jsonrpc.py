from json import loads, dumps

from twisted.trial.unittest import TestCase
from twisted.internet.defer import Deferred
from twisted.python.failure import Failure

from ...reqresp.testing import RequestClient
from ..jsonrpc import (JSONRPCClient, ServiceAPIError, JSONRequestHandler, MethodHandler,
                       HandlerNotFound)


class JSONRPCClientTests(TestCase):

    def setUp(self):
        self.reqclient = RequestClient()
        self.jsonclient = JSONRPCClient(self.reqclient)

    def test_request_generation(self):
        """
        Requests meet the JSON-RPC 2.0 specification requirements.
        """
        self.jsonclient.request("foo")
        expected = {
            "jsonrpc": "2.0",
            "method": "foo",
            "params": {},
            "id": 1
        }

        [request] = self.reqclient.requests
        self.assertEqual(loads(request.data), expected)

    def test_request_with_paramethers(self):
        """
        Parameters are encoded as attributes of a JSON object (in the parlance of the spec,
        "by-name").
        """
        self.jsonclient.request("foo", bar=1, baz="quux", obj={"hey": "there"}, arr=[1,2])
        expected = {
            "jsonrpc": "2.0",
            "method": "foo",
            "params": {"bar": 1, "baz": "quux", "obj": {"hey": "there"}, "arr": [1, 2]},
            "id": 1
        }

        [request] = self.reqclient.requests
        self.assertEqual(loads(request.data), expected)

    def test_success_response_handling(self):
        """
        The L{JSONRPCClient.request} method returns a Deferred which will fire with the "result"
        member of the JSON in the response.
        """
        request_result = self.jsonclient.request("foo")
        [request] = self.reqclient.requests
        request.response_deferred.callback(dumps({"result": "hey"}))
        self.assertEqual(self.successResultOf(request_result), "hey")

    def test_error_response_handling(self):
        """
        The L{JSONRPCClient.request} method returns a Deferred which will error out with a Failure
        derived from the "error" member of the JSON in the response.
        """
        request_result = self.jsonclient.request("foo")
        [request] = self.reqclient.requests
        request.response_deferred.callback(
            dumps({"error": {"code": 50, "message": "got an error!", "data": [1, 2, 3]}}))

        failure = self.failureResultOf(request_result)
        failure.trap(ServiceAPIError)
        self.assertEqual(failure.value.method, "foo")
        self.assertEqual(failure.value.code, 50)
        self.assertEqual(failure.value.message, "got an error!")
        self.assertEqual(failure.value.data, [1, 2, 3])


class TestMethodHandler(object):
    def __init__(self):
        self.requests = []

    def gotRequest(self, method, **params):
        self.requests.append((method, params))
        return Deferred()


class JSONRequestHandlerTests(TestCase):

    def setUp(self):
        self.method_handler = TestMethodHandler()
        self.request_handler = JSONRequestHandler(self.method_handler)

    def test_request_dispatching(self):
        """
        Requests will invoke the C{gotRequest} method of the method handler with the method name
        and parameters.
        """
        request = {
            "method": "foo",
            "params": {"a": 1}
        }
        self.request_handler.handle(dumps(request))
        self.assertEqual(self.method_handler.requests, [("foo", {"a": 1})])

    def test_success_result_sending(self):
        """
        Results returned from the gotRequest method are serialized and put into a JSON-RPC
        response object.
        """
        request = {"method": "foo", "params": {}}
        handler_result = self.request_handler.handle(dumps(request))
        handler_result.callback({"structured": "result!"})
        ultimate = self.successResultOf(handler_result)
        expected = {
            "jsonrpc": "2.0",
            "result": {"structured": "result!"},
            "id": 1
        }
        self.assertEqual(loads(ultimate), expected)

    def test_error_result_sending(self):
        """
        Failing Deferreds returned from the gotRequest method are serialized as JSON-RPC response
        objects with an "error" key.
        """
        request = {"method": "foo", "params": {}}
        handler_result = self.request_handler.handle(dumps(request))
        handler_result.errback(Failure(ZeroDivisionError("heyo")))
        ultimate = self.successResultOf(handler_result)
        expected = {
            "jsonrpc": "2.0",
            "error": {
                "code": -32000,
                "message": "heyo",
                "data": ("Traceback (most recent call last):\n"
                         "Failure: exceptions.ZeroDivisionError: heyo\n")
            },
            "id": 1
        }
        self.assertEqual(loads(ultimate), expected)
        self.flushLoggedErrors(ZeroDivisionError)

    def test_synchronous_error_result_sending(self):
        """
        Synchronous exceptions from the method handler's gotRequest method are handled the same way
        as failures.
        """
        request = {"method": "foo", "params": {}}
        self.method_handler.gotRequest = lambda method, **params: 1 / 0

        handler_result = self.request_handler.handle(dumps(request))
        ultimate = self.successResultOf(handler_result)
        self.assertEqual(loads(ultimate)["error"]["message"], "integer division or modulo by zero")
        self.flushLoggedErrors(ZeroDivisionError)

    def test_error_result_sending_with_custom_json_error_code(self):
        """
        If an exception has a C{json_rpc_error_code} attribute, it will be used as the JSON-RPC
        error code.
        """
        exception = ZeroDivisionError("heyo")
        exception.json_rpc_error_code = 55
        request = {"method": "foo", "params": {}}
        handler_result = self.request_handler.handle(dumps(request))
        handler_result.errback(exception)
        ultimate = self.successResultOf(handler_result)
        self.assertEqual(loads(ultimate)["error"]["code"], 55)
        self.flushLoggedErrors(ZeroDivisionError)

    def test_error_logging(self):
        """Exceptions raised from a handler are logged."""
        request = {"method": "foo", "params": {}}
        self.method_handler.gotRequest = lambda method, **params: 1 / 0
        self.request_handler.handle(dumps(request))
        [failure] = self.flushLoggedErrors(ZeroDivisionError)
        failure.trap(ZeroDivisionError)

    def test_asynchronous_error_logging(self):
        """Failing Deferreds returned from a handler are logged."""
        request = {"method": "foo", "params": {}}
        handler_result = self.request_handler.handle(dumps(request))
        handler_result.errback(Failure(ZeroDivisionError("heyo")))
        [failure] = self.flushLoggedErrors(ZeroDivisionError)
        failure.trap(ZeroDivisionError)


class MethodHandlerTests(TestCase):
    def test_handler_not_found(self):
        """
        If a handler for a method can't be found, a L{HandlerNotFound} exception is raised.
        """
        mh = MethodHandler()
        exception = self.assertRaises(HandlerNotFound, mh.gotRequest, "foo")
        self.assertEqual(exception.method, "foo")

    def test_invoke_handler(self):
        """
        L{MethodHandler.gotResult} invokes methods named remote_* to handle the requests.
        """
        mh = MethodHandler()
        calls = []
        mh.remote_foo = lambda **kw: calls.append(kw)
        mh.gotRequest("foo", a=1, b=[1, 2])
        self.assertEqual(calls, [{"a": 1, "b": [1, 2]}])

    def test_handler_response(self):
        """
        The result of the remote_* method is returned directly from L{MethodHandler.gotResult}.
        """
        mh = MethodHandler()
        calls = []
        mh.remote_foo = lambda **kw: "HOOPLA"
        result = mh.gotRequest("foo")
        self.assertEqual(result, "HOOPLA")

    def test_error_response(self):
        """
        Errors from the remote_* method are propagated out directly.
        """
        mh = MethodHandler()
        calls = []
        def remote_foo():
            1 / 0
        mh.remote_foo = remote_foo
        self.assertRaises(ZeroDivisionError, mh.gotRequest, "foo")
