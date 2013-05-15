"""
An implementation of RPC on top of txscale.reqresp using JSON-RPC, as defined by
http://www.jsonrpc.org/specification.

Restrictions added to the specification:

- params are always by-name, not by-position (i.e., use objects instead of arrays for params)
- request IDs are set to a static value and otherwise ignored, because request/response routing
  is already handled by txscale.reqresp.
"""

import json

from zope.interface import implementer

from twisted.internet.defer import maybeDeferred
from twisted.python import log

from txscale.reqresp.interfaces import IRequestHandler


class ServiceAPIError(Exception):
    """
    Raised to client callers when the server gave sent an error response.
    """
    def __init__(self, method, code, message, data):
        self.method = method
        self.code = code
        self.message = message
        self.data = data
        return super(ServiceAPIError, self).__init__(
            "Error %s from method %s: %s\n%s"% (code, method, message, data))


class HandlerNotFound(Exception):
    """
    Raised when a handler for a request can't be found.
    """
    def __init__(self, method):
        self.method = method
        super(HandlerNotFound, self).__init__("Couldn't find a handler for method %s" % (method,))


class JSONRPCClient(object):
    """
    A client for sending requests over a request/response transport using JSON.
    """

    def __init__(self, request_response_client):
        """
        @param request_response_client: The request/response client to use as a transport.
        @type request_response_client: L{txscale.reqresp.interfaces.IRequester}
        """
        self.rr_client = request_response_client

    def request(self, method, **params):
        """
        Send a JSON-RPC request over the request/response client.
        """
        payload = {
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
            "id": 1
        }
        data = json.dumps(payload)
        result = self.rr_client.request(data)
        return result.addCallback(self._gotResponse, method)

    def _gotResponse(self, data, method):
        """
        Unpack the response that was received from the server.

        @param data: A JSON string as returned from the request/response layer.
        """
        payload = json.loads(data)
        if "result" in payload:
            return payload["result"]
        elif "error" in payload:
            error = payload["error"]
            raise ServiceAPIError(method, error["code"], error["message"], error["data"])


@implementer(IRequestHandler)
class JSONRequestHandler(object):
    """
    The service-side handler for JSON-RPC requests.
    """

    def __init__(self, method_handler):
        """
        @param method_handler: The object to which methods will be dispatched.
        """
        self.method_handler = method_handler

    def handle(self, data):
        """
        @param data: A JSON string that was sent from the client.
        @return: A JSON string representing a response to the client.
        """
        payload = json.loads(data)
        method = payload["method"]
        params = payload["params"]
        result = maybeDeferred(self.method_handler.gotRequest, method, **params)
        result.addCallback(self._gotResult)
        result.addErrback(self._gotError, method, params)
        result.addCallback(json.dumps)
        return result

    def _gotResult(self, result):
        """
        Pack up the method handler's result into a JSON-RPC response object.

        @param object: The return value of the method handler, to be packed into the JSON-RPC
            result.
        """
        return {
            "jsonrpc": "2.0",
            "result": result,
            "id": 1
        }

    def _gotError(self, failure, method, params):
        log.err(failure, "Error while handling %s %s" % (method, params))  # XXX test this
        # TODO: Only include tracebacks in private/debug services!
        if hasattr(failure.value, "json_rpc_error_code"):
            error_code = failure.value.json_rpc_error_code
        else:
            error_code = -32000
        return {
            "jsonrpc": "2.0",
            "error": {
                "code": error_code,
                "message": failure.getErrorMessage(),
                "data": failure.getTraceback()
            },
            "id": 1
        }


class MethodHandler(object):
    """
    Subclass this to define your application's service API..

    Implement remote_* methods.
    """

    def gotRequest(self, method, **params):
        """
        Handle a request by dispatching to a remote_* method.
        """
        handler = getattr(self, 'remote_' + method, None)
        if not handler:
            raise HandlerNotFound(method)

        result = handler(**params)
        return result
