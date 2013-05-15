import time

from twisted.internet import reactor
from twisted.internet.endpoints import TCP4ClientEndpoint

from txscale.reqresp.redis import RedisResponder
from txscale.rpc.jsonrpc import JSONRequestHandler, MethodHandler


class EchoHandler(MethodHandler):

    def __init__(self):
        self.requests = 0
        self.mark = None

    def remote_echo(self, data):
        now = time.time()
        if self.mark is None:
            self.mark = now
        elif now - self.mark >= 1:
            self.mark = now
            print "rps", self.requests
            self.requests = 0
        self.requests += 1
        return data

redis_endpoint = TCP4ClientEndpoint(reactor, "localhost", 6379)
responder = RedisResponder(redis_endpoint)
echo_handler = EchoHandler()
handler = JSONRequestHandler(echo_handler)
responder.listen("echo-serv", handler)


if __name__ == '__main__':
    import sys
    from twisted.python.log import startLogging
    startLogging(sys.stdout)
    reactor.addSystemEventTrigger("before", "shutdown", responder.stop)
    reactor.run()
