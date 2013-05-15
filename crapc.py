import time

from twisted.internet import reactor
from twisted.internet.endpoints import TCP4ClientEndpoint
from twisted.python import log

from txscale.reqresp.redis import RedisRequester, ServiceGracefullyDisappearedError, TimeOutError
from txscale.rpc.jsonrpc import JSONRPCClient


redis_endpoint = TCP4ClientEndpoint(reactor, "localhost", 6379)

requester = RedisRequester("echo-serv", redis_endpoint, reactor)
client = JSONRPCClient(requester)


responses = 0


def gotResponse(response):
    global responses, mark
    responses += 1
    step = time.time()
    if step - mark >= 1:
        print "mark. %s rps" % (responses,)
        responses = 0
        mark = step
    return doit()


def doit():
    d = client.request("echo", data=["hell", {"yeah": "woo"}, 45])
    return d.addCallback(gotResponse).addErrback(gotError)


def gotError(failure):
    failure.trap(ServiceGracefullyDisappearedError, TimeOutError)
    print failure.getErrorMessage()
    print "ignoring error. continuing."
    return doit()

mark = time.time()
doit().addErrback(log.err)

if __name__ == '__main__':
    import sys
    from twisted.python.log import startLogging
    startLogging(sys.stdout)
    reactor.run()
