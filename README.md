#txScale

This package aims to provide the following overarching features, tightly
integrated with Twisted.

  (Put the word "Easy" at the beginning of each of these points...)

  * Development of stateless RPC services (maybe with multiple different protocols)
  * Deployment of those RPC services
  * Scaling of those RPC services to multiple cores and multiple servers.
  * Exposure of these scalable services over standard public protocols
    (a straightforward JSON/HTTP API).


###Done:

* Implement a JSON-RPC RPC layer. I think this is done, and even fairly compliant, but there may
  be some issues.

###To Do:

* Request/Response transports (in this order):
  * Redis req/resp transport (in progress)
  * txAMQP req/resp transport (this one should less time since I've done it before)
  * txZMQ req/resp transport.
  * Maybe even a low-level TCP req/resp transport, utilizing txLoadBalancer.
* RPC layers (in this order):
  * AMP
  * Avro
  * Thrift
  * Protobuf
* Simple HTTP/JSON front-end
  * This will need to have a little bit of glue-code for each of the RPC layers. Most RPC layers
    are very similar in their API: arbitrary basic Python types in, arbitrary basic Python types
    out.