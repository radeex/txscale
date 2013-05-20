#txScale (real name TBD)

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
  * Thrift
  * Protobuf
  * Avro
* Simple HTTP/JSON front-end
  * This will need to have a little bit of glue-code for each of the RPC layers. Most RPC layers
    are very similar in their API: arbitrary basic Python types in, arbitrary basic Python types
    out.
* Synchronous Python client (maybe using synchronous-deferred, or maybe not)
* clients for other languages


###Release Criteria:

I'll announce and release an unsupported version of txScale once I do the following things:

* decide on a name other than txScale
* at least one req/resp protocol is implemented and has 100% test coverage
* at least one RPC protocol is implemented and has 100% test coverage (DONE!)
* a basic HTTP/JSON front-end is implemented and has 100% test coverage
* servers have a configurable concurrency
* there's some way to deploy servers based on a simple configuration
* I have some super simple benchmarks implemented in a way that can be easily run
* I have a demo application (maybe using the synchronous Python client in e.g. a Pyramid web app)

Things I want to do before I release a backwards-compatible, "supported" version:

* servers call pauseProducing if incoming requests exceeds configured concurrency
  (or the Tubes equiv)
* number-of-requests has a hard limit on the client
* figure out and implement thorough logging. Logging is really important!
  * There should be options exposed by the library to specify what kind of stuff is logged
  * Log format should be very concise and descriptive.
  * All log data should be structured.
    * context - the name of the txScale subcomponent, like "redis" or "json-rpc"
    * event_name - something like "message_received" or "backlog_exceeded" -- unique when
      combined with the context
    * per-event additional key/value pairs
* some consideration for centralized log management (hopefully, just integrating with an existing
  open source structured log aggregation system)
