#txScale

This package aims to provide the following overarching features, tightly
integrated with Twisted.

  (Put the word "Easy" at the beginning of each of these points...)

  * Development of stateless RPC services (maybe with multiple different protocols)
  * Deployment of those RPC services
  * Scaling of those RPC services to multiple cores and multiple servers.
  * Exposure of these scalable services over standard public protocols
    (for now, a straightforward JSON/HTTP API).

Some random notes:

* I'm going to be experimenting with Redis as a transport for requests and responses.
* I'm also going to implement an AMQP backend, as I've done that before and I know it works.
* Scaling up an RPC server should be as easy as running another instance of it,
  with a common configuration.
* Internal and public APIs should be implemented in the same way.
* I'm really not opinionated about the RPC protocol to use. Avro, Thrift,
  protobuf, JSON junk, it's all the same to me.
* There will be opinions, however, about how AMQP and Redis are used to get a trivially
  configured scalable service.
* I'm also a little opinionated about how the HTTP-based public API should
  work.
