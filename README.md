# AMQPClient

[![Build Status](https://travis-ci.org/JuliaComputing/AMQPClient.jl.svg?branch=master)](https://travis-ci.org/JuliaComputing/AMQPClient.jl)
[![Coverage Status](https://coveralls.io/repos/JuliaComputing/AMQPClient.jl/badge.svg?branch=master&service=github)](https://coveralls.io/github/JuliaComputing/AMQPClient.jl?branch=master)
[![codecov.io](http://codecov.io/github/JuliaComputing/AMQPClient.jl/coverage.svg?branch=master)](http://codecov.io/github/JuliaComputing/AMQPClient.jl?branch=master)

A Julia [AMQP (Advanced Message Queuing Protocol)](http://www.amqp.org/) Client.

Supports protocol version 0.9.1 and [RabbitMQ](https://www.rabbitmq.com/) extensions.
 
This library has been tested with RabbitMQ, though it should also work with other AMQP 0.9.1 compliant systems.

# Using AMQPClient:

- [Connections and Channels](CONNECTIONS.md)
- [Exchanges and Queues](QUEUES.md)
- [Sending and Receiving Messages](SENDRECV.md)

Note: These documents may not mention all implemented APIs yet. Please look at the protocol references or exported methods of the package to get the complete list.

### Protocol reference:

- [AMQP v0.9.1](http://www.amqp.org/resources/download)
- [RabbitMQ Extensions](https://www.rabbitmq.com/extensions.html)
