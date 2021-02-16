## Exchanges and Queues

Constants representing the standard exchange types are available as: `EXCHANGE_TYPE_DIRECT`, `EXCHANGE_TYPE_FANOUT`, `EXCHANGE_TYPE_TOPIC`, and `EXCHANGE_TYPE_HEADERS`.

Exchanges can be delcared and deleted using the `exchange_declare` and `exchange_delete` APIs. They return a boolean to indicate success (`true`) or failure (`false`).
Declaring an already existing exchange simply attaches to it, a new exchange is created otherwise.

```julia
# declare (create if they do not exist) new exchange
EXCG_DIRECT = "MyDirectExcg"
EXCG_FANOUT = "MyFanoutExcg"
@assert exchange_declare(chan1, EXCG_DIRECT, EXCHANGE_TYPE_DIRECT)
@assert exchange_declare(chan1, EXCG_FANOUT, EXCHANGE_TYPE_FANOUT)

# operate with the exchanges...

# delete exchanges
@assert exchange_delete(chan1, EXCG_DIRECT)
@assert exchange_delete(chan1, EXCG_FANOUT)
```

Queues can similarly be declared and deleted.
Attaching to an existing queue also returns the number of pending messages and the number of consumers attached to the queue.

```julia
QUEUE1 = "MyQueue"
success, queue_name, message_count, consumer_count = queue_declare(chan1, QUEUE1)
@assert success

# operate with the queue

# delete the queue
success, message_count = queue_delete(chan1, QUEUE1)
@assert success
```

Messages are routed by binding queues and exchanges to other exchanges. The type of exchange and the routing key configured determine the path.

```julia
ROUTE1 = "routingkey1"
# bind QUEUE1 to EXCG_DIRECT,
# specifying that only messages with routing key ROUTE1 should be delivered to QUEUE1
@assert queue_bind(chan1, QUEUE1, EXCG_DIRECT, ROUTE1)

# operate with the queue

# remove the binding
@assert queue_unbind(chan1, QUEUE1, EXCG_DIRECT, ROUTE1)
```

Messages on a queue can be purged:

```julia
success, message_count = queue_purge(chan1, QUEUE1)
@assert success
@info("messages purged", message_count)
```
