## Sending and Receiving Messages

An AMQP message is represented by the `Message` type. Receiving a message from a queue returns an instance of this type. To send a `Message` must be created first.

Messages can also have one or more of these properties:

| property name    | description                                                                        |
| ---------------- | ---------------------------------------------------------------------------------- |
| content_type     | MIME content type (MIME typing)                                                    |
| content_encoding | MIME content encoding (MIME typing)                                                |
| headers          | message header field table (For applications, and for header exchange routing)     |
| delivery_mode    | `NONPERSISTENT` or `PERSISTENT` (For queues that implement persistence)            |
| priority         | message priority, 0 to 9 (For queues that implement priorities)                    |
| correlation_id   | application correlation identifier (For application use, no formal behaviour)      |
| reply_to         | address to reply to (For application use, no formal behaviour)                     |
| expiration       | message expiration specification (For application use, no formal behaviour)        |
| message_id       | application message identifier (For application use, no formal behaviour)          |
| timestamp        | message timestamp (For application use, no formal behaviour)                       |
| message_type     | message type name (For application use, no formal behaviour)                       |
| user_id          | creating user id (For application use, no formal behaviour)                        |
| app_id           | creating application id (For application use, no formal behaviour)                 |
| cluster_id       | reserved, must be empty (Deprecated, was old cluster-id property)                  |

A message received from a queue can also have the following attributes:

| attribute name   | type        | description                                                                                                       |
| ---------------- | ----------- | ----------------------------------------------------------------------------------------------------------------- |
| consumer_tag     | String      | Identifier for the queue consumer, valid within the current channel.                                              |
| delivery_tag     | Int64       | A tag to refer to a delivery attempt. This can be used to acknowledge/reject the message.                         |
| redelivered      | Bool        | Whether this message was delivered earlier, but was rejected ot not acknowledged.                                 |
| exchange         | String      | Name of the exchange that the message was originally published to. May be empty, indicating the default exchange. |
| routing_key      | String      | The routing key name specified when the message was published.                                                    |
| remaining        | Int32       | Number of messages remaining in the queue.                                                                        |


````julia
# create a message with 10 bytes of random value as data
msg =  Message(rand(UInt8, 10))

# create a persistent plain text message
data = convert(Vector{UInt8}, "hello world")
msg = Message(data, content_type="text/plain", delivery_mode=PERSISTENT)
````

Messages are published to an exchange, optionally specifying a routing key.

````julia
EXCG_DIRECT = "MyDirectExcg"
ROUTE1 = "routingkey1"

basic_publish(chan1, msg; exchange=EXCG_DIRECT, routing_key=ROUTE1)
````

To poll a queue for messages:

````julia
maybe_msg = basic_get(chan1, QUEUE1, false)

# check if we got a message
if !isnull(maybe_msg)
    msg = get(maybe_msg)

    # process msg...

    # acknowledge receipt
    basic_ack(chan1, msg.delivery_tag)
end
````

To subscribe for messages (register an asynchronous callback):

````julia
# define a callback function to process messages
function consumer(msg)
    # process msg...

    # acknowledge receipt
    basic_ack(chan1, msg.delivery_tag)
end

# subscribe and register the callback function
success, consumer_tag = basic_consume(chan1, QUEUE1, consumer)

@assert success
println("consumer registered with tag $consumer_tag")

# go ahead with other stuff...
# or wait for an indicator for shutdown

# unsubscribe the consumer from the queue
basic_cancel(chan1, consumer_tag)
````
