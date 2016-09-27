## Connections and Channels

More than one connection can be made to a single server, though one is sufficient for most cases.

The IANA assigned port number for AMQP is 5672. It is available as the constant `AMQPClient.AMQP_DEFAULT_PORT`.

The `AMQPPLAIN` authentication mechanism is supported as of now.

````julia
using AMQPClient

port = AMQPClient.AMQP_DEFAULT_PORT
login = get_userid()  # default is usually "guest"
password = get_password()  # default is usually "guest"
auth_params = Dict{String,Any}("MECHANISM"=>"AMQPLAIN", "LOGIN"=>login, "PASSWORD"=>password)

conn = connection(;virtualhost="/", host="localhost", port=port, auth_params=auth_params)
````

Multiple channels can be multiplexed over a single connection. Channels are identified by their numeric id.

An existing channel can be attached to, or a new one created if it does not exist.

Specifying `AMQPClient.UNUSED_CHANNEL` as channel id during creation will automatically assign an unused id.

````julia
chan1 = channel(conn, AMQPClient.UNUSED_CHANNEL, true)

# to attach to a channel only if it already exists:
chanid = 2
chan2 = channel(conn, chanid)

# to specify a channel id and create if it does not exists yet:
chanid = 3
chan3 = channel(conn, chanid, true)
````

Channels and connections remain open until they are closed or they run into an error. The server can also initiate a close in some cases.

Channels represent logical multiplexing over a single connection, so closing a connection implicitly closes all its channels.

````julia
if isopen(conn)
    close(conn)
    # close is an asynchronous operation. To wait for the negotiation to complete:
    AMQPClient.wait_for_state(conn, AMQPClient.CONN_STATE_CLOSED)
end
# an individual channel can be closed similarly too
````

If a channel or connection is closed due to an error or by the server, the `closereason` attribute (type `CloseReason`) of the channel or connection object
may contain the error code and diagnostic message.

````julia
if !isnull(conn.closereason)
    reason = get(conn.closereason)
    println("Error code: ", reason.code)
    println("Message: ", reason.msg)
end
````
