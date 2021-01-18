module AMQPTestRPC

using AMQPClient, Test, Random

const JULIA_HOME = Sys.BINDIR
const QUEUE_RPC = "queue_rpc"
const NRPC_MSGS = 100
const NRPC_CLNTS = 4
const NRPC_SRVRS = 4
const server_lck = Ref(ReentrantLock())
const servers_done = Channel{Int}(NRPC_SRVRS)
const server_rpc_count = Ref(0)

function test_rpc_client(reply_queue_id; virtualhost="/", host="localhost", port=AMQPClient.AMQP_DEFAULT_PORT, auth_params=AMQPClient.DEFAULT_AUTH_PARAMS, amqps=amqps)
    rpc_queue_name = QUEUE_RPC * ((amqps === nothing) ? "amqp" : "amqps")
    # open a connection
    @info("client opening connection", reply_queue_id)
    conn = connection(;virtualhost=virtualhost, host=host, port=port, auth_params=auth_params, amqps=amqps)

    # open a channel
    @debug("client opening channel")
    chan1 = channel(conn, AMQPClient.UNUSED_CHANNEL, true)

    # create a reply queue for a client
    queue_name = rpc_queue_name * "_" * string(reply_queue_id) * "_" * string(getpid())
    @debug("client creating queue", queue_name)
    success, queue_name, message_count, consumer_count = queue_declare(chan1, queue_name; exclusive=true)
    @test success

    @debug("client testing rpc")
    rpc_reply_count = 0
    rpc_fn = (rcvd_msg) -> begin
        rpc_reply_count += 1

        msg_str = String(rcvd_msg.data)
        @debug("client", reply_quque_id, msg_str)

        basic_ack(chan1, rcvd_msg.delivery_tag)
    end

    # start a consumer task
    success, consumer_tag = basic_consume(chan1, queue_name, rpc_fn)
    @test success

    correlation_id = 0

    # publish NRPC_MSGS messages to the queue
    while correlation_id < NRPC_MSGS
        correlation_id += 1
        M = Message(Vector{UInt8}("hello from " * queue_name), content_type="text/plain", delivery_mode=PERSISTENT, reply_to=queue_name, correlation_id=string(correlation_id))
        basic_publish(chan1, M; exchange=default_exchange_name(), routing_key=rpc_queue_name)
        # sleep a random time between 1 and 5 seconds between requests
        sleep(rand())
    end

    while (rpc_reply_count < NRPC_MSGS)
        sleep(1)
    end

    @debug("client closing down", reply_queue_id)
    success, message_count = queue_purge(chan1, queue_name)
    @test success
    @test message_count == 0

    @test basic_cancel(chan1, consumer_tag)

    success, message_count = queue_delete(chan1, queue_name)
    @test success
    @test message_count == 0

    # close channels and connection
    close(chan1)
    AMQPClient.wait_for_state(chan1, AMQPClient.CONN_STATE_CLOSED)
    @test !isopen(chan1)

    close(conn)
    AMQPClient.wait_for_state(conn, AMQPClient.CONN_STATE_CLOSED)
    @test !isopen(conn)

    @info("client done", reply_queue_id, rpc_reply_count)
end


function test_rpc_server(my_server_id; virtualhost="/", host="localhost", port=AMQPClient.AMQP_DEFAULT_PORT, auth_params=AMQPClient.DEFAULT_AUTH_PARAMS, amqps=amqps)
    rpc_queue_name = QUEUE_RPC * ((amqps === nothing) ? "amqp" : "amqps")
    # open a connection
    @info("server opening connection", my_server_id)
    conn = connection(;virtualhost=virtualhost, host=host, port=port, auth_params=auth_params, amqps=amqps)

    # open a channel
    @debug("server opening channel", my_server_id)
    chan1 = channel(conn, AMQPClient.UNUSED_CHANNEL, true)

    # create queues (no need to bind if we are using the default exchange)
    lock(server_lck[]) do
        @debug("server creating queues", my_server_id)
        # this is the callback queue
        success, message_count, consumer_count = queue_declare(chan1, rpc_queue_name)
        @test success
    end

    # test RPC
    @debug("server testing rpc", my_server_id)
    rpc_fn = (rcvd_msg) -> begin
        rpc_count = lock(server_lck[]) do
            server_rpc_count[] = server_rpc_count[] + 1
        end
        @test :reply_to in keys(rcvd_msg.properties)
        reply_to = convert(String, rcvd_msg.properties[:reply_to])
        correlation_id = convert(String, rcvd_msg.properties[:correlation_id])

        resp_str = "$(my_server_id) received msg $(rpc_count) - $(reply_to): $(String(rcvd_msg.data))"
        @debug("server response", resp_str)

        M = Message(Vector{UInt8}(resp_str), content_type="text/plain", delivery_mode=PERSISTENT, correlation_id=correlation_id)
        basic_publish(chan1, M; exchange=default_exchange_name(), routing_key=reply_to)

        basic_ack(chan1, rcvd_msg.delivery_tag)
    end

    # start a consumer task
    success, consumer_tag = basic_consume(chan1, rpc_queue_name, rpc_fn)
    @test success

    server_done = false
    while !server_done
        sleep(5)
        lock(server_lck[]) do
            server_done = (server_rpc_count[] >= NRPC_MSGS*NRPC_CLNTS)
            @debug("rpc_count", server_rpc_count[], my_server_id)
        end
    end

    @debug("server closing down", my_server_id)
    @test basic_cancel(chan1, consumer_tag)
    @debug("server cancelled consumer", my_server_id)

    lock(server_lck[]) do
        take!(servers_done)
        # the last server to finish will purge and delete the queue
        if length(servers_done.data) == 0
            success, message_count = queue_purge(chan1, rpc_queue_name)
            @test success
            @test message_count == 0
            @debug("server purged queue", my_server_id)

            success, message_count = queue_delete(chan1, rpc_queue_name)
            @test success
            @test message_count == 0
            @debug("server deleted rpc queue", my_server_id)
        end
    end

    # close channels and connection
    close(chan1)
    AMQPClient.wait_for_state(chan1, AMQPClient.CONN_STATE_CLOSED)
    @test !isopen(chan1)

    close(conn)
    AMQPClient.wait_for_state(conn, AMQPClient.CONN_STATE_CLOSED)
    @test !isopen(conn)

    @info("server done", my_server_id)
    nothing
end

function runtests(; host="localhost", port=AMQPClient.AMQP_DEFAULT_PORT, amqps=nothing)
    @info("testing multiple client server rpc")
    server_rpc_count[] = 0
    
    for idx in 1:NRPC_SRVRS
        put!(servers_done, idx)
    end

    @sync begin
        for idx in 1:NRPC_SRVRS
            @async begin
                try
                    test_rpc_server(idx, host=host, port=port, amqps=amqps)
                catch ex
                    @error("server exception", exception=(ex,catch_backtrace()))
                    rethrow()
                end
            end
        end

        for idx in 1:NRPC_CLNTS
            @async begin
                try
                    test_rpc_client(idx, host=host, port=port, amqps=amqps)
                catch ex
                    @error("client exception", exception=(ex,catch_backtrace()))
                    rethrow()
                end
            end
        end
    end

    @info("testing multiple client server rpc done")
end

end # module AMQPTestRPC