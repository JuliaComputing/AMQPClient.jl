module AMQPTestRPC

using AMQPClient, Test, Random

const JULIA_HOME = Sys.BINDIR

const QUEUE_RPC = "queue_rpc"

testlog(msg) = println(msg)
reply_queue_id = 0
server_id = 0

const NRPC_MSGS = 100
const NRPC_CLNTS = 4
const NRPC_SRVRS = 4

function test_rpc_client(;virtualhost="/", host="localhost", port=AMQPClient.AMQP_DEFAULT_PORT, auth_params=AMQPClient.DEFAULT_AUTH_PARAMS)
    # open a connection
    testlog("client opening connection...")
    conn = connection(;virtualhost=virtualhost, host=host, port=port, auth_params=auth_params)

    # open a channel
    testlog("client opening channel...")
    chan1 = channel(conn, AMQPClient.UNUSED_CHANNEL, true)

    # create a reply queue for a client
    global reply_queue_id
    reply_queue_id += 1
    queue_name = QUEUE_RPC * "_" * string(reply_queue_id) * "_" * string(getpid())
    testlog("client creating queue " * queue_name * "...")
    success, message_count, consumer_count = queue_declare(chan1, queue_name; exclusive=true)

    testlog("client testing rpc...")
    rpc_reply_count = 0
    rpc_fn = (rcvd_msg) -> begin
        rpc_reply_count += 1

        msg_str = String(rcvd_msg.data)
        println("client ", msg_str)

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
        basic_publish(chan1, M; exchange=default_exchange_name(), routing_key=QUEUE_RPC)
        # sleep a random time between 1 and 5 seconds between requests
        sleep(rand())
    end

    while (rpc_reply_count < NRPC_MSGS)
        sleep(1)
    end

    testlog("client closing down...")
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

    testlog("client done.")
end

function test_rpc_server(;virtualhost="/", host="localhost", port=AMQPClient.AMQP_DEFAULT_PORT, auth_params=AMQPClient.DEFAULT_AUTH_PARAMS)
    global server_id
    server_id += 1
    my_server_id = server_id

    # open a connection
    testlog("server $my_server_id opening connection...")
    conn = connection(;virtualhost=virtualhost, host=host, port=port, auth_params=auth_params)

    # open a channel
    testlog("server $my_server_id opening channel...")
    chan1 = channel(conn, AMQPClient.UNUSED_CHANNEL, true)

    # create queues (no need to bind if we are using the default exchange)
    testlog("server $my_server_id creating queues...")
    # this is the callback queue
    success, message_count, consumer_count = queue_declare(chan1, QUEUE_RPC)
    @test success

    # test RPC
    testlog("server $my_server_id testing rpc...")
    global rpc_count = 0
    rpc_fn = (rcvd_msg) -> begin
        global rpc_count
        rpc_count += 1

        @test :reply_to in keys(rcvd_msg.properties)
        reply_to = convert(String, rcvd_msg.properties[:reply_to])
        correlation_id = convert(String, rcvd_msg.properties[:correlation_id])

        resp_str = "$(my_server_id) received msg $(rpc_count) - $(reply_to): $(String(rcvd_msg.data))"
        println("server ", resp_str)

        M = Message(Vector{UInt8}(resp_str), content_type="text/plain", delivery_mode=PERSISTENT, correlation_id=correlation_id)
        basic_publish(chan1, M; exchange=default_exchange_name(), routing_key=reply_to)

        basic_ack(chan1, rcvd_msg.delivery_tag)
    end

    # start a consumer task
    success, consumer_tag = basic_consume(chan1, QUEUE_RPC, rpc_fn)
    @test success

    while (rpc_count < NRPC_MSGS*NRPC_CLNTS)
        sleep(1)
    end

    testlog("server $my_server_id closing down...")
    success, message_count = queue_purge(chan1, QUEUE_RPC)
    @test success
    @test message_count == 0

    @test basic_cancel(chan1, consumer_tag)

    success, message_count = queue_delete(chan1, QUEUE_RPC)
    @test success
    @test message_count == 0
    testlog("server $my_server_id deleted rpc queue")

    # close channels and connection
    close(chan1)
    AMQPClient.wait_for_state(chan1, AMQPClient.CONN_STATE_CLOSED)
    @test !isopen(chan1)

    close(conn)
    AMQPClient.wait_for_state(conn, AMQPClient.CONN_STATE_CLOSED)
    @test !isopen(conn)

    testlog("server $my_server_id done.")
    nothing
end

function runtests()
    testlog("testing multiple client server rpc")
    clients = Vector{Task}()
    servers = Vector{Task}()

    for idx in 1:NRPC_SRVRS
        push!(servers, @async test_rpc_server())
    end

    for idx in 1:NRPC_CLNTS
        push!(clients, @async test_rpc_client())
    end

    tasks_active = NRPC_CLNTS + NRPC_SRVRS
    while tasks_active > 0
        tasks_active = 0
        for idx in 1:NRPC_SRVRS
            istaskdone(servers[idx]) || (tasks_active += 1)
        end
        for idx in 1:NRPC_CLNTS
            istaskdone(clients[idx]) || (tasks_active += 1)
        end
        sleep(5)
    end
    testlog("done")
end
end # module AMQPTestRPC
