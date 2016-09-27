module AMPQTestCoverage

using AMQPClient
using Base.Test

const EXCG_DIRECT = "ExcgDirect"
const EXCG_FANOUT = "ExcgFanout"
const QUEUE1 = "queue1"
const ROUTE1 = "key1"

testlog(msg) = println(msg)

function runtests(;virtualhost="/", host="localhost", port=AMQPClient.AMQP_DEFAULT_PORT, auth_params=AMQPClient.DEFAULT_AUTH_PARAMS)
    # open a connection
    testlog("opening connection...")
    conn = connection(;virtualhost=virtualhost, host=host, port=port, auth_params=auth_params)

    # open a channel
    testlog("opening channel...")
    chan1 = channel(conn, AMQPClient.UNUSED_CHANNEL, true)
    @test chan1.id == 1

    # create exchanges
    testlog("creating exchanges...")
    @test exchange_declare(chan1, EXCG_DIRECT, EXCHANGE_TYPE_DIRECT; arguments=Dict{String,Any}("Hello"=>"World", "Foo"=>"bar"))
    @test exchange_declare(chan1, EXCG_FANOUT, EXCHANGE_TYPE_FANOUT)

    # create and bind queues
    testlog("creating queues...")
    success, message_count, consumer_count = queue_declare(chan1, QUEUE1)
    @test success
    @test message_count == 0
    @test consumer_count == 0

    @test queue_bind(chan1, QUEUE1, EXCG_DIRECT, ROUTE1)

    # rabbitmq 3.6.5 does not support qos
    # basic_qos(chan1, 1024*10, 10, false)

    M = Message(convert(Vector{UInt8}, "hello world"), content_type="text/plain", delivery_mode=PERSISTENT)

    testlog("testing basic publish and get...")
    # publish 10 messages
    for idx in 1:10
        basic_publish(chan1, M; exchange=EXCG_DIRECT, routing_key=ROUTE1)
    end

    # basic get 10 messages
    for idx in 1:10
        result = basic_get(chan1, QUEUE1, false)
        @test !isnull(result)
        rcvd_msg = get(result)
        basic_ack(chan1, rcvd_msg.delivery_tag)
        @test rcvd_msg.remaining == (10-idx)
        @test rcvd_msg.exchange == EXCG_DIRECT
        @test rcvd_msg.redelivered == false
        @test rcvd_msg.routing_key == ROUTE1
        @test rcvd_msg.data == M.data
    end

    # basic get returns null if no more messages
    @test isnull(basic_get(chan1, QUEUE1, false))

    ## test reject and requeue
    #basic_publish(chan1, M; exchange=EXCG_DIRECT, routing_key=ROUTE1)

    #result = basic_get(chan1, QUEUE1, false)
    #@test !isnull(result)
    #rcvd_msg = get(result)
    #@test rcvd_msg.redelivered == false

    #basic_reject(chan1, rcvd_msg.delivery_tag; requeue=true)

    #result = basic_get(chan1, QUEUE1, false)
    #@test !isnull(result)
    #rcvd_msg = get(result)
    #@test rcvd_msg.redelivered == true

    #basic_ack(chan1, rcvd_msg.delivery_tag)

    testlog("testing basic consumer...")
    # start a consumer task
    global msg_count = 0
    consumer_fn = (rcvd_msg) -> begin
        @test rcvd_msg.exchange == EXCG_DIRECT
        @test rcvd_msg.redelivered == false
        @test rcvd_msg.routing_key == ROUTE1
        @test rcvd_msg.data == M.data
        global msg_count
        msg_count += 1
        println("received msg $(msg_count): $(String(rcvd_msg.data))")
        basic_ack(chan1, rcvd_msg.delivery_tag)
    end
    success, consumer_tag = basic_consume(chan1, QUEUE1, consumer_fn)
    @test success

    # publish 10 messages
    for idx in 1:10
        basic_publish(chan1, M; exchange=EXCG_DIRECT, routing_key=ROUTE1)
    end

    # wait for a reasonable time to receive all messages
    for idx in 1:10
        (msg_count == 10) && break
        sleep(1)
    end
    @test msg_count == 10

    # cancel the consumer task
    @test basic_cancel(chan1, consumer_tag)

    # test transactions
    testlog("testing tx...")
    @test tx_select(chan1)
    @test tx_commit(chan1)
    @test tx_rollback(chan1)

    if 120 >= conn.conn.heartbeat > 0
        c = conn.conn
        testlog("testing heartbeats (waiting $(3*c.heartbeat) secs)...")
        ts1 = c.heartbeat_time_server
        tc1 = c.heartbeat_time_client
        sleeptime = c.heartbeat/2
        for idx in 1:6
            (c.heartbeat_time_server > ts1) && (c.heartbeat_time_client > tc1) && break
            sleep(sleeptime)
        end
        @test c.heartbeat_time_server > ts1
        @test c.heartbeat_time_client > tc1
    else
        testlog("not testing heartbeats (wait too long at $(3*conn.conn.heartbeat) secs)")
    end

    testlog("closing down...")
    success, message_count = queue_purge(chan1, QUEUE1)
    @test success
    @test message_count == 0

    @test queue_unbind(chan1, QUEUE1, EXCG_DIRECT, ROUTE1)
    success, message_count = queue_delete(chan1, QUEUE1)
    @test success
    @test message_count == 0

    # delete exchanges
    @test exchange_delete(chan1, EXCG_DIRECT; nowait=true)
    @test exchange_delete(chan1, EXCG_FANOUT)

    # close channels and connection
    close(chan1)
    AMQPClient.wait_for_state(chan1, AMQPClient.CONN_STATE_CLOSED)
    @test !isopen(chan1)

    close(conn)
    AMQPClient.wait_for_state(conn, AMQPClient.CONN_STATE_CLOSED)
    @test !isopen(conn)

    testlog("done.")
    nothing
end

end # module AMPQTestCoverage
