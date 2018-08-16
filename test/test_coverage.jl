module AMQPTestCoverage

using AMQPClient, Test, Random

const JULIA_HOME = Sys.BINDIR

const EXCG_DIRECT = "ExcgDirect"
const EXCG_FANOUT = "ExcgFanout"
const QUEUE1 = "queue1"
const ROUTE1 = "key1"

testlog(msg) = println(msg)

function runtests(;virtualhost="/", host="localhost", port=AMQPClient.AMQP_DEFAULT_PORT, auth_params=AMQPClient.DEFAULT_AUTH_PARAMS)
    verify_spec()
    test_types()
    @test default_exchange_name("direct") == "amq.direct"
    @test default_exchange_name() == ""
    @test AMQPClient.method_name(AMQPClient.TAMQPMethodPayload(:Basic, :Ack, (1, false))) == "Basic.Ack"

    # open a connection
    testlog("opening connection...")
    conn = connection(;virtualhost=virtualhost, host=host, port=port, auth_params=auth_params)

    # open a channel
    testlog("opening channel...")
    chan1 = channel(conn, AMQPClient.UNUSED_CHANNEL, true)
    @test chan1.id == 1

    # test default exchange names
    @test default_exchange_name() == ""
    @test default_exchange_name(EXCHANGE_TYPE_DIRECT) == "amq.direct"

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

    M = Message(Vector{UInt8}("hello world"), content_type="text/plain", delivery_mode=PERSISTENT)

    testlog("testing basic publish and get...")
    # publish 10 messages
    for idx in 1:10
        basic_publish(chan1, M; exchange=EXCG_DIRECT, routing_key=ROUTE1)
    end

    # basic get 10 messages
    for idx in 1:10
        result = basic_get(chan1, QUEUE1, false)
        @test result !== nothing
        rcvd_msg = result
        basic_ack(chan1, rcvd_msg.delivery_tag)
        @test rcvd_msg.remaining == (10-idx)
        @test rcvd_msg.exchange == EXCG_DIRECT
        @test rcvd_msg.redelivered == false
        @test rcvd_msg.routing_key == ROUTE1
        @test rcvd_msg.data == M.data
        @test :content_type in keys(rcvd_msg.properties)
        @test convert(String, rcvd_msg.properties[:content_type]) == "text/plain"
    end

    # basic get returns null if no more messages
    @test basic_get(chan1, QUEUE1, false) === nothing

    ## test reject and requeue
    basic_publish(chan1, M; exchange=EXCG_DIRECT, routing_key=ROUTE1)

    result = basic_get(chan1, QUEUE1, false)
    @test result !== nothing
    rcvd_msg = result
    @test rcvd_msg.redelivered == false

    basic_reject(chan1, rcvd_msg.delivery_tag; requeue=true)

    result = basic_get(chan1, QUEUE1, false)
    @test result !== nothing
    rcvd_msg = result
    @test rcvd_msg.redelivered == true

    basic_ack(chan1, rcvd_msg.delivery_tag)

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

    # test heartbeats
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

function verify_spec()
    ALLCLASSES = (:Connection, :Basic, :Channel, :Confirm, :Exchange, :Queue, :Tx)
    for n in ALLCLASSES
        @test n in keys(AMQPClient.CLASSNAME_MAP)
    end
    for (n,v) in keys(AMQPClient.CLASSMETHODNAME_MAP)
        @test n in ALLCLASSES
    end
end

function test_types()
    d = Dict{String,Any}(
        "bool"      => 0x1,
        "int"       => 10,
        "uint"      => 0x1,
        "float"     => rand(),
        "shortstr"  => convert(AMQPClient.TAMQPShortStr, randstring(10)),
        "longstr"   => convert(AMQPClient.TAMQPLongStr, randstring(1024)))
    ft = convert(AMQPClient.TAMQPFieldTable, d)
    iob = IOBuffer()
    show(iob, ft)
    @test length(take!(iob)) > 0

    fields = [Pair{Symbol,AMQPClient.TAMQPField}(:bit,          AMQPClient.TAMQPBit(0x1)),
              Pair{Symbol,AMQPClient.TAMQPField}(:shortstr,     convert(AMQPClient.TAMQPShortStr, randstring(10))),
              Pair{Symbol,AMQPClient.TAMQPField}(:longstr,      convert(AMQPClient.TAMQPLongStr, randstring(1024))),
              Pair{Symbol,AMQPClient.TAMQPField}(:fieldtable,   ft)]
    show(iob, fields)
    @test length(take!(iob)) > 0
end

end # module AMQPTestCoverage
