module AMQPTestCoverage

using AMQPClient, Test, Random

const JULIA_HOME = Sys.BINDIR

const EXCG_DIRECT = "ExcgDirect"
const EXCG_FANOUT = "ExcgFanout"
const QUEUE1 = "queue1"
const ROUTE1 = "key1"
const invalid_auth_params = Dict{String,Any}("MECHANISM"=>"AMQPLAIN", "LOGIN"=>randstring(10), "PASSWORD"=>randstring(10))

function runtests(;virtualhost="/", host="localhost", port=AMQPClient.AMQP_DEFAULT_PORT, auth_params=AMQPClient.DEFAULT_AUTH_PARAMS, amqps=nothing, keepalive=true, heartbeat=true)
    verify_spec()
    test_types()
    @test default_exchange_name("direct") == "amq.direct"
    @test default_exchange_name() == ""
    @test AMQPClient.method_name(AMQPClient.TAMQPMethodPayload(:Basic, :Ack, (1, false))) == "Basic.Ack"

    # test failure on invalid auth_params
    @test_throws AMQPClient.AMQPClientException connection(;virtualhost=virtualhost, host=host, port=port, amqps=amqps, auth_params=invalid_auth_params)

    conn_ref = nothing

    # open a connection
    @info("opening connection")
    connection(;virtualhost=virtualhost, host=host, port=port, amqps=amqps, auth_params=auth_params, send_queue_size=512, keepalive=keepalive, heartbeat=heartbeat) do conn
        @test conn.conn.sendq.sz_max == 512

        # open a channel
        @info("opening channel")
        channel(conn, AMQPClient.UNUSED_CHANNEL, true) do chan1
            @test chan1.id == 1
            @test conn.conn.sendq.sz_max == 512

            # test default exchange names
            @test default_exchange_name() == ""
            @test default_exchange_name(EXCHANGE_TYPE_DIRECT) == "amq.direct"

            # create exchanges
            @info("creating exchanges")
            @test exchange_declare(chan1, EXCG_DIRECT, EXCHANGE_TYPE_DIRECT; arguments=Dict{String,Any}("Hello"=>"World", "Foo"=>"bar"))
            @test exchange_declare(chan1, EXCG_FANOUT, EXCHANGE_TYPE_FANOUT)
            # redeclaring the exchange with same attributes should be fine
            @test exchange_declare(chan1, EXCG_FANOUT, EXCHANGE_TYPE_FANOUT)
            # redeclaring an existing exchange with different attributes should fail
            @test_throws AMQPClient.AMQPClientException exchange_declare(chan1, EXCG_FANOUT, EXCHANGE_TYPE_DIRECT)
        end

        chan_ref = nothing
        # must reconnect as channel gets closed after a channel exception
        channel(conn, AMQPClient.UNUSED_CHANNEL, true) do chan1
            @test chan1.id == 1

            # create and bind queues
            @info("creating queues")
            success, queue_name, message_count, consumer_count = queue_declare(chan1, QUEUE1)
            @test success
            @test message_count == 0
            @test consumer_count == 0

            @test queue_bind(chan1, QUEUE1, EXCG_DIRECT, ROUTE1)

            # rabbitmq 3.6.5 does not support qos
            # basic_qos(chan1, 1024*10, 10, false)

            M = Message(Vector{UInt8}("hello world"), content_type="text/plain", delivery_mode=PERSISTENT)

            @info("testing basic publish and get")
            # publish 10 messages
            for idx in 1:10
                basic_publish(chan1, M; exchange=EXCG_DIRECT, routing_key=ROUTE1)
                flush(chan1)
                @test !isready(chan1.conn.sendq)
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

            @info("testing basic consumer")
            # start a consumer task
            global msg_count = 0
            consumer_fn = (rcvd_msg) -> begin
                @test rcvd_msg.exchange == EXCG_DIRECT
                @test rcvd_msg.redelivered == false
                @test rcvd_msg.routing_key == ROUTE1
                global msg_count
                msg_count += 1
                if msg_count <= 10
                    @test rcvd_msg.data == M.data
                else
                    @test rcvd_msg.data == UInt8[]
                end
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

            @info("testing empty messages")
            # Test sending and receiving empty message
            M_empty = Message(Vector{UInt8}(), content_type="text/plain", delivery_mode=PERSISTENT)
            basic_publish(chan1, M_empty; exchange=EXCG_DIRECT, routing_key=ROUTE1)

            M_no_ct = Message(Vector{UInt8}(), delivery_mode=PERSISTENT)
            basic_publish(chan1, M_no_ct; exchange=EXCG_DIRECT, routing_key=ROUTE1)

            println("Waiting")
            # wait for a reasonable time to receive last two messages
            for idx in 1:5
                (msg_count == 12) && break
                sleep(1)
            end
            println("Waited")
            @test msg_count == 12

            # cancel the consumer task
            @test basic_cancel(chan1, consumer_tag)

            # test transactions
            @info("testing tx")
            @test tx_select(chan1)
            @test tx_commit(chan1)
            @test tx_rollback(chan1)

            # test heartbeats
            if 120 >= conn.conn.heartbeat > 0
                c = conn.conn
                @info("testing heartbeats (waiting $(3*c.heartbeat) secs)...")
                ts1 = c.heartbeat_time_server
                tc1 = c.heartbeat_time_client
                sleeptime = c.heartbeat/2
                for idx in 1:6
                    (c.heartbeat_time_server > ts1) && (c.heartbeat_time_client > tc1) && break
                    sleep(sleeptime)
                end
                @test c.heartbeat_time_server > ts1
                @test c.heartbeat_time_client > tc1
            elseif conn.conn.heartbeat == 0
                @info("heartbeat disabled")
            else
                @info("not testing heartbeats (wait too long at $(3*conn.conn.heartbeat) secs)")
            end

            @info("closing down")
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

            chan_ref = chan1 # to do additional tests on a closed channel
        end

        close(chan_ref) # closing a closed channel should not be an issue
        AMQPClient.wait_for_state(chan_ref, AMQPClient.CONN_STATE_CLOSED)
        @test !isopen(chan_ref)

        conn_ref = conn  # to do additional tests on a closed connection
    end

    # closing a closed connection should not be an issue
    close(conn_ref)
    AMQPClient.wait_for_state(conn_ref, AMQPClient.CONN_STATE_CLOSED)
    @test !isopen(conn_ref)

    @info("done")
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
        "shortstr"  => AMQPClient.TAMQPShortStr(randstring(10)),
        "longstr"   => AMQPClient.TAMQPLongStr(randstring(1024)))
    ft = AMQPClient.TAMQPFieldTable(d)
    iob = IOBuffer()
    show(iob, ft)
    @test length(take!(iob)) > 0

    fields = [Pair{Symbol,AMQPClient.TAMQPField}(:bit,          AMQPClient.TAMQPBit(0x1)),
              Pair{Symbol,AMQPClient.TAMQPField}(:shortstr,     AMQPClient.TAMQPShortStr(randstring(10))),
              Pair{Symbol,AMQPClient.TAMQPField}(:longstr,      AMQPClient.TAMQPLongStr(randstring(1024))),
              Pair{Symbol,AMQPClient.TAMQPField}(:fieldtable,   ft)]
    show(iob, fields)
    @test length(take!(iob)) > 0

    mpayload = AMQPClient.TAMQPMethodPayload(:Channel, :Open, ("",))
    show(iob, mpayload)
    @test length(take!(iob)) > 0

    mfprop = AMQPClient.TAMQPFrameProperties(AMQPClient.TAMQPChannel(0), AMQPClient.TAMQPPayloadSize(100))
    show(iob, mfprop)
    @test length(take!(iob)) > 0

    mframe = AMQPClient.TAMQPMethodFrame(mfprop, mpayload)
    show(iob, mframe)
    @test length(take!(iob)) > 0

    fields = AMQPClient.TAMQPFieldValue[
        AMQPClient.TAMQPFieldValue(true),
        AMQPClient.TAMQPFieldValue(1.1),
        AMQPClient.TAMQPFieldValue(1),
        AMQPClient.TAMQPFieldValue("hello world"),
        AMQPClient.TAMQPFieldValue(Dict{String,Int}("one"=>1, "two"=>2)),
    ]

    fieldarray = AMQPClient.TAMQPFieldArray(fields)
    simplified_fields = AMQPClient.simplify(fieldarray)
    @test simplified_fields == Any[
        0x01,
        1.1,
        1,
         "hello world",
         Dict{String, Any}("two" => 2, "one" => 1)
    ]

    iob = PipeBuffer()
    write(iob, hton(AMQPClient.TAMQPLongUInt(10)))
    write(iob, UInt8[1,2,3,4,5,6,7,8,9,0])
    barr = read(iob, AMQPClient.TAMQPByteArray)
    @test barr.len == 10
    @test barr.data == UInt8[1,2,3,4,5,6,7,8,9,0]
end

end # module AMQPTestCoverage

