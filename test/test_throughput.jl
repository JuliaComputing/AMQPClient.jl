module AMPQTestThroughput

using AMQPClient
using Base.Test

const EXCG_DIRECT = "amq.direct"
const QUEUE1 = "queue1"
const ROUTE1 = "key1"
const MSG_SIZE = 1024
const NMSGS = 10^6
const no_ack = true

const M = Message(rand(UInt8, 1024), content_type="application/octet-stream", delivery_mode=PERSISTENT)

testlog(msg) = println(msg)

function setup(;virtualhost="/", host="localhost", port=AMQPClient.AMQP_DEFAULT_PORT, auth_params=AMQPClient.DEFAULT_AUTH_PARAMS)
    # open a connection
    testlog("opening connection...")
    conn = connection(;virtualhost=virtualhost, host=host, port=port, auth_params=auth_params)

    # open a channel
    testlog("opening channel...")
    chan1 = channel(conn, AMQPClient.UNUSED_CHANNEL, true)
    @test chan1.id == 1

    # create and bind queues
    testlog("creating queues...")
    success, message_count, consumer_count = queue_declare(chan1, QUEUE1)
    @test success
    @test message_count == 0

    @test queue_bind(chan1, QUEUE1, EXCG_DIRECT, ROUTE1)

    conn, chan1
end

function teardown(conn, chan1, delete=false)
    testlog("closing down...")
    if delete
        success, message_count = queue_purge(chan1, QUEUE1)
        @test success
        @test message_count == 0

        @test queue_unbind(chan1, QUEUE1, EXCG_DIRECT, ROUTE1)
        success, message_count = queue_delete(chan1, QUEUE1)
        @test success
        @test message_count == 0
    end

    # close channels and connection
    close(chan1)
    AMQPClient.wait_for_state(chan1, AMQPClient.CONN_STATE_CLOSED)
    @test !isopen(chan1)

    close(conn)
    AMQPClient.wait_for_state(conn, AMQPClient.CONN_STATE_CLOSED)
    @test !isopen(conn)
end

function publish(conn, chan1)
    testlog("starting basic publisher...")
    # publish N messages
    for idx in 1:NMSGS
        basic_publish(chan1, M; exchange=EXCG_DIRECT, routing_key=ROUTE1)
        if (idx % 10000) == 0
            println("publishing $idx ...")
            sleep(1)
        end
    end
end

function consume(conn, chan1)
    testlog("starting basic consumer...")
    # start a consumer task
    global msg_count = 0
    global start_time = time()
    global end_time = 0
    consumer_fn = (rcvd_msg) -> begin
        global msg_count
        global end_time
        msg_count += 1
        if ((msg_count % 10000) == 0) || (msg_count == NMSGS)
            #basic_ack(chan1, 0; all_upto=true)
            println("ack sent $msg_count ...")
        end
        no_ack || basic_ack(chan1, rcvd_msg.delivery_tag)
        if msg_count == NMSGS
            end_time = time()
        end
    end
    success, consumer_tag = basic_consume(chan1, QUEUE1, consumer_fn; no_ack=no_ack)
    @test success

    # wait to receive all messages
    while msg_count < NMSGS
        sleep(2)
    end

    # cancel the consumer task
    @test basic_cancel(chan1, consumer_tag)

    # time to send and receive
    total_time = max(end_time - start_time, 1)
    println("time to send and receive $NMSGS messages: $(end_time - start_time) secs @ $(NMSGS/total_time) msgs per second")
end

function run_publisher()
    conn, chan1 = AMPQTestThroughput.setup()
    AMPQTestThroughput.publish(conn, chan1)
    AMPQTestThroughput.teardown(conn, chan1, false) # exit without destroying queue
    nothing
end

function run_consumer()
    conn, chan1 = AMPQTestThroughput.setup()
    AMPQTestThroughput.consume(conn, chan1)
    println("waiting for publisher to exit gracefully...")
    sleep(10)  # wait for publisher to exit gracefully
    AMPQTestThroughput.teardown(conn, chan1, true)
    nothing
end

end # module AMPQTestThroughput
