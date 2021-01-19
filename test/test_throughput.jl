module AMQPTestThroughput

using AMQPClient, Test, Random

const JULIA_HOME = Sys.BINDIR

const EXCG_DIRECT = "amq.direct"
const QUEUE1 = "queue1"
const ROUTE1 = "key1"
const MSG_SIZE = 1024
const NMSGS = 10^5
const no_ack = true

const M = Message(rand(UInt8, 1024), content_type="application/octet-stream", delivery_mode=PERSISTENT)

function setup(;virtualhost="/", host="localhost", port=AMQPClient.AMQP_DEFAULT_PORT, auth_params=AMQPClient.DEFAULT_AUTH_PARAMS, tls=false)
    # open a connection
    @debug("opening connection")
    amqps = tls ? amqps_configure() : nothing
    conn = connection(;virtualhost=virtualhost, host=host, port=port, auth_params=auth_params, amqps=amqps)

    # open a channel
    @debug("opening channel")
    chan1 = channel(conn, AMQPClient.UNUSED_CHANNEL, true)
    @test chan1.id == 1

    # create and bind queues
    @debug("creating queues")
    success, name, message_count, consumer_count = queue_declare(chan1, QUEUE1)
    @test success
    @test message_count == 0

    @test queue_bind(chan1, QUEUE1, EXCG_DIRECT, ROUTE1)

    conn, chan1
end

function teardown(conn, chan1, delete=false)
    @info("closing down")
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
    @info("starting basic publisher")
    # publish N messages
    for idx in 1:NMSGS
        basic_publish(chan1, M; exchange=EXCG_DIRECT, routing_key=ROUTE1)
        if (idx % 10000) == 0
            @info("publishing", idx)
            sleep(1)
        end
    end
end

function consume(conn, chan1)
    @info("starting basic consumer")
    # start a consumer task
    msg_count = 0
    start_time = time()
    end_time = 0
    consumer_fn = (rcvd_msg) -> begin
        msg_count += 1
        if ((msg_count % 10000) == 0) || (msg_count == NMSGS)
            #basic_ack(chan1, 0; all_upto=true)
            @info("ack sent", msg_count)
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
        @info("$msg_count of $NMSGS messages processed")
        sleep(2)
    end

    # cancel the consumer task
    @test basic_cancel(chan1, consumer_tag)

    # time to send and receive
    total_time = max(end_time - start_time, 1)
    @info("time to send and receive", message_count=NMSGS, total_time, rate=NMSGS/total_time)
end

function run_publisher()
    host = ARGS[2]
    port = parse(Int, ARGS[3])
    tls = parse(Bool, ARGS[4])
    conn, chan1 = AMQPTestThroughput.setup(; host=host, port=port, tls=tls)
    AMQPTestThroughput.publish(conn, chan1)
    AMQPTestThroughput.teardown(conn, chan1, false) # exit without destroying queue
    nothing
end

function run_consumer()
    host = ARGS[2]
    port = parse(Int, ARGS[3])
    tls = parse(Bool, ARGS[4])
    conn, chan1 = AMQPTestThroughput.setup(; host=host, port=port, tls=tls)
    AMQPTestThroughput.consume(conn, chan1)
    @debug("waiting for publisher to exit gracefully...")
    sleep(10)  # wait for publisher to exit gracefully
    AMQPTestThroughput.teardown(conn, chan1, true)
    nothing
end

function spawn_test(script, flags, host, port, tls)
    opts = Base.JLOptions()
    inline_flag = opts.can_inline == 1 ? `` : `--inline=no`
    cov_flag = (opts.code_coverage == 1) ? `--code-coverage=user` :
                 (opts.code_coverage == 2) ? `--code-coverage=all` :
                 ``
    srvrscript = joinpath(dirname(@__FILE__), script)
    srvrcmd = `$(joinpath(JULIA_HOME, "julia")) $cov_flag $inline_flag $srvrscript $flags $host $port $tls`
    @debug("Running tests from ", script, flags, host, port, tls)
    ret = run(srvrcmd)
    @debug("Finished ", script, flags, host, port, tls)
    nothing
end

function runtests(; host="localhost", port=AMQPClient.AMQP_DEFAULT_PORT, tls=false)
    @sync begin
        @info("starting consumer")
        consumer = @async spawn_test("test_throughput.jl", "--runconsumer", host, port, tls)
        sleep(10)
        @info("starting publisher")
        publisher = @async spawn_test("test_throughput.jl", "--runpublisher", host, port, tls)
    end
    nothing
end

end # module AMQPTestThroughput

!isempty(ARGS) && (ARGS[1] == "--runpublisher") && AMQPTestThroughput.run_publisher()
!isempty(ARGS) && (ARGS[1] == "--runconsumer")  && AMQPTestThroughput.run_consumer()
