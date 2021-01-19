using AMQPClient

include("test_coverage.jl")
include("test_throughput.jl")
include("test_rpc.jl")

AMQPTestCoverage.runtests()
AMQPTestThroughput.runtests()
AMQPTestRPC.runtests()

if length(ARGS) > 0
    amqps_host = ARGS[1]
    AMQPTestCoverage.runtests(; host=amqps_host, port=AMQPClient.AMQPS_DEFAULT_PORT, amqps=amqps_configure())
    AMQPTestThroughput.runtests(; host=amqps_host, port=AMQPClient.AMQPS_DEFAULT_PORT, tls=true)
    AMQPTestRPC.runtests(; host=amqps_host, port=AMQPClient.AMQPS_DEFAULT_PORT, amqps=amqps_configure())
end

exit(0)
