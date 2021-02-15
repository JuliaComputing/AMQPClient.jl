using AMQPClient

include("test_coverage.jl")
include("test_throughput.jl")
include("test_rpc.jl")

AMQPTestCoverage.runtests()
AMQPTestThroughput.runtests()
AMQPTestRPC.runtests()

if length(ARGS) > 0
    amqps_host = ARGS[1]
    virtualhost = ARGS[2]
    port = AMQPClient.AMQPS_DEFAULT_PORT

    login = ENV["AMQPPLAIN_LOGIN"]
    password = ENV["AMQPPLAIN_PASSWORD"]
    auth_params = Dict{String,Any}("MECHANISM"=>"AMQPLAIN", "LOGIN"=>login, "PASSWORD"=>password)

    AMQPTestCoverage.runtests(; host=amqps_host, port=AMQPClient.AMQPS_DEFAULT_PORT, virtualhost=virtualhost, amqps=amqps_configure(), auth_params=auth_params)
    AMQPTestThroughput.runtests(; host=amqps_host, port=AMQPClient.AMQPS_DEFAULT_PORT, tls=true)
    AMQPTestRPC.runtests(; host=amqps_host, port=AMQPClient.AMQPS_DEFAULT_PORT, amqps=amqps_configure())
end

exit(0)
