using AMQPClient
using Test

include("test_coverage.jl")
include("test_throughput.jl")
include("test_rpc.jl")

@testset "AMQPClient" begin
    @testset "AMQP" begin
        @testset "Functionality" begin
            for keepalive in [true, false]
                for heartbeat in (true, false)
                    @testset "keepalive=$keepalive,heartbeat=$heartbeat" begin
                        AMQPTestCoverage.runtests(; keepalive=keepalive, heartbeat=heartbeat)
                    end
                end
            end
        end
        @testset "Throughput" begin
            AMQPTestThroughput.runtests()
        end
        @testset "RPC" begin
            AMQPTestRPC.runtests()
        end
    end

    if length(ARGS) > 0
        @testset "AMQPS" begin
            amqps_host = ARGS[1]
            virtualhost = ARGS[2]
            port = AMQPClient.AMQPS_DEFAULT_PORT

            login = ENV["AMQPPLAIN_LOGIN"]
            password = ENV["AMQPPLAIN_PASSWORD"]
            auth_params = Dict{String,Any}("MECHANISM"=>"AMQPLAIN", "LOGIN"=>login, "PASSWORD"=>password)

            @testset "Functionality" begin
                for keepalive in [true, false]
                    for heartbeat in (true, false)
                        @testset "keepalive=$keepalive,heartbeat=$heartbeat" begin
                            AMQPTestCoverage.runtests(;
                                host=amqps_host,
                                port=AMQPClient.AMQPS_DEFAULT_PORT,
                                virtualhost=virtualhost,
                                amqps=amqps_configure(),
                                auth_params=auth_params,
                                keepalive=keepalive,
                                heartbeat=heartbeat)
                        end
                    end
                end
            end
            @testset "Throughput" begin
                AMQPTestThroughput.runtests(; host=amqps_host, port=AMQPClient.AMQPS_DEFAULT_PORT, tls=true)
            end
            @testset "RPC" begin
                AMQPTestRPC.runtests(; host=amqps_host, port=AMQPClient.AMQPS_DEFAULT_PORT, amqps=amqps_configure())
            end
        end
    end
end

exit(0)
