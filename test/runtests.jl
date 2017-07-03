include("test_coverage.jl")
include("test_throughput.jl")
include("test_rpc.jl")

AMQPTestCoverage.runtests()
AMQPTestThroughput.runtests()
AMQPTestRPC.runtests()
