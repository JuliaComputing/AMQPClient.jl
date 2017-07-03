include("test_coverage.jl")
include("test_throughput.jl")
include("test_rpc.jl")

AMPQTestCoverage.runtests()
AMPQTestThroughput.runtests()
AMQPTestRPC.runtests()
