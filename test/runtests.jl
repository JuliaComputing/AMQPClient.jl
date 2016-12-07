include("test_coverage.jl")
include("test_throughput.jl")

AMPQTestCoverage.runtests()
AMPQTestThroughput.runtests()
