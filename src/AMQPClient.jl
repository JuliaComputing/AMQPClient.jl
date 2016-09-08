module AMQPClient

using Compat
using Base.I18n
import Base: write, read, read!, close, convert, show, isopen

# enable logging only during debugging
using Logging
#const logger = Logging.configure(filename="elly.log", level=DEBUG)
const logger = Logging.configure(level=DEBUG)
macro logmsg(s)
    quote
        debug("[", myid(), "-", "] ", $(esc(s)))
    end
end
#macro logmsg(s)
#end

# Client property info that gets sent to the server on connection startup
const CLIENT_IDENTIFICATION = Dict{String,Any}(
    "product" => "Julia AMQPClient",
    "product_version" => string(VERSION),
    "capabilities" => Dict{String,Any}()
)

include("types.jl")
include("spec.jl")
include("auth.jl")
include("protocol.jl")
include("convert.jl")
include("show.jl")

export channel
export read, read!, close, convert, show

end # module
