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

export connection, channel
export exchange_declare, exchange_delete, exchange_bind, exchange_unbind
export queue_declare, queue_bind, queue_unbind, queue_purge, queue_delete
export tx_select, tx_commit, tx_rollback
export EXCHANGE_TYPE_DIRECT, EXCHANGE_TYPE_FANOUT, EXCHANGE_TYPE_TOPIC, EXCHANGE_TYPE_HEADERS
export read, read!, close, convert, show

end # module
