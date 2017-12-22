__precompile__(true)
module AMQPClient

using Compat
using Base.I18n
import Base: write, read, read!, close, convert, show, isopen

# enable logging only during debugging
#using Logging
#function __init__()
#    #Logging.configure(filename="AMQPClient.log", level=DEBUG)
#    Logging.configure(level=DEBUG)
#end
#macro logmsg(s)
#    quote
#        debug("[", myid(), "-", "] ", $(esc(s)))
#    end
#end
macro logmsg(s)
end

# Client property info that gets sent to the server on connection startup
const CLIENT_IDENTIFICATION = Dict{String,Any}(
    "product" => "Julia AMQPClient",
    "product_version" => string(VERSION),
    "capabilities" => Dict{String,Any}()
)

include("types.jl")
include("spec.jl")
include("message.jl")
include("auth.jl")
include("protocol.jl")
include("convert.jl")
include("show.jl")

export connection, channel, CloseReason
export exchange_declare, exchange_delete, exchange_bind, exchange_unbind, default_exchange_name
export queue_declare, queue_bind, queue_unbind, queue_purge, queue_delete
export tx_select, tx_commit, tx_rollback
export basic_qos, basic_consume, basic_cancel, basic_publish, basic_get, basic_ack, basic_reject, basic_recover
export confirm_select
export EXCHANGE_TYPE_DIRECT, EXCHANGE_TYPE_FANOUT, EXCHANGE_TYPE_TOPIC, EXCHANGE_TYPE_HEADERS
export read, read!, close, convert, show
export Message, set_properties, PERSISTENT, NON_PERSISTENT

end # module
