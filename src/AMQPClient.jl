module AMQPClient

import Base: write, read, read!, close, convert, show, isopen

using Sockets

const DEBUG = false

# 0.7: use builtin logging by enabling following statement
# using Logging; Logging.global_logger(Logging.ConsoleLogger(stderr, Logging.Debug))
macro debug(s)
    esc(:(DEBUG && Base.@debug($s)))
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
