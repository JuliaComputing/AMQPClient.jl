# default client timeout to use with blocking methods after which they throw an error
# Julia Timer converts seconds to milliseconds and adds 1 to it before passing it to libuv, hence the magic numbers to prevent overflow
const DEFAULT_TIMEOUT = round(Int, typemax(Int)/1000) - 1
const DEFAULT_CONNECT_TIMEOUT = round(Int, typemax(Int)/1000) - 1

# ----------------------------------------
# IO for types begin
# ----------------------------------------
function read(io::IO, ::Type{TAMQPBit})
    TAMQPBit(ntoh(read(io, UInt8)))
end

function write(io::IO, b::TAMQPBit)
    write(io, hton(b.val))
end

function read(io::IO, ::Type{TAMQPFrameProperties})
    TAMQPFrameProperties(
        ntoh(read(io, fieldtype(TAMQPFrameProperties, :channel))),
        ntoh(read(io, fieldtype(TAMQPFrameProperties, :payloadsize))),
    )
end

write(io::IO, p::TAMQPFrameProperties) = write(io, hton(p.channel), hton(p.payloadsize))

function read!(io::IO, b::TAMQPBodyPayload)
    read!(io, b.data)
    b
end

write(io::IO, b::TAMQPBodyPayload) = write(io, b.data)

function read(io::IO, ::Type{TAMQPShortStr})
    len = ntoh(read(io, TAMQPOctet))
    TAMQPShortStr(len, read!(io, Vector{UInt8}(undef, len)))
end

function read(io::IO, ::Type{TAMQPLongStr})
    len = ntoh(read(io, TAMQPLongUInt))
    TAMQPLongStr(len, read!(io, Vector{UInt8}(undef, len)))
end

function read(io::IO, ::Type{TAMQPByteArray})
    len = ntoh(read(io, TAMQPLongUInt))
    TAMQPByteArray(len, read!(io, Vector{UInt8}(undef, len)))
end

write(io::IO, s::T) where {T<:Union{TAMQPShortStr,TAMQPLongStr,TAMQPByteArray}} = write(io, hton(s.len), s.data)

function read(io::IO, ::Type{TAMQPFieldValue})
    c = read(io, Char)
    v = read(io, FieldValueIndicatorMap[c])
    T = FieldValueIndicatorMap[c]
    TAMQPFieldValue{T}(c, v)
end

write(io::IO, fv::TAMQPFieldValue) = write(io, fv.typ, fv.fld)

read(io::IO, ::Type{TAMQPFieldValuePair}) = TAMQPFieldValuePair(read(io, TAMQPFieldName), read(io, TAMQPFieldValue))

write(io::IO, fv::TAMQPFieldValuePair) = write(io, fv.name, fv.val)

function read(io::IO, ::Type{TAMQPFieldTable})
    len = ntoh(read(io, fieldtype(TAMQPFieldTable, :len)))
    @debug("read fieldtable length $(len)")
    buff = read!(io, Vector{UInt8}(undef, len))
    data = TAMQPFieldValuePair[]
    iob = IOBuffer(buff)
    while !eof(iob)
        push!(data, read(iob, TAMQPFieldValuePair))
    end
    TAMQPFieldTable(len, data)
end

function write(io::IO, ft::TAMQPFieldTable)
    @debug("write fieldtable nfields $(length(ft.data))")
    iob = IOBuffer()
    for fv in ft.data
        write(iob, fv)
    end
    buff = take!(iob)
    len = convert(fieldtype(TAMQPFieldTable, :len), length(buff))
    @debug("write fieldtable length $len type: $(typeof(len))")
    l = write(io, hton(len))
    if len > 0
        l += write(io, buff)
    end
    l
end

"""
Read a generic frame. All frames have the following wire format:

0      1         3      7                  size+7     size+8
+------+---------+---------+ +-------------+ +-----------+
| type | channel | size    | |    payload  | | frame-end |
+------+---------+---------+ +-------------+ +-----------+
octet    short     long       'size' octets      octet
"""
function read(io::IO, ::Type{TAMQPGenericFrame})
    hdr = ntoh(read(io, fieldtype(TAMQPGenericFrame, :hdr)))
    @assert hdr in (1,2,3,8)
    props = read(io, fieldtype(TAMQPGenericFrame, :props))
    @debug("reading generic frame type:$hdr, channel:$(props.channel), payloadsize:$(props.payloadsize)")
    payload = read!(io, TAMQPBodyPayload(Vector{TAMQPOctet}(undef, props.payloadsize)))
    fend = ntoh(read(io, fieldtype(TAMQPGenericFrame, :fend)))
    @assert fend == FrameEnd
    TAMQPGenericFrame(hdr, props, payload, fend)
end

write(io::IO, f::TAMQPGenericFrame) = write(io, hton(f.hdr), f.props, f.payload, f.fend)

# """
# Given a generic frame, convert it to appropriate exact frame type.
# """
#function narrow_frame(f::TAMQPGenericFrame)
#    if f.hdr == FrameMethod
#        return TAMQPMethodFrame(f)
#    end
#    throw(AMQPProtocolException("Unknown frame type $(f.hdr)"))
#end

function method_name(payload::TAMQPMethodPayload)
    c = CLASS_MAP[payload.class]
    m = c.method_map[payload.method]
    #(c.name, m.name)
    string(c.name) * "." * string(m.name)
end

"""
Validate if the method frame is for the given class and method.
"""
function is_method(m::TAMQPMethodFrame, class::Symbol, method::Symbol)
    c = CLASS_MAP[m.payload.class]
    if c.name === class
        m = c.method_map[m.payload.method]
        return m.name === method
    end
    false
end

function method_key(classname::Symbol, methodname::Symbol)
    class = CLASSNAME_MAP[classname]
    method = CLASSMETHODNAME_MAP[classname,methodname]
    (FrameMethod, class.id, method.id)
end
frame_key(frame_type) = (UInt8(frame_type),)

# ----------------------------------------
# IO for types end
# ----------------------------------------

# ----------------------------------------
# Connection and Channel begin
# ----------------------------------------

const UNUSED_CHANNEL = -1
const DEFAULT_CHANNEL = 0
const DEFAULT_CHANNELMAX = 256
const DEFAULT_AUTH_PARAMS = Dict{String,Any}("MECHANISM"=>"AMQPLAIN", "LOGIN"=>"guest", "PASSWORD"=>"guest")

const CONN_STATE_CLOSED = 0
const CONN_STATE_OPENING = 1
const CONN_STATE_OPEN = 2
const CONN_STATE_CLOSING = 3
const CONN_MAX_QUEUED = 1024 #typemax(Int)

abstract type AbstractChannel end

mutable struct Connection
    virtualhost::String
    host::String
    port::Int
    sock::Union{TCPSocket, Nothing}

    properties::Dict{Symbol,Any}
    capabilities::Dict{String,Any}
    channelmax::TAMQPShortInt
    framemax::TAMQPLongInt
    heartbeat::TAMQPShortInt

    state::UInt8
    sendq::Channel{TAMQPGenericFrame}
    sendlck::Channel{UInt8}
    channels::Dict{TAMQPChannel, AbstractChannel}

    sender::Union{Task, Nothing}
    receiver::Union{Task, Nothing}
    heartbeater::Union{Task, Nothing}

    heartbeat_time_server::Float64
    heartbeat_time_client::Float64

    function Connection(virtualhost::String="/", host::String="localhost", port::Int=AMQP_DEFAULT_PORT)
        sendq = Channel{TAMQPGenericFrame}(CONN_MAX_QUEUED)
        sendlck = Channel{UInt8}(1)
        put!(sendlck, 1)
        new(virtualhost, host, port, nothing,
            Dict{Symbol,Any}(), Dict{String,Any}(), 0, 0, 0,
            CONN_STATE_CLOSED, sendq, sendlck, Dict{TAMQPChannel, AbstractChannel}(),
            nothing, nothing, nothing,
            0.0, 0.0)
    end
end

mutable struct MessageConsumer
    chan_id::TAMQPChannel
    consumer_tag::String
    recvq::Channel{Message}
    callback::Function
    receiver::Task

    function MessageConsumer(chan_id::TAMQPChannel, consumer_tag::String, callback::Function; buffer_size::Int=typemax(Int))
        c = new(chan_id, consumer_tag, Channel{Message}(buffer_size), callback)
        c.receiver = @async connection_processor(c, "Consumer $consumer_tag", channel_message_consumer)
        c
    end
end

close(consumer::MessageConsumer) = close(consumer.recvq)

mutable struct MessageChannel <: AbstractChannel
    id::TAMQPChannel
    conn::Connection
    state::UInt8
    flow::Bool

    recvq::Channel{TAMQPGenericFrame}
    receiver::Union{Task, Nothing}
    callbacks::Dict{Tuple,Tuple{Function,Any}}

    partial_msgs::Vector{Message} # holds partial messages while they are getting read (message bodies arrive in sequence)
    chan_get::Channel{Union{Message, Nothing}}  # channel used for received messages, in sync get call (TODO: maybe type more strongly?)
    consumers::Dict{String,MessageConsumer}

    closereason::Union{CloseReason, Nothing}

    function MessageChannel(id, conn)
        new(id, conn, CONN_STATE_CLOSED, true,
            Channel{TAMQPGenericFrame}(CONN_MAX_QUEUED), nothing, Dict{Tuple,Tuple{Function,Any}}(),
            Message[], Channel{Union{Message, Nothing}}(1), Dict{String,MessageConsumer}(),
            nothing)
    end
end

sock(c::MessageChannel) = sock(c.conn)
sock(c::Connection) = c.sock

isopen(c::Connection) = c.sock !== nothing && isopen(c.sock)
isopen(c::MessageChannel) = isopen(c.conn) && (c.id in keys(c.conn.channels))

get_property(c::MessageChannel, s::Symbol, default) = get_property(c.conn, s, default)
get_property(c::Connection, s::Symbol, default) = get(c.properties, s, default)

send(c::MessageChannel, f, msgframes::Vector=[]) = send(c.conn, f, msgframes)
function send(c::Connection, f, msgframes::Vector=[])
    #uncomment to enable synchronization (not required till we have preemptive tasks or threads)
    lck = take!(c.sendlck)
    try
        put!(c.sendq, TAMQPGenericFrame(f))
        for m in msgframes
            put!(c.sendq, TAMQPGenericFrame(m))
        end
    finally
        put!(c.sendlck, lck)
    end
    nothing
end
function send(c::MessageChannel, payload::TAMQPMethodPayload, msg::Union{Message, Nothing}=nothing)
    logstrmsg = msg === nothing ? "without" : "with"
    @debug("sending $(method_name(payload)) $logstrmsg content")
    frameprop = TAMQPFrameProperties(c.id,0)
    if msg !== nothing
        msgframes = []
        message = msg

        # send message header frame
        hdrpayload = TAMQPHeaderPayload(payload.class, message)
        push!(msgframes, TAMQPContentHeaderFrame(frameprop, hdrpayload))

        # send one or more message body frames
        offset = 1
        msglen = length(message.data)
        while offset <= msglen
            msgend = min(msglen, offset + c.conn.framemax - 1)
            bodypayload = TAMQPBodyPayload(message.data[offset:msgend])
            offset = msgend + 1
            push!(msgframes, TAMQPContentBodyFrame(frameprop, bodypayload))
        end

        send(c, TAMQPMethodFrame(frameprop, payload), msgframes)
    else
        send(c, TAMQPMethodFrame(frameprop, payload))
    end
end

# ----------------------------------------
# Async message handler framework begin
# ----------------------------------------
function wait_for_state(c, states; interval=1, timeout=typemax(Int))
    t1 = time()
    while !(c.state in states)
        ((time() - t1) > timeout) && (return false)
        sleep(interval)
    end
    true
end

function connection_processor(c, name, fn)
    @debug("Starting $name task")
    try
        while true
            fn(c)
        end
    catch err
        reason = "$name task exiting."
        if isa(c, MessageConsumer)
            close(c)
            reason = reason * " Unhandled exception: $err"
            #showerror(STDERR, err)
            @debug(reason)
        else
            isconnclosed = !isopen(c)
            ischanclosed = isa(c, MessageChannel) && isa(err, InvalidStateException) && err.state == :closed
            if ischanclosed || isconnclosed
                reason = reason * " Connection closed"
                if c.state !== CONN_STATE_CLOSING
                    reason = reason * " by peer"
                    close(c, false, true)
                end
                @debug(reason)
            else
                reason = reason * " Unhandled exception: $err"
                #showerror(STDERR, err)
                @debug(reason)
                close(c, false, true)
                #rethrow(err)
            end
        end
    end
end

function connection_sender(c::Connection)
    msg = take!(c.sendq)
    @debug("==> sending on conn $(c.virtualhost)")
    nbytes = write(sock(c), msg)
    @debug("==> sent $nbytes bytes")

    # update heartbeat time for client
    c.heartbeat_time_client = time()

    nothing
end

function connection_receiver(c::Connection)
    f = read(sock(c), TAMQPGenericFrame)

    # update heartbeat time for server
    c.heartbeat_time_server = time()

    channelid = f.props.channel
    @debug("<== read message on conn $(c.virtualhost) for chan $channelid")
    if !(channelid in keys(c.channels))
        @debug("Discarding message for unknown channel $channelid")
    end
    chan = channel(c, channelid)
    put!(chan.recvq, f)
    nothing
end

function connection_heartbeater(c::Connection)
    sleep(c.heartbeat)

    isopen(c) || throw(AMQPClientException("Connection closed"))

    now = time()
    if (now - c.heartbeat_time_client) > c.heartbeat
        send_connection_heartbeat(c)
    end

    if (now - c.heartbeat_time_server) > (2 * c.heartbeat)
        @debug("server heartbeat missed for $(now - c.heartbeat_time_server) seconds")
        close(c, false, false)
    end
    nothing
end

function channel_receiver(c::MessageChannel)
    f = take!(c.recvq)
    if f.hdr == FrameMethod
        m = TAMQPMethodFrame(f)
        @debug("<== channel: $(f.props.channel), class:$(m.payload.class), method:$(m.payload.method)")
        cbkey = (f.hdr, m.payload.class, m.payload.method)
    elseif f.hdr == FrameHeartbeat
        m = TAMQPHeartBeatFrame(f)
        @debug("<== channel: $(f.props.channel), heartbeat")
        cbkey = (f.hdr,)
    elseif f.hdr == FrameHeader
        m = TAMQPContentHeaderFrame(f)
        @debug("<== channel: $(f.props.channel), contentheader")
        cbkey = (f.hdr,)
    elseif f.hdr == FrameBody
        m = TAMQPContentBodyFrame(f)
        @debug("<== channel: $(f.props.channel), contentbody")
        cbkey = (f.hdr,)
    else
        m = f
        @debug("<== channel: $(f.props.channel), unhandled frame type $(f.hdr)")
        cbkey = (f.hdr,)
    end
    (cb,ctx) = get(c.callbacks, cbkey, (on_unexpected_message, nothing))
    @assert f.props.channel == c.id
    cb(c, m, ctx)
    nothing
end

function channel_message_consumer(c::MessageConsumer)
    m = take!(c.recvq)
    c.callback(m)
    nothing
end

clear_handlers(c::MessageChannel) = (empty!(c.callbacks); nothing)
function handle(c::MessageChannel, classname::Symbol, methodname::Symbol, cb=nothing, ctx=nothing)
    cbkey = method_key(classname, methodname)
    if cb == nothing
        delete!(c.callbacks, cbkey)
    else
        c.callbacks[cbkey] = (cb, ctx)
    end
    nothing
end
function handle(c::MessageChannel, frame_type::Integer, cb=nothing, ctx=nothing)
    cbkey = frame_key(frame_type)
    if cb == nothing
        delete!(c.callbacks, cbkey)
    else
        c.callbacks[cbkey] = (cb, ctx)
    end
    nothing
end

# ----------------------------------------
# Async message handler framework end
# ----------------------------------------

# ----------------------------------------
# Open channel / connection begin
# ----------------------------------------

function find_unused_channel(c::Connection)
    k = keys(c.channels)
    maxid = c.channelmax
    for id in 0:maxid
        if !(id in k)
            return id
        end
    end
    throw(AMQPClientException("No free channel available (max: $maxid)"))
end
channel(c::MessageChannel, id::Integer) = channel(c.conn, id)
channel(c::Connection, id::Integer) = c.channels[id]
channel(c::MessageChannel, id::Integer, create::Bool) = channel(c.conn, id, create)
function channel(c::Connection, id::Integer, create::Bool; connect_timeout=DEFAULT_CONNECT_TIMEOUT)
    if create
        if id == UNUSED_CHANNEL
            id = find_unused_channel(c)
        elseif id in keys(c.channels)
            throw(AMQPClientException("Channel Id $id is already in use"))
        end
        chan = MessageChannel(id, c)
        chan.state = CONN_STATE_OPENING
        c.channels[chan.id] = chan

        if id != DEFAULT_CHANNEL
            # open the channel
            chan.receiver = @async connection_processor(chan, "ChannelReceiver($(chan.id))", channel_receiver)
            handle(chan, :Channel, :OpenOk, on_channel_open_ok)
            send_channel_open(chan)

            if !wait_for_state(chan, CONN_STATE_OPEN; timeout=connect_timeout)
                throw(AMQPClientException("Channel handshake failed"))
            end
        end
    else
        chan = channel(c, id)
    end
    chan
end

function connection(;virtualhost="/", host="localhost", port=AMQPClient.AMQP_DEFAULT_PORT, auth_params=AMQPClient.DEFAULT_AUTH_PARAMS, channelmax=AMQPClient.DEFAULT_CHANNELMAX, framemax=0, heartbeat=0, connect_timeout=AMQPClient.DEFAULT_CONNECT_TIMEOUT)
    @debug "connecting to $(host):$(port)$(virtualhost)"
    conn = AMQPClient.Connection(virtualhost, host, port)
    chan = channel(conn, AMQPClient.DEFAULT_CHANNEL, true)

    # setup handler for Connection.Start
    ctx = Dict(:auth_params=>auth_params, :channelmax=>channelmax, :framemax=>framemax, :heartbeat=>heartbeat)
    AMQPClient.handle(chan, :Connection, :Start, AMQPClient.on_connection_start, ctx)

    # open socket and start processor tasks
    conn.sock = connect(conn.host, conn.port)
    conn.sender = @async   AMQPClient.connection_processor(conn, "ConnectionSender", AMQPClient.connection_sender)
    conn.receiver = @async AMQPClient.connection_processor(conn, "ConnectionReceiver", AMQPClient.connection_receiver)
    chan.receiver = @async AMQPClient.connection_processor(chan, "ChannelReceiver($(chan.id))", AMQPClient.channel_receiver)

    # initiate handshake
    conn.state = chan.state = AMQPClient.CONN_STATE_OPENING
    write(AMQPClient.sock(chan), AMQPClient.ProtocolHeader)

    if !AMQPClient.wait_for_state(conn, AMQPClient.CONN_STATE_OPEN; timeout=connect_timeout) || !AMQPClient.wait_for_state(chan, AMQPClient.CONN_STATE_OPEN; timeout=connect_timeout)
        throw(AMQPClientException("Connection handshake failed"))
    end
    chan
end


# ----------------------------------------
# Open channel / connection end
# ----------------------------------------

# ----------------------------------------
# Close channel / connection begin
# ----------------------------------------

function close(chan::MessageChannel, handshake::Bool=true, by_peer::Bool=false, reply_code=ReplySuccess, reply_text="", class_id=0, method_id=0)
    (chan.state == CONN_STATE_CLOSED) && (return nothing)

    conn = chan.conn

    if chan.id == DEFAULT_CHANNEL
        # default channel represents the connection
        close(conn, handshake, by_peer, reply_code, reply_text, class_id, method_id)
    elseif chan.state != CONN_STATE_CLOSING
        # send handshake if needed and when called the first time
        chan.state = CONN_STATE_CLOSING
        if handshake && !by_peer
            send_channel_close(chan, reply_code, reply_text, class_id, method_id)
        end
    end

    # release resources when closed by peer or when closing abruptly
    if !handshake || by_peer
        close(chan.recvq)
        close(chan.chan_get)
        map(close, values(chan.consumers))
        empty!(chan.consumers)
        chan.receiver = nothing
        chan.callbacks = Dict{Tuple,Tuple{Function,Any}}()
        delete!(chan.conn.channels, chan.id)
        chan.state = CONN_STATE_CLOSED
    end
    nothing
end

function close(conn::Connection, handshake::Bool=true, by_peer::Bool=false, reply_code=ReplySuccess, reply_text="", class_id=0, method_id=0)
    (conn.state == CONN_STATE_CLOSED) && (return nothing)

    # send handshake if needed and when called the first time
    if conn.state != CONN_STATE_CLOSING
        conn.state = CONN_STATE_CLOSING

        # close all other open channels
        for open_channel in collect(values(conn.channels))
            if open_channel.id != DEFAULT_CHANNEL
                close(open_channel, false, by_peer)
            end
        end

        # send handshake if needed
        if handshake && !by_peer
            send_connection_close(conn, reply_code, reply_text, class_id, method_id)
        end
    end

    if !handshake || by_peer
        # close socket
        close(conn.sock)
        conn.sock = nothing

        # reset all members
        conn.properties = Dict{Symbol,Any}()
        conn.capabilities = Dict{String,Any}()
        conn.channelmax = 0
        conn.framemax = 0
        conn.heartbeat = 0

        # close and reset the sendq channel
        close(conn.sendq)
        conn.sendq = Channel{TAMQPGenericFrame}(CONN_MAX_QUEUED)

        # reset the tasks
        conn.sender = nothing
        conn.receiver = nothing
        conn.heartbeater = nothing
        conn.state = CONN_STATE_CLOSED
    end
    nothing
end

# ----------------------------------------
# Close channel / connection end
# ----------------------------------------

# ----------------------------------------
# Connection and Channel end
# ----------------------------------------

# ----------------------------------------
# Exchange begin
# ----------------------------------------
const EXCHANGE_TYPE_DIRECT = "direct"      # must be implemented by servers
const EXCHANGE_TYPE_FANOUT = "fanout"      # must be implemented by servers
const EXCHANGE_TYPE_TOPIC = "topic"        # optional, must test before typing to open
const EXCHANGE_TYPE_HEADERS = "headers"    # optional, must test before typing to open

# The server MUST, in each virtual host, pre­declare an exchange instance for each standard 
# exchange type that it implements, where the name of the exchange instance, if defined, is "amq." 
# followed by the exchange type name.
# The server MUST pre­declare a direct exchange with no public name to act as the default 
# exchange for content Publish methods and for default queue bindings.
default_exchange_name(excg_type) = ("amq." * excg_type)
default_exchange_name() = ""

function _wait_resp(sendmethod, chan::MessageChannel, default_result::T, 
        nowait::Bool=true, resp_handler=nothing, resp_class=nothing, resp_meth=nothing,
        timeout_result::T=default_result, timeout::Int=DEFAULT_TIMEOUT) where {T}
    result = default_result
    if !nowait
        reply = Channel{T}(1)
        # timer to time the request out, in case of an error
        t = Timer(timeout) do t
            try
                put!(reply, timeout_result)
            catch
            end
        end
        # register a callback
        handle(chan, resp_class, resp_meth, resp_handler, reply)
    end

    sendmethod()

    if !nowait
        # wait for response
        result = take!(reply)
        close(reply)
    end
    result
end

function exchange_declare(chan::MessageChannel, name::String, typ::String;
        passive::Bool=false, durable::Bool=false, auto_delete::Bool=false,
        nowait::Bool=false, timeout::Int=DEFAULT_TIMEOUT,
        arguments::Dict{String,Any}=Dict{String,Any}())
    (isempty(name) || startswith(name, "amq.")) && !passive && throw(AMQPClientException("Exchange name '$name' is reserved. Use a different name."))
    if auto_delete
        @debug("Warning: auto_delete exchange types are deprecated")
    end

    _wait_resp(chan, true, nowait, on_exchange_declare_ok, :Exchange, :DeclareOk, false, timeout) do
        send_exchange_declare(chan, name, typ, passive, durable, auto_delete, nowait, arguments)
    end
end

function exchange_delete(chan::MessageChannel, name::String; if_unused::Bool=false, nowait::Bool=false, timeout::Int=DEFAULT_TIMEOUT)
    (isempty(name) || startswith(name, "amq.")) && throw(AMQPClientException("Exchange name '$name' is reserved. Use a different name."))
    _wait_resp(chan, true, nowait, on_exchange_delete_ok, :Exchange, :DeleteOk, false, timeout) do
        send_exchange_delete(chan, name, if_unused, nowait)
    end
end

function exchange_bind(chan::MessageChannel, dest::String, src::String, routing_key::String;
        nowait::Bool=false, timeout::Int=DEFAULT_TIMEOUT,
        arguments::Dict{String,Any}=Dict{String,Any}())
    _wait_resp(chan, true, nowait, on_exchange_bind_ok, :Exchange, :BindOk, false, timeout) do
        send_exchange_bind(chan, dest, src, routing_key, nowait, arguments)
    end
end

function exchange_unbind(chan::MessageChannel, dest::String, src::String, routing_key::String;
        nowait::Bool=false, timeout::Int=DEFAULT_TIMEOUT,
        arguments::Dict{String,Any}=Dict{String,Any}())
    _wait_resp(chan, true, nowait, on_exchange_unbind_ok, :Exchange, :UnbindOk, false, timeout) do
        send_exchange_unbind(chan, dest, src, routing_key, nowait, arguments)
    end
end

# ----------------------------------------
# Exchange end
# ----------------------------------------

# ----------------------------------------
# Queue begin
# ----------------------------------------
"""Declare a queue (or query an existing queue).
Returns a tuple: (boolean success/failure, queue name, message count, consumer count)
"""
function queue_declare(chan::MessageChannel, name::String;
        passive::Bool=false, durable::Bool=false, exclusive::Bool=false, auto_delete::Bool=false,
        nowait::Bool=false, timeout::Int=DEFAULT_TIMEOUT,
        arguments::Dict{String,Any}=Dict{String,Any}())
    _wait_resp(chan, (true, TAMQPMessageCount(0), Int32(0)), nowait, on_queue_declare_ok, :Queue, :DeclareOk, (false, TAMQPMessageCount(0), Int32(0)), timeout) do
        send_queue_declare(chan, name, passive, durable, exclusive, auto_delete, nowait, arguments)
    end
end

function queue_bind(chan::MessageChannel, queue_name::String, excg_name::String, routing_key::String; nowait::Bool=false, timeout::Int=DEFAULT_TIMEOUT, arguments::Dict{String,Any}=Dict{String,Any}())
    _wait_resp(chan, true, nowait, on_queue_bind_ok, :Queue, :BindOk, false, timeout) do
        send_queue_bind(chan, queue_name, excg_name, routing_key, nowait, arguments)
    end
end

function queue_unbind(chan::MessageChannel, queue_name::String, excg_name::String, routing_key::String; arguments::Dict{String,Any}=Dict{String,Any}(), timeout::Int=DEFAULT_TIMEOUT)
    nowait = false
    _wait_resp(chan, true, nowait, on_queue_unbind_ok, :Queue, :UnbindOk, false, timeout) do
        send_queue_unbind(chan, queue_name, excg_name, routing_key, arguments)
    end
end

"""Purge messages from a queue.
Returns a tuple: (boolean success/failure, message count)
"""
function queue_purge(chan::MessageChannel, name::String; nowait::Bool=false, timeout::Int=DEFAULT_TIMEOUT)
    _wait_resp(chan, (true,TAMQPMessageCount(0)), nowait, on_queue_purge_ok, :Queue, :PurgeOk, (false,TAMQPMessageCount(0)), timeout) do
        send_queue_purge(chan, name, nowait)
    end
end

"""Delete a queue.
Returns a tuple: (boolean success/failure, message count)
"""
function queue_delete(chan::MessageChannel, name::String; if_unused::Bool=false, if_empty::Bool=false, nowait::Bool=false, timeout::Int=DEFAULT_TIMEOUT)
    _wait_resp(chan, (true,TAMQPMessageCount(0)), nowait, on_queue_delete_ok, :Queue, :DeleteOk, (false,TAMQPMessageCount(0)), timeout) do
        send_queue_delete(chan, name, if_unused, if_empty, nowait)
    end
end

# ----------------------------------------
# Queue end
# ----------------------------------------

# ----------------------------------------
# Tx begin
# ----------------------------------------

function _tx(sendmethod, chan::MessageChannel, respmethod::Symbol, on_resp, timeout::Int)
    nowait = false
    _wait_resp(chan, true, nowait, on_resp, :Tx, respmethod, false, timeout) do
        sendmethod(chan)
    end
end

tx_select(chan::MessageChannel; timeout::Int=DEFAULT_TIMEOUT) = _tx(send_tx_select, chan, :SelectOk, on_tx_select_ok, timeout)
tx_commit(chan::MessageChannel; timeout::Int=DEFAULT_TIMEOUT) = _tx(send_tx_commit, chan, :CommitOk, on_tx_commit_ok, timeout)
tx_rollback(chan::MessageChannel; timeout::Int=DEFAULT_TIMEOUT) = _tx(send_tx_rollback, chan, :RollbackOk, on_tx_rollback_ok, timeout)

# ----------------------------------------
# Tx end
# ----------------------------------------

# ----------------------------------------
# Basic begin
# ----------------------------------------

function basic_qos(chan::MessageChannel, prefetch_size, prefetch_count, apply_global::Bool; timeout::Int=DEFAULT_TIMEOUT)
    nowait = false
    _wait_resp(chan, true, nowait, on_basic_qos_ok, :Basic, :QosOk, false, timeout) do
        send_basic_qos(chan, prefetch_size, prefetch_count, apply_global)
    end
end

"""Start a queue consumer.

queue: queue name
consumer_tag: id of the consumer, server generates a unique tag if this is empty
no_local: do not deliver own messages
no_ack: no acknowledgment needed, server automatically and silently acknowledges delivery (speed at the cost of reliability)
exclusive: request exclusive access (only this consumer can access the queue)
nowait: do not send a reply method
"""
function basic_consume(chan::MessageChannel, queue::String, consumer_fn::Function; consumer_tag::String="", no_local::Bool=false, no_ack::Bool=false,
    exclusive::Bool=false, nowait::Bool=false, arguments::Dict{String,Any}=Dict{String,Any}(), timeout::Int=DEFAULT_TIMEOUT, buffer_sz::Int=typemax(Int))
    result = _wait_resp(chan, (true, ""), nowait, on_basic_consume_ok, :Basic, :ConsumeOk, (false, ""), timeout) do
        send_basic_consume(chan, queue, consumer_tag, no_local, no_ack, exclusive, nowait, arguments)
    end

    # setup a message consumer
    if result[1]
        consumer_tag = result[2]
        chan.consumers[consumer_tag] = MessageConsumer(chan.id, consumer_tag, consumer_fn; buffer_size=buffer_sz)
    end
    result
end

"""Cancels a consumer.

This does not affect already delivered messages, but it does mean the server will not send any more messages for that consumer. The client may receive an arbitrary number of 
messages in between sending the cancel method and receiving the cancel­ok reply. 
"""
function basic_cancel(chan::MessageChannel, consumer_tag::String; nowait::Bool=false, timeout::Int=DEFAULT_TIMEOUT)
    result = _wait_resp(chan, (true, ""), nowait, on_basic_cancel_ok, :Basic, :CancelOk, (false, ""), timeout) do
        send_basic_cancel(chan, consumer_tag, nowait)
    end

    # clear a message consumer
    if result[1]
        if consumer_tag in keys(chan.consumers)
            close(chan.consumers[consumer_tag])
            delete!(chan.consumers, consumer_tag)
        end
    end
    result[1]
end

"""Publish a message

This method publishes a message to a specific exchange. The message will be routed to queues as defined by the exchange
configuration and distributed to any active consumers when the transaction, if any, is committed.
"""
function basic_publish(chan::MessageChannel, msg::Message; exchange::String="", routing_key::String="", mandatory::Bool=false, immediate::Bool=false)
    send_basic_publish(chan, msg, exchange, routing_key, mandatory, immediate)
end

const GET_EMPTY_RESP = nothing
function basic_get(chan::MessageChannel, queue::String, no_ack::Bool)
    send_basic_get(chan, queue, no_ack)
    take!(chan.chan_get)
end

basic_ack(chan::MessageChannel, delivery_tag::TAMQPDeliveryTag; all_upto::Bool=false) = send_basic_ack(chan, delivery_tag, all_upto)
basic_reject(chan::MessageChannel, delivery_tag::TAMQPDeliveryTag; requeue::Bool=false) = send_basic_reject(chan, delivery_tag, requeue)

function basic_recover(chan::MessageChannel, requeue::Bool=false; async::Bool=false, timeout::Int=DEFAULT_TIMEOUT)
    _wait_resp(chan, true, async, on_basic_recover_ok, :Basic, :RecoverOk, false, timeout) do
        send_basic_recover(chan, requeue, async)
    end
end


# ----------------------------------------
# Basic end
# ----------------------------------------

# ----------------------------------------
# Confirm begin
# ----------------------------------------

function confirm_select(chan::MessageChannel; nowait::Bool=false, timeout::Int=DEFAULT_TIMEOUT)
    _wait_resp(chan, true, nowait, on_confirm_select_ok, :Confirm, :SelectOk, false, timeout) do
        send_confirm_select(chan, nowait)
    end
end

# ----------------------------------------
# Confirm end
# ----------------------------------------

# ----------------------------------------
# send and recv for methods begin
# ----------------------------------------

function on_unexpected_message(c::MessageChannel, m::TAMQPMethodFrame, ctx)
    @debug("Unexpected message on channel $(c.id): class:$(m.payload.class), method:$(m.payload.method)")
    nothing
end

function on_unexpected_message(c::MessageChannel, f, ctx)
    @debug("Unexpected message on channel $(c.id): frame type: $(f.hdr)")
    nothing
end

function _on_ack(chan::MessageChannel, m::TAMQPMethodFrame, class::Symbol, method::Symbol, ctx)
    @assert is_method(m, class, method)
    if ctx !== nothing
        put!(ctx, true)
    end
    handle(chan, class, method)
    nothing
end

_send_close_ok(context_class::Symbol, chan::MessageChannel) = send(chan, TAMQPMethodPayload(context_class, :CloseOk, ()))

function _on_close_ok(context_class::Symbol, chan::MessageChannel, m::TAMQPMethodFrame, ctx)
    @assert is_method(m, context_class, :CloseOk)
    close(chan, false, true)
    nothing
end

function _send_close(context_class::Symbol, chan::MessageChannel, reply_code=ReplySuccess, reply_text="", class_id=0, method_id=0)
    chan.closereason = CloseReason(TAMQPReplyCode(reply_code), convert(TAMQPReplyText, reply_text), TAMQPClassId(class_id), TAMQPMethodId(method_id))
    if context_class === :Channel && chan.id == DEFAULT_CHANNEL
        @debug("closing channel 0 is equivalent to closing the connection!")
        context_class = :Connection
    end
    context_chan_id = context_class === :Connection ? 0 : chan.id

    _send_close(context_class, context_chan_id, chan.conn, reply_code, reply_text, class_id, method_id, chan.id)
end

_send_close(context_class::Symbol, context_chan_id, conn::Connection, reply_code=ReplySuccess, reply_text="", class_id=0, method_id=0, chan_id=0) =
    send(conn, TAMQPMethodFrame(TAMQPFrameProperties(context_chan_id,0), TAMQPMethodPayload(context_class, :Close, (TAMQPReplyCode(reply_code), convert(TAMQPReplyText, reply_text), TAMQPClassId(class_id), TAMQPMethodId(method_id)))))

send_connection_close_ok(chan::MessageChannel) = _send_close_ok(:Connection, chan)
on_connection_close_ok(chan::MessageChannel, m::TAMQPMethodFrame, ctx) = _on_close_ok(:Connection, chan, m, ctx)
function on_connection_close(chan::MessageChannel, m::TAMQPMethodFrame, ctx)
    @assert is_method(m, :Connection, :Close)
    @assert chan.id == DEFAULT_CHANNEL
    chan.closereason = CloseReason(m.payload.fields[1].second, m.payload.fields[2].second, m.payload.fields[3].second, m.payload.fields[4].second)
    send_connection_close_ok(chan)
    t1 = time()
    while isready(chan.conn.sendq) && ((time() - t1) < 5)
        yield() # wait 5 seconds (arbirtary) for the message to get sent
    end
    close(chan, false, true)
end
function on_channel_close(chan::MessageChannel, m::TAMQPMethodFrame, ctx)
    @assert is_method(m, :Channel, :Close)
    @assert chan.id != DEFAULT_CHANNEL
    chan.closereason = CloseReason(m.payload.fields[1].second, m.payload.fields[2].second, m.payload.fields[3].second, m.payload.fields[4].second)
    send_channel_close_ok(chan)
    close(chan, false, true)
end

send_connection_close(chan::MessageChannel, reply_code=ReplySuccess, reply_text="", class_id=0, method_id=0) = _send_close(:Connection, chan, reply_code, reply_text, class_id, method_id)
send_connection_close(conn::Connection, reply_code=ReplySuccess, reply_text="", class_id=0, method_id=0) = _send_close(:Connection, 0, conn, reply_code, reply_text, class_id, method_id)

send_channel_close_ok(chan::MessageChannel) = _send_close_ok(:Channel, chan)
on_channel_close_ok(chan::MessageChannel, m::TAMQPMethodFrame, ctx) = _on_close_ok(:Channel, chan, m, ctx)
send_channel_close(chan::MessageChannel, reply_code=ReplySuccess, reply_text="", class_id=0, method_id=0) = _send_close(:Channel, chan, reply_code, reply_text, class_id, method_id)

function on_connection_start(chan::MessageChannel, m::TAMQPMethodFrame, ctx)
    @assert is_method(m, :Connection, :Start)
    @assert chan.id == DEFAULT_CHANNEL
    conn = chan.conn

    # setup server properties and capabilities
    merge!(conn.properties, Dict{Symbol,Any}(m.payload.fields...))
    server_props = convert(Dict{String,Any}, get_property(chan, :ServerProperties, Dict{String,Any}()))
    if "capabilities" in keys(server_props)
        for f in server_props["capabilities"].fld.data
            conn.capabilities[convert(String, f.name)] = f.val.fld
        end
    end

    handle(chan, :Connection, :Start)
    auth_params = ctx[:auth_params]
    delete!(ctx, :auth_params)  # we cont need auth params any more
    handle(chan, :Connection, :Tune, on_connection_tune, ctx)
    send_connection_start_ok(chan, auth_params)
    nothing
end

function send_connection_start_ok(chan::MessageChannel, auth_params::Dict{String,Any})
    conn = chan.conn

    # set up client_props
    client_props = copy(CLIENT_IDENTIFICATION)
    client_cap = client_props["capabilities"]
    server_cap = conn.capabilities
    @debug("server capabilities: $server_cap")
    if "consumer_cancel_notify" in keys(server_cap)
        client_cap["consumer_cancel_notify"] = server_cap["consumer_cancel_notify"]
    end
    if "connection.blocked" in keys(server_cap)
        client_cap["connection.blocked"] = server_cap["connection.blocked"]
    end
    @debug("client_props: $(client_props)")

    # assert that auth mechanism is supported
    mechanism = auth_params["MECHANISM"]
    mechanisms = split(get_property(chan, :Mechanisms, ""), ' ')
    @debug("mechanism: $mechanism, supported mechanisms: $(mechanisms)")
    @assert mechanism in mechanisms

    # set up locale
    # pick up one of the server locales
    locales = split(get_property(chan, :Locales, ""), ' ')
    @debug("supported locales: $(locales)")
    client_locale = locales[1]
    @debug("client_locale: $(client_locale)")

    # respond to login
    auth_resp = AUTH_PROVIDERS[mechanism](auth_params)
    @debug("auth_resp: $(auth_resp)")

    send(chan, TAMQPMethodPayload(:Connection, :StartOk, (client_props, mechanism, auth_resp, client_locale)))
    nothing
end

function on_connection_tune(chan::MessageChannel, m::TAMQPMethodFrame, ctx)
    @assert is_method(m, :Connection, :Tune)
    @assert chan.id == DEFAULT_CHANNEL
    conn = chan.conn

    conn.channelmax = m.payload.fields[1].second
    conn.framemax = m.payload.fields[2].second
    conn.heartbeat = m.payload.fields[3].second
    handle(chan, FrameHeartbeat, on_connection_heartbeat)
    send_connection_tune_ok(chan, ctx[:channelmax], ctx[:framemax], ctx[:heartbeat])
    handle(chan, :Connection, :Tune)
    handle(chan, :Connection, :OpenOk, on_connection_open_ok, ctx)
    send_connection_open(chan)
    nothing
end

function send_connection_tune_ok(chan::MessageChannel, channelmax=0, framemax=0, heartbeat=0)
    conn = chan.conn

    # set channelmax and framemax
    (channelmax > 0) && (conn.channelmax = channelmax)
    (framemax > 0) && (conn.framemax = framemax)

    # negotiate heartbeat (min of what expected by both parties)
    if heartbeat > 0 && conn.heartbeat > 0
        conn.heartbeat = min(conn.heartbeat, heartbeat)
    else
        conn.heartbeat = max(conn.heartbeat, heartbeat)
    end

    @debug("channelmax: $(conn.channelmax), framemax: $(conn.framemax), heartbeat: $(conn.heartbeat)")
    send(chan, TAMQPMethodPayload(:Connection, :TuneOk, (conn.channelmax, conn.framemax, conn.heartbeat)))

    # start heartbeat timer
    conn.heartbeater = @async connection_processor(conn, "HeartBeater", connection_heartbeater)
    nothing
end

send_connection_open(chan::MessageChannel) = send(chan, TAMQPMethodPayload(:Connection, :Open, (chan.conn.virtualhost, "", false)))

function on_connection_open_ok(chan::MessageChannel, m::TAMQPMethodFrame, ctx)
    @assert is_method(m, :Connection, :OpenOk)
    @assert chan.id == DEFAULT_CHANNEL
    conn = chan.conn

    conn.state = CONN_STATE_OPEN
    chan.state = CONN_STATE_OPEN

    handle(chan, :Connection, :Close, on_connection_close, ctx)
    handle(chan, :Connection, :CloseOk, on_connection_close_ok, ctx)
    handle(chan, :Connection, :OpenOk)

    nothing
end

send_connection_heartbeat(conn::Connection) = send(conn, TAMQPHeartBeatFrame())
on_connection_heartbeat(chan::MessageChannel, h::TAMQPHeartBeatFrame, ctx) = nothing

send_channel_open(chan::MessageChannel) = send(chan, TAMQPMethodPayload(:Channel, :Open, ("",)))
send_channel_flow(chan::MessageChannel, flow::Bool) = send(chan, TAMQPMethodPayload(:Channel, :Flow, (flow,)))

function on_channel_open_ok(chan::MessageChannel, m::TAMQPMethodFrame, ctx)
    chan.state = CONN_STATE_OPEN
    handle(chan, :Channel, :Flow,       on_channel_flow, :Flow)
    handle(chan, :Channel, :FlowOk,     on_channel_flow, :FlowOk)
    handle(chan, :Channel, :Close,      on_channel_close)
    handle(chan, :Channel, :CloseOk,    on_channel_close_ok)
    handle(chan, :Basic,   :GetOk,      on_basic_get_empty_or_ok)
    handle(chan, :Basic,   :GetEmpty,   on_basic_get_empty_or_ok)
    handle(chan, :Basic,   :Deliver,    on_basic_get_empty_or_ok)
    handle(chan, FrameHeader, on_channel_message_in)
    handle(chan, FrameBody, on_channel_message_in)
    nothing
end

function on_channel_flow(chan::MessageChannel, m::TAMQPMethodFrame, ctx)
    @assert is_method(m, :Channel, ctx)
    chan.flow = m.payload.fields[1].second
    @debug("channel $(chan.id) flow is now $(chan.flow)")
    nothing
end

send_exchange_declare(chan::MessageChannel, name::String, typ::String, passive::Bool, durable::Bool, auto_delete::Bool, nowait::Bool, arguments::Dict{String,Any}) =
    send(chan, TAMQPMethodPayload(:Exchange, :Declare, (0, name, typ, passive, durable, auto_delete, false, nowait, arguments)))
send_exchange_delete(chan::MessageChannel, name::String, if_unused::Bool, nowait::Bool) = send(chan, TAMQPMethodPayload(:Exchange, :Delete, (0, name, if_unused, nowait)))
_send_exchange_bind_unbind(chan::MessageChannel, meth::Symbol, dest::String, src::String, routing_key::String, nowait::Bool, arguments::Dict{String,Any}) =
    send(chan, TAMQPMethodPayload(:Exchange, meth, (0, dest, src, routing_key, nowait, arguments)))
send_exchange_bind(chan::MessageChannel, dest::String, src::String, routing_key::String, nowait::Bool, arguments::Dict{String,Any}) = _send_exchange_bind_unbind(chan, :Bind, dest, src, routing_key, nowait, arguments)
send_exchange_unbind(chan::MessageChannel, dest::String, src::String, routing_key::String, nowait::Bool, arguments::Dict{String,Any}) = _send_exchange_bind_unbind(chan, :Unbind, dest, src, routing_key, nowait, arguments)

on_exchange_declare_ok(chan::MessageChannel, m::TAMQPMethodFrame, ctx) = _on_ack(chan, m, :Exchange, :DeclareOk, ctx)
on_exchange_delete_ok(chan::MessageChannel, m::TAMQPMethodFrame, ctx) = _on_ack(chan, m, :Exchange, :DeleteOk, ctx)
on_exchange_bind_ok(chan::MessageChannel, m::TAMQPMethodFrame, ctx) = _on_ack(chan, m, :Exchange, :BindOk, ctx)
on_exchange_unbind_ok(chan::MessageChannel, m::TAMQPMethodFrame, ctx) = _on_ack(chan, m, :Exchange, :UnbindOk, ctx)

send_queue_declare(chan::MessageChannel, name::String, passive::Bool, durable::Bool, exclusive::Bool, auto_delete::Bool, nowait::Bool, arguments::Dict{String,Any}) =
    send(chan, TAMQPMethodPayload(:Queue, :Declare, (0, name, passive, durable, exclusive, auto_delete, nowait, arguments)))
send_queue_bind(chan::MessageChannel, queue_name::String, excg_name::String, routing_key::String, nowait::Bool, arguments::Dict{String,Any}) =
    send(chan, TAMQPMethodPayload(:Queue, :Bind, (0, queue_name, excg_name, routing_key, nowait, arguments)))
send_queue_unbind(chan::MessageChannel, queue_name::String, excg_name::String, routing_key::String, arguments::Dict{String,Any}) = send(chan, TAMQPMethodPayload(:Queue, :Unbind, (0, queue_name, excg_name, routing_key, arguments)))
send_queue_purge(chan::MessageChannel, name::String, nowait::Bool) = send(chan, TAMQPMethodPayload(:Queue, :Purge, (0, name, nowait)))
send_queue_delete(chan::MessageChannel, name::String, if_unused::Bool, if_empty::Bool, nowait::Bool) = send(chan, TAMQPMethodPayload(:Queue, :Delete, (0, name, if_unused, if_empty, nowait)))

function on_queue_declare_ok(chan::MessageChannel, m::TAMQPMethodFrame, ctx)
    @assert is_method(m, :Queue, :DeclareOk)
    if ctx !== nothing
        name = convert(String, m.payload.fields[1].second)
        msg_count = m.payload.fields[2].second
        consumer_count = m.payload.fields[3].second
        put!(ctx, (true, msg_count, consumer_count))
    end
    handle(chan, :Queue, :DeclareOk)
    nothing
end

function _on_queue_purge_delete_ok(method::Symbol, chan::MessageChannel, m::TAMQPMethodFrame, ctx)
    @assert is_method(m, :Queue, method)
    if ctx !== nothing
        msg_count = m.payload.fields[1].second
        put!(ctx, (true, msg_count))
    end
    handle(chan, :Queue, method)
    nothing
end

on_queue_purge_ok(chan::MessageChannel, m::TAMQPMethodFrame, ctx) = _on_queue_purge_delete_ok(:PurgeOk, chan, m, ctx)
on_queue_delete_ok(chan::MessageChannel, m::TAMQPMethodFrame, ctx) = _on_queue_purge_delete_ok(:DeleteOk, chan, m, ctx)
on_queue_bind_ok(chan::MessageChannel, m::TAMQPMethodFrame, ctx) = _on_ack(chan, m, :Queue, :BindOk, ctx)
on_queue_unbind_ok(chan::MessageChannel, m::TAMQPMethodFrame, ctx) = _on_ack(chan, m, :Queue, :UnbindOk, ctx)

_send_tx(chan::MessageChannel, method::Symbol) = send(chan, TAMQPMethodPayload(:Tx, method, ()))
send_tx_select(chan::MessageChannel) = _send_tx(chan, :Select)
send_tx_commit(chan::MessageChannel) = _send_tx(chan, :Commit)
send_tx_rollback(chan::MessageChannel) = _send_tx(chan, :Rollback)

on_tx_select_ok(chan::MessageChannel, m::TAMQPMethodFrame, ctx) = _on_ack(chan, m, :Tx, :SelectOk, ctx)
on_tx_commit_ok(chan::MessageChannel, m::TAMQPMethodFrame, ctx) = _on_ack(chan, m, :Tx, :CommitOk, ctx)
on_tx_rollback_ok(chan::MessageChannel, m::TAMQPMethodFrame, ctx) = _on_ack(chan, m, :Tx, :RollbackOk, ctx)

send_basic_qos(chan::MessageChannel, prefetch_size, prefetch_count, apply_global::Bool) = send(chan, TAMQPMethodPayload(:Basic, :Qos, (prefetch_size, prefetch_count, apply_global)))

send_basic_consume(chan::MessageChannel, queue::String, consumer_tag::String, no_local::Bool, no_ack::Bool, exclusive::Bool, nowait::Bool, arguments::Dict{String,Any}) =
    send(chan, TAMQPMethodPayload(:Basic, :Consume, (0, queue, consumer_tag, no_local, no_ack, exclusive, nowait, arguments)))

send_basic_cancel(chan::MessageChannel, consumer_tag::String, nowait::Bool) = send(chan, TAMQPMethodPayload(:Basic, :Cancel, (consumer_tag, nowait)))
send_basic_publish(chan::MessageChannel, msg::Message, exchange::String, routing_key::String, mandatory::Bool=false, immediate::Bool=false) =
    send(chan, TAMQPMethodPayload(:Basic, :Publish, (0, exchange, routing_key, mandatory, immediate)), msg)
send_basic_get(chan::MessageChannel, queue::String, no_ack::Bool) = send(chan, TAMQPMethodPayload(:Basic, :Get, (0, queue, no_ack)))
send_basic_ack(chan::MessageChannel, delivery_tag::TAMQPDeliveryTag, all_upto::Bool) = send(chan, TAMQPMethodPayload(:Basic, :Ack, (delivery_tag, all_upto)))
send_basic_reject(chan::MessageChannel, delivery_tag::TAMQPDeliveryTag, requeue::Bool) = send(chan, TAMQPMethodPayload(:Basic, :Reject, (delivery_tag, requeue)))
send_basic_recover(chan::MessageChannel, requeue::Bool, async::Bool) = send(chan, TAMQPMethodPayload(:Basic, async ? :RecoverAsync : :Recover, (requeue,)))

on_basic_qos_ok(chan::MessageChannel, m::TAMQPMethodFrame, ctx) = _on_ack(chan, m, :Basic, :QosOk, ctx)
function _on_basic_consume_cancel_ok(method::Symbol, chan::MessageChannel, m::TAMQPMethodFrame, ctx)
    @assert is_method(m, :Basic, method)
    if ctx !== nothing
        consumer_tag = convert(String, m.payload.fields[1].second)
        put!(ctx, (true, consumer_tag))
    end
    handle(chan, :Basic, method)
    nothing
end
on_basic_consume_ok(chan::MessageChannel, m::TAMQPMethodFrame, ctx) = _on_basic_consume_cancel_ok(:ConsumeOk, chan, m, ctx)
on_basic_cancel_ok(chan::MessageChannel, m::TAMQPMethodFrame, ctx) = _on_basic_consume_cancel_ok(:CancelOk, chan, m, ctx)
on_basic_recover_ok(chan::MessageChannel, m::TAMQPMethodFrame, ctx) = _on_ack(chan, m, :Basic, :RecoverOk, ctx)

function on_basic_get_empty_or_ok(chan::MessageChannel, m::TAMQPMethodFrame, ctx)
    if is_method(m, :Basic, :GetEmpty)
        put!(chan.chan_get, GET_EMPTY_RESP)
    else
        msg = Message(UInt8[])
        if is_method(m, :Basic, :Deliver)
            msg.consumer_tag = m.payload.fields[1].second
            msg.delivery_tag = m.payload.fields[2].second
            msg.redelivered = convert(Bool, m.payload.fields[3].second)
            msg.exchange = convert(String, m.payload.fields[4].second)
            msg.routing_key = convert(String, m.payload.fields[5].second)
        else
            msg = Message(UInt8[])
            msg.delivery_tag = m.payload.fields[1].second
            msg.redelivered = convert(Bool, m.payload.fields[2].second)
            msg.exchange = convert(String, m.payload.fields[3].second)
            msg.routing_key = convert(String, m.payload.fields[4].second)
            msg.remaining = m.payload.fields[5].second
        end

        # wait for message header and body
        push!(chan.partial_msgs, msg)
    end
    nothing
end

function on_channel_message_in(chan::MessageChannel, m::TAMQPContentHeaderFrame, ctx)
    msg = chan.partial_msgs[1]
    msg.properties = m.hdrpayload.proplist
    msg.data = Vector{UInt8}(undef, m.hdrpayload.bodysize)
    msg.filled = 0
    nothing
end

function on_channel_message_in(chan::MessageChannel, m::TAMQPContentBodyFrame, ctx)
    msg = chan.partial_msgs[1]
    data = m.payload.data
    startpos = msg.filled + 1
    endpos = min(length(msg.data), msg.filled + length(data))
    msg.data[startpos:endpos] = data
    msg.filled = endpos

    if msg.filled >= length(msg.data)
        # got all data for msg
        if isempty(msg.consumer_tag)
            put!(chan.chan_get, popfirst!(chan.partial_msgs))
        elseif msg.consumer_tag in keys(chan.consumers)
            put!(chan.consumers[msg.consumer_tag].recvq, popfirst!(chan.partial_msgs))
        else
            @debug("discarding message, no consumer with tag $(msg.consumer_tag)")
        end
    end

    nothing
end

on_confirm_select_ok(chan::MessageChannel, m::TAMQPMethodFrame, ctx) = _on_ack(chan, m, :Confirm, :SelectOk, ctx)

# ----------------------------------------
# send and recv for methods end
# ----------------------------------------
