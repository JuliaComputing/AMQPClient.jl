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
    TAMQPShortStr(len, read!(io, Array(UInt8, len)))
end

function read(io::IO, ::Type{TAMQPLongStr})
    len = ntoh(read(io, TAMQPLongUInt))
    TAMQPLongStr(len, read!(io, Array(UInt8, len)))
end

write{T<:Union{TAMQPShortStr,TAMQPLongStr}}(io::IO, s::T) = write(io, hton(s.len), s.data)

function read(io::IO, ::Type{TAMQPFieldValue})
    c = read(io, Char)
    v = read(io, FieldValueIndicatorMap[c])
    TAMQPFieldValue(c, v)
end

write(io::IO, fv::TAMQPFieldValue) = write(io, fv.typ, fv.fld)

read(io::IO, ::Type{TAMQPFieldValuePair}) = TAMQPFieldValuePair(read(io, TAMQPFieldName), read(io, TAMQPFieldValue))

write(io::IO, fv::TAMQPFieldValuePair) = write(io, fv.name, fv.val)

function read(io::IO, ::Type{TAMQPFieldTable})
    len = ntoh(read(io, fieldtype(TAMQPFieldTable, :len)))
    @logmsg("read fieldtable length $(len)")
    buff = read!(io, Array(UInt8, len))
    data = TAMQPFieldValuePair[]
    iob = IOBuffer(buff)
    while !eof(iob)
        push!(data, read(iob, TAMQPFieldValuePair))
    end
    TAMQPFieldTable(len, data)
end

function write(io::IO, ft::TAMQPFieldTable)
    @logmsg("write fieldtable nfields $(length(ft.data))")
    iob = IOBuffer()
    for fv in ft.data
        write(iob, fv)
    end
    buff = takebuf_array(iob)
    len = convert(fieldtype(TAMQPFieldTable, :len), length(buff))
    @logmsg("write fieldtable length $len type: $(typeof(len))")
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
    @logmsg("reading generic frame type:$hdr, channel:$(props.channel), payloadsize:$(props.payloadsize)")
    payload = read!(io, TAMQPBodyPayload(Array(TAMQPOctet, props.payloadsize)))
    fend = ntoh(read(io, fieldtype(TAMQPGenericFrame, :fend)))
    @assert fend == FrameEnd
    TAMQPGenericFrame(hdr, props, payload, fend)
end

write(io::IO, f::TAMQPGenericFrame) = write(io, hton(f.hdr), f.props, f.payload, f.fend)

"""
Given a generic frame, convert it to appropriate exact frame type.
"""
function narrow_frame(f::TAMQPGenericFrame)
    if f.hdr == FrameMethod
        return TAMQPMethodFrame(f)
    end
    throw(AMQPProtocolException("Unknown frame type $(f.hdr)"))
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

type Connection
    host::String
    port::Int
    sock::Nullable{TCPSocket}
    properties::Dict{Symbol,Any}
    capabilities::Dict{String,Any}

    function Connection(host::String="localhost", port::Int=AMQP_DEFAULT_PORT)
        new(host, port, nothing, Dict{Symbol,Any}(), Dict{String,Any}())
    end
end

sock(c::Connection) = get(c.sock)
function close(c::Connection)
    if !isnull(c.sock)
        close(get(c.sock))
        c.sock = nothing
    end
end

get_property(c::Connection, s::Symbol, default) = get(c.properties, s, default)

function recv_method_frame(c::Connection)
    f = read(sock(c), TAMQPGenericFrame)
    m = TAMQPMethodFrame(f)
    @logmsg("class:$(m.payload.class), method:$(m.payload.method)")
    m
end

send_method_frame(c::Connection, m::TAMQPMethodFrame) = write(sock(c), TAMQPGenericFrame(m))

function recv_connection_start(c::Connection)
    m = recv_method_frame(c)
    @assert is_method(m, :Connection, :Start)
    merge!(c.properties, Dict{Symbol,Any}(m.payload.fields...))
    server_props = convert(Dict{String,Any}, get_property(c, :ServerProperties, Dict{String,Any}()))
    if "capabilities" in keys(server_props)
        capabilities = Dict{String,Any}(String(n) => v.fld for (n,v) in convert(Dict{Any,Any}, server_props["capabilities"].fld))
        merge!(c.capabilities, capabilities)
    end
    m
end

function auth_resp_amqplain(auth_params::Dict{String,Any})
    params = Dict{String,Any}("LOGIN" => auth_params["LOGIN"], "PASSWORD" => auth_params["PASSWORD"])
    iob = IOBuffer()
    write(iob, convert(TAMQPFieldTable, params))
    bytes = takebuf_array(iob)
    skipbytes = sizeof(fieldtype(TAMQPFieldTable, :len))
    bytes = bytes[(skipbytes+1):end]
    convert(TAMQPLongStr, bytes)
end

function send_connection_start_ok(c::Connection, auth_params::Dict{String,Any}=Dict{String,Any}())
    # set up client_props
    client_props = copy(CLIENT_IDENTIFICATION)
    client_cap = client_props["capabilities"]
    server_cap = c.capabilities
    @logmsg("$(c.capabilities)")
    if "consumer_cancel_notify" in keys(server_cap)
        client_cap["consumer_cancel_notify"] = server_cap["consumer_cancel_notify"]
    end
    if "connection.blocked" in keys(server_cap)
        client_cap["connection.blocked"] = server_cap["connection.blocked"]
    end
    @logmsg("client_props: $(client_props)")

    # assert that auth mechanism is supported
    mechanism = auth_params["MECHANISM"]
    mechanisms = split(get_property(c, :Mechanisms, ""), ' ')
    @logmsg("mechanism: $mechanism, supported mechanisms: $(mechanisms)")
    @assert mechanism in mechanisms

    # set up locale
    client_locale = locale()
    if isempty(client_locale)
        # pick up one of the server locales
        locales = split(get_property(c, :Locales, ""), ' ')
        @logmsg("supported locales: $(locales)")
        client_locale = locales[1]
    end
    @logmsg("client_locale: $(client_locale)")

    # respond to login
    auth_resp = AUTH_PROVIDERS[mechanism](auth_params)
    @logmsg("auth_resp: $(auth_resp)")

    props = TAMQPFrameProperties(0,0)
    delete!(client_props, "capabilities")
    payload = TAMQPMethodPayload(:Connection, :StartOk, (client_props, mechanism, auth_resp, client_locale))
    @logmsg("sending connection start_ok")
    send_method_frame(c, TAMQPMethodFrame(props, payload))
end

function negotiate(c::Connection, auth_params::Dict{String,Any}=Dict{String,Any}())
    @logmsg("connecting to $(c.host):$(c.port)")
    c.sock = Nullable(connect(c.host, c.port))
    write(sock(c), ProtocolHeader)
    #try
        m = recv_connection_start(c)
        send_connection_start_ok(c, auth_params)
        m = recv_method_frame(c)
        #mechanisms = split(get_property(c, :Mechanisms, ""), ' ')
        #@logmsg("supported mechanisms: $(mechanisms)")
        m
    #catch ex
    #    println(stacktrace())
    #    @logmsg("Exception $ex")
    #    # check if server suggests a different protocol
    #    buff = Array(UInt8, 3)
    #    read!(sock(c), buff)
    #    if buff == LiteralAMQP[2:end]
    #        read(sock(c), UInt8)
    #        sver = read!(sock(c), buff)
    #        sproto = VersionNumber(sver...)
    #        cproto = VersionNumber(ProtocolVersion...)
    #        msg = "Protocol mismatch: client:$cproto, server:$sproto"
    #    else
    #        msg = "Unknown server protocol"
    #    end
    #    close(c)
    #    throw(AMQPProtocolException(msg))
    #end
end
