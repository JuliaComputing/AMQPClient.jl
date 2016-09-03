function read(io::IO, ::Type{TAMQPFrameProperties})
    TAMQPFrameProperties(
        ntoh(read(io, fieldtype(TAMQPFrameProperties, :channel))),
        ntoh(read(io, fieldtype(TAMQPFrameProperties, :payloadsize))),
    )
end

function read!(io::IO, b::TAMQPBodyPayload)
    read!(io, b.data)
    b
end

function read(io::IO, ::Type{TAMQPShortStr})
    len = ntoh(read(io, TAMQPOctet))
    TAMQPShortStr(len, read!(io, Array(UInt8, len)))
end

function read(io::IO, ::Type{TAMQPLongStr})
    len = ntoh(read(io, TAMQPLongUInt))
    TAMQPLongStr(len, read!(io, Array(UInt8, len)))
end

function read(io::IO, ::Type{TAMQPFieldValue})
    c = read(io, Char)
    v = read(io, FieldValueIndicatorMap[c])
    TAMQPFieldValue(c, v)
end

function read(io::IO, ::Type{TAMQPFieldValuePair})
    TAMQPFieldValuePair(read(io, TAMQPFieldName), read(io, TAMQPFieldValue))
end

function read(io::IO, ::Type{TAMQPFieldTable})
    len = ntoh(read(io, fieldtype(TAMQPFieldTable, :len)))
    buff = read!(io, Array(UInt8, len))
    data = TAMQPFieldValuePair[]
    iob = IOBuffer(buff)
    while !eof(iob)
        push!(data, read(iob, TAMQPFieldValuePair))
    end
    TAMQPFieldTable(len, data)
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

"""
Given a generic frame, convert it to appropriate exact frame type.
"""
function narrow_frame(f::TAMQPGenericFrame)
    if f.hdr == 1
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
    sock::TCPSocket
    properties::Dict{Symbol,Any}

    function Connection(host::String="localhost", port::Int=AMQP_DEFAULT_PORT)
        @logmsg("connecting to $host:$port")
        new(host, port, connect(host, port), Dict{Symbol,Any}())
    end
end

close(c::Connection) = close(c.sock)
get_property(c::Connection, s::Symbol, default) = get(c.properties, s, default)

function negotiate(c::Connection)
    write(c.sock, ProtocolHeader)
    #try
        f = read(c.sock, TAMQPGenericFrame)
        m = TAMQPMethodFrame(f)
        @logmsg("class:$(m.payload.class), method:$(m.payload.method)")
        @assert is_method(m, :Connection, :Start)
        merge!(c.properties, Dict{Symbol,Any}(m.payload.fields...))
        mechanisms = split(get_property(c, :Mechanisms, ""))
        @logmsg("supported mechanisms: $(mechanisms)")
        m
    #catch ex
    #    println(stacktrace())
    #    @logmsg("Exception $ex")
    #    # check if server suggests a different protocol
    #    buff = Array(UInt8, 3)
    #    read!(c.sock, buff)
    #    if buff == LiteralAMQP[2:end]
    #        read(c.sock, UInt8)
    #        sver = read!(c.sock, buff)
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
