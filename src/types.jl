const LiteralAMQP = UInt8[65, 77, 81, 80] # "AMQP"
const ProtocolId = UInt8(0)
const ProtocolVersion = UInt8[0, 9, 1]

const ProtocolHeader = vcat(LiteralAMQP, ProtocolId, ProtocolVersion)

const ContentWeight = 0x00
const FrameEnd = 0xCE
const HeartBeat = UInt8[8, 0, 0, FrameEnd]


#typealias TAMQPBit                  UInt8
typealias TAMQPBool                 UInt8 # 0 = FALSE, else TRUE
typealias TAMQPScale                UInt8 # number of decimal digits
typealias TAMQPOctet                UInt8
typealias TAMQPShortShortInt        UInt8
typealias TAMQPShortShortUInt       UInt8
typealias TAMQPShortInt             Int16
typealias TAMQPShortUInt            UInt16
typealias TAMQPLongInt              Int32
typealias TAMQPLongUInt             UInt32
typealias TAMQPLongLongInt          Int64
typealias TAMQPLongLongUInt         UInt64
typealias TAMQPFloat                Float32
typealias TAMQPDouble               Float64
typealias TAMQPTimeStamp            TAMQPLongLongUInt

immutable TAMQPBit
    val::UInt8
end

function TAMQPBit(b::TAMQPBit, pos::Int)
    TAMQPBit((b >> (pos-1)) & 0x1)
end

function TAMQPBit(b::TAMQPBit, setbit::TAMQPBit, pos::Int)
    TAMQPBit(b.val & (setbit.val << (pos-1)))
end

immutable TAMQPDecimalValue
    scale::TAMQPScale
    val::TAMQPLongUInt
end

immutable TAMQPShortStr
    len::TAMQPOctet
    data::Vector{Int8}
end

immutable TAMQPLongStr
    len::TAMQPLongUInt
    data::Vector{UInt8}
end

typealias TAMQPFieldName TAMQPShortStr

immutable TAMQPFieldValue{T}
    typ::Char  # as in FieldValueIndicatorMap
    fld::T
end

function TAMQPFieldValue(typ::Char, fld)
    T = FieldValueIndicatorMap[typ]
    TAMQPFieldValue{T}(typ, fld)
end

immutable TAMQPFieldValuePair
    name::TAMQPFieldName
    val::TAMQPFieldValue
end

immutable TAMQPFieldArray
    len::TAMQPLongInt
    data::Vector{TAMQPFieldValue}
end

immutable TAMQPFieldTable
    len::TAMQPLongUInt
    data::Vector{TAMQPFieldValuePair}
end

typealias TAMQPField Union{TAMQPBit, TAMQPOctet, TAMQPShortInt, TAMQPShortUInt, TAMQPLongInt, TAMQPLongUInt, TAMQPLongLongInt, TAMQPLongLongUInt, TAMQPShortStr, TAMQPLongStr, TAMQPTimeStamp, TAMQPFieldTable}

const FieldValueIndicatorMap = Dict{Char,Type}(
    't' => TAMQPBool,
    'b' => TAMQPShortShortInt,
    'B' => TAMQPShortShortUInt,
    'U' => TAMQPShortInt,
    'u' => TAMQPShortUInt,
    'I' => TAMQPLongInt,
    'i' => TAMQPLongUInt,
    'L' => TAMQPLongLongInt,
    'l' => TAMQPLongLongUInt,
    'f' => TAMQPFloat,
    'd' => TAMQPDouble,
    'D' => TAMQPDecimalValue,
    's' => TAMQPShortUInt,
    'S' => TAMQPLongStr,
    'x' => TAMQPLongStr,
    'A' => TAMQPFieldArray,
    'T' => TAMQPTimeStamp,
    'F' => TAMQPFieldTable,
    'V' => Void
)

const FieldIndicatorMap = Dict{Type,Char}(v=>n for (n,v) in FieldValueIndicatorMap)

typealias TAMQPChannel          TAMQPShortUInt
typealias TAMQPPayloadSize      TAMQPLongUInt
typealias TAMQPContentBodySize  TAMQPLongLongUInt
typealias TAMQPClassId          UInt16
typealias TAMQPMethodId         UInt16
typealias TAMQPContentClass     TAMQPClassId

immutable TAMQPFrameProperties
    channel::TAMQPChannel
    payloadsize::TAMQPPayloadSize
end

immutable TAMQPPropertyFlags
    flags::UInt16
    nextval::Nullable{TAMQPPropertyFlags}
end
TAMQPPropertyFlags(flags::UInt16) = TAMQPPropertyFlags(flags, nothing)

immutable TAMQPBodyPayload
    # TODO: may be better to allow sub arrays, for efficient writing of large messages
    data::Vector{TAMQPOctet}
end

immutable TAMQPMethodPayload
    class::TAMQPClassId
    method::TAMQPMethodId
    fields::Vector{Pair{Symbol,TAMQPField}}

    TAMQPMethodPayload(p::TAMQPBodyPayload) = TAMQPMethodPayload(p.data)
    TAMQPMethodPayload(b::Vector{TAMQPOctet}) = TAMQPMethodPayload(IOBuffer(b))
    function TAMQPMethodPayload(io)
        class = ntoh(read(io, TAMQPClassId))
        method = ntoh(read(io, TAMQPMethodId))
        args = methodargs(class, method)
        fields = Array(Pair{Symbol,TAMQPField}, length(args))
        @logmsg("reading method payload class:$class, method:$method, nargs:$(length(args))")
        bitpos = 0
        bitval = TAMQPBit(0)
        for idx in 1:length(fields)
            fld = args[idx]
            @logmsg("reading field $(fld.first) of type $(fld.second)")
            if fld.second === TAMQPBit
                bitpos += 1
                (bitpos == 1) && (bitval = read(io, fld.second))
                v = TAMQPBit(bitval, bitpos)
                (bitpos == 8) && (bitpos == 0)
            else
                bitpos = 0
                v = read(io, fld.second)
            end
            issubtype(fld.second, Integer) && (v = ntoh(v))
            fields[idx] = Pair{Symbol,TAMQPField}(fld.first, v)
        end
        new(class, method, fields)
    end
    function TAMQPMethodPayload(class_name::Symbol, method_name::Symbol, fldvals)
        class = CLASSNAME_MAP[class_name]
        method = CLASSMETHODNAME_MAP[(class_name,method_name)]
        fields = Pair{Symbol,TAMQPField}[]
        for idx in 1:length(method.args)
            (argname,argtype) = method.args[idx]
            push!(fields, Pair{Symbol,TAMQPField}(argname, convert(argtype, fldvals[idx])))
        end
        new(class.id, method.id, fields)
    end
end

immutable TAMQPHeaderPayload
    class::TAMQPContentClass
    weight::UInt16  # must be ContentWeight
    bodysize::TAMQPContentBodySize
    propflags::TAMQPPropertyFlags
    proplist::Dict{Symbol,TAMQPField}

    TAMQPHeaderPayload(p::TAMQPBodyPayload) = TAMQPHeaderPayload(p.data)
    TAMQPHeaderPayload(b::Vector{TAMQPOctet}) = TAMQPHeaderPayload(IOBuffer(b))
    function TAMQPHeaderPayload(io)
        class = ntoh(read(io, TAMQPClassId))
        wt = ntoh(read(io, UInt16))
        @assert wt === ContentWeight
        bodysize = ntoh(read(io, TAMQPContentBodySize))
        propflags = TAMQPPropertyFlags(ntoh(read(io, UInt16)))
    
        for prop in SORTED_PROPERTIES
            if (propflags & prop.mask) > 0x0000
                proplist[prop.name] = read(io, prop.typ)
            end
        end
        new(class, ContentWeight, bodysize, propflags, proplist)
    end
    function TAMQPHeaderPayload(class::TAMQPContentClass, message)
        bodysize = length(message.data)
        propflags = 0x0000
        for name in keys(message.properties)
            propflags = propflags & PROPERTIES[name].mask
        end
        new(class, ContentWeight, bodysize, TAMQPPropertyFlags(propflags), message.properties)
    end
end

# Generic frame, used to read any frame
immutable TAMQPGenericFrame
    hdr::UInt8
    props::TAMQPFrameProperties
    payload::TAMQPBodyPayload
    fend::UInt8 # must be FrameEnd
end

# Type = 1, "METHOD": method frame
immutable TAMQPMethodFrame
    props::TAMQPFrameProperties
    payload::TAMQPMethodPayload
end

function TAMQPMethodFrame(f::TAMQPGenericFrame)
    @logmsg("Frame Conversion: generic => method")
    @assert f.hdr == FrameMethod
    TAMQPMethodFrame(f.props, TAMQPMethodPayload(f.payload))
end

function TAMQPGenericFrame(f::TAMQPMethodFrame)
    @logmsg("Frame Conversion method => generic")
    iob = IOBuffer()
    methpayload = f.payload
    write(iob, hton(methpayload.class))
    write(iob, hton(methpayload.method))
    bitpos = 0
    bitval = TAMQPBit(0)
    for (n,v) in methpayload.fields
        if isa(v, TAMQPBit)
            bitpos += 1
            bitval = TAMQPBit(bitval, v, bitpos)
            if bitpos == 8
                write(iob, bitval)
                bitpos = 0
                bitval = TAMQPBit(0)
            end
        else
            if bitpos > 0
                write(iob, bitval)
                bitpos = 0
                bitval = TAMQPBit(0)
            end
            issubtype(typeof(v), Integer) && (v = hton(v))
            write(iob, v)
        end
    end
    if bitpos > 0
        write(iob, bitval)
    end
    bodypayload = TAMQPBodyPayload(takebuf_array(iob))
    TAMQPGenericFrame(FrameMethod, TAMQPFrameProperties(f.props.channel, length(bodypayload.data)), bodypayload, FrameEnd)
end

# Type = 2, "HEADER": content header frame.
immutable TAMQPContentHeaderFrame
    props::TAMQPFrameProperties
    hdrpayload::TAMQPHeaderPayload
end

function TAMQPContentHeaderFrame(f::TAMQPGenericFrame)
    @logmsg("Frame Conversion: generic => contentheader")
    @assert f.hdr == FrameHeader
    TAMQPContentHeaderFrame(f.props, TAMQPHeaderPayload(f.payload))
end

function TAMQPGenericFrame(f::TAMQPContentHeaderFrame)
    @logmsg("Frame Conversion contentheader => generic")
    iob = IOBuffer()
    hdrpayload = f.hdrpayload
    propflags = hdrpayload.propflags

    write(iob, hton(hdrpayload.class))
    write(iob, hton(hdrpayload.weight))
    write(iob, hton(hdrpayload.bodysize))
    write(iob, hton(propflags.flags))

    flags = propflags.flags
    for prop in SORTED_PROPERTIES
        if (flags & prop.mask) > 0x0000
            write(io, proplist[prop.name])
        end
    end
    bodypayload = TAMQPBodyPayload(takebuf_array(iob))
    TAMQPGenericFrame(FrameHeader, TAMQPFrameProperties(f.props.channel, length(bodypayload.data)), bodypayload, FrameEnd)
end

# Type = 3, "BODY": content body frame.
immutable TAMQPContentBodyFrame
    props::TAMQPFrameProperties
    payload::TAMQPBodyPayload
end

function TAMQPContentBodyFrame(f::TAMQPGenericFrame)
    @logmsg("Frame Conversion: generic => contentbody")
    @assert f.hdr == FrameBody
    TAMQPContentBodyFrame(f.props, f.payload)
end

function TAMQPGenericFrame(f::TAMQPContentBodyFrame)
    @logmsg("Frame Conversion contentbody => generic")
    TAMQPGenericFrame(FrameBody, TAMQPFrameProperties(f.props.channel, length(f.payload.data)), f.payload, FrameEnd)
end

# Type = 4, "HEARTBEAT": heartbeat frame.
immutable TAMQPHeartBeatFrame
end

function TAMQPHeartBeatFrame(f::TAMQPGenericFrame)
    @assert f.hdr == FrameHeartbeat
    TAMQPHeartBeatFrame()
end

function TAMQPGenericFrame(f::TAMQPHeartBeatFrame)
    @logmsg("Frame Conversion heartbeat => generic")
    TAMQPGenericFrame(FrameHeartbeat, TAMQPFrameProperties(DEFAULT_CHANNEL, 0), TAMQPBodyPayload(TAMQPOctet[]), FrameEnd)
end

immutable TAMQPContent
    hdr::TAMQPContentHeaderFrame
    body::Vector{TAMQPContentBodyFrame}
end

immutable TAMQPMethod
    frame::TAMQPMethodFrame
    content::Nullable{TAMQPContent}
end

# Exceptions
type AMQPProtocolException <: Exception
    msg::String
end

type AMQPClientException <: Exception
    msg::String
end

# Spec code gen types
immutable MethodSpec
    id::Int
    name::Symbol
    respname::Symbol
    args::Vector{Pair{Symbol,DataType}}
end

immutable ClassSpec
    id::Int
    name::Symbol 
    method_map::Dict{Int,MethodSpec}
end

immutable CloseReason
    code::Int16
    msg::TAMQPShortStr
    classid::TAMQPClassId
    methodid::TAMQPMethodId
end

# Utility Methods for Types
method(classid::TAMQPClassId, methodid::TAMQPMethodId) = CLASS_MAP[classid].method_map[methodid]
methodargs(classid::TAMQPClassId, methodid::TAMQPMethodId) = method(classid, methodid).args
function displayname(classid::TAMQPClassId, methodid::TAMQPMethodId)
    c = CLASS_MAP[classid]
    m = c.method_map[methodid]
    "$(c.name).$(m.name)"
end
