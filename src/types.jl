const LiteralAMQP = UInt8[65, 77, 81, 80] # "AMQP"
const ProtocolId = UInt8(0)
const ProtocolVersion = UInt8[0, 9, 1]

const ProtocolHeader = vcat(LiteralAMQP, ProtocolId, ProtocolVersion)

const ContentWeight = 0x00
const FrameEnd = 0xCE
const HeartBeat = UInt8[8, 0, 0, FrameEnd]


typealias TAMQPBit                  UInt8
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

typealias TAMQPContentClass     TAMQPOctet
typealias TAMQPChannel          TAMQPShortUInt
typealias TAMQPPayloadSize      TAMQPLongUInt
typealias TAMQPContentBodySize  TAMQPLongLongUInt
typealias TAMQPClassId          UInt16
typealias TAMQPMethodId         UInt16

immutable TAMQPFrameProperties
    channel::TAMQPChannel
    payloadsize::TAMQPPayloadSize
end

immutable TAMQPPropertyFlags
    hdr::UInt16
    nextval::Nullable{TAMQPPropertyFlags}
end

immutable TAMQPBodyPayload
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
        for idx in 1:length(fields)
            fld = args[idx]
            @logmsg("reading field $(fld.first) of type $(fld.second)")
            v = read(io, fld.second)
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
    weight::UInt8   # must be ContentWeight
    bodysize::TAMQPContentBodySize
    propflags::TAMQPPropertyFlags
    proplist::Vector{TAMQPField}
end

# Generic frame, used to read any frame
immutable TAMQPGenericFrame
    hdr::UInt8 # must be 3
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
    @assert f.hdr == 1
    TAMQPMethodFrame(f.props, TAMQPMethodPayload(f.payload))
end

function TAMQPGenericFrame(f::TAMQPMethodFrame)
    @logmsg("Frame Conversion method => generic")
    iob = IOBuffer()
    methpayload = f.payload
    write(iob, hton(methpayload.class))
    write(iob, hton(methpayload.method))
    for (n,v) in methpayload.fields
        issubtype(typeof(v), Integer) && (v = hton(v))
        write(iob, v)
    end
    bodypayload = TAMQPBodyPayload(takebuf_array(iob))
    TAMQPGenericFrame(FrameMethod, TAMQPFrameProperties(f.props.channel, length(bodypayload.data)), bodypayload, FrameEnd)
end

# Type = 2, "HEADER": content header frame.
immutable TAMQPContentHeaderFrame
    props::TAMQPFrameProperties
    hdrpayload::TAMQPHeaderPayload

    function TAMQPContentHeaderFrame(f::TAMQPGenericFrame)
        @assert f.hdr == 2
        new(f.props, TAMQPHeaderPayload(f.payload))
    end
end

# Type = 3, "BODY": content body frame.
immutable TAMQPContentBodyFrame
    props::TAMQPFrameProperties
    payload::TAMQPBodyPayload

    function TAMQPContentBodyFrame(f::TAMQPGenericFrame)
        @assert f.hdr == 3
        new(f.props, f.payload)
    end
end

# Type = 4, "HEARTBEAT": heartbeat frame.
immutable TAMQPHeartBeatFrame
    function TAMQPHeartBeatFrame(f::TAMQPGenericFrame)
        @assert f.hdr == 8
        new()
    end
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

# Utility Methods for Types
method(classid::TAMQPClassId, methodid::TAMQPMethodId) = CLASS_MAP[classid].method_map[methodid]
methodargs(classid::TAMQPClassId, methodid::TAMQPMethodId) = method(classid, methodid).args
function displayname(classid::TAMQPClassId, methodid::TAMQPMethodId)
    c = CLASS_MAP[classid]
    m = c.method_map[methodid]
    "$(c.name).$(m.name)"
end
