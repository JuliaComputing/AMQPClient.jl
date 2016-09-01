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
    data::Vector{UInt8}
end

immutable TAMQPLongStr
    len::TAMQPLongUInt
    data::Vector{UInt8}
end

typealias TAMQPFieldName TAMQPShortStr

immutable TAMQPFieldValue
    typ::Char  # as in FieldValueIndicatorMap
    fld::Any
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

typealias TAMQPField Union{TAMQPBit, TAMQPOctet, TAMQPShortUInt, TAMQPLongUInt, TAMQPLongLongUInt, TAMQPShortStr, TAMQPLongStr, TAMQPTimeStamp, TAMQPFieldTable}

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
    's' => TAMQPShortStr,
    'S' => TAMQPLongStr,
    'A' => TAMQPFieldArray,
    'T' => TAMQPTimeStamp,
    'F' => TAMQPFieldTable,
    'V' => Void
)

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

immutable TAMQPMethodPayload
    class::TAMQPClassId
    method::TAMQPMethodId
    fields::Vector{TAMQPField}
end

immutable TAMQPMethodFrame
    hdr::UInt8   # must be 1
    props::TAMQPFrameProperties
    payload::TAMQPMethodPayload
    fend::UInt8  # must be FrameEnd
end

immutable TAMQPPropertyFlags
    hdr::UInt16
    nextval::Nullable{TAMQPPropertyFlags}
end

immutable TAMQPHeaderPayload
    class::TAMQPContentClass
    weight::UInt8   # must be ContentWeight
    bodysize::TAMQPContentBodySize
    propflags::TAMQPPropertyFlags
    proplist::Vector{TAMQPField}
end

immutable TAMQPBodyPayload
    data::Vector{TAMQPOctet}
end

immutable TAMQPContentBody
    hdr::UInt8 # must be 3
    props::TAMQPFrameProperties
    payload::TAMQPBodyPayload
    fend::UInt8 # must be FrameEnd
end

immutable TAMQPContent
    hdr::UInt8  # must be 2
    props::TAMQPFrameProperties
    hdrpayload::TAMQPHeaderPayload
    fend::UInt8 # must be FrameEnd
    body::Vector{TAMQPContentBody}
end

immutable TAMQPMethod
    frame::TAMQPMethodFrame
    content::Nullable{TAMQPContent}
end
