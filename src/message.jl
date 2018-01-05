struct PropertySpec
    name::Symbol
    typ::Type
    mask::UInt16
end

const NON_PERSISTENT = TAMQPOctet(1)
const PERSISTENT = TAMQPOctet(2)

const PROPERTIES = Dict{Symbol, PropertySpec}(
    :content_type     => PropertySpec(:content_type,     TAMQPShortStr,   0x0001 << 15), # MIME content type (MIME typing)
    :content_encoding => PropertySpec(:content_encoding, TAMQPShortStr,   0x0001 << 14), # MIME content encoding (MIME typing)
    :headers          => PropertySpec(:headers,          TAMQPFieldTable, 0x0001 << 13), # message header field table (For applications, and for header exchange routing)
    :delivery_mode    => PropertySpec(:delivery_mode,    TAMQPOctet,      0x0001 << 12), # non-persistent (1) or persistent (2) (For queues that implement persistence)
    :priority         => PropertySpec(:priority,         TAMQPOctet,      0x0001 << 11), # message priority, 0 to 9 (For queues that implement priorities)
    :correlation_id   => PropertySpec(:correlation_id,   TAMQPShortStr,   0x0001 << 10), # application correlation identifier (For application use, no formal behaviour)
    :reply_to         => PropertySpec(:reply_to,         TAMQPShortStr,   0x0001 << 9),  # address to reply to (For application use, no formal behaviour)
    :expiration       => PropertySpec(:expiration,       TAMQPShortStr,   0x0001 << 8),  # message expiration specification (For application use, no formal behaviour)
    :message_id       => PropertySpec(:message_id,       TAMQPShortStr,   0x0001 << 7),  # application message identifier (For application use, no formal behaviour)
    :timestamp        => PropertySpec(:timestamp,        TAMQPTimeStamp,  0x0001 << 6),  # message timestamp (For application use, no formal behaviour)
    :message_type     => PropertySpec(:message_type,     TAMQPShortStr,   0x0001 << 5),  # message type name (For application use, no formal behaviour)
    :user_id          => PropertySpec(:user_id,          TAMQPShortStr,   0x0001 << 4),  # creating user id (For application use, no formal behaviour)
    :app_id           => PropertySpec(:app_id,           TAMQPShortStr,   0x0001 << 3),  # creating application id (For application use, no formal behaviour)
    :cluster_id       => PropertySpec(:cluster_id,       TAMQPShortStr,   0x0001 << 2)   # reserved, must be empty (Deprecated, was old cluster-id property)
)

const SORTED_PROPERTY_NAMES = [:content_type, :content_encoding, :headers, :delivery_mode, :priority, :correlation_id, :reply_to, :expiration, :message_id, :timestamp, :message_type, :user_id, :app_id, :cluster_id]
const SORTED_PROPERTIES = [PROPERTIES[k] for k in SORTED_PROPERTY_NAMES]

mutable struct Message
    data::Vector{UInt8}
    properties::Dict{Symbol,TAMQPField}

    filled::Int

    consumer_tag::String
    delivery_tag::TAMQPDeliveryTag
    redelivered::Bool
    exchange::String
    routing_key::String
    remaining::TAMQPMessageCount
end

function Message(data::Vector{UInt8}; kwargs...)
    msg = Message(data, Dict{Symbol,TAMQPField}(), length(data), "", TAMQPDeliveryTag(0), false, "", "", TAMQPMessageCount(0))
    set_properties(msg; kwargs...)
    msg
end

function set_properties(msg::Message; kwargs...)
    for (k,v) in kwargs
        if v === nothing
            delete!(msg.properties, k)
        else
            msg.properties[k] = convert(PROPERTIES[k].typ, v)
        end
    end
    nothing
end
