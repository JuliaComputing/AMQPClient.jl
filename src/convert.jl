convert{T<:Union{TAMQPShortStr,TAMQPLongStr}}(::Type{Any}, s::T) = convert(String, s)
convert{T<:Union{TAMQPShortStr,TAMQPLongStr}}(::Type{String}, s::T) = String(convert(Array{UInt8,1}, s.data))
convert{T<:Union{TAMQPShortStr,TAMQPLongStr}}(::Type{T}, s::AbstractString) = T(length(s), String(s).data)
convert(::Type{TAMQPLongStr}, d::Vector{UInt8}) = TAMQPLongStr(length(d), d)

convert{T}(::Type{TAMQPFieldValue{T}}, v::T) = TAMQPFieldValue{T}(FieldIndicatorMap[T], v)

as_fval{T}(v::T) = convert(TAMQPFieldValue{T}, v)
as_fval(v::Dict{String,Any}) = convert(TAMQPFieldValue{TAMQPFieldTable}, convert(TAMQPFieldTable, v))
as_fval(v::String) = convert(TAMQPFieldValue{TAMQPLongStr}, convert(TAMQPLongStr, v))

convert(::Type{Any}, t::TAMQPFieldTable) = convert(Dict{Any,Any}, t)
convert{K,V}(::Type{Dict{K,V}}, t::TAMQPFieldTable) = Dict{K,V}(f.name => f.val for f in t.data)
function convert(::Type{TAMQPFieldTable}, d::Dict{String,Any})
    #@logmsg("converting to TAMQPFieldTable")
    data = TAMQPFieldValuePair[]
    for (n,v) in d
        push!(data, TAMQPFieldValuePair(n, as_fval(v)))
    end
    TAMQPFieldTable(length(data), data)
end

convert(::Type{Any}, t::TAMQPFieldArray) = convert(Vector, t)
convert{T}(::Type{Vector{T}}, t::TAMQPFieldArray) = convert(Vector{T}, t.data)

convert(::Type{Bool}, b::TAMQPBit) = Bool(b.val & 0x1)
convert(::Type{TAMQPBit}, b::Bool) = TAMQPBit(convert(UInt8, b))
convert{T<:Integer}(::Type{TAMQPBit}, b::T) = convert(TAMQPBit, Bool(b))
