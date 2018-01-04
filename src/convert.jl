convert(::Type{Any}, s::T)            where {T<:Union{TAMQPShortStr,TAMQPLongStr}} = convert(String, s)
convert(::Type{String}, s::T)         where {T<:Union{TAMQPShortStr,TAMQPLongStr}} = String(convert(Vector{UInt8}, s.data))
convert(::Type{T}, s::AbstractString) where {T<:Union{TAMQPShortStr,TAMQPLongStr}} = T(length(s), Vector{UInt8}(s))
convert(::Type{TAMQPLongStr}, d::Vector{UInt8}) = TAMQPLongStr(length(d), d)

convert(::Type{TAMQPFieldValue{T}}, v::T) where {T} = TAMQPFieldValue{T}(FieldIndicatorMap[T], v)

as_fval(v::T) where {T} = convert(TAMQPFieldValue{T}, v)
as_fval(v::Dict{String,Any}) = convert(TAMQPFieldValue{TAMQPFieldTable}, convert(TAMQPFieldTable, v))
as_fval(v::String) = convert(TAMQPFieldValue{TAMQPLongStr}, convert(TAMQPLongStr, v))

convert(::Type{Any}, t::TAMQPFieldTable) = convert(Dict{Any,Any}, t)
convert(::Type{Dict{K,V}}, t::TAMQPFieldTable) where {K, V} = Dict{K,V}(f.name => f.val for f in t.data)
function convert(::Type{TAMQPFieldTable}, d::Dict{String,Any})
    data = TAMQPFieldValuePair[]
    for (n,v) in d
        push!(data, TAMQPFieldValuePair(convert(TAMQPShortStr,n), as_fval(v)))
    end
    TAMQPFieldTable(length(data), data)
end

convert(::Type{Any}, t::TAMQPFieldArray) = convert(Vector, t)
convert(::Type{Vector{T}}, t::TAMQPFieldArray) where {T} = convert(Vector{T}, t.data)

convert(::Type{Bool}, b::TAMQPBit) = Bool(b.val & 0x1)
convert(::Type{TAMQPBit}, b::Bool) = TAMQPBit(convert(UInt8, b))
convert(::Type{TAMQPBit}, b::T) where {T<:Integer} = convert(TAMQPBit, Bool(b))
