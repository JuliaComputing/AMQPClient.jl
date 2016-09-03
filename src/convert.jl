convert{T<:Union{TAMQPShortStr,TAMQPLongStr}}(::Type{Any}, s::T) = convert(String, s)
convert{T<:Union{TAMQPShortStr,TAMQPLongStr}}(::Type{String}, s::T) = String(s.data)

convert(::Type{Any}, t::TAMQPFieldTable) = convert(Dict{Any,Any}, t)
convert{K,V}(::Type{Dict{K,V}}, t::TAMQPFieldTable) = Dict{K,V}(f.name => f.val for f in t.data)

convert(::Type{Any}, t::TAMQPFieldArray) = convert(Vector, t)
convert{T}(::Type{Vector{T}}, t::TAMQPFieldArray) = convert(Vector{T}, t.data)
