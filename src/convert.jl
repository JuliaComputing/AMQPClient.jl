convert(::Type{String}, s::T) where {T<:Union{TAMQPShortStr,TAMQPLongStr,TAMQPByteArray}} = String(copy(s.data))
convert(::Type{Bool}, b::TAMQPBit) = Bool(b.val & 0x1)

simplify(val::T) where {T <: Union{TAMQPShortStr,TAMQPLongStr,TAMQPByteArray}} = String(copy(val.data))
simplify(val::TAMQPFieldArray) = [simplify(elem) for elem in val.data]
simplify(table::TAMQPFieldTable) = Dict{String,Any}(simplify(f.name)=>simplify(f.val) for f in table.data)
simplify(val::TAMQPFieldValue) = simplify(val.fld)
simplify(x) = x
