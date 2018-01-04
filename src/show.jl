function show(io::IO, p::TAMQPFrameProperties)
    print(io, "Channel $(p.channel), Size $(p.payloadsize) bytes")
end

function show(io::IO, p::TAMQPMethodPayload)
    print(io, displayname(p.class, p.method))
end

function show(io::IO, m::TAMQPMethodFrame)
    print(io, "MethodFrame ", m.payload, "(", length(m.payload.fields), " fields...)")
    if isa(io, IOContext)
        if !((:limit => true) in io)
            print(io, '\n')
            show(io, m.payload.fields)
        end
    end
end

function show(io::IO, f::TAMQPFieldValue)
    show(io, f.fld)
end

function show(io::IO, f::TAMQPFieldValuePair)
    indent = isa(io, IOContext) ? get(io, :indent, "") : ""
    print(io, indent)
    show(io, f.name)
    print(io, " => ")
    show(io, f.val)
end

function show(io::IO, f::TAMQPFieldTable)
    indent = isa(io, IOContext) ? get(io, :indent, "") : ""
    println(io, "FieldTable")
    ioc = IOContext(io, :indent => (indent * "    "))
    idx = 1
    for fpair in f.data
        (idx > 1) && print(ioc, '\n')
        show(ioc, fpair)
        idx += 1
    end
end

function show(io::IO, s::T) where {T<:Union{TAMQPShortStr,TAMQPLongStr}}
    print(io, convert(String, s))
end

function show(io::IO, fields::Vector{Pair{Symbol,TAMQPField}})
    indent = isa(io, IOContext) ? get(io, :indent, "") : ""
    println(io, indent, "Fields:")
    indent = indent * "    "
    ioc = IOContext(io, :indent => indent)
    idx = 1
    for fld in fields
        (idx > 1) && print(ioc, '\n')
        print(ioc, indent)
        show(ioc, fld.first)
        print(ioc, " => ")
        show(ioc, fld.second)
        idx += 1
    end
end
