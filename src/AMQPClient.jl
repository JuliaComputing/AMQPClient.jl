module AMQPClient

using Compat
import Base: read, read!, close, convert, show

# enable logging only during debugging
using Logging
#const logger = Logging.configure(filename="elly.log", level=DEBUG)
const logger = Logging.configure(level=DEBUG)
macro logmsg(s)
    quote
        debug("[", myid(), "-", "] ", $(esc(s)))
    end
end
#macro logmsg(s)
#end

include("types.jl")
include("spec.jl")
include("protocol.jl")
include("convert.jl")
include("show.jl")

export Connection
export read, read!, close, convert, show

end # module
