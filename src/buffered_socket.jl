const TLS_BUSY_READ_SECS = 1
const TLS_BUSY_READ_YIELD_SECS = 0.001
const TLS_READBUFF_SIZE = MbedTLS.MBEDTLS_SSL_MAX_CONTENT_LEN * 5
const TLS_MIN_WRITEBUFF_SIZE = MbedTLS.MBEDTLS_SSL_MAX_CONTENT_LEN
const TCP_MAX_WRITEBUFF_SIZE = 1024*512
const TCP_MIN_WRITEBUFF_SIZE = 1024*64

struct BufferedTLSSocket <: IO
    in::IOBuffer                # no read lock, single task reads socket and distributes messages to channels
    out::IOBuffer
    sock::MbedTLS.SSLContext
    readbuff::Vector{UInt8}
    out_lck::ReentrantLock      # protect out::IOBuffer when there are multiple channels on the connection

    function BufferedTLSSocket(sock::MbedTLS.SSLContext; readbuff_size::Int=TLS_READBUFF_SIZE)
        new(PipeBuffer(), PipeBuffer(), sock, Vector{UInt8}(undef, readbuff_size), ReentrantLock())
    end
end

isopen(bio::BufferedTLSSocket) = isopen(bio.sock)
close(bio::BufferedTLSSocket) = close(bio.sock)

function read(bio::BufferedTLSSocket, ::Type{UInt8})
    fill_in(bio, 1)
    read(bio.in, UInt8)
end

function read(bio::BufferedTLSSocket, T::Union{Type{Int16},Type{UInt16},Type{Int32},Type{UInt32},Type{Int64},Type{UInt64},Type{Int128},Type{UInt128},Type{Float16},Type{Float32},Type{Float64}})
    fill_in(bio, sizeof(T))
    read(bio.in, T)
end

function read!(bio::BufferedTLSSocket, buff::Vector{UInt8})
    fill_in(bio, length(buff))
    read!(bio.in, buff)
end

function peek(bio::BufferedTLSSocket, T::Union{Type{Int16},Type{UInt16},Type{Int32},Type{UInt32},Type{Int64},Type{UInt64},Type{Int128},Type{UInt128},Type{Float16},Type{Float32},Type{Float64}})
    fill_in(bio, sizeof(T))
    peek(bio.in, T)
end

function fill_in(bio::BufferedTLSSocket, atleast::Int)
    avail = bytesavailable(bio.in)
    if atleast > avail
        while (atleast > avail) && isopen(bio.sock)
            bytes_read = isreadable(bio.sock) ? readbytes!(bio.sock, bio.readbuff; all=false) : 0
            if bytes_read > 0
                avail += Base.write_sub(bio.in, bio.readbuff, 1, bytes_read)
            else
                MbedTLS.wait_for_decrypted_data(bio.sock)
            end
        end
    end
end

function write(bio::BufferedTLSSocket, data::UInt8)
    lock(bio.out_lck) do
        write(bio.out, data)
    end
end
function write(bio::BufferedTLSSocket, data::Union{Int16,UInt16,Int32,UInt32,Int64,UInt64,Int128,UInt128,Float16,Float32,Float64})
    lock(bio.out_lck) do
        write(bio.out, data)
    end
end
function write(bio::BufferedTLSSocket, data::Array)
    lock(bio.out_lck) do
        write(bio.out, data)
    end
end
function flush(bio::BufferedTLSSocket)
    lock(bio.out_lck) do
        write(bio.sock, take!(bio.out))
    end
    nothing
end