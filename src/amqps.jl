function default_tls_debug(level, filename, number, msg)
    @debug(level, filename, number, msg)
end

function default_tls_rng()
    entropy = MbedTLS.Entropy()
    rng = MbedTLS.CtrDrbg()
    MbedTLS.seed!(rng, entropy)
    rng
end

"""
    amqps_configure(;
        cacerts = nothing,
        verify = MbedTLS.MBEDTLS_SSL_VERIFY_NONE,
        client_cert = nothing,
        client_key = nothing
    )

Creates and returns a configuration for making AMQPS connections.
- cacerts: A CA certificate file (or it's contents) to use for certificate verification.
- verify: Whether to verify server certificate. Default is false if cacerts is not provided and true if it is.
- client_cert and client_key: The client certificate and corresponding private key to use. Default is nothing (no client certificate). Values can either be the file name or certificate/key contents.
"""
function amqps_configure(;
    rng = default_tls_rng(),
    cacerts::Union{String,Nothing} = nothing,
    verify::Int64 = (cacerts === nothing) ? MbedTLS.MBEDTLS_SSL_VERIFY_NONE : MbedTLS.MBEDTLS_SSL_VERIFY_REQUIRED,
    client_cert::Union{String,Nothing} = nothing,
    client_key::Union{String,Nothing} = nothing,
    debug::Union{Function,Nothing} = nothing)

    conf = MbedTLS.SSLConfig()
    MbedTLS.config_defaults!(conf)
    MbedTLS.rng!(conf, rng)
    (debug === nothing) || MbedTLS.dbg!(conf, debug)

    if cacerts !== nothing
        if isfile(cacerts)
            # if it is a file name instead of certificate contents, read the contents
            cacerts = read(cacerts, String)
        end
        MbedTLS.ca_chain!(conf, MbedTLS.crt_parse(cacerts))
    end
    MbedTLS.authmode!(conf, verify)

    if (client_cert !== nothing) && (client_key !== nothing)
        if isfile(client_cert)
            # if it is a file name instead of certificate contents, read the contents
            client_cert = read(client_cert, String)
        end
        if isfile(client_key)
            client_key = read(client_key, String)
        end
        key = MbedTLS.PKContext()
        MbedTLS.parse_key!(key, client_key)
        MbedTLS.own_cert!(conf, MbedTLS.crt_parse(client_cert), key)
    end

    conf
end

function setup_tls(sock::TCPSocket, hostname::String, ssl_options::MbedTLS.SSLConfig)
    @debug("setting up TLS")

    ctx = MbedTLS.SSLContext()
    MbedTLS.setup!(ctx, ssl_options)
    MbedTLS.set_bio!(ctx, sock)
    MbedTLS.hostname!(ctx, hostname)
    MbedTLS.handshake(ctx)
    @debug("TLS setup done")

    BufferedTLSSocket(ctx)
end