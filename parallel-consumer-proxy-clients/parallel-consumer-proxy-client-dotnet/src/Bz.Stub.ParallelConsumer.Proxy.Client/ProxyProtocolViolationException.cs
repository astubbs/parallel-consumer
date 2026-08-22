// Copyright (C) 2026 Antony Stubbs and contributors

namespace Bz.Stub.ParallelConsumer.Proxy.Client;

/// <summary>
/// The proxy did something the protocol forbids, and the session cannot continue.
/// </summary>
/// <remarks>
/// This is not a load condition or a transient fault: it names a peer that broke the contract - a
/// dispatch past the declared in-flight ceiling, a handshake reply that is not <c>Configured</c>,
/// or a <c>Configured</c> missing a value the specification says is always sent.
/// <para>
/// A gRPC CLIENT cannot answer a violation with a status code the way the proxy can: only a server
/// sets a status, and a client that cancels its call is observed by the peer as <c>CANCELLED</c>.
/// So the session is cancelled and this exception carries the diagnosis, rather than the
/// <c>FAILED_PRECONDITION</c> the specification's wording implies.
/// </para>
/// </remarks>
public sealed class ProxyProtocolViolationException : InvalidOperationException
{
    /// <summary>Creates the exception with a message describing the violation.</summary>
    /// <param name="message">What the peer did.</param>
    public ProxyProtocolViolationException(string message)
        : base(message)
    {
    }

    /// <summary>Creates the exception with a message and the error underneath it.</summary>
    /// <param name="message">What the peer did.</param>
    /// <param name="innerException">The error that revealed it.</param>
    public ProxyProtocolViolationException(string message, Exception innerException)
        : base(message, innerException)
    {
    }

    /// <summary>Creates the exception with the default message.</summary>
    public ProxyProtocolViolationException()
        : base("the proxy broke the session protocol")
    {
    }
}
