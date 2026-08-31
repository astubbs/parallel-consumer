// Copyright (C) 2026 Antony Stubbs and contributors

using Bz.Stub.ParallelConsumer.Proxy.Protocol.V1;

namespace Bz.Stub.ParallelConsumer.Proxy.Client;

/// <summary>
/// The EFFECTIVE configuration this session is running with - what the proxy replied, after its own
/// defaults and the capability negotiation. Assert on this, never on <see cref="ClientOptions"/>.
/// </summary>
public sealed record SessionInfo
{
    /// <summary>The subscription the proxy is running with.</summary>
    public IReadOnlyList<string> Topics { get; init; } = Array.Empty<string>();

    /// <summary>The subscription pattern, when the subscription was given as one.</summary>
    public string? TopicPattern { get; init; }

    /// <summary>
    /// The proxy's in-flight ceiling, and therefore this client's dispatch-queue depth. Always
    /// finite and always reported; absence would be a protocol violation, never "unlimited".
    /// </summary>
    public int MaxConcurrency { get; init; }

    /// <summary>
    /// How many executors to run. A pure function of connect-time configuration, computed once,
    /// sent once, never revised - and clients must not assume any formula relating it to
    /// <see cref="MaxConcurrency"/>.
    /// </summary>
    public int ExecutorCount { get; init; }

    /// <summary>
    /// The negotiated capability set: the intersection of what this client declared and what the
    /// proxy implements. Neither side sends a message whose token is outside it.
    /// </summary>
    public IReadOnlySet<string> Capabilities { get; init; } = new HashSet<string>();

    /// <summary>
    /// Where terminally failed records go, when the session negotiated the
    /// <see cref="Client.Capabilities.Terminal"/> token and a topic was configured.
    /// </summary>
    public string? TerminalTopic { get; init; }

    /// <summary>
    /// Whether a capability token survived the handshake. Every duty in this protocol is gated by
    /// one, so this is how a client decides what it owes.
    /// </summary>
    /// <param name="token">A token from <see cref="Client.Capabilities"/>.</param>
    /// <returns>Whether the token is in the negotiated set.</returns>
    public bool Negotiated(string token) => Capabilities.Contains(token);

    internal static SessionInfo From(Configured configured) => new()
    {
        Topics = configured.Topics.ToArray(),
        TopicPattern = configured.HasTopicPattern ? configured.TopicPattern : null,
        MaxConcurrency = configured.MaxConcurrency,
        ExecutorCount = configured.ExecutorCount,
        Capabilities = new HashSet<string>(configured.Capabilities, StringComparer.Ordinal),
        TerminalTopic = configured.HasTerminalTopic ? configured.TerminalTopic : null,
    };
}
