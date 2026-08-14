// Copyright (C) 2026 Antony Stubbs and contributors

using Google.Protobuf.WellKnownTypes;
using Wire = Bz.Stub.ParallelConsumer.Proxy.Protocol.V1;

namespace Bz.Stub.ParallelConsumer.Proxy.Client;

/// <summary>
/// How records are ordered for concurrent processing. <see cref="Default"/> takes the proxy's own
/// default, which it reports back in <see cref="SessionInfo"/> - so a caller asserts what it got,
/// never what it asked for.
/// </summary>
public enum ProcessingOrder
{
    /// <summary>Take the proxy's default.</summary>
    Default = 0,

    /// <summary>No ordering guarantee; maximum concurrency.</summary>
    Unordered = 1,

    /// <summary>Records of one partition are processed in order.</summary>
    Partition = 2,

    /// <summary>Records sharing a key are processed in order; distinct keys run concurrently.</summary>
    Key = 3,
}

/// <summary>
/// Everything a session is configured with. All of it except the <c>Sidecar</c> members travels in
/// the connect-time <c>Configure</c> message and nowhere else - nothing reaches the proxy by argv,
/// environment variable or file, because configuration is code and it travels from the user's own
/// language.
/// </summary>
/// <remarks>
/// Every tunable is nullable and <see langword="null"/> means "take the proxy's default", which is
/// the wire's own convention - so the two agree without a translation table.
/// </remarks>
public sealed record ClientOptions
{
    /// <summary>
    /// The ABSOLUTE path of the sidecar binary.
    /// </summary>
    /// <remarks>
    /// Never resolved through <c>PATH</c>, a relative lookup, or any directory an attacker could
    /// influence: this process hands that binary the Kafka credentials, so which binary runs is
    /// security-relevant. It is launched directly and never through a shell.
    /// </remarks>
    public required string SidecarPath { get; init; }

    /// <summary>
    /// Arguments passed to the sidecar binary verbatim. They carry no proxy configuration; the
    /// conformance harness takes its fixture selection this way, which is its own documented
    /// exception rather than a licence to configure a shipped sidecar by flag.
    /// </summary>
    public IReadOnlyList<string> SidecarArguments { get; init; } = Array.Empty<string>();

    /// <summary>
    /// Where the sidecar's standard error is copied, if anywhere. Its standard output is the
    /// lifecycle channel and belongs to this library.
    /// </summary>
    public TextWriter? SidecarErrorLog { get; init; }

    /// <summary>
    /// The subscription, fixed for the sidecar's lifetime. Exactly one of this and
    /// <see cref="TopicPattern"/> must be given.
    /// </summary>
    public IReadOnlyList<string> Topics { get; init; } = Array.Empty<string>();

    /// <summary>The subscription as a pattern. See <see cref="Topics"/>.</summary>
    public string? TopicPattern { get; init; }

    /// <summary>
    /// The proxy's in-flight ceiling, and therefore this client's dispatch-queue depth.
    /// <see langword="null"/> takes the proxy's default. There is no "unlimited".
    /// </summary>
    public int? MaxConcurrency { get; init; }

    /// <summary>
    /// The Kafka connection settings and credentials the proxy builds its clients from.
    /// </summary>
    /// <remarks>
    /// THIS CARRIES CREDENTIALS. This library never logs the map, never echoes an entry of it in an
    /// exception message, and never writes it to argv, an environment variable or a temp file - it
    /// travels the stream and nowhere else. Hold your own code to the same rule.
    /// </remarks>
    public IReadOnlyDictionary<string, string> KafkaProperties { get; init; } =
        new Dictionary<string, string>();

    /// <summary>
    /// The capability tokens to declare. <see langword="null"/> declares
    /// <see cref="Capabilities.Implemented"/> - what this client actually honours - rather than the
    /// empty list, which on the wire means the whole v1 baseline.
    /// </summary>
    public IReadOnlyList<string>? Capabilities { get; init; }

    /// <summary>The ordering guarantee to ask the engine for.</summary>
    public ProcessingOrder Ordering { get; init; } = ProcessingOrder.Default;

    /// <summary>How often the engine commits offsets.</summary>
    public TimeSpan? CommitInterval { get; init; }

    /// <summary>How long a failed record waits before it is retried.</summary>
    public TimeSpan? DefaultMessageRetryDelay { get; init; }

    /// <summary>How long the engine's drain may take at shutdown.</summary>
    public TimeSpan? DrainTimeout { get; init; }

    /// <summary>
    /// Where terminally failed records are produced. It only takes effect when the session also
    /// negotiates the <see cref="Client.Capabilities.Terminal"/> token; the effective
    /// <see cref="SessionInfo.TerminalTopic"/> is what says whether it did.
    /// </summary>
    public string? TerminalTopic { get; init; }

    /// <summary>A tag for the engine's own metrics and logging.</summary>
    public string? InstanceTag { get; init; }

    internal void Validate()
    {
        if (string.IsNullOrWhiteSpace(SidecarPath))
        {
            throw new ArgumentException("SidecarPath is required", nameof(ClientOptions));
        }

        if (!Path.IsPathRooted(SidecarPath))
        {
            throw new ArgumentException(
                $"SidecarPath must be absolute, got '{SidecarPath}' - a relative or PATH-resolved " +
                "sidecar is a binary an attacker can influence",
                nameof(ClientOptions));
        }

        if (Topics.Count == 0 == string.IsNullOrEmpty(TopicPattern))
        {
            throw new ArgumentException(
                "exactly one of Topics or TopicPattern must be set", nameof(ClientOptions));
        }

        if (MaxConcurrency is < 1)
        {
            throw new ArgumentException(
                $"MaxConcurrency must be >= 1, or null for the proxy's default, got {MaxConcurrency}",
                nameof(ClientOptions));
        }
    }

    /// <summary>Renders these options as the first message of a fresh session.</summary>
    internal Wire.Configure ToConfigure()
    {
        var configure = new Wire.Configure();
        configure.Topics.AddRange(Topics);
        foreach (var property in KafkaProperties)
        {
            configure.KafkaProperties.Add(property.Key, property.Value);
        }

        configure.Capabilities.AddRange(Capabilities ?? Client.Capabilities.Implemented);

        if (!string.IsNullOrEmpty(TopicPattern))
        {
            configure.TopicPattern = TopicPattern;
        }

        if (MaxConcurrency is { } maxConcurrency)
        {
            configure.MaxConcurrency = maxConcurrency;
        }

        if (Ordering != ProcessingOrder.Default)
        {
            configure.Ordering = (Wire.ProcessingOrder)(int)Ordering;
        }

        if (CommitInterval is { } commitInterval)
        {
            configure.CommitInterval = Duration.FromTimeSpan(commitInterval);
        }

        if (DefaultMessageRetryDelay is { } retryDelay)
        {
            configure.DefaultMessageRetryDelay = Duration.FromTimeSpan(retryDelay);
        }

        if (DrainTimeout is { } drainTimeout)
        {
            configure.DrainTimeout = Duration.FromTimeSpan(drainTimeout);
        }

        if (!string.IsNullOrEmpty(TerminalTopic))
        {
            configure.TerminalTopic = TerminalTopic;
        }

        if (!string.IsNullOrEmpty(InstanceTag))
        {
            configure.PcInstanceTag = InstanceTag;
        }

        return configure;
    }
}
