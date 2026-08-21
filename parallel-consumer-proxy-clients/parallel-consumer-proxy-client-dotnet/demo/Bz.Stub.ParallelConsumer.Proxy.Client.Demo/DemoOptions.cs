// Copyright (C) 2026 Antony Stubbs and contributors

using System.Globalization;

namespace Bz.Stub.ParallelConsumer.Proxy.Client.Demo;

/// <summary>
/// The demo's dials, and the interface EVERY language's demo mirrors - the flags, their defaults,
/// their environment variables and the precedence between them.
/// </summary>
/// <remarks>
/// The contract is <c>parallel-consumer-proxy/demo/README.md</c>; the reference implementation of
/// this type is the Java demo's <c>DemoOptions</c>, and this is that surface in C#. Flags beat the
/// environment beats the defaults - a container passes configuration by environment while a person
/// at a terminal passes flags, and either must be able to override the other.
/// <para>
/// R39 constrains how configuration reaches the PROXY. A demo is an application, so its flags are
/// not a violation of it. Without this note somebody reads <c>--records</c> as breaking the plan's
/// own rule and deletes it.
/// </para>
/// </remarks>
internal sealed record DemoOptions
{
    /// <summary>Prefix for every environment variable this demo reads, so a reader greps one string.</summary>
    public const string EnvironmentPrefix = "PC_DEMO_";

    public int Records { get; private init; } = 2000;

    public int DelayMs { get; private init; } = 2;

    public int MaxConcurrency { get; private init; } = 100;

    public int Partitions { get; private init; } = 10;

    public int ReplayFactor { get; private init; } = 20;

    /// <summary>
    /// The broker to use, when the caller supplied one. Null means "start one" - the Testcontainers
    /// default. Inside the demo's own container this is always present, because a demo container is
    /// never granted the host Docker socket and so cannot start a broker of its own; it reaches a
    /// compose sibling instead.
    /// </summary>
    public string? Bootstrap { get; private init; }

    /// <summary>The topic to use, when the caller supplied one. Null means the demo names its own.</summary>
    public string? Topic { get; private init; }

    /// <summary>The records the big replay consumes in total, including the small replay's own.</summary>
    public int BigReplayRecords => Records * Math.Max(1, ReplayFactor);

    /// <summary>True when the big replay is worth running at all; a factor of 1 or less skips it.</summary>
    public bool BigReplayWanted => ReplayFactor > 1;

    /// <summary>Whether the caller asked for the usage text rather than a run.</summary>
    /// <remarks>
    /// Handled here rather than only in <c>run.sh</c>, because the script is not the only way in:
    /// <c>docker compose run demo --help</c> reaches this program directly, and answering that with
    /// "unknown option: --help" would be a poor first impression.
    /// </remarks>
    /// <param name="arguments">The process arguments.</param>
    /// <returns>Whether help was asked for.</returns>
    public static bool IsHelpRequested(IEnumerable<string> arguments) =>
        arguments.Any(argument =>
            string.Equals(argument, "-h", StringComparison.Ordinal)
            || string.Equals(argument, "--help", StringComparison.Ordinal));

    /// <summary>
    /// Parses the demo's command line, falling back to the environment and then to the defaults.
    /// </summary>
    /// <param name="arguments">
    /// The process arguments, which may legitimately be empty - that is the double-click case, and
    /// it must work.
    /// </param>
    /// <param name="environment">
    /// The environment to read, passed in rather than read from <see cref="Environment"/> so this
    /// is testable without mutating the process's own environment.
    /// </param>
    /// <returns>The effective options.</returns>
    /// <exception cref="ArgumentException">
    /// On an unknown flag, a missing value, or a value that is not a number in range. A demo that
    /// silently ignored a misspelled flag would report numbers for settings nobody asked for.
    /// </exception>
    public static DemoOptions Parse(
        IReadOnlyList<string> arguments, IReadOnlyDictionary<string, string> environment)
    {
        var options = FromEnvironment(environment);

        for (var index = 0; index < arguments.Count; index++)
        {
            var flag = arguments[index];
            switch (flag)
            {
                case "--records":
                    options = options with { Records = Positive(flag, Value(arguments, ++index, flag)) };
                    break;
                case "--delay-ms":
                    options = options with { DelayMs = NonNegative(flag, Value(arguments, ++index, flag)) };
                    break;
                case "--concurrency":
                    options = options with
                    {
                        MaxConcurrency = Positive(flag, Value(arguments, ++index, flag)),
                    };
                    break;
                case "--partitions":
                    options = options with { Partitions = Positive(flag, Value(arguments, ++index, flag)) };
                    break;
                case "--replay-factor":
                    // 1 or less skips the big replay, so this one is allowed to be zero
                    options = options with
                    {
                        ReplayFactor = NonNegative(flag, Value(arguments, ++index, flag)),
                    };
                    break;
                case "--bootstrap":
                    options = options with { Bootstrap = Value(arguments, ++index, flag) };
                    break;
                case "--topic":
                    options = options with { Topic = Value(arguments, ++index, flag) };
                    break;
                default:
                    throw new ArgumentException($"unknown option: {flag}", nameof(arguments));
            }
        }

        return options.Validated();
    }

    /// <summary>
    /// The effective configuration, for printing before the run.
    /// </summary>
    /// <remarks>
    /// A number without its settings is not reproducible, so this is part of the contract every
    /// language's demo keeps rather than a debugging aid. THE BOOTSTRAP ADDRESS IS DELIBERATELY
    /// ABSENT: own-cluster mode puts a user's real broker address there, and the credential-hygiene
    /// rule that binds the proxy binds a demo too - nothing logged, nothing echoed.
    /// </remarks>
    /// <returns>The fingerprint block, one setting per line.</returns>
    public override string ToString() => string.Create(
        CultureInfo.InvariantCulture,
        $"records = {Records}\n  delayMs = {DelayMs}\n  maxConcurrency = {MaxConcurrency}\n  partitions = {Partitions}\n  replayFactor = {ReplayFactor}");

    /// <summary>
    /// Every flag's environment variable, applied in one place so the two spellings of a setting
    /// cannot drift apart: the flag's name, upper-snake-cased, under the <c>PC_DEMO_</c> prefix.
    /// </summary>
    private static DemoOptions FromEnvironment(IReadOnlyDictionary<string, string> environment)
    {
        var options = new DemoOptions();

        if (Named(environment, "RECORDS") is { } records)
        {
            options = options with { Records = Positive(EnvironmentPrefix + "RECORDS", records) };
        }

        if (Named(environment, "DELAY_MS") is { } delayMs)
        {
            options = options with { DelayMs = NonNegative(EnvironmentPrefix + "DELAY_MS", delayMs) };
        }

        if (Named(environment, "CONCURRENCY") is { } concurrency)
        {
            options = options with
            {
                MaxConcurrency = Positive(EnvironmentPrefix + "CONCURRENCY", concurrency),
            };
        }

        if (Named(environment, "PARTITIONS") is { } partitions)
        {
            options = options with { Partitions = Positive(EnvironmentPrefix + "PARTITIONS", partitions) };
        }

        if (Named(environment, "REPLAY_FACTOR") is { } replayFactor)
        {
            options = options with
            {
                ReplayFactor = NonNegative(EnvironmentPrefix + "REPLAY_FACTOR", replayFactor),
            };
        }

        if (Named(environment, "BOOTSTRAP") is { } bootstrap)
        {
            options = options with { Bootstrap = bootstrap };
        }

        if (Named(environment, "TOPIC") is { } topic)
        {
            options = options with { Topic = topic };
        }

        return options;
    }

    /// <summary>An environment value, trimmed, or null when it is absent or blank.</summary>
    private static string? Named(IReadOnlyDictionary<string, string> environment, string suffix) =>
        environment.TryGetValue(EnvironmentPrefix + suffix, out var raw) && !string.IsNullOrWhiteSpace(raw)
            ? raw.Trim()
            : null;

    private static string Value(IReadOnlyList<string> arguments, int index, string flag) =>
        index >= arguments.Count
            ? throw new ArgumentException($"{flag} needs a value", nameof(arguments))
            : arguments[index];

    private static int Positive(string flag, string raw)
    {
        var parsed = Number(flag, raw);
        return parsed < 1
            ? throw new ArgumentException($"{flag} must be at least 1, got {parsed}", nameof(flag))
            : parsed;
    }

    private static int NonNegative(string flag, string raw)
    {
        var parsed = Number(flag, raw);
        return parsed < 0
            ? throw new ArgumentException($"{flag} must not be negative, got {parsed}", nameof(flag))
            : parsed;
    }

    private static int Number(string flag, string raw) =>
        int.TryParse(raw.Trim(), NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed)
            ? parsed
            : throw new ArgumentException($"{flag} needs a whole number, got '{raw}'", nameof(flag));

    private DemoOptions Validated()
    {
        // Checked as a long here rather than trusted as an int later: Records * ReplayFactor
        // overflows silently, and a wrapped value turns the big replay into a tiny one that still
        // prints a confident throughput figure.
        var bigReplay = (long)Records * Math.Max(1, ReplayFactor);
        return bigReplay > int.MaxValue
            ? throw new ArgumentException(
                string.Create(
                    CultureInfo.InvariantCulture,
                    $"--records times --replay-factor is {bigReplay}, which is more records than the demo can count; lower one of them"),
                nameof(Records))
            : this;
    }
}
