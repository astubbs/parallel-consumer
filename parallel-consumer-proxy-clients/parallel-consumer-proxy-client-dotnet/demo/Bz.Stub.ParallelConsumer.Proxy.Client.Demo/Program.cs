// Copyright (C) 2026 Antony Stubbs and contributors

using System.Collections.Concurrent;
using System.Diagnostics;
using System.Globalization;
using System.Text;
using Confluent.Kafka;

namespace Bz.Stub.ParallelConsumer.Proxy.Client.Demo;

/// <summary>
/// <b>The .NET demo.</b> The same records through .NET's own Kafka client, one at a time, and
/// through this module's client library over a real sidecar.
/// </summary>
/// <remarks>
/// THE CONTRACT IS <c>parallel-consumer-proxy/demo/README.md</c>, and this keeps it: the same
/// flags with the same defaults, the same <c>PC_DEMO_</c> environment variables with the same
/// precedence, the effective configuration printed before anything runs, the same two tables in the
/// same order, and no latency anywhere. A reader who has run one language's demo has run them all.
/// <para>
/// TWO ARMS, WHICH IS THE WHOLE CONTRACT OUTSIDE JAVA:
/// </para>
/// <list type="bullet">
///   <item><b>AK core (Confluent.Kafka)</b> - a plain <c>Confluent.Kafka</c> consumer, one record at
///     a time. Always spelled "AK core", never bare "core", which reads as
///     <c>parallel-consumer-core</c> - and always carrying the client's own name, because "AK core"
///     is a CATEGORY and a reader cannot judge a comparison without knowing what produced it.</item>
///   <item><b>dotnet-grpc (this client)</b> - this module's client library over a sidecar it spawns
///     itself, which is what the second half of the label names. ON
///     THIS PATH THE APPLICATION DOES NO KAFKA I/O: the sidecar owns the consumer, the producer,
///     the group membership and the offsets. In a genuinely foreign application that is the whole
///     story - it needs no Kafka client library at all. In this demo it is a statement about the
///     PATH and not about the process: the same process creates the topic, produces the backlog and
///     runs the AK core arm with an ordinary Kafka client, because a comparison needs both
///     sides.</item>
/// </list>
/// <para>
/// JAVA CARRIES FOUR MORE ARMS AND THIS DELIBERATELY DOES NOT. Java is the only place where the
/// sidecar hop can be priced honestly, because every arm there runs in one JVM against one broker
/// with one workload. Here the two arms are two different client libraries as well as two different
/// engines, so the gap between them is not a wire cost and is not read as one.
/// </para>
/// </remarks>
internal static class Program
{
    /// <summary>No arm may take longer than this before the demo calls it stalled rather than slow.</summary>
    private static readonly TimeSpan ArmBudget = TimeSpan.FromMinutes(10);

    /// <summary>
    /// THE FIRST THING THIS DEMO PRINTS, AND IT NAMES THE PRODUCT.
    /// </summary>
    /// <remarks>
    /// Contract, not decoration. A reader who runs this and is met with
    /// <c>dotnet-grpc: the proxy granted 100 executor threads</c> has been told nothing about what
    /// they are looking at - not the product's name, not what is about to happen. Every language
    /// prints this same banner, differing only in its own name, so a visitor moving between two
    /// demos recognises the second one instantly.
    /// </remarks>
    private const string Banner = """
        ================================================================
          PARALLEL CONSUMER  -  .NET demo
          The same records, twice: one at a time, then all at once.
        ================================================================
        """;

    /// <summary>
    /// The serial arm's label: the ROLE and the CLIENT, because "AK core" alone is a category.
    /// </summary>
    /// <remarks>
    /// The answer differs in every language - <c>rdkafka</c> in Ruby, <c>franz-go</c> in Go,
    /// <c>kafkajs</c> in TypeScript - and a reader evaluating "is this fast in my language" is
    /// really asking about the client they already use. In .NET the answer is
    /// <c>Confluent.Kafka</c>, and there is no second serious answer to choose between; the demo's
    /// own README says so rather than leaving the reader to wonder.
    /// </remarks>
    private const string AkCore = "AK core (Confluent.Kafka)";

    /// <summary>The sidecar arm's label, naming what drives it: this module's own client library.</summary>
    private const string SidecarArm = "dotnet-grpc (this client)";

    /// <summary>
    /// The sidecar arm's identifier, for places a label cannot go.
    /// </summary>
    /// <remarks>
    /// Consumer group names travel to the broker and end up in metric names, so the label's spaces
    /// and brackets have no business there. The label is for the reader; this is for Kafka.
    /// </remarks>
    private const string SidecarArmId = "dotnet-grpc";

    /// <summary>
    /// Stands in for a null key when counting distinct keys.
    /// </summary>
    /// <remarks>
    /// Every other key is counted by its base64 text, which cannot contain <c>&lt;</c>, so this can
    /// never collide with a real one. A null key is one distinct key, not an absent record.
    /// </remarks>
    private const string NullKeyToken = "<null>";

    private static async Task<int> Main(string[] arguments)
    {
        // BEFORE EVERYTHING, including the argument parsing: a reader who mistypes a flag should
        // still be told what they were trying to run.
        Console.WriteLine(Banner);

        if (DemoOptions.IsHelpRequested(arguments))
        {
            Usage();
            return 0;
        }

        DemoOptions options;
        try
        {
            options = DemoOptions.Parse(arguments, EnvironmentVariables());
        }
        catch (ArgumentException badArgument)
        {
            await Console.Error.WriteLineAsync(badArgument.Message).ConfigureAwait(false);
            Usage();

            // A misspelled flag must not be reported as a result for settings nobody asked for.
            return 2;
        }

        try
        {
            await using var broker = await DemoBroker
                .ResolveAsync(options.Bootstrap, CancellationToken.None).ConfigureAwait(false);
            var topic = options.Topic ?? string.Create(
                CultureInfo.InvariantCulture, $"pc-demo-{Stopwatch.GetTimestamp()}");
            await RunAsync(options, broker, topic).ConfigureAwait(false);
            return 0;
        }
#pragma warning disable CA1031 // The demo's one top-level handler: a stack trace is not a result.
        catch (Exception failure)
        {
            await Console.Error.WriteLineAsync($"The demo did not finish: {failure}").ConfigureAwait(false);
            return 1;
        }
#pragma warning restore CA1031
    }

    private static void Usage() => Console.WriteLine(
        """

        usage: demo/run.sh [options]

          --records N        records in the comparison replay   (default 2000)
          --delay-ms N       simulated work per record, ms      (default 2)
          --concurrency N    max in-flight records              (default 100)
          --partitions N     partitions on the demo topic       (default 10)
          --replay-factor N  big replay = records x N; 1 skips  (default 20)
          --bootstrap ADDR   an existing broker; omit to start one
          --topic NAME       an existing topic; omit to create one

        Every flag has an environment variable: --delay-ms is PC_DEMO_DELAY_MS.
        Flags beat the environment beats the defaults.
        """);

    private static Dictionary<string, string> EnvironmentVariables()
    {
        var environment = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (System.Collections.DictionaryEntry entry in Environment.GetEnvironmentVariables())
        {
            if (entry.Key is string name && entry.Value is string value)
            {
                environment[name] = value;
            }
        }

        return environment;
    }

    private static async Task RunAsync(DemoOptions options, DemoBroker broker, string topic)
    {
        // THE FINGERPRINT, FIRST AND ALWAYS. A number without its settings is not reproducible.
        // The bootstrap address is not in it: own-cluster mode puts a user's real broker there.
        Console.WriteLine($"\nEffective configuration:\n  {options}\n  topic = {topic}");

        await broker.EnsureTopicAsync(topic, options.Partitions).ConfigureAwait(false);
        broker.Seed(topic, 0, options.Records);

        var small = new List<ArmResult>
        {
            await AkCoreArmAsync(options, broker, topic, options.Records).ConfigureAwait(false),
            await SidecarArmAsync(options, broker, topic, options.Records).ConfigureAwait(false),
        };
        var baseline = small.Find(result => string.Equals(result.Arm, AkCore, StringComparison.Ordinal));
        Report(
            string.Create(
                CultureInfo.InvariantCulture,
                $"Small replay - both arms over the same {options.Records} records (the comparison)"),
            small,
            baseline,
            acrossReplays: false);

        if (!options.BigReplayWanted)
        {
            Console.WriteLine(string.Create(
                CultureInfo.InvariantCulture, $"\nBig replay skipped (--replay-factor {options.ReplayFactor})."));
            return;
        }

        var total = options.BigReplayRecords;
        broker.Seed(topic, options.Records, total);

        // AK CORE IS EXCLUDED HERE BECAUSE IT DOES NOT GO PARALLEL. It would need total * delayMs
        // milliseconds to finish a backlog the sidecar arm clears in seconds, and a demo that makes
        // a reader wait that long to learn nothing new is not worth the wall clock.
        var big = new List<ArmResult>
        {
            await SidecarArmAsync(options, broker, topic, total).ConfigureAwait(false),
        };
        Report(
            string.Create(
                CultureInfo.InvariantCulture,
                $"Big replay - {total} records, parallel arms only (AK core is serial and would take {total * options.DelayMs / 1000}s+)"),
            big,
            baseline,
            acrossReplays: true);
    }

    /// <summary>The serial arm: .NET's own Kafka client, one record at a time, the same wait.</summary>
    private static async Task<ArmResult> AkCoreArmAsync(
        DemoOptions options, DemoBroker broker, string topic, int target)
    {
        Console.WriteLine(string.Create(
            CultureInfo.InvariantCulture, $"\n=== {AkCore} starting over {target} records ==="));

        using var consumer = new ConsumerBuilder<byte[], byte[]>(
            new ConsumerConfig(broker.ConsumerProperties(GroupId("ak-core")))).Build();
        consumer.Subscribe(topic);

        // The clock starts AFTER the consumer is built and stops before it closes, because this arm
        // is the denominator of every ratio in both tables and the other arm charges itself for
        // neither client construction nor teardown.
        var startedAt = Stopwatch.GetTimestamp();
        var processed = 0;
        var keys = NewKeySet();
        while (processed < target)
        {
            // The one arm that does not wait on a completion still needs the budget, or a backlog
            // shorter than the target spins here forever with no output.
            if (Stopwatch.GetElapsedTime(startedAt) > ArmBudget)
            {
                throw new InvalidOperationException(string.Create(
                    CultureInfo.InvariantCulture, $"{AkCore} stalled at {processed} of {target}"));
            }

            var delivered = consumer.Consume(TimeSpan.FromMilliseconds(500));
            if (delivered?.Message is null)
            {
                continue;
            }

            keys.TryAdd(KeyToken(delivered.Message.Key), 0);
            await SimulatedWorkAsync(options.DelayMs, CancellationToken.None).ConfigureAwait(false);
            processed++;
        }

        return Finished(AkCore, Stopwatch.GetElapsedTime(startedAt), processed, keys.Count);
    }

    /// <summary>
    /// The client library over a real sidecar - the arm the whole design exists for.
    /// </summary>
    /// <remarks>
    /// IT GOES THROUGH THE CLIENT LIBRARY, NOT THE PROTOCOL. An earlier version of the Java demo
    /// spoke gRPC by hand; it proved the engine worked and said nothing about the client library,
    /// which is the artifact users actually touch. This arm spawns the sidecar the way an
    /// application does - by asking the library to - and runs an ordinary delegate on the records.
    /// </remarks>
    private static async Task<ArmResult> SidecarArmAsync(
        DemoOptions options, DemoBroker broker, string topic, int target)
    {
        Console.WriteLine(string.Create(
            CultureInfo.InvariantCulture, $"\n=== {SidecarArm} starting over {target} records ==="));

        var sidecar = SidecarCommand.Resolve();
        var processed = 0;
        var keys = NewKeySet();
        var reachedTarget = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var client = await ParallelConsumerClient.ConnectAsync(new ClientOptions
        {
            SidecarPath = sidecar.Path,
            SidecarArguments = sidecar.Arguments,
            SidecarErrorLog = SidecarErrorLog(),
            Topics = [topic],
            MaxConcurrency = options.MaxConcurrency,
            Ordering = ProcessingOrder.Unordered,
            KafkaProperties = broker.ConsumerProperties(GroupId(SidecarArmId)),
            InstanceTag = "pc-dotnet-demo",
        }).ConfigureAwait(false);

        var startedAt = Stopwatch.GetTimestamp();
        var session = client.PollAsync(async (record, cancellationToken) =>
        {
            // On arrival, matching the serial arm: the column is "unique keys SEEN", which is a
            // property of what the engine handed this function, not of the work afterwards.
            keys.TryAdd(KeyToken(record.Key), 0);
            await SimulatedWorkAsync(options.DelayMs, cancellationToken).ConfigureAwait(false);
            if (Interlocked.Increment(ref processed) >= target)
            {
                reachedTarget.TrySetResult();
            }

            return Outcome.Succeed();
        });

        return await AwaitedAsync(
                SidecarArm, startedAt, reachedTarget.Task, session, () => processed, () => keys.Count, target)
            .ConfigureAwait(false);
    }

    /// <summary>
    /// THE SIMULATED WORK, AND THE ONE PLACE THIS DEMO DIVERGES FROM THE OTHERS.
    /// </summary>
    /// <remarks>
    /// The contract says a blocking sleep is fine in C#, and it names Python and TypeScript as the
    /// exceptions. IT IS NOT FINE HERE, and the reason is this client's own shape rather than the
    /// language's: the library's executors are <see cref="Task"/>s on the thread pool, so a hundred
    /// of them blocking in <c>Thread.Sleep</c> is a hundred pool threads occupied, and the pool
    /// injects new ones at roughly one per second once its core count is used up. The sidecar arm
    /// would then report the thread pool's injection rate rather than the engine's throughput - a
    /// number that looks like a measurement and is not one.
    /// <para>
    /// So the wait is an awaited timer, which is what "non-occupying" means in a language whose
    /// concurrency is tasks rather than threads - the contract's own rule for Python and TypeScript,
    /// applied for the same reason one language over. BOTH ARMS USE IT: the AK core arm holds one
    /// record at a time, so it cannot starve anything, but an arm that is the denominator of every
    /// ratio must not differ from its numerator by the wait primitive as well as by the transport.
    /// </para>
    /// </remarks>
    private static Task SimulatedWorkAsync(int delayMs, CancellationToken cancellationToken) =>
        delayMs <= 0 ? Task.CompletedTask : Task.Delay(delayMs, cancellationToken);

    /// <summary>
    /// Where the sidecar's own logging goes. Discarded by default, because the sidecar is a JVM with
    /// a logging framework attached and its output is not what this demo is showing; set
    /// <c>PC_DEMO_SIDECAR_LOG</c> to any value to see it, which is the first thing to do when the
    /// sidecar arm will not start.
    /// </summary>
    private static TextWriter? SidecarErrorLog() =>
        string.IsNullOrEmpty(Environment.GetEnvironmentVariable("PC_DEMO_SIDECAR_LOG"))
            ? null
            : Console.Error;

    /// <summary>
    /// Waits for the arm to reach its target, for its session to end, or for the budget - whichever
    /// comes first.
    /// </summary>
    /// <remarks>
    /// REACHING THE TARGET IS NOT THE ONLY THING THAT ENDS A SESSION: a failed or completed stream
    /// ends it too. Without the count check below, a broken run prints a plausible row at a
    /// plausible rate and exits 0, which is the worst thing a demo whose shape ten other languages
    /// copy can do.
    /// </remarks>
    private static async Task<ArmResult> AwaitedAsync(
        string arm,
        long startedAt,
        Task reachedTarget,
        Task session,
        Func<int> processed,
        Func<int> uniqueKeys,
        int target)
    {
        var ended = await Task.WhenAny(reachedTarget, session).WaitAsync(ArmBudget).ConfigureAwait(false);
        if (ended == session)
        {
            // Faulted sessions rethrow here, with the session's first fatal error.
            await session.ConfigureAwait(false);
        }

        var count = processed();
        return count < target
            ? throw new InvalidOperationException(string.Create(
                CultureInfo.InvariantCulture, $"{arm} ended early at {count} of {target}"))
            : Finished(arm, Stopwatch.GetElapsedTime(startedAt), count, uniqueKeys());
    }

    private static ArmResult Finished(string arm, TimeSpan elapsed, int processed, int uniqueKeys)
    {
        Console.WriteLine(string.Create(
            CultureInfo.InvariantCulture,
            $"=== {arm} finished: {processed} records over {uniqueKeys} keys in {(long)elapsed.TotalMilliseconds}ms ==="));
        return new ArmResult(arm, elapsed, processed, uniqueKeys);
    }

    /// <summary>
    /// The set an arm counts its distinct keys in.
    /// </summary>
    /// <remarks>
    /// Concurrent in both arms even though only one of them needs it to be: the serial arm is the
    /// denominator of every ratio in both tables, and it must not differ from its numerator in how
    /// it arrives at a figure the two are then compared on.
    /// </remarks>
    private static ConcurrentDictionary<string, byte> NewKeySet() => new(StringComparer.Ordinal);

    /// <summary>
    /// One record key, as the text the distinct-key count is taken over.
    /// </summary>
    /// <remarks>
    /// Base64 rather than the key's own characters, because keys are BYTES on this wire and an
    /// arbitrary byte string is not text. A null key is one distinct key of its own - it is not the
    /// same as an empty one, and it is not an absent record.
    /// </remarks>
    private static string KeyToken(byte[]? key) =>
        key is null ? NullKeyToken : Convert.ToBase64String(key);

    /// <summary>A fresh group per arm per replay, so every arm reads the same records from the beginning.</summary>
    private static string GroupId(string arm) =>
        string.Create(CultureInfo.InvariantCulture, $"pc-demo-{arm}-{Stopwatch.GetTimestamp()}");

    /// <summary>
    /// One of the demo's two tables. Same columns, same order, in every language.
    /// </summary>
    /// <remarks>
    /// FOUR OF THE FIVE FIGURES SAY SOMETHING DIFFERENT. Elapsed, msg/s and the ratio are this
    /// machine on this run and mean nothing anywhere else; RECORDS and KEYS are deterministic, so
    /// every language over the same backlog must print the same two numbers and any language that
    /// does not has a real defect. That is what makes the table demonstrate the run rather than
    /// assert it - a short arm is a FAILED arm, not a fast one - and it is what
    /// <c>bin/ci-demo-conformance.sh</c> compares languages on.
    /// <para>
    /// No latency, in either table. The backlog is pre-produced, so the workload is closed-loop and
    /// a per-record timing would be flattered by however far an arm had fallen behind.
    /// </para>
    /// </remarks>
    private static void Report(
        string title, IReadOnlyList<ArmResult> results, ArmResult? baseline, bool acrossReplays)
    {
        var table = new StringBuilder("\n\n").Append(title).Append('\n');
        table.AppendLine(string.Format(
            CultureInfo.InvariantCulture,
            "  {0,-26} {1,10} {2,14} {3,14} {4,10} {5,8}",
            "arm",
            "elapsed",
            "msg/s",
            acrossReplays ? "vs AK core*" : "vs AK core",
            "records",
            "keys"));
        foreach (var result in results)
        {
            var ratio = baseline is null || baseline.RatePerSecond == 0
                ? "-"
                : string.Create(
                    CultureInfo.InvariantCulture, $"{result.RatePerSecond / baseline.RatePerSecond:F1}x");
            table.AppendLine(string.Format(
                CultureInfo.InvariantCulture,
                "  {0,-26} {1,9:F1}s {2,14} {3,14} {4,10} {5,8}",
                result.Arm,
                (long)result.Elapsed.TotalMilliseconds / 1000d,
                ((long)result.RatePerSecond).ToString("N0", CultureInfo.InvariantCulture),
                ratio,
                result.Processed.ToString("N0", CultureInfo.InvariantCulture),
                result.UniqueKeys.ToString("N0", CultureInfo.InvariantCulture)));
        }

        if (acrossReplays)
        {
            table.Append("\n  * against the SMALL replay's AK core arm. Across replays, so not like-for-like.\n");
        }

        Console.WriteLine(table.ToString());
    }
}
