// Copyright (C) 2026 Antony Stubbs and contributors

using System.Globalization;
using System.Text;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Testcontainers.Kafka;

namespace Bz.Stub.ParallelConsumer.Proxy.Client.Demo;

/// <summary>
/// The broker the demo reads from, however the reader got here.
/// </summary>
/// <remarks>
/// Two ways in, and the second is a rule rather than a convenience:
/// <list type="bullet">
///   <item>NOTHING SUPPLIED - the demo starts a real broker in a container, because that is what a
///     user actually runs.</item>
///   <item>AN ADDRESS SUPPLIED - the demo uses it and starts nothing. This is how the demo runs
///     INSIDE its own container, and it is not optional there: a demo container is never granted
///     the host Docker socket (plan unit U35), so it could not start a broker even if it wanted to.
///     It reaches a compose sibling on the demo's own network instead.</item>
/// </list>
/// The same door serves own-cluster mode, where the address is the user's real cluster - so nothing
/// here logs or echoes it.
/// </remarks>
internal sealed class DemoBroker : IAsyncDisposable
{
    /// <summary>
    /// The broker image, pinned as a literal.
    /// </summary>
    /// <remarks>
    /// The Java demo DERIVES this from the Kafka client on its own classpath (CP major = AK major +
    /// 4). .NET cannot: Confluent.Kafka's version tracks librdkafka, which has no such mapping to a
    /// broker release. So this is pinned, and it tracks the same two places the compose file beside
    /// it does - the root pom's <c>kafka.version</c>, through that mapping.
    /// </remarks>
    private const string BrokerImage = "confluentinc/cp-kafka:7.9.0";

    /// <summary>
    /// The key space the seeded records spread over. Ordering is UNORDERED in both arms, so this
    /// changes nothing today; it exists so that a key-ordered lane added later has more than one key
    /// to shard across, rather than needing the seeding rewritten first.
    /// </summary>
    private const int KeySpace = 1000;

    private readonly KafkaContainer? _container;

    private DemoBroker(string bootstrap, KafkaContainer? container)
    {
        Bootstrap = bootstrap;
        _container = container;
    }

    /// <summary>
    /// Where the broker is. NEVER PRINTED - see <see cref="DemoOptions.ToString"/> for why.
    /// </summary>
    public string Bootstrap { get; }

    /// <summary>Uses the supplied broker, or starts one when none was supplied.</summary>
    /// <param name="supplied">The address from <c>--bootstrap</c> or the environment, or null.</param>
    /// <param name="cancellationToken">Bounds the container start.</param>
    /// <returns>The broker.</returns>
    public static async Task<DemoBroker> ResolveAsync(string? supplied, CancellationToken cancellationToken)
    {
        if (!string.IsNullOrWhiteSpace(supplied))
        {
            // deliberately not logged: own-cluster mode puts a real address here
            Console.WriteLine("Using the broker supplied by the caller.");
            return new DemoBroker(NormaliseBootstrap(supplied.Trim()), null);
        }

        Console.WriteLine($"No broker supplied, starting one in a container: {BrokerImage}");
        var container = new KafkaBuilder(BrokerImage)
            // The demo forms one consumer group per arm, one after another, and the default
            // three-second settling delay would be charged to every one of them. The compose file
            // beside this module sets the same value for the same reason.
            .WithEnvironment("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "500")
            .Build();
        await container.StartAsync(cancellationToken).ConfigureAwait(false);
        return new DemoBroker(NormaliseBootstrap(container.GetBootstrapAddress()), container);
    }

    /// <summary>Creates the demo's topic, tolerating one a previous run already left behind.</summary>
    /// <param name="topic">The topic to create.</param>
    /// <param name="partitions">How many partitions it must have.</param>
    /// <returns>A task that completes once the topic exists with the requested partition count.</returns>
    public async Task EnsureTopicAsync(string topic, int partitions)
    {
        using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = Bootstrap })
            .Build();
        try
        {
            await admin.CreateTopicsAsync(
                    [new TopicSpecification { Name = topic, NumPartitions = partitions, ReplicationFactor = 1 }])
                .ConfigureAwait(false);
            Console.WriteLine(string.Create(
                CultureInfo.InvariantCulture, $"Created topic {topic} with {partitions} partitions"));
        }
        catch (CreateTopicsException failure)
            when (failure.Results.All(result => result.Error.Code == ErrorCode.TopicAlreadyExists))
        {
            // Reusing a topic silently is fine; reusing one with a DIFFERENT partition count is not,
            // because the effective-configuration block would print a --partitions value that never
            // applied - and that block is the demo's whole reproducibility promise.
            var existing = admin.GetMetadata(topic, TimeSpan.FromSeconds(30)).Topics.Single().Partitions.Count;
            if (existing != partitions)
            {
                throw new InvalidOperationException(string.Create(
                    CultureInfo.InvariantCulture,
                    $"topic {topic} already exists with {existing} partitions, but this run asked for {partitions} - pass --topic to name a fresh one, or --partitions {existing}"));
            }

            Console.WriteLine(string.Create(
                CultureInfo.InvariantCulture,
                $"Topic {topic} already exists with the requested {partitions} partitions, reusing it"));
        }
    }

    /// <summary>
    /// Produces the backlog both arms then replay.
    /// </summary>
    /// <remarks>
    /// Pre-produced rather than produced alongside the arms, and that is what makes the workload
    /// closed-loop - which is in turn why no arm reports latency.
    /// </remarks>
    /// <param name="topic">The topic to seed.</param>
    /// <param name="from">The first record ordinal to produce.</param>
    /// <param name="to">One past the last record ordinal to produce.</param>
    public void Seed(string topic, int from, int to)
    {
        if (to <= from)
        {
            return;
        }

        Console.WriteLine(string.Create(CultureInfo.InvariantCulture, $"Producing records {from} to {to}..."));

        // A delivery report that FAILED is not an exception at the call site, and a demo that
        // reported a full backlog while running its arms against a short one would print numbers
        // for a workload that never existed. So the first failure is captured and raised below.
        Error? firstFailure = null;
        using (var producer = new ProducerBuilder<byte[], byte[]>(
                   new ProducerConfig { BootstrapServers = Bootstrap, LingerMs = 20 }).Build())
        {
            for (var ordinal = from; ordinal < to; ordinal++)
            {
                var message = new Message<byte[], byte[]>
                {
                    Key = Encoding.UTF8.GetBytes(
                        string.Create(CultureInfo.InvariantCulture, $"key-{ordinal % KeySpace}")),
                    Value = Encoding.UTF8.GetBytes(
                        string.Create(CultureInfo.InvariantCulture, $"record-{ordinal}")),
                };
                Produce(producer, topic, message, report =>
                {
                    if (report.Error.IsError)
                    {
                        Interlocked.CompareExchange(ref firstFailure, report.Error, null);
                    }
                });
            }

            producer.Flush(TimeSpan.FromMinutes(2));
        }

        if (firstFailure is not null)
        {
            throw new InvalidOperationException(
                $"the demo could not seed its backlog: {firstFailure.Reason}");
        }

        Console.WriteLine(string.Create(CultureInfo.InvariantCulture, $"Produced {to - from} records"));
    }

    /// <summary>
    /// The Kafka properties each arm's consumer needs to reach this broker - the AK core arm builds
    /// its own consumer from them, and the sidecar arm sends them to the proxy, which builds one.
    /// </summary>
    /// <remarks>
    /// <c>enable.auto.commit</c> is here because Parallel Consumer owns offset commits and refuses a
    /// consumer with auto-commit on. The sidecar forces the setting itself whatever the map says, so
    /// on that path this line is redundant; the AK core arm needs it because Kafka's own default is
    /// true, and an auto-committing serial arm would be doing measurably less work than the arm it
    /// is the denominator of.
    /// </remarks>
    /// <param name="groupId">The consumer group for this arm and replay.</param>
    /// <returns>The properties, in the wire's own string-to-string shape.</returns>
    public Dictionary<string, string> ConsumerProperties(string groupId) => new(StringComparer.Ordinal)
    {
        ["bootstrap.servers"] = Bootstrap,
        ["group.id"] = groupId,
        ["auto.offset.reset"] = "earliest",
        ["enable.auto.commit"] = "false",
    };

    /// <summary>Stops the container, if this demo started one.</summary>
    /// <returns>A task that completes once the broker is gone.</returns>
    public async ValueTask DisposeAsync()
    {
        if (_container is not null)
        {
            await _container.DisposeAsync().ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Produces one record, waiting out a full local queue rather than failing on it.
    /// </summary>
    /// <remarks>
    /// librdkafka's produce is non-blocking and throws <c>Local_QueueFull</c> when its send buffer is
    /// full, where the Java producer would simply block. The big replay seeds tens of thousands of
    /// records in a tight loop, so this is reached in an ordinary run rather than under stress; the
    /// wait lets the background sender drain and retries the same record.
    /// </remarks>
    private static void Produce(
        IProducer<byte[], byte[]> producer,
        string topic,
        Message<byte[], byte[]> message,
        Action<DeliveryReport<byte[], byte[]>> onDelivered)
    {
        while (true)
        {
            try
            {
                producer.Produce(topic, message, onDelivered);
                return;
            }
            catch (ProduceException<byte[], byte[]> full) when (full.Error.Code == ErrorCode.Local_QueueFull)
            {
                producer.Poll(TimeSpan.FromMilliseconds(100));
            }
        }
    }

    /// <summary>
    /// Reduces a bootstrap address to the <c>host:port</c> list Kafka's own clients expect,
    /// whatever URL decoration it arrived with.
    /// </summary>
    /// <remarks>
    /// NOT COSMETIC, AND FINDING IT COST THE SIDECAR ARM A WHOLE RUN. Testcontainers for .NET hands
    /// its address back as a URI - measured, on this machine: <c>plaintext://127.0.0.1:62347/</c>,
    /// lower-cased scheme and TRAILING SLASH included. librdkafka accepts that string, so the AK
    /// core arm works with it untouched. The sidecar's consumer is a JAVA client, and Java's
    /// <c>bootstrap.servers</c> parser rejects it - the trailing slash above all, which no amount of
    /// scheme-stripping removes. The address travels to the proxy in <c>Configure</c>, so the arm
    /// died at session construction with a deliberately reason-free error (R48 withholds it, because
    /// a Kafka <c>ConfigException</c> embeds property values and those may be credentials).
    /// <para>
    /// Each comma-separated entry is normalised independently, so a caller's own multi-broker
    /// <c>--bootstrap</c> survives, and an entry that is already <c>host:port</c> is left exactly as
    /// it was rather than round-tripped through a parser that could rewrite it.
    /// </para>
    /// </remarks>
    private static string NormaliseBootstrap(string address) => string.Join(
        ',',
        address.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Select(entry => entry.Contains("://", StringComparison.Ordinal)
                ? string.Create(CultureInfo.InvariantCulture, $"{new Uri(entry).Host}:{new Uri(entry).Port}")
                : entry));
}
