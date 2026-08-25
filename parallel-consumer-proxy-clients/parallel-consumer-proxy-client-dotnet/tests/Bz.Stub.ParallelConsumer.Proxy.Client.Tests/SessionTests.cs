// Copyright (C) 2026 Antony Stubbs and contributors

using System.Collections.Concurrent;
using Xunit;

namespace Bz.Stub.ParallelConsumer.Proxy.Client.Tests;

/// <summary>
/// Wave one's whole claim: one record, end to end, against the real wire.
/// </summary>
public sealed class SessionTests
{
    /// <summary>Bounds the whole test, spawn and JVM startup included.</summary>
    private static readonly TimeSpan Deadline = TimeSpan.FromSeconds(90);

    /// <summary>
    /// How long the test watches for a second delivery after reporting success. This is a wait for
    /// an event that should never come, not a race against one that should - the harness's
    /// redelivery path is fast.
    /// </summary>
    private static readonly TimeSpan RedeliverySettle = TimeSpan.FromSeconds(3);

    /// <summary>
    /// The scenario name is the conformance suite's identity, so this test carries it verbatim.
    /// </summary>
    /// <remarks>
    /// The committed offset itself is engine state no client can see, and the harness has no
    /// verdict channel - it exits 0 whatever happened. So the client-side assertion is the
    /// wire-observable consequence: the record arrives once, the success report is followed by
    /// silence rather than a redelivery, and the session closes cleanly.
    /// </remarks>
    [Fact(DisplayName = "a-processed-record-advances-the-committed-offset")]
    public async Task AProcessedRecordAdvancesTheCommittedOffset()
    {
        const string scenario = Scenarios.ProcessedRecordAdvancesOffset;
        var harness = ConformanceHarness.ForScenario(scenario);

        using var deadline = new CancellationTokenSource(Deadline);

        await using var client = await ParallelConsumerClient.ConnectAsync(
            new ClientOptions
            {
                SidecarPath = harness.Path,
                SidecarArguments = harness.Arguments,

                // THE SCENARIO NAME IS ALSO THE TOPIC NAME - the harness seeds its records on the
                // topic it is named after.
                Topics = new[] { scenario },

                // The mock harness builds mock Kafka clients and reads no properties. Real
                // credentials never belong in a conformance test.
                KafkaProperties = new Dictionary<string, string>(),
                InstanceTag = "dotnet-client-wave-one",
            },
            deadline.Token);

        var session = client.Session;
        Assert.True(session.MaxConcurrency >= 1, $"effective max_concurrency was {session.MaxConcurrency}");
        Assert.True(session.ExecutorCount >= 1, $"effective executor_count was {session.ExecutorCount}");
        Assert.True(
            session.Negotiated(Capabilities.Dispatch),
            $"dispatch was not negotiated; the session's capabilities were [{string.Join(", ", session.Capabilities)}]");

        var seen = new ConcurrentQueue<InboundRecord>();
        var firstDelivery = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var polling = client.PollAsync(
            (record, _) =>
            {
                seen.Enqueue(record);
                firstDelivery.TrySetResult();
                return new ValueTask<Outcome>(Outcome.Succeed());
            },
            deadline.Token);

        await firstDelivery.Task.WaitAsync(deadline.Token);

        // A success is followed by silence. If the report had not landed, or had not been honoured,
        // the record would come back.
        await Task.Delay(RedeliverySettle, deadline.Token);

        // The client-initiated shutdown, then the session's own completion: stop hand-out, let the
        // executing records report, half-close, and reap the sidecar by closing its lifecycle pipe.
        await client.DisposeAsync();
        await polling;

        var delivered = seen.ToArray();
        Assert.Single(delivered);

        var record = delivered[0];
        Assert.Equal(scenario, record.Topic);
        Assert.Equal(1, record.Attempt);
        Assert.False(record.HasFailedBefore);
        Assert.Null(record.LastFailureReason);
        Assert.NotNull(record.Value);
        Assert.NotEmpty(record.Value!);
    }
}
