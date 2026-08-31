// Copyright (C) 2026 Antony Stubbs and contributors

using Google.Protobuf;
using Bz.Stub.ParallelConsumer.Proxy.Protocol.V1;

namespace Bz.Stub.ParallelConsumer.Proxy.Client;

/// <summary>
/// A record the user's function asks the proxy to produce on success.
/// </summary>
/// <remarks>
/// Workers never touch Kafka themselves. Output rides the success report and the proxy produces it
/// with its own producer, before the input record's offset may become eligible to commit.
/// </remarks>
/// <param name="Topic">The destination topic.</param>
/// <param name="Key">The key, or <see langword="null"/> for a null key.</param>
/// <param name="Value">The value, or <see langword="null"/> for a tombstone.</param>
public sealed record OutboundRecord(string Topic, byte[]? Key = null, byte[]? Value = null)
{
    internal ProduceRecord ToProduceRecord()
    {
        var produce = new ProduceRecord { Topic = Topic };
        if (Key is not null)
        {
            produce.Key = ByteString.CopyFrom(Key);
        }

        if (Value is not null)
        {
            produce.Value = ByteString.CopyFrom(Value);
        }

        return produce;
    }
}

/// <summary>
/// What the user's function decided about one record: it succeeded (optionally producing records),
/// or it failed with a reason.
/// </summary>
/// <remarks>
/// C# has exceptions, so there are two spellings of failure and exactly one meaning: returning
/// <see cref="Fail"/> and throwing are both reported as a <c>Failure</c> outcome, and the
/// translation from a thrown exception happens once, in one place
/// (<see cref="ParallelConsumerClient"/>). Nothing else in this library catches a processor's
/// exception.
/// </remarks>
public readonly record struct Outcome
{
    private Outcome(string? failureReason, IReadOnlyList<OutboundRecord> produce)
    {
        FailureReason = failureReason;
        Produce = produce;
    }

    /// <summary>Whether the record was processed successfully.</summary>
    public bool IsSuccess => FailureReason is null;

    /// <summary>
    /// The failure text when this is a failure, otherwise <see langword="null"/>. It rides back on
    /// the redelivery as <see cref="InboundRecord.LastFailureReason"/> and reaches the proxy's
    /// logs: do not put record payload or credentials in it.
    /// </summary>
    public string? FailureReason { get; }

    /// <summary>Records the proxy should produce on success. Empty means produce nothing.</summary>
    public IReadOnlyList<OutboundRecord> Produce { get; }

    /// <summary>The record was processed. Nothing to produce.</summary>
    public static Outcome Succeed() => new(null, Array.Empty<OutboundRecord>());

    /// <summary>
    /// The record was processed, and the proxy should produce these records with its own producer.
    /// This is the only sanctioned route for worker output to Kafka.
    /// </summary>
    /// <param name="records">The records to produce.</param>
    public static Outcome SucceedProducing(params OutboundRecord[] records) =>
        new(null, records ?? Array.Empty<OutboundRecord>());

    /// <summary>
    /// The record failed. It returns to the engine's retry scheduling, exactly as an in-process
    /// user function throwing would leave it.
    /// </summary>
    /// <param name="reason">Why it failed. Worker-supplied text; see <see cref="FailureReason"/>.</param>
    public static Outcome Fail(string reason) =>
        new(reason ?? string.Empty, Array.Empty<OutboundRecord>());
}

/// <summary>
/// The user's function: take a record, do the work, say what happened.
/// </summary>
/// <remarks>
/// A thrown exception is translated to a failure outcome, so an application that already signals
/// failure by throwing needs no adapter; <see cref="Outcome.Fail"/> is for the case where failure
/// is an expected result rather than an exceptional one.
/// <para>
/// The <see cref="CancellationToken"/> is the application's, not a per-record deadline: this
/// protocol never builds one, because the lease is connection liveness rather than a time budget
/// for the user's function.
/// </para>
/// </remarks>
/// <param name="record">The record to process.</param>
/// <param name="cancellationToken">The application's cancellation token.</param>
/// <returns>The outcome to report.</returns>
public delegate ValueTask<Outcome> RecordProcessor(InboundRecord record, CancellationToken cancellationToken);
