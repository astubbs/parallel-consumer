// Copyright (C) 2026 Antony Stubbs and contributors

using Bz.Stub.ParallelConsumer.Proxy.Protocol.V1;

namespace Bz.Stub.ParallelConsumer.Proxy.Client;

/// <summary>
/// One Kafka record as the user's function sees it, plus the delivery state an in-process function
/// would have had.
/// </summary>
/// <remarks>
/// Keys and values are BYTES. The proxy does not deserialize and neither does this library:
/// deserialization belongs to the user's code, in the user's language.
/// <para>
/// Null and empty are different, deliberately, and both preserved: a null <see cref="Key"/> is a
/// null key and a null <see cref="Value"/> is a tombstone, neither of which is an empty array.
/// </para>
/// </remarks>
public sealed record InboundRecord
{
    /// <summary>The source topic.</summary>
    public string Topic { get; init; } = string.Empty;

    /// <summary>The source partition.</summary>
    public int Partition { get; init; }

    /// <summary>The record's offset in its partition.</summary>
    public long Offset { get; init; }

    /// <summary>The record's key, or <see langword="null"/> for a null key.</summary>
    public byte[]? Key { get; init; }

    /// <summary>The record's value, or <see langword="null"/> for a tombstone.</summary>
    public byte[]? Value { get; init; }

    /// <summary>
    /// Which delivery attempt this is: 1 on first delivery, 2 on the first redelivery. Product
    /// data, distinct from the fencing epoch - which also counts redeliveries that consumed no
    /// attempt.
    /// </summary>
    public int Attempt { get; init; }

    /// <summary>
    /// When this record last failed, or <see langword="null"/> on a first delivery. Presence is the
    /// wire's way of saying "this has failed before"; there is no zero timestamp to test for.
    /// </summary>
    public DateTimeOffset? LastFailureAt { get; init; }

    /// <summary>
    /// The previous failure's text, verbatim, or <see langword="null"/> on a first delivery. It is
    /// worker-supplied and may embed record payload: treat it as untrusted input.
    /// </summary>
    public string? LastFailureReason { get; init; }

    /// <summary>Whether this delivery follows a recorded failure.</summary>
    public bool HasFailedBefore => LastFailureAt is not null;

    internal static InboundRecord From(DispatchRecord dispatched)
    {
        var record = dispatched.Record;
        return new InboundRecord
        {
            Topic = record?.Topic ?? string.Empty,
            Partition = record?.Partition ?? 0,
            Offset = record?.Offset ?? 0,
            // HasKey / HasValue, not an emptiness test: proto3 explicit presence is what carries
            // the null-versus-empty distinction the wire exists to preserve.
            Key = record is { HasKey: true } ? record.Key.ToByteArray() : null,
            Value = record is { HasValue: true } ? record.Value.ToByteArray() : null,
            Attempt = dispatched.Attempt,
            LastFailureAt = dispatched.LastFailureAt?.ToDateTimeOffset(),
            LastFailureReason = dispatched.HasLastFailureReason ? dispatched.LastFailureReason : null,
        };
    }
}
