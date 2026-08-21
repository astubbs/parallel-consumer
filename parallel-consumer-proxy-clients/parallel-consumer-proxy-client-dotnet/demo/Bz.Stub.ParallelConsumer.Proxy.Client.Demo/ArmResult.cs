// Copyright (C) 2026 Antony Stubbs and contributors

namespace Bz.Stub.ParallelConsumer.Proxy.Client.Demo;

/// <summary>
/// What one arm achieved: how long it took, over how many records, and across how many keys.
/// </summary>
/// <remarks>
/// <see cref="Processed"/> and <see cref="UniqueKeys"/> are the two DETERMINISTIC figures in the
/// tables, and that is why they are here rather than only in a log line. Throughput alone cannot
/// show the work happened - a short arm is a failed arm rather than a fast one - and elapsed and
/// msg/s can never be compared between two languages on two machines, while these two must agree
/// exactly. <c>bin/ci-demo-conformance.sh</c> leans on precisely that.
/// </remarks>
/// <param name="Arm">The arm's label, as it appears in the tables - the role AND the client.</param>
/// <param name="Elapsed">Wall clock, measured from the first poll to the last outcome.</param>
/// <param name="Processed">How many records the arm's own function ran on.</param>
/// <param name="UniqueKeys">
/// How many distinct record keys the arm's own function saw. It shows the backlog was really spread
/// rather than one key repeated, which a record count alone cannot.
/// </param>
internal sealed record ArmResult(string Arm, TimeSpan Elapsed, int Processed, int UniqueKeys)
{
    /// <summary>
    /// Throughput, which is the only TIMING figure this demo reports.
    /// </summary>
    /// <remarks>
    /// The backlog is pre-produced, so the workload is closed-loop and a per-record latency would be
    /// flattered by however far an arm had fallen behind. Throughput is the honest number this shape
    /// can produce, so no arm reports any other timing.
    /// </remarks>
    public double RatePerSecond => Elapsed.TotalSeconds > 0 ? Processed / Elapsed.TotalSeconds : 0;
}
