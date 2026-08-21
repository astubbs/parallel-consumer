// Copyright (C) 2026 Antony Stubbs and contributors

namespace Bz.Stub.ParallelConsumer.Proxy.Client.Demo;

/// <summary>What one arm achieved: how long it took, and over how many records.</summary>
/// <param name="Arm">The arm's name, as it appears in the tables.</param>
/// <param name="Elapsed">Wall clock, measured from the first poll to the last outcome.</param>
/// <param name="Processed">How many records the arm's own function ran on.</param>
internal sealed record ArmResult(string Arm, TimeSpan Elapsed, int Processed)
{
    /// <summary>
    /// Throughput, which is the ONLY figure this demo reports.
    /// </summary>
    /// <remarks>
    /// The backlog is pre-produced, so the workload is closed-loop and a per-record latency would be
    /// flattered by however far an arm had fallen behind. Throughput is the honest number this shape
    /// can produce, so no arm reports anything else.
    /// </remarks>
    public double RatePerSecond => Elapsed.TotalSeconds > 0 ? Processed / Elapsed.TotalSeconds : 0;
}
