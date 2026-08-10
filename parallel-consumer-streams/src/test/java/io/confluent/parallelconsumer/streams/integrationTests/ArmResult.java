package io.confluent.parallelconsumer.streams.integrationTests;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import io.confluent.parallelconsumer.streams.benchmark.LatencyDistribution;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * What one arm produced. Several statistics, deliberately, because which of them carries a claim is decided per
 * experiment and argued there - see
 * {@code docs/solutions/best-practices/choose-the-statistic-that-states-the-claim.md}.
 *
 * <h2>The three time figures, and why all three are kept</h2>
 * <ul>
 *   <li><b>Time to drain</b> is what an operator feels: topology RUNNING to the last record completed. It
 *       includes partition assignment and the first poll, which are real costs but are not the property under
 *       test, and whose share shrinks as the backlog deepens. Reported, never asserted on.</li>
 *   <li><b>Sustained catch-up rate</b> is the property: completions per second across the middle of the drain,
 *       discarding the first and last decile. The trim is not cosmetic - a cold JVM is still compiling in the
 *       first second, and Parallel Consumer's own {@code DynamicLoadFactor} deliberately does not scale for the
 *       first two seconds of any run, so an untrimmed rate would carry both of those into the answer.</li>
 *   <li><b>In-chain latency</b> is per-record, from entering the processor chain to completing. Under a
 *       saturated backlog it is mostly a restatement of the drain rate, so it is reported rather than
 *       asserted; under paced arrival it is the interesting one.</li>
 * </ul>
 *
 * @author Antony Stubbs
 */
public final class ArmResult {

    /**
     * Fraction of the drain discarded from each end before computing the sustained rate. A tenth at each end
     * removes JIT and the load-factor ramp at the head, and the ragged last-few-workers tail at the end,
     * without narrowing the window so far that it stops being a measurement of the run.
     */
    private static final double TRIM_FRACTION = 0.10d;

    private final String arm;
    private final boolean seamOn;
    private final int recordCount;
    private final int distinctKeys;
    private final long timeToDrainMillis;
    private final double sustainedRatePerSecond;
    private final LatencyDistribution inChainLatency;
    private final LatencyDistribution endToEndLatency;
    private final long recordsDispatchedToPool;
    private final long splitPollWaits;
    private final long wakesOnWork;
    private final long outputEndOffsetDelta;

    ArmResult(final String arm,
              final boolean seamOn,
              final int recordCount,
              final int distinctKeys,
              final long timeToDrainMillis,
              final double sustainedRatePerSecond,
              final LatencyDistribution inChainLatency,
              final LatencyDistribution endToEndLatency,
              final long recordsDispatchedToPool,
              final long splitPollWaits,
              final long wakesOnWork,
              final long outputEndOffsetDelta) {
        this.endToEndLatency = endToEndLatency;
        this.arm = arm;
        this.seamOn = seamOn;
        this.recordCount = recordCount;
        this.distinctKeys = distinctKeys;
        this.timeToDrainMillis = timeToDrainMillis;
        this.sustainedRatePerSecond = sustainedRatePerSecond;
        this.inChainLatency = inChainLatency;
        this.recordsDispatchedToPool = recordsDispatchedToPool;
        this.splitPollWaits = splitPollWaits;
        this.wakesOnWork = wakesOnWork;
        this.outputEndOffsetDelta = outputEndOffsetDelta;
    }

    public String getArm() {
        return arm;
    }

    public boolean isSeamOn() {
        return seamOn;
    }

    public int getRecordCount() {
        return recordCount;
    }

    public int getDistinctKeys() {
        return distinctKeys;
    }

    public long getTimeToDrainMillis() {
        return timeToDrainMillis;
    }

    public double getSustainedRatePerSecond() {
        return sustainedRatePerSecond;
    }

    public LatencyDistribution getInChainLatency() {
        return inChainLatency;
    }

    /**
     * Per-record latency from the moment the producer was handed the record to the moment its processing
     * completed - so it includes the wait for a free StreamThread, which {@link #getInChainLatency()} cannot
     * see.
     * <p>
     * That blind spot was not theoretical. A steady-state arm measured on in-chain latency alone reported
     * 0.99x and read as a null result, because once a record is running it costs the same in either arm. The
     * queueing that head-of-line blocking creates happens before the record enters the chain.
     */
    public LatencyDistribution getEndToEndLatency() {
        return endToEndLatency;
    }

    /**
     * The dispatch marker. Zero in a seam-on arm means records never travelled the PC path and the arm's
     * numbers belong to something else.
     */
    public long getRecordsDispatchedToPool() {
        return recordsDispatchedToPool;
    }

    public long getSplitPollWaits() {
        return splitPollWaits;
    }

    public long getWakesOnWork() {
        return wakesOnWork;
    }

    /**
     * How far the output topic's end offset moved while this arm ran, read from the broker rather than from
     * anything the test counted. The independent half of the drain-complete definition.
     */
    public long getOutputEndOffsetDelta() {
        return outputEndOffsetDelta;
    }

    /**
     * Completions per second over the trimmed middle of the drain.
     *
     * @param completionNanos every record's completion timestamp, in any order
     */
    static double sustainedRate(final List<Long> completionNanos) {
        List<Long> sorted = new ArrayList<>(completionNanos);
        Collections.sort(sorted);

        int trim = (int) Math.floor(sorted.size() * TRIM_FRACTION);
        int from = trim;
        int to = sorted.size() - trim - 1;
        if (to <= from) {
            // Too few samples to trim meaningfully - use the whole span rather than pretending to a window.
            from = 0;
            to = sorted.size() - 1;
        }

        long spanNanos = sorted.get(to) - sorted.get(from);
        if (spanNanos <= 0L) {
            return 0d;
        }
        // to - from records COMPLETED across the span; the record at `from` marks the window's start rather
        // than being counted inside it, which is what makes this a rate and not an off-by-one.
        return (to - from) / (spanNanos / 1_000_000_000d);
    }

    @Override
    public String toString() {
        return String.format("%s[seam=%s n=%d keys=%d drain=%dms sustained=%.1f/s dispatchedToPool=%d "
                        + "splitPollWaits=%d wakes=%d outputDelta=%d | %s]",
                arm, seamOn ? "ON" : "OFF", recordCount, distinctKeys, timeToDrainMillis,
                sustainedRatePerSecond, recordsDispatchedToPool, splitPollWaits, wakesOnWork,
                outputEndOffsetDelta, inChainLatency);
    }
}
