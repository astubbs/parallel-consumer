package bz.stub.parallelconsumer.sandbox;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.apache.kafka.common.annotation.InterfaceStability;

import java.time.Duration;
import java.util.Objects;

/**
 * When a sandbox run should stop generating and close: after so many records, after so long, or not at all.
 * <p>
 * <b>Reaching a bound closes the instance drain first</b> (R33, R17): the records already buffered are dispatched
 * and their offsets commit before the consumer goes, so what a test reads after the close is the end of the run
 * rather than the middle of it. Without a bound the run ends when its handle is closed, which is what an
 * interactive demo wants and what a test almost never does.
 */
@InterfaceStability.Unstable
public final class Bound {

    private static final Bound NONE = new Bound(-1, null);

    private final long records;

    private final Duration duration;

    private Bound(long records, Duration duration) {
        this.records = records;
        this.duration = duration;
    }

    /**
     * Generate until the handle is closed.
     */
    public static Bound none() {
        return NONE;
    }

    /**
     * Stop after this many records <b>in total</b>, across every topic the definition routes - not per topic,
     * which is how the rate is counted. A bound of one on a two-route definition therefore generates one record,
     * not two.
     */
    public static Bound afterRecords(long records) {
        if (records < 1) {
            throw new IllegalArgumentException("A record bound of " + records + " would generate nothing - use "
                    + "Bound.none() for an unbounded run");
        }
        return new Bound(records, null);
    }

    /**
     * Stop after this much wall-clock time from the first record.
     */
    public static Bound after(Duration duration) {
        Objects.requireNonNull(duration, "A duration must be supplied");
        if (duration.isNegative() || duration.isZero()) {
            throw new IllegalArgumentException("A duration bound of " + duration + " would generate nothing - use "
                    + "Bound.none() for an unbounded run");
        }
        return new Bound(-1, duration);
    }

    boolean isBounded() {
        return records > 0 || duration != null;
    }

    boolean reachedByCount(long generated) {
        return records > 0 && generated >= records;
    }

    boolean reachedByTime(long elapsedNanos) {
        return duration != null && elapsedNanos >= durationNanos();
    }

    private long durationNanos() {
        // Duration.toNanos overflows past ~292 years; nothing near that is a sandbox run, and the check is here
        // so that a nonsense bound fails loudly rather than wrapping into the past.
        return duration.toNanos();
    }

    @Override
    public String toString() {
        if (records > 0) {
            return "after " + records + " records";
        }
        if (duration != null) {
            return "after " + duration;
        }
        return "unbounded";
    }
}
