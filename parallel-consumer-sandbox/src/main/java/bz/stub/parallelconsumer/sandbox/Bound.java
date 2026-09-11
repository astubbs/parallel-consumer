package bz.stub.parallelconsumer.sandbox;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.apache.kafka.common.annotation.InterfaceStability;

import java.time.Duration;
import java.util.Objects;

/**
 * When a sandbox run should stop generating and close: after so many records, after so long, or not at all.
 *
 * <h2>Reaching a bound waits for every record to be accounted for, then closes drain first</h2>
 * Either bound - the count or the duration - ends the same way (R33, R17): the generator stops publishing, then
 * waits until every record it published is accounted for, and only then closes the instance. So what a test reads
 * after the close is the end of the run rather than the middle of it. Without a bound the run ends when its handle
 * is closed, which is what an interactive demo wants and what a test almost never does.
 * <p>
 * <b>The wait is on what the instance reports, and not on a drain</b>, because a drain-first close is not the same
 * thing as finishing: it transitions to closing once nothing is awaiting selection, while the worker pool may
 * still hold queued tasks, and the close then clears that queue.
 * {@link SandboxConsumer#awaitEveryPublishedRecordCommitted()} owns that reasoning, and the refusal the wait
 * raises when the run never gets there.
 * <p>
 * <b>A record that parks is accounted for too.</b> Parking is a terminal outcome - the record holds no worker and
 * is never retried - and its partition's committed offset stays at its own offset for good, so the wait counts a
 * partition done when its published records equal what the commit says is complete plus what is parked on it.
 * <b>A run that parks is therefore an ordinary bounded run</b>, which is what the README's own quickstart is; an
 * earlier version of this refused one and told the caller to use {@link #none()} instead (astubbs#504).
 */
@InterfaceStability.Unstable
public final class Bound {

    /**
     * The unbounded bound, shared because it holds nothing - see {@link #none()}.
     */
    private static final Bound NONE = new Bound(-1, null);

    /**
     * The record count to stop after, or -1 when this bound is not a count. A count rather than an
     * {@code OptionalLong} because this type is on the Java 8 release target and is compared on a hot enough path
     * to be worth staying primitive; -1 is the not-declared value because {@link #afterRecords(long)} refuses
     * anything below one.
     */
    private final long records;

    /**
     * The wall-clock span to stop after, or null when this bound is not a duration. The two fields are exclusive:
     * every factory below sets exactly one of them, which is why there is no third field saying which.
     */
    private final Duration duration;

    /**
     * Private because the three factories are the whole of the vocabulary - a bound is none, a count, or a
     * duration, and a caller assembling one from two raw values could ask for a fourth thing that has no meaning.
     */
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

    /**
     * Whether this bound will ever be reached. {@link Sandbox#awaitBound(Duration)} refuses an unbounded run
     * rather than waiting out a timeout that could never have been satisfied.
     */
    boolean isBounded() {
        return records > 0 || duration != null;
    }

    /**
     * Whether this many records reaches a count bound. Answers false for a duration bound and for none, so the
     * generator can ask both questions of every bound without asking first which kind it holds.
     *
     * @param generated records generated so far, across every topic
     */
    boolean reachedByCount(long generated) {
        return records > 0 && generated >= records;
    }

    /**
     * Whether this much elapsed time reaches a duration bound. False for a count bound and for none, for the same
     * reason as {@link #reachedByCount(long)}.
     *
     * @param elapsedNanos nanoseconds since the first record, measured as a difference of two
     *                     {@code System.nanoTime()} readings rather than against a wall clock
     */
    boolean reachedByTime(long elapsedNanos) {
        return duration != null && elapsedNanos >= durationNanos();
    }

    /**
     * The duration bound in nanoseconds, to compare against the generator's own elapsed-nanos reading.
     */
    private long durationNanos() {
        // No check of our own, deliberately: Duration.toNanos() THROWS ArithmeticException past about 292 years
        // rather than wrapping, so a nonsense bound already fails loudly and a guard here would only restate it.
        // An earlier version of this comment claimed a check that was never written.
        return duration.toNanos();
    }

    /**
     * Rendered into the run's opening log line and into the generator's "did not stop" error, so it reads as the
     * end of a sentence about what the run is doing.
     */
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
