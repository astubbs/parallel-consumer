package io.confluent.parallelconsumer.streams;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import java.util.concurrent.atomic.AtomicLong;

/**
 * The dispatch marker, and the registration accounting that goes with it.
 * <p>
 * The interesting failures here are silent, so output correctness is not evidence that the seam
 * worked. Two ways a green test could lie:
 * <ul>
 *   <li>Records quietly took the <em>stock</em> path and produced perfect output - the seam was never
 *       exercised. {@link #getRecordsDispatchedToPool()} is incremented at exactly one place, the submission
 *       to the worker pool in {@link PcTaskDispatcher}, so a non-zero reading cannot be produced by any
 *       other route.</li>
 *   <li>Records were handed to {@code WorkManager} and silently dropped. {@code EpochAndRecordsMap}'s
 *       constructor skips any partition whose assignment epoch is null with nothing but a {@code log.warn},
 *       so a missing {@code onPartitionsAssigned} registers <em>zero</em> records and looks like an idle
 *       topology. {@link #getRecordsOfferedToWorkManager()} counts what went in and
 *       {@link #getRecordsAcceptedByWorkManager()} counts what survived; a gap between them is that bug.</li>
 * </ul>
 * Static for the same reason {@link PcDispatchSwitch} is - there is no seam through {@code KafkaStreams} to
 * hand a task a collaborator.
 *
 * @author Antony Stubbs
 */
public final class PcDispatchCounters {

    private static final AtomicLong RECORDS_OFFERED_TO_WORK_MANAGER = new AtomicLong();
    private static final AtomicLong RECORDS_ACCEPTED_BY_WORK_MANAGER = new AtomicLong();
    private static final AtomicLong RECORDS_DISPATCHED_TO_POOL = new AtomicLong();
    private static final AtomicLong RECORDS_COMPLETED_SUCCESSFULLY = new AtomicLong();
    private static final AtomicLong RECORDS_FAILED = new AtomicLong();
    private static final AtomicLong SPLIT_POLL_WAITS = new AtomicLong();
    private static final AtomicLong WAKES_ON_WORK = new AtomicLong();

    private PcDispatchCounters() {
    }

    /**
     * Records handed to {@code WorkManager.registerWork} by {@code StreamTask.addRecords}.
     */
    public static long getRecordsOfferedToWorkManager() {
        return RECORDS_OFFERED_TO_WORK_MANAGER.get();
    }

    /**
     * Records that survived {@code EpochAndRecordsMap}'s epoch filter. Must equal
     * {@link #getRecordsOfferedToWorkManager()}; anything less is the silent-drop bug.
     */
    public static long getRecordsAcceptedByWorkManager() {
        return RECORDS_ACCEPTED_BY_WORK_MANAGER.get();
    }

    /**
     * <b>The dispatch marker.</b> Incremented only where a {@code WorkContainer} is submitted to the worker
     * pool - so it is zero unless records genuinely travelled the PC path.
     */
    public static long getRecordsDispatchedToPool() {
        return RECORDS_DISPATCHED_TO_POOL.get();
    }

    public static long getRecordsCompletedSuccessfully() {
        return RECORDS_COMPLETED_SUCCESSFULLY.get();
    }

    public static long getRecordsFailed() {
        return RECORDS_FAILED.get();
    }

    /**
     * <b>The wake-on-work marker.</b> How many times a {@code StreamThread} took the split-wait branch -
     * short poll, then wait on {@link PcWorkSignal} - instead of blocking in {@code Consumer#poll()} for the
     * whole {@code poll.ms}.
     * <p>
     * Zero means the mechanism never ran, which makes any timing improvement in the same run somebody else's
     * doing. A benchmark that reads zero here has measured something other than what it claims to measure,
     * however good the number looks.
     */
    public static long getSplitPollWaits() {
        return SPLIT_POLL_WAITS.get();
    }

    /**
     * How many of those waits ended because work became available rather than because the budget ran out.
     * <p>
     * Deliberately a second field rather than a flag on the first: {@link #getSplitPollWaits()} answers "did
     * the code path run", this answers "did it help", and one counter cannot say both. A run with many split
     * waits and no wakes is the mechanism firing and never paying - a real finding, and invisible if the two
     * were merged.
     */
    public static long getWakesOnWork() {
        return WAKES_ON_WORK.get();
    }

    static void onOfferedToWorkManager(final long count) {
        RECORDS_OFFERED_TO_WORK_MANAGER.addAndGet(count);
    }

    static void onAcceptedByWorkManager(final long count) {
        RECORDS_ACCEPTED_BY_WORK_MANAGER.addAndGet(count);
    }

    static void onDispatchedToPool() {
        RECORDS_DISPATCHED_TO_POOL.incrementAndGet();
    }

    static void onCompletedSuccessfully() {
        RECORDS_COMPLETED_SUCCESSFULLY.incrementAndGet();
    }

    static void onFailed() {
        RECORDS_FAILED.incrementAndGet();
    }

    static void onSplitPollWait() {
        SPLIT_POLL_WAITS.incrementAndGet();
    }

    static void onWakeOnWork() {
        WAKES_ON_WORK.incrementAndGet();
    }

    /**
     * Tests reset before each scenario - the counters are process-wide, and surefire reuses forks.
     */
    public static void reset() {
        RECORDS_OFFERED_TO_WORK_MANAGER.set(0);
        RECORDS_ACCEPTED_BY_WORK_MANAGER.set(0);
        RECORDS_DISPATCHED_TO_POOL.set(0);
        RECORDS_COMPLETED_SUCCESSFULLY.set(0);
        RECORDS_FAILED.set(0);
        SPLIT_POLL_WAITS.set(0);
        WAKES_ON_WORK.set(0);
    }
}
