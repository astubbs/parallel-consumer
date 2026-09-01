package bz.stub.parallelconsumer.proxy.lifecycle;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Waits for work sitting in a foreign process before letting the engine close (KTD17, R12).
 *
 * <h2>Why the proxy needs a drain of its own</h2>
 *
 * Core's drain does not cover this engine, and the reason is structural rather than a bug to fix upstream.
 * {@code AbstractParallelEoSStreamProcessor.drain()} decides it may transition to closing from
 * {@code isRecordsAwaitingProcessing()}, which resolves to the shard queue plus whether the control thread is
 * done; {@code innerDoClose} then awaits only the worker thread pool. But {@code ExternalEngine} forces that
 * pool to a single thread which <b>returns the moment it has dispatched</b> - the record's real work is
 * happening in another process entirely. {@code WorkManager.hasWorkInFlight()} exists and is not consulted on
 * that path.
 * <p>
 * So a record sitting in a Python worker is counted by nothing core waits on, and an ordinary close would
 * walk straight past it. This class is the wait that does not exist upstream.
 *
 * <h2>What it will not do</h2>
 *
 * <b>It never invents an outcome for a record it has not heard about.</b> On timeout it closes anyway and
 * lets the ordinary path commit what genuinely resolved; everything else stays uncommitted and is redelivered
 * to whoever picks up the partition. Marking an unreported record as succeeded to make a shutdown tidy is
 * silent data loss, and marking it failed is a fabricated failure - redelivery is the only honest answer
 * available to a process that is going away.
 *
 * @author Antony Stubbs
 */
@Slf4j
public final class DrainCoordinator {

    /**
     * Everything the drain does to the world, in one seam. This is what lets the wait be tested without a
     * broker, a client, or a process to kill - and the negative control in the tests removes only the wait
     * while leaving every one of these behaviours identical.
     */
    public interface DrainTarget {

        /** Stop dispatching anything new, so the in-flight set can only shrink. */
        void stopAcceptingNewWork();

        /**
         * Tell the client to stop handing records to its workers and report what it already holds. Sent
         * BEFORE the wait: a client that has not been asked to wind down has no particular reason to hurry,
         * so waiting first would be waiting on reports nobody prompted.
         */
        void tellClientToShutDown();

        /** How many records are out with the client right now. */
        int foreignRecordsInFlight();

        /** Let the ordinary path commit what resolved and leave the group. */
        void closeEngineDrainingFirst();
    }

    /** Whether the foreign work came home, or the clock ran out on it. */
    public enum Outcome {
        /** Everything the client held was reported before the timeout. */
        DRAINED,
        /** The timeout fired with work still out; what resolved is committed, the rest will be redelivered. */
        TIMED_OUT
    }

    private final DrainTarget target;

    private final Duration drainTimeout;

    private final Duration pollInterval;

    private final AtomicBoolean drained = new AtomicBoolean();

    private volatile Outcome outcome;

    private DrainCoordinator(DrainTarget target, Duration drainTimeout, Duration pollInterval) {
        this.target = target;
        this.drainTimeout = drainTimeout;
        this.pollInterval = pollInterval;
    }

    public static DrainCoordinator of(DrainTarget target, Duration drainTimeout, Duration pollInterval) {
        return new DrainCoordinator(target, drainTimeout, pollInterval);
    }

    /**
     * Runs the drain, once. Repeat calls return the first outcome rather than closing the engine again -
     * shutdown can be reached from the watchdog and from an explicit request at the same moment, and a
     * double close is not a harmless retry.
     */
    public Outcome drain() {
        if (!drained.compareAndSet(false, true)) {
            log.debug("Drain already run; returning {}", outcome);
            return outcome;
        }

        target.stopAcceptingNewWork();
        target.tellClientToShutDown();

        outcome = awaitForeignWork();

        target.closeEngineDrainingFirst();
        return outcome;
    }

    private Outcome awaitForeignWork() {
        long deadline = System.nanoTime() + drainTimeout.toNanos();
        long pollMs = Math.max(1, pollInterval.toMillis());

        int outstanding = target.foreignRecordsInFlight();
        while (outstanding > 0) {
            if (System.nanoTime() >= deadline) {
                log.warn("Drain timed out after {} with {} record(s) still held by the client. What resolved "
                        + "is committed; the rest are left uncommitted and will be redelivered.",
                        drainTimeout, outstanding);
                return Outcome.TIMED_OUT;
            }
            try {
                Thread.sleep(pollMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("Drain interrupted with {} record(s) still held; leaving them for redelivery",
                        outstanding);
                return Outcome.TIMED_OUT;
            }
            outstanding = target.foreignRecordsInFlight();
        }

        log.info("Drain complete: the client returned everything it held");
        return Outcome.DRAINED;
    }
}
