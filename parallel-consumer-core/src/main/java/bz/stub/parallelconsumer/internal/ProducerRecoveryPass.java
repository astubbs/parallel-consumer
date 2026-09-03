package bz.stub.parallelconsumer.internal;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.internal.utils.ThrowableUtils;
import bz.stub.parallelconsumer.state.PartitionState;
import com.facebook.infer.annotation.ThreadConfined;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.time.Instant;
import java.util.function.Consumer;
import java.util.function.IntSupplier;
import java.util.function.Supplier;

/**
 * The control thread's recovery from a producer the broker reported invalid, run at the top of every pass (KTD4):
 * when a condition is recorded, take the producer write lock, abort and discard the producer, drain the mailbox so
 * every result of the aborted transaction is accounted for, replay the work that transaction discarded (KTD5),
 * release the lock, and then - outside it - build and initialise the replacement (KTD7). A replacement that cannot
 * be built yet is retried on a later pass with backoff; one that can never be built ends the instance, naming the
 * transactional id. Nothing thrown here may escape: the supervisor treats an exception escaping the control loop as
 * fatal, which is the outcome this exists to avoid.
 * <p>
 * Skipped once the instance is CLOSING or CLOSED: a close during an outage would otherwise wait on a rebuild that
 * blocks up to {@code max.block.ms} for a producer nobody will use, and {@code ProducerManager.close} releases the
 * parked workers on its own. DRAINING still recovers, because a drain needs a producer to finish the work in flight.
 * <p>
 * Its own class rather than a method of the processor, so the two halves of a recovery read side by side: the
 * lock-and-replay step here, which needs the processor's mailbox and state, and the replacement in
 * {@link ProducerRecovery}, which needs the producer. The processor hands it the three things it needs and nothing
 * else.
 */
@Slf4j
@RequiredArgsConstructor
class ProducerRecoveryPass<K, V> {

    private final ProducerRecovery<K, V> recovery;

    /** The instance's current state, read on every pass. */
    private final Supplier<State> state;

    /**
     * The write-locked step: drain the mailbox, then put the ledger's records back into processing. Returns how
     * many records the replay put back.
     */
    private final IntSupplier replayWorkDiscardedByAbortedTransaction;

    /** Records the failure the instance ends with, if none is recorded yet, and moves it to CLOSING. */
    private final Consumer<Exception> closeWith;

    /**
     * Visible for testing - the state gate is driven directly through the processor, because the window between
     * CLOSING and the manager closing is on the control thread only.
     */
    @ThreadConfined(PartitionState.CONTROL_THREAD)
    void run() {
        State current = state.get();
        if (current == State.CLOSING || current == State.CLOSED) {
            log.debug("Not recovering the producer: the instance is {}", current);
            return;
        }
        if (!recovery.isRecoveryAttemptDue(Instant.now())) {
            return;
        }
        try {
            if (recovery.pendingInvalidation().isPresent() || recovery.isReplayOwed()) {
                boolean entered;
                try {
                    entered = recovery.beginReplacement();
                } catch (InterruptedException wokeUp) {
                    // This thread's own wake-up, not a stop signal: notifySomethingToDo interrupts the control thread
                    // whenever the write lock is not HELD, and it is not held while this pass is waiting for it - a
                    // worker holding the produce lock through its user function keeps the wait open for up to the
                    // commit-lock timeout, and the rebalance that fenced the producer ends with onPartitionsAssigned,
                    // which notifies. Shutdown travels in the state, never in the flag. Clear it and return: the
                    // condition stays recorded, so the next pass retries. Same shape as the mailbox poll's own catch.
                    Thread.interrupted();
                    log.debug("Interrupted while waiting for the producer write lock to begin recovery - a wake-up, not a " +
                            "shutdown; the condition stays recorded and the next pass retries");
                    return;
                }
                if (!entered) {
                    return; // the wait elapsed; a retry is scheduled and the condition stays recorded
                }
                try {
                    int restored = replayWorkDiscardedByAbortedTransaction.getAsInt();
                    recovery.replayCompleted(restored); // only now: a throw above leaves the replay owed, and the next pass runs it first
                } finally {
                    recovery.releaseCommitLockAfterReplacement();
                }
            }
            ReplacementOutcome outcome = recovery.completeReplacement();
            if (outcome.isTerminal()) {
                closeWith.accept(outcome.getFailure());
            }
        } catch (RuntimeException e) {
            log.error("Producer recovery pass failed unexpectedly; it will be attempted again on a later pass: {}",
                    ThrowableUtils.describeWithRootCause(e), e);
            recovery.deferAfterFailedPass("the recovery pass failed with " + e.getClass().getName());
        } catch (Error fatal) {
            // Not retried, and not left to the supervisor either: its catch is Exception, so an Error would leave
            // the instance RUNNING with every worker parked on the produce lock for good. Leave RUNNING first - that
            // is what releases them - and record why, then let it go.
            closeWith.accept(new PCInternalRuntimeException("Producer recovery failed with an Error; the instance is closing", fatal));
            throw fatal;
        }
    }
}
