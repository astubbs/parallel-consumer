package bz.stub.parallelconsumer.internal;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.internal.utils.ThrowableUtils;
import bz.stub.parallelconsumer.metrics.PCMetrics;
import bz.stub.parallelconsumer.metrics.PCMetricsDef;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.micrometer.core.instrument.Tag;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

import static bz.stub.parallelconsumer.internal.utils.StringUtils.msg;

/**
 * The replacement half of producer recovery, beside {@link ProducerManager}, which keeps the locks, the producer and
 * the commit. This owns what the broker reported, whether a usable producer exists, when the next attempt may run,
 * and the two steps that replace the producer: abort-and-discard under the manager's write lock, then
 * build-and-initialise outside it (KTD4, KTD7). {@link ProducerRecoveryPass} drives those two steps from the control
 * thread, with the processor's drain-and-replay between them; the manager consults this on every produce and commit
 * to decide whether to park, proceed or unwind.
 * <p>
 * Only the manager constructs one, and hands it back through {@link ProducerManager#recovery()}.
 */
@Slf4j
public class ProducerRecovery<K, V> {

    /**
     * Whether a usable producer exists, and what the produce and commit paths do when none does (KTD7).
     */
    public enum Availability {
        /** A producer is initialised and in use. */
        AVAILABLE,
        /**
         * The broker reported the producer invalid and a replacement is owed: no new work is handed out, no commit
         * is attempted, and a worker reaching the produce lock waits here instead of timing out.
         */
        REPLACING,
        /** No producer will ever be available again - a terminal build failure, or the manager is closed. */
        TERMINAL
    }

    private final ProducerManager<K, V> manager;
    private final ParallelConsumerOptions<K, V> options;
    private final PCMetrics pcMetrics;

    /**
     * How a replacement producer is built, present only where PC built the producer itself. Its presence is
     * {@link #canRecover()}: the single gate every detection site consults before recording a condition.
     */
    private final Optional<ReplacementProducerSource<K, V>> replacementProducerSource;

    /**
     * Guards the availability state and its schedule. A plain monitor: held only for the state reads and writes
     * below, never while acquiring or waiting on the manager's transaction lock, and never across a client call -
     * detection records under it and returns, recovery takes the write lock first and touches this only to change
     * state. That ordering is what makes a worker that detects while holding the produce read lock unable to
     * deadlock against the control thread's recovery.
     * <p>
     * <b>Cleared suspicion, 2026-09-02: no lock-ordering cycle between this monitor and the transaction lock.</b>
     * Suspected because a worker calls {@link #recordInvalidation} while holding the produce read lock, and recovery
     * needs the write lock. The discriminator is what each holder DOES while holding: every {@code synchronized}
     * block on this monitor performs field reads and writes and a {@code wait}/{@code notifyAll}, and none of them
     * touches the transaction lock, so there is no edge for a cycle to close on. What would reopen it: a lock
     * acquisition or client call added inside one of these blocks. No gate checks that; {@code @GuardedBy} keeps the
     * fields under the monitor but cannot see what else a block does.
     */
    private final Object availabilityMonitor = new Object();

    @GuardedBy("availabilityMonitor")
    private Availability availability = Availability.AVAILABLE;

    /** When the next recovery attempt may run; {@link Instant#EPOCH} when it may run at once. */
    @GuardedBy("availabilityMonitor")
    private Instant nextRecoveryAttemptAt = Instant.EPOCH;

    /** Replacement builds that failed retriably since the last successful one; sets the backoff. */
    @GuardedBy("availabilityMonitor")
    private int failedReplacementAttempts = 0;

    /**
     * Recoveries completed with no successful commit between them (R24). Read by the control thread when a condition
     * is recorded and by whichever thread commits - the poll thread does, through the revoke-path commit - so atomic.
     */
    private final AtomicInteger consecutiveRecoveriesWithoutCommit = new AtomicInteger();

    /** The condition the recovery in progress is answering; for the log line and the failure that names it. */
    private volatile Throwable conditionUnderRecovery;

    /**
     * Ends the produce-lock wait when the processor has left RUNNING or PAUSED, so a close during an outage releases
     * the parked workers at once rather than after the shutdown timeout. Supplied by the processor; false until then.
     */
    private volatile BooleanSupplier suspensionEndsWhen = () -> false;

    /** How often a parked worker re-checks whether the processor is shutting down. */
    static final Duration SUSPENSION_POLL = Duration.ofMillis(100);

    // test hooks: package-private so a test can pace the backoff without a 1 s floor
    volatile Duration recoveryBackoffInitial = ProducerRecoveryPolicy.RECOVERY_BACKOFF_INITIAL;
    volatile Duration recoveryBackoffMax = ProducerRecoveryPolicy.RECOVERY_BACKOFF_MAX;

    /**
     * The first recoverable condition observed since the last recovery, from whichever thread observed it. Only the
     * first is kept: recovery answers all of them the same way, and the first is the one that names the cause.
     * Cleared by the recovery that consumes it.
     */
    private final AtomicReference<Throwable> pendingInvalidation = new AtomicReference<>();

    /**
     * Whether the replay of the work the aborted transaction discarded (KTD5) is still owed: set by
     * {@link #beginReplacement()} in the same step that consumes the pending condition, cleared by
     * {@link #replayCompleted(int)} only after the drain and the replay have returned normally. Between the two the
     * ledger is intact but nothing else records that it has not been replayed; without this flag a listener
     * throwing inside the drain left the next pass to build the replacement straight away, and the next commit
     * trimmed the ledger for output the broker never saw.
     * <p>
     * Written by the control thread only - both writers run inside the recovery pass. Volatile rather than
     * monitor-guarded so a reader on another thread (a test, an operator's diagnostic) sees the current value
     * without an ordering to reason about; no decision is made on it from any thread but the writer's.
     */
    private volatile boolean replayOwed;

    /**
     * How many replays have put discarded work back into the shards. A worker dispatched before a replay and
     * reaching the produce lock after it would produce its record ahead of the restored, lower offsets in the same
     * ordered shard, so the manager's produce-lock acquisition refuses it and the batch re-queues behind them (the
     * review of astubbs#410, finding 5). Stamped on every batch at dispatch by the control thread, the thread that
     * also runs the replay, so the two are totally ordered; compared under the monitor, where the replay increments
     * it.
     */
    @GuardedBy("availabilityMonitor")
    private long replayGeneration;

    ProducerRecovery(ProducerManager<K, V> manager,
                     ParallelConsumerOptions<K, V> options,
                     PCMetrics pcMetrics,
                     Optional<ReplacementProducerSource<K, V>> replacementProducerSource) {
        this.manager = manager;
        this.options = options;
        this.pcMetrics = pcMetrics;
        this.replacementProducerSource = replacementProducerSource;
        pcMetrics.gaugeFromMetricDef(PCMetricsDef.PRODUCER_CONSECUTIVE_RECOVERIES, this, ProducerRecovery::getConsecutiveRecoveriesWithoutCommit);
    }

    /**
     * Parks the calling worker while the producer is being replaced. A timed loop rather than a plain wait so the
     * shutdown signal is noticed without anyone having to notify for it.
     */
    void awaitProducerAvailable() {
        synchronized (availabilityMonitor) {
            while (availability == Availability.REPLACING) {
                if (suspensionEndsWhen.getAsBoolean()) {
                    throw new ProducerInvalidatedException("Producing was suspended while the producer was being replaced, " +
                            "and the processor is shutting down: this record cannot be produced now", conditionUnderRecovery);
                }
                try {
                    availabilityMonitor.wait(SUSPENSION_POLL.toMillis());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new ProducerInvalidatedException("Interrupted while waiting for a replacement producer", e);
                }
            }
            if (availability == Availability.TERMINAL) {
                throw new ProducerInvalidatedException("No usable producer: the replacement failed terminally or the " +
                        "producer manager is closed, so this record cannot be produced", conditionUnderRecovery);
            }
        }
    }

    /**
     * @return the condition the recovery in progress is answering, for the failure that names it; null before the
     *         first
     */
    Throwable conditionUnderRecovery() {
        return conditionUnderRecovery;
    }

    /**
     * The one detect-and-record step both entry points share - the produce path in
     * {@link bz.stub.parallelconsumer.ParallelEoSStreamProcessor} and the commit path in the manager. A recoverable
     * condition found in {@code failure}'s cause chain, on a producer PC can rebuild, is recorded and handed back as
     * the exception that unwinds the caller; anything else is the caller's to treat as before.
     *
     * @return the exception to unwind with, or empty when {@code failure} is not a recoverable condition or this
     *         producer cannot be recovered
     */
    public Optional<ProducerInvalidatedException> recordIfRecoverable(Throwable failure) {
        Optional<Throwable> condition = RecoverableProducerCondition.find(failure);
        if (condition.isPresent() && canRecover()) {
            recordInvalidation(condition.get());
            return Optional.of(new ProducerInvalidatedException(condition.get()));
        }
        return Optional.empty();
    }

    /**
     * @return true where PC built the producer and so can build another, and the commit mode is the transactional
     *         one whose control loop performs the recovery; false on the producer-instance path, and in the
     *         consumer-commit modes, where PC's producer is non-transactional and there is nothing to recover
     */
    public boolean canRecover() {
        // Both halves, because detection runs on every produce and commit path while recovery runs only from the
        // transactional commit loop. With the first half alone, a PC-built producer in a consumer-commit mode that
        // met a condition in the recoverable set was recorded as REPLACING, nothing ever replaced it, and every
        // worker parked on the produce lock for the life of the instance. Found by the simplify pass's efficiency
        // reviewer as a poll that could never converge.
        return replacementProducerSource.isPresent() && options.isUsingTransactionCommitMode();
    }

    /**
     * @return true while a producer is initialised and in use
     */
    public boolean isProducerAvailable() {
        synchronized (availabilityMonitor) {
            return availability == Availability.AVAILABLE;
        }
    }

    /**
     * @return the current replay generation, for the control thread to stamp on a batch at dispatch
     */
    public long replayGeneration() {
        synchronized (availabilityMonitor) {
            return replayGeneration;
        }
    }

    /**
     * @return true between {@link #beginReplacement()} and {@link #replayCompleted(int)}: the aborted transaction's
     *         work has not yet been put back, and no replacement may be built until it has
     */
    public boolean isReplayOwed() {
        return replayOwed;
    }

    /**
     * The caller's drain and replay returned normally. Control thread only, under the write lock still.
     *
     * @param restored how many records the replay put back; a replay that put back none moved no offset below any
     *                 record in flight, so the generation - and with it every parked worker - is left alone
     */
    public void replayCompleted(int restored) {
        replayOwed = false;
        if (restored > 0) {
            synchronized (availabilityMonitor) {
                replayGeneration++;
            }
        }
    }

    /**
     * A recovery pass failed outside the replacement build - in the drain or the replay - and will be retried:
     * paced by the same backoff a failed build gets, so a listener that throws on every drain does not spin the
     * control loop.
     */
    public void deferAfterFailedPass(String why) {
        scheduleRetry(why);
    }

    /**
     * @return true while the broker has reported the producer invalid and its replacement has not yet been published
     */
    public boolean isReplacing() {
        synchronized (availabilityMonitor) {
            return availability == Availability.REPLACING;
        }
    }

    /**
     * @return recoveries completed since the last successful commit - the signal R24 asks for
     */
    public int getConsecutiveRecoveriesWithoutCommit() {
        return consecutiveRecoveriesWithoutCommit.get();
    }

    /** A commit succeeded on the current producer: the run of recoveries without progress is over. */
    void commitSucceeded() {
        consecutiveRecoveriesWithoutCommit.set(0);
    }

    /** The manager is closing: nothing will be available again, and every parked worker is released. */
    void markTerminal() {
        synchronized (availabilityMonitor) {
            availability = Availability.TERMINAL;
            availabilityMonitor.notifyAll();
        }
    }

    /**
     * The processor tells recovery how to notice a shutdown, so a worker parked on the produce lock during an
     * outage is released when the processor leaves RUNNING or PAUSED.
     */
    public void setSuspensionEndsWhen(BooleanSupplier shuttingDown) {
        this.suspensionEndsWhen = shuttingDown;
    }

    /**
     * Records that the broker has reported the producer invalid. Any thread may call this - a worker from a send
     * future, the control thread from a commit, the poll thread from the revoke-path commit - and none of them
     * blocks: recovery is the control thread's, on its next pass. Only the first condition since the last recovery is
     * kept.
     */
    public void recordInvalidation(Throwable condition) {
        boolean recorded = pendingInvalidation.compareAndSet(null, condition);
        synchronized (availabilityMonitor) {
            if (availability == Availability.AVAILABLE) {
                // the window in which the producer is known-invalid but work is still handed to it closes here, on
                // the detecting thread, not on the control thread's next pass (KTD7)
                availability = Availability.REPLACING;
                int consecutive = consecutiveRecoveriesWithoutCommit.get();
                // the first recovery in a run happens at once; the ones after it, with no successful commit
                // between, are paced so a rebuild-then-refence loop does not run at the commit cadence
                nextRecoveryAttemptAt = consecutive == 0 ? Instant.EPOCH : Instant.now().plus(ProducerRecoveryPolicy.backoffFor(consecutive, recoveryBackoffInitial, recoveryBackoffMax));
            }
        }
        if (recorded) {
            log.debug("Recorded producer invalidation for recovery: {}", condition.toString());
        }
    }

    /**
     * @return true when the control thread should run a recovery pass now - a condition is recorded or a deferred
     *         replacement is due
     */
    public boolean isRecoveryAttemptDue(Instant now) {
        synchronized (availabilityMonitor) {
            return availability == Availability.REPLACING && !now.isBefore(nextRecoveryAttemptAt);
        }
    }

    /**
     * @return how long until the next recovery attempt may run, while one is owed; empty otherwise. The control loop
     *         caps its mailbox wait on this, so it wakes for the attempt with every worker parked.
     */
    public Optional<Duration> timeUntilNextRecoveryAttempt(Instant now) {
        synchronized (availabilityMonitor) {
            if (availability != Availability.REPLACING) {
                return Optional.empty();
            }
            Duration until = Duration.between(now, nextRecoveryAttemptAt);
            return Optional.of(until.isNegative() ? Duration.ZERO : until);
        }
    }

    private void scheduleRetry(String why) {
        Duration delay;
        synchronized (availabilityMonitor) {
            failedReplacementAttempts++;
            delay = ProducerRecoveryPolicy.backoffFor(failedReplacementAttempts, recoveryBackoffInitial, recoveryBackoffMax);
            nextRecoveryAttemptAt = Instant.now().plus(delay);
        }
        log.warn("Producer recovery deferred for {}: {}", delay, why);
    }

    /**
     * The first half of a recovery, under the manager's write lock (KTD4): abort what can be aborted, close the
     * invalidated producer, and leave the manager with no producer. The caller drains its mailbox and replays the
     * discarded work before calling {@link #releaseCommitLockAfterReplacement()}, then {@link #completeReplacement()}
     * outside the lock. The write lock is entered by waiting on it directly, never through the manager's
     * commit-lock acquisition and its not-safe-for-multi-threaded-access guard: the revoke-path commit holds this
     * lock during the very rebalance that fences a producer, so the two are correlated and that guard would throw
     * exactly when recovery is most needed.
     *
     * @return true when the lock was entered and the producer discarded; false when the wait elapsed, in which case
     *         a retry is scheduled and nothing changed
     */
    public boolean beginReplacement() throws InterruptedException {
        Throwable condition = pendingInvalidation.get();
        Duration lockTimeout = options.getCommitLockAcquisitionTimeout();
        boolean entered = manager.getProducerTransactionLock().writeLock().tryLock(lockTimeout.toMillis(), TimeUnit.MILLISECONDS);
        if (!entered) {
            scheduleRetry(msg("the write lock was held by another thread for the whole {} wait", lockTimeout));
            return false;
        }
        try {
            if (condition != null) {
                // absent on a pass that re-enters only to finish an owed replay: keep the label of the condition it answers
                conditionUnderRecovery = condition;
            }
            pendingInvalidation.set(null); // consumed: a condition recorded from here on belongs to the next recovery
            replayOwed = true; // and stays owed until the caller's drain and replay have returned normally
            ProducerWrapper<K, V> discarded = manager.discardProducer();
            if (discarded != null) {
                abortQuietly(discarded);
                closeQuietly(discarded, "the invalidated producer");
            }
            return true;
        } catch (RuntimeException | Error unexpected) {
            // Error included, not only RuntimeException: an OutOfMemoryError from the abort or the close would
            // otherwise leave the write lock held for good, and the close that follows an Error commits under that
            // very lock - so the instance would hang in shutdown instead of ending. The review that asked for this
            // decision is on astubbs#225's PR; the lock is released, the Error still ends the instance.
            manager.releaseCommitLock();
            throw unexpected;
        }
    }

    public void releaseCommitLockAfterReplacement() {
        manager.releaseCommitLock();
    }

    private void abortQuietly(ProducerWrapper<K, V> discarded) {
        try {
            discarded.abortTransaction();
            log.debug("Aborted the open transaction on the invalidated producer");
        } catch (RuntimeException e) {
            // expected for a fenced producer: kafka-clients' beginAbort rethrows the fatal error, and the broker has
            // already aborted the transaction. Kafka Streams swallows exactly this in StreamsProducer.abortTransaction.
            log.debug("Abort on the invalidated producer threw, as a fenced producer's does; the broker has already aborted it: {}", e.toString());
        }
    }

    private void closeQuietly(ProducerWrapper<K, V> discarded, String what) {
        try {
            discarded.close(ProducerRecoveryPolicy.DISCARDED_PRODUCER_CLOSE_TIMEOUT);
        } catch (RuntimeException e) {
            log.warn("Closing {} failed within {}; continuing with the recovery: {}", what, ProducerRecoveryPolicy.DISCARDED_PRODUCER_CLOSE_TIMEOUT, e.toString());
        }
    }

    /**
     * The second half of a recovery, outside the write lock and under {@link Availability#REPLACING} (KTD7): build
     * the replacement through the source and initialise its transactions, which fences the producer it replaces.
     * Both block up to {@code max.block.ms}, which is why this runs outside the lock the revoke callback may be
     * waiting on. A retriable failure schedules the next attempt with backoff; {@code AuthorizationException} and
     * {@code UnsupportedVersionException} are terminal. Failures are reported through the exception type and the
     * {@code transactional.id} only, never the raw cause message - a {@code ConfigException} embeds the offending
     * configuration value (R7).
     */
    public ReplacementOutcome completeReplacement() {
        ReplacementProducerSource<K, V> source = replacementProducerSource.orElseThrow(() ->
                new IllegalStateException("Bug: recovery attempted on the producer-instance path, where canRecover() is false"));
        if (replayOwed) {
            // the caller's drain or replay threw after beginReplacement consumed the condition; building now would
            // let the next commit trim a ledger that was never put back. The next pass re-enters the lock and
            // replays first.
            scheduleRetry("the replay of the work the aborted transaction discarded is still owed; it runs first on the next pass");
            return new ReplacementOutcome(ReplacementOutcome.Kind.DEFERRED, null);
        }
        int attempt;
        synchronized (availabilityMonitor) {
            attempt = failedReplacementAttempts + 1;
        }
        String condition = conditionUnderRecovery == null ? "unknown" : conditionUnderRecovery.getClass().getSimpleName();
        // declared outside the try so a replacement that was built but failed to initialise can be closed: nothing
        // else holds a reference to it, and each leaked KafkaProducer keeps its network thread
        ProducerWrapper<K, V> replacement = null;
        int consecutive;
        try {
            replacement = source.build();
            replacement.initTransactions();
            manager.publishProducer(replacement);
            consecutive = consecutiveRecoveriesWithoutCommit.incrementAndGet();
            synchronized (availabilityMonitor) {
                availability = Availability.AVAILABLE;
                failedReplacementAttempts = 0;
                nextRecoveryAttemptAt = Instant.EPOCH;
                availabilityMonitor.notifyAll();
            }
        } catch (RuntimeException failure) {
            if (replacement != null) {
                closeQuietly(replacement, "the replacement that failed to initialise");
            }
            String failureType = describeType(failure);
            if (ProducerRecoveryPolicy.isTerminalBuildFailure(failure)) {
                synchronized (availabilityMonitor) {
                    availability = Availability.TERMINAL;
                    availabilityMonitor.notifyAll();
                }
                var terminal = new ProducerInvalidatedException(msg(
                        "The replacement producer for transactional.id '{}' cannot be built or initialised ({}), and " +
                                "retrying cannot fix that - check the TransactionalId ACL for the prefix this id carries",
                        source.getTransactionalId(), failureType), ProducerRecoveryPolicy.sanitised(failure));
                log.error("Producer recovery terminal: condition {}, attempt {}: {}", condition, attempt, terminal.getMessage());
                return new ReplacementOutcome(ReplacementOutcome.Kind.TERMINAL, terminal);
            }
            scheduleRetry(msg("building or initialising the replacement for transactional.id '{}' failed with {} (attempt {})",
                    source.getTransactionalId(), failureType, attempt));
            return new ReplacementOutcome(ReplacementOutcome.Kind.DEFERRED, null);
        }
        // The replacement is published and in use from here on, whatever the record-keeping below does. The counter
        // is the user's MeterRegistry - third-party code that has thrown from inside PC before
        // (docs/solutions/runtime-errors/a-throwing-meter-registry-kills-the-poll-thread-and-strands-close.md) - and
        // a throw from it inside the try above turned a completed replacement into a "deferred" outcome that
        // scheduled a second rebuild against a producer already serving traffic.
        try {
            pcMetrics.getCounterFromMetricDef(PCMetricsDef.PRODUCER_RECOVERIES, Tag.of("condition", condition)).increment();
        } catch (RuntimeException registryThrew) {
            log.warn("The MeterRegistry threw while recording a producer recovery; the recovery itself is complete: {}", registryThrew.toString());
        }
        logRecovery(condition, "replaced", attempt, consecutive, source.getTransactionalId());
        return new ReplacementOutcome(ReplacementOutcome.Kind.REPLACED, null);
    }

    /**
     * The failure's class, and its root cause's where that differs - a factory's failure arrives wrapped as
     * {@link bz.stub.parallelconsumer.ExceptionInUserFunctionException}, which names nothing on its own. Types only,
     * never messages: a {@code ConfigException}'s message carries the offending configuration value (R7).
     */
    private static String describeType(Throwable failure) {
        String type = failure.getClass().getName();
        Optional<Throwable> root = ThrowableUtils.innermostInCauseChain(failure, ignored -> true);
        if (root.isPresent() && root.get() != failure) {
            return type + " (root cause " + root.get().getClass().getName() + ")";
        }
        return type;
    }

    /**
     * The R22 record of a recovery: what the broker said, what PC did, and how many times in a row.
     */
    private void logRecovery(String condition, String outcome, int attempt, int consecutiveRecoveries, String transactionalId) {
        if (consecutiveRecoveries > 1) {
            log.error("Producer recovery {}: condition {}, attempt {}, transactional.id '{}', {} consecutive recoveries with no " +
                            "successful commit between them - the instance is alive but not progressing",
                    outcome, condition, attempt, transactionalId, consecutiveRecoveries);
        } else {
            log.warn("Producer recovery {}: condition {}, attempt {}, transactional.id '{}'", outcome, condition, attempt, transactionalId);
        }
    }

    /**
     * @return the condition recovery is owed, if any
     */
    public Optional<Throwable> pendingInvalidation() {
        return Optional.ofNullable(pendingInvalidation.get());
    }
}
