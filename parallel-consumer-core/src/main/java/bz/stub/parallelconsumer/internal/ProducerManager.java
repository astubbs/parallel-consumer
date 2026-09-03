package bz.stub.parallelconsumer.internal;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 * Modifications Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.internal.utils.ThrowableUtils;
import bz.stub.parallelconsumer.*;
import bz.stub.parallelconsumer.metrics.PCMetrics;
import bz.stub.parallelconsumer.metrics.PCMetricsDef;
import io.micrometer.core.instrument.Tag;
import bz.stub.parallelconsumer.state.WorkManager;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.clients.producer.internals.RecordAccumulator;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.InvalidProducerEpochException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import com.google.errorprone.annotations.concurrent.GuardedBy;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Future;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static bz.stub.parallelconsumer.internal.utils.StringUtils.msg;

/**
 * Sub system for interacting with the Producer and managing transactions (and thus offset committing through the
 * Producer).
 */
@Slf4j
@ToString(onlyExplicitlyIncluded = true)
public class ProducerManager<K, V> extends AbstractOffsetCommitter<K, V> implements OffsetCommitter {

    /**
     * The producer in use. Volatile and replaceable: recovery drops the invalidated one under the write lock (the
     * field reads null while no usable producer exists - see {@link #producer()}) and publishes the replacement once
     * it is initialised. After construction, every path that needs a producer goes through {@link #producer()},
     * which is what makes an unavailable producer a {@link ProducerInvalidatedException} rather than a null
     * dereference; the constructor and {@link #initProducer()} read the field directly, and run before any
     * replacement can exist.
     */
    @Getter
    protected volatile ProducerWrapper<K, V> producerWrapper;

    private final ParallelConsumerOptions<K, V> options;

    /**
     * The {@link KafkaProducer} isn't actually completely thread safe, at least when using it transactionally. We must
     * be careful not to send messages to the producer, while we are committing a transaction - "Cannot call send in
     * state COMMITTING_TRANSACTION".
     * <p>
     * We also need to use this as a synchronisation barrier on transactions - so that when we start a commit cycle, we
     * first block any further records from being sent, then drain ourselves to get all sent records ack'd, and then
     * commit the tx during the synchronisation barrier, then unlock the barrier.
     * <p>
     * This could be implemented more simply, using the new micro Actor system, by sending {@link ProducerRecord}s as
     * actor messages, and having the controller process the {@link ProducerManager}s actor queue (send the queued up
     * records). However, given our implementation, that would have the side effect of all producer record sending being
     * done by the controller thread. Now as the Producer is thread safe - it uses the {@link RecordAccumulator}
     * effectively as it's Actor bus, and all network communication, amongst other things, are done through a separate
     * thread. However, before sending records to the accumulator, some non-trivial work is done while still in the
     * multithreading context - most particularly (because it's probably the slowest part) is the serialisation of the
     * payload. By moving to the new micro Actor framework, that serialisation would then be done in the controller.
     * Give the existing shared state system using the {@link ReentrantReadWriteLock} works really well, and so sending
     * work is done by worker threads, I'm hesitant to give up the performance over simplification in this case.
     */
    @Getter
    private final ReentrantReadWriteLock producerTransactionLock = new ReentrantReadWriteLock(true);

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
        /** No producer will ever be available again - a terminal build failure, or this manager is closed. */
        TERMINAL
    }

    /**
     * Guards the availability state and its schedule. A plain monitor: held only for the state reads and writes
     * below, never while acquiring or waiting on {@link #producerTransactionLock}, and never across a client call -
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

    private final PCMetrics pcMetrics;

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
     * Installed on every send. Built once, because whether this manager uses transactions is already decided before
     * it exists: {@link ProducerWrapper#isConfiguredForTransactions()} reads a {@code final} field the wrapper
     * resolves in its own constructor, so the value cannot change afterwards - the mode decision does not belong in a
     * branch evaluated per completion. ({@link #initProducer()} only enforces that the flag agrees with the
     * configured {@link ParallelConsumerOptions.CommitMode}; it does not determine it.)
     * <p>
     * Throwing from a producer callback is only safe when NOT using transactions. The comment here used to say exactly
     * that, while installing a throwing callback unconditionally, transactional mode included.
     * <p>
     * {@code KafkaProducer#doSend} invokes this callback from inside its own {@code catch (ApiException)} handler, and
     * only <em>afterwards</em> calls {@code transactionManager.maybeTransitionToErrorState(e)}. A throw escapes before
     * that runs, so a terminally failed send never moves the transaction into an abortable state. The records already
     * accepted stay in it, the next commit succeeds, and a {@code read_committed} consumer sees a PARTIAL result set
     * for one source offset - exactly what the all-or-none guarantee denies. Observed as "poison-key-0 has 2 of 5" by
     * {@code TransactionalPartialResultSetIT}.
     * <p>
     * Not throwing costs nothing that was load-bearing: the failure still reaches the work container either way,
     * because {@code processAndProduceResults} waits on each returned {@link Future} and an exceptionally-completed
     * send fails the record for retry. Note the throw was only ever observable on that synchronous pre-accumulator
     * path in the first place - when a send fails asynchronously, Kafka's own {@code ProducerBatch} catches and logs
     * whatever a callback throws, so it was already inert there in both modes.
     */
    // TODO(refactor): PCInternalRuntimeException misnames a failed send; throw a specific subclass and rename `exception` to `sendFailure`
    //  The whole summary must stay on the TODO line itself: bin/todo-index.sh indexes only that physical
    //  line, so anything wrapped onto a continuation is dropped from docs/todo-index.md.
    //  Detail, including why the subclass alone is not enough: docs/refactoring.md, internal/ProducerManager.java.
    private final Callback sendCallback;

    /**
     * How a replacement producer is built, present only where PC built the producer itself. Its presence is
     * {@link #canRecover()}: the single gate every detection site consults before recording a condition.
     */
    private final Optional<ReplacementProducerSource<K, V>> replacementProducerSource;

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
     * ordered shard, so {@link #acquireProduceLock} refuses it and the batch re-queues behind them (the review of
     * astubbs#410, finding 5). Stamped on every batch at dispatch by the control thread, the thread that also runs the
     * replay, so the two are totally ordered; compared under the monitor, where the replay increments it.
     */
    @GuardedBy("availabilityMonitor")
    private long replayGeneration;

    public ProducerManager(ProducerWrapper<K, V> newProducer,
                           ConsumerManager<K, V> newConsumer,
                           WorkManager<K, V> wm,
                           ParallelConsumerOptions<K, V> options) {
        this(newProducer, newConsumer, wm, options, Optional.empty());
    }

    public ProducerManager(ProducerWrapper<K, V> newProducer,
                           ConsumerManager<K, V> newConsumer,
                           WorkManager<K, V> wm,
                           ParallelConsumerOptions<K, V> options,
                           Optional<ReplacementProducerSource<K, V>> replacementProducerSource) {
        super(newConsumer, wm);
        this.producerWrapper = newProducer;
        this.options = options;
        this.replacementProducerSource = replacementProducerSource;
        this.pcMetrics = wm.getPcMetrics();
        pcMetrics.gaugeFromMetricDef(PCMetricsDef.PRODUCER_CONSECUTIVE_RECOVERIES, this, ProducerManager::getConsecutiveRecoveriesWithoutCommit);

        boolean usingTransactions = producerWrapper.isConfiguredForTransactions();
        this.sendCallback = (RecordMetadata metadata, Exception exception) -> {
            if (exception != null) {
                log.error("Error producing result message", exception);
                if (!usingTransactions) {
                    throw new PCInternalRuntimeException("Error producing result message", exception);
                }
            }
        };

        initProducer();
    }

    /**
     * Checks the initial producer against the commit mode and initialises its transactions. The transaction lock is
     * a final field, constructed once: inherited from astubbs#262 is the warning that constructing it here would let
     * a replacement path silently swap the lock out from under the thread holding it.
     */
    private void initProducer() {
        if (options.isUsingTransactionalProducer()) {
            if (!producerWrapper.isConfiguredForTransactions()) {
                throw new IllegalArgumentException("Using transactional option, yet Producer doesn't have a transaction ID - Producer needs a transaction id");
            }
            try {
                log.debug("Initialising producer transaction session...");
                producerWrapper.initTransactions();
            } catch (KafkaException e) {
                log.error("Make sure your producer is setup for transactions - specifically make sure it's {} is set.", ProducerConfig.TRANSACTIONAL_ID_CONFIG, e);
                throw e;
            }
        } else {
            if (producerWrapper.isConfiguredForTransactions()) {
                throw new IllegalArgumentException("Using non-transactional producer option, but Producer has a transaction ID - "
                        + "the Producer must not have a transaction ID for this option. This is because having such an ID forces the "
                        + "Producer into transactional mode - i.e. you cannot use it without using transactions.");
            }
        }
    }

    /**
     * Produce a message back to the broker.
     * <p>
     * Implementation uses the blocking API, by blocking on produce ack results (in batches when the flatMap version of
     * producing a list of records is used). Performance upgrade in later versions (confluentinc#356). This is of course not an
     * issue for the more common use case of PC where messages aren't produced
     * ({@link ParallelEoSStreamProcessor#poll}), and the {@code produce ack block} is still multi-threaded after all.
     * <p>
     * May block while a transaction is in progress - see
     * {@link ParallelConsumerOptions.CommitMode#PERIODIC_TRANSACTIONAL_PRODUCER}.
     *
     * @see ParallelConsumerOptions.CommitMode#PERIODIC_TRANSACTIONAL_PRODUCER
     * @see ParallelStreamProcessor#pollAndProduceMany
     */
    public List<ParallelConsumer.Tuple<ProducerRecord<K, V>, Future<RecordMetadata>>> produceMessages(List<ProducerRecord<K, V>> outMsgs) {
        ensureProduceStarted();
        lazyMaybeBeginTransaction();

        List<ParallelConsumer.Tuple<ProducerRecord<K, V>, Future<RecordMetadata>>> futures = new ArrayList<>(outMsgs.size());
        ProducerWrapper<K, V> producer = producer();
        for (ProducerRecord<K, V> rec : outMsgs) {
            log.trace("Producing {}", rec);
            var future = producer.send(rec, sendCallback);
            futures.add(ParallelConsumer.Tuple.pairOf(rec, future));
        }
        return futures;
    }

    /**
     * Optimistic locking for synchronising on the producer to ensure single writer for transaction state. The other
     * methods that manipulate the transaction must be single writer - i.e. from the controller thread actually doing
     * the commit.
     * <p>
     * Thread safe.
     */
    private void lazyMaybeBeginTransaction() {
        if (options.isUsingTransactionCommitMode()) {
            boolean txNotBegunAlready = !producer().isTransactionOpen();
            if (txNotBegunAlready) {
                syncBeginTransaction();
            }
        }
    }

    /**
     * Pessimistic lock (synchronized method) on beginning a transaction
     * <p>
     * Thread safe.
     */
    private synchronized void syncBeginTransaction() {
        boolean txNotBegunAlready = !producer().isTransactionOpen();
        if (txNotBegunAlready) {
            beginTransaction();
        }
    }

    protected void releaseProduceLock(ProducingLock lock) {
        lock.unlock();
    }

    /**
     * Takes the produce (read) lock, waiting out any window in which no usable producer exists (KTD7, R15).
     * <p>
     * The wait for availability happens BEFORE the read lock is taken and is re-checked after, so the
     * {@link ProducingLock} returned is always a held read lock and {@code cleanUpContext} stays its single release
     * point. While the producer is being replaced the bounded wait on the lock does not time out - it re-waits - so an
     * outage never surfaces to the user function as a produce-lock timeout. The wait ends with
     * {@link ProducerInvalidatedException} when the replacement fails terminally, the manager is closed, or the
     * processor is shutting down, so a record that can never be produced fails instead of holding the shutdown up.
     */
    protected ProducingLock acquireProduceLock(PollContextInternal<K, V> context) throws java.util.concurrent.TimeoutException {
        ReentrantReadWriteLock.ReadLock readLock = producerTransactionLock.readLock();
        Duration produceLockTimeout = options.getProduceLockAcquisitionTimeout();
        while (true) {
            awaitProducerAvailable();
            log.debug("Acquiring produce lock (timeout: {})...", produceLockTimeout);
            boolean lockAcquired;
            try {
                lockAcquired = readLock.tryLock(produceLockTimeout.toMillis(), TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                throw new PCInternalRuntimeException("Interrupted while waiting to get produce lock (timeout was set to {})", e, produceLockTimeout);
            }
            if (!lockAcquired) {
                if (isReplacing()) {
                    log.debug("Produce lock wait elapsed while the producer is being replaced - waiting again rather than failing the record");
                    continue;
                }
                throw new java.util.concurrent.TimeoutException(msg("Timeout while waiting to get produce lock (was set to {}). " +
                        "Commit taking too long? Try increasing the produce lock timeout.", produceLockTimeout));
            }
            if (isProducerAvailable()) {
                java.util.OptionalLong dispatchedAt = context.replayGenerationAtDispatch();
                if (dispatchedAt.isPresent() && dispatchedAt.getAsLong() != replayGeneration()) {
                    // dispatched before a replay put lower offsets back into its shard: producing now would put this
                    // record ahead of them in the replacement's transaction. Release the hold this method took -
                    // the lock is never handed out, so nothing else would - and fail the batch so it re-queues
                    // behind the restored work; ordered selection then takes the lower offset first.
                    readLock.unlock();
                    throw new ProducerInvalidatedException("The producer was replaced and the work its aborted transaction " +
                            "discarded was put back into processing while this record was on its way to the produce lock: " +
                            "it re-queues behind that work so ordered shards produce the earlier offset first", conditionUnderRecovery);
                }
                log.debug("Produce lock acquired (context: {}).", context.getOffsets());
                return new ProducingLock(context, readLock);
            }
            // the producer went away between the availability check and the lock: park again, without the lock
            readLock.unlock();
        }
    }

    /**
     * Parks the calling worker while the producer is being replaced. A timed loop rather than a plain wait so the
     * shutdown signal is noticed without anyone having to notify for it.
     */
    private void awaitProducerAvailable() {
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
     * @return the producer in use
     * @throws ProducerInvalidatedException while none exists - the paths that reach here without going through
     *                                      {@link #acquireProduceLock} are the commit paths, which check
     *                                      {@link #isProducerAvailable()} first, so this is a backstop
     */
    private ProducerWrapper<K, V> producer() {
        ProducerWrapper<K, V> current = producerWrapper;
        if (current == null) {
            throw new ProducerInvalidatedException("No usable producer while the replacement is being built", conditionUnderRecovery);
        }
        return current;
    }

    /**
     * First lock, so no other records can be sent. Then wait for the producer to get all its {@code acks} complete by
     * calling {@link Producer#flush()}.
     */
    @Override
    protected void preAcquireOffsetsToCommit() throws java.util.concurrent.TimeoutException, InterruptedException {
        acquireCommitLock();
        flush();
    }


    /**
     * Wait for all in flight records to be ack'd before continuing, so they are all in the tx.
     */
    private void flush() {
        ProducerWrapper<K, V> current = producerWrapper;
        if (current == null) {
            log.debug("No producer to flush: the replacement is being built");
            return;
        }
        current.flush();
    }

    /**
     * Only release lock when commit successful
     */
    @Override
    protected void postCommit() {
        if (producerTransactionLock.getWriteHoldCount() > 1) // sanity
            throw new ConcurrentModificationException("Lock held too many times, won't be released problem and will cause deadlock");

        releaseCommitLock();
    }

    /**
     * @see InvalidProducerEpochException
     * @see KafkaProducer#commitTransaction()
     */
    @Override
    protected void commitOffsets(@NonNull Map<TopicPartition, OffsetAndMetadata> offsetsToSend, @NonNull ConsumerGroupMetadata groupMetadata) {
        log.debug("Transactional offset commit starting");
        if (!options.isUsingTransactionalProducer()) {
            throw new IllegalStateException("Bug: cannot use if not using transactional producer");
        }

        // producer commit lock should already be acquired at this point, before work was retrieved to commit,
        // so that more messages don't sneak into this tx block - the consumer records of which won't yet be
        // in this offset collection
        ensureCommitLockHeld();
        if (!isProducerAvailable()) {
            // reachable from the revoke-path commit during an outage; the control thread does not attempt commits then
            throw new ProducerInvalidatedException("Commit skipped: no usable producer while the replacement is built", conditionUnderRecovery);
        }

        //
        try {
            lazyMaybeBeginTransaction(); // if not using a produce flow or if no records sent yet, a tx will need to be started here (as no records are being produced)
        } catch (RuntimeException e) {
            throw invalidatedOrRethrow(e, false);
        }
        try {
            producer().sendOffsetsToTransaction(offsetsToSend, groupMetadata);
        } catch (ProducerFencedException e) {
            // todo consider wrapping all client calls with a catch and new exception in the ProducerWrapper, so can get stack traces
            //  see APIException#fillInStackTrace
            throw invalidatedOrRethrow(e, true);
        } catch (RuntimeException e) {
            throw invalidatedOrRethrow(e, false);
        }

        // see {@link KafkaProducer#commit} this can be interrupted and is safe to retry
        boolean committed = false;
        int retryCount = 0;
        int arbitrarilyChosenLimitForArbitraryErrorSituation = 200;
        Exception lastErrorSavedForRethrow = null;
        while (!committed) {
            if (retryCount > arbitrarilyChosenLimitForArbitraryErrorSituation) {
                String msg = msg("Retired too many times ({} > limit of {}), giving up. See error above.", retryCount, arbitrarilyChosenLimitForArbitraryErrorSituation);
                log.error(msg, lastErrorSavedForRethrow);
                throw new PCInternalRuntimeException(msg, lastErrorSavedForRethrow);
            }
            try {
                if (producer().isMockProducer()) {
                    commitTransaction();
                } else {
                    // TODO talk about alternatives to this brute force approach for retrying committing transactions
                    boolean retrying = retryCount > 0;
                    if (retrying) {
                        if (producer().isTransactionCompleting()) {
                            // try wait again
                            commitTransaction();
                        }
                        // getMessage() is nullable, and this runs while already handling an error - an exception with
                        // no message (an NPE from the producer, say) turned the recovery path into a second failure.
                        // The null-check above guards the reference, not the message.
                        String lastErrorMessage = lastErrorSavedForRethrow == null
                                ? ""
                                : Objects.toString(lastErrorSavedForRethrow.getMessage(), "");
                        boolean transactionModeIsReady = !lastErrorMessage.contains("Invalid transition attempted from state READY to state COMMITTING_TRANSACTION");
                        if (transactionModeIsReady) {
                            // try again
                            log.error("Transaction was already in READY state - tx completed between interrupt and retry");
                        }
                    } else {
                        // happy path
                        commitTransaction();
                    }
                }

                committed = true;
                consecutiveRecoveriesWithoutCommit.set(0);
                if (retryCount > 0) {
                    log.warn("Commit success, but took {} tries.", retryCount);
                }
            }
            /*
            Producer#begin does not throw any retriable exceptions

            Producer#commit throws the following exceptions:

             // terminal general
             AuthorizationException – fatal error indicating that the configured transactional.id is not authorized. See the exception for more details
             KafkaException – if the producer has encountered a previous fatal or abortable error, or for any other unexpected error

             // terminal tx
             IllegalStateException – if no transactional.id has been configured or no transaction has been started
             UnsupportedVersionException – fatal error indicating the broker does not support transactions (i.e. if its version is lower than 0.11.0.0)
             ProducerFencedException – fatal error indicating another producer with the same transactional.id is active
             InvalidProducerEpochException – if the producer has attempted to produce with an old epoch to the partition leader. See the exception for more details
             - as per - InvalidProducerEpochException javadoc the, the tx should be aborted and the Producer initialised again, so to fail
               this we will just fail fast and have to be restarted

             // retriable tx
             TimeoutException – if the time taken for committing the transaction has surpassed max.block.ms.
             InterruptException – if the thread is interrupted while blocked

             Only catch and retry the retriable ones, others fail fast the control thread
             */ catch (TimeoutException | InterruptException e) {
                log.warn("Commit exception, will retry, have tried {} times (see KafkaProducer#commit)", retryCount, e);
                lastErrorSavedForRethrow = e;
                retryCount++;
            }
        }
    }

    private void commitTransaction() {
        try {
            producer().commitTransaction();
        } catch (RuntimeException e) {
            throw invalidatedOrRethrow(e, false);
        }
    }

    /**
     * The commit path as a detection site (the produce path has its own, in
     * {@code ParallelEoSStreamProcessor#processAndProduceResults}, and the revoke-path commit arrives here too).
     * <p>
     * Where PC can recover, a recoverable condition found anywhere in {@code failure}'s cause chain is recorded and
     * the operation unwinds with {@link ProducerInvalidatedException}. Where it cannot - the caller supplied a
     * finished {@link Producer} instance - the outcome is what it was before recovery existed:
     * {@link ProducerFencedException} from {@code sendOffsetsToTransaction} wrapped as an internal error, everything
     * else propagating as it arrived. That includes {@code TimeoutException}, which the commit retry loop must still
     * see raw.
     *
     * @param wrapFencedAsInternal the pre-recovery treatment at the send-offsets site only
     * @return the exception to throw; never null
     */
    private RuntimeException invalidatedOrRethrow(RuntimeException failure, boolean wrapFencedAsInternal) {
        Optional<ProducerInvalidatedException> invalidated = recordIfRecoverable(failure);
        if (invalidated.isPresent()) {
            return invalidated.get();
        }
        if (wrapFencedAsInternal && failure instanceof ProducerFencedException) {
            return new PCInternalRuntimeException(failure);
        }
        return failure;
    }

    /**
     * The one detect-and-record step both entry points share - the produce path in
     * {@link bz.stub.parallelconsumer.ParallelEoSStreamProcessor} and the commit path here. A recoverable condition
     * found in {@code failure}'s cause chain, on a producer PC can rebuild, is recorded and handed back as the
     * exception that unwinds the caller; anything else is the caller's to treat as before.
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
     *         one whose control loop performs the recovery; false on the producer-instance path, and in
     *         the consumer-commit modes, where every condition keeps its pre-recovery behaviour
     */
    public boolean canRecover() {
        // Both halves, because detection runs on every produce and commit path while recovery runs only from the
        // transactional commit loop. With the first half alone, a PC-built producer in a consumer-commit mode that
        // met a recoverable condition (an idempotent producer's OutOfOrderSequenceException, say) was recorded as
        // REPLACING, nothing ever replaced it, and every worker parked on the produce lock for the life of the
        // instance. Found by the simplify pass's efficiency reviewer as a poll that could never converge.
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

    /**
     * The processor tells this manager how to notice a shutdown, so a worker parked on the produce lock during an
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
     * The first half of a recovery, under the write lock (KTD4): abort what can be aborted, close the invalidated
     * producer, and leave the manager with no producer. The caller drains its mailbox and replays the discarded work
     * before calling {@link #releaseCommitLockAfterReplacement()}, then {@link #completeReplacement()} outside the
     * lock. The write lock is entered by waiting on it directly, never through {@link #acquireCommitLock()}'s
     * not-safe-for-multi-threaded-access guard: the revoke-path commit holds this lock during the very rebalance that
     * fences a producer, so the two are correlated and that guard would throw exactly when recovery is most needed.
     *
     * @return true when the lock was entered and the producer discarded; false when the wait elapsed, in which case
     *         a retry is scheduled and nothing changed
     */
    public boolean beginReplacement() throws InterruptedException {
        Throwable condition = pendingInvalidation.get();
        Duration lockTimeout = options.getCommitLockAcquisitionTimeout();
        boolean entered = producerTransactionLock.writeLock().tryLock(lockTimeout.toMillis(), TimeUnit.MILLISECONDS);
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
            ProducerWrapper<K, V> discarded = producerWrapper;
            producerWrapper = null;
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
            releaseCommitLock();
            throw unexpected;
        }
    }

    public void releaseCommitLockAfterReplacement() {
        releaseCommitLock();
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
            producerWrapper = replacement;
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
                                "retrying cannot fix that - check the TransactionalId ACL for this id",
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
     * The failure's class, and its root cause's where that differs - a build failure arrives wrapped as
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

    private void beginTransaction() {
        /*
         FYI:
         // terminal general
         AuthorizationException – fatal error indicating that the configured transactional.id is not authorized. See the exception for more details
         KafkaException – if the producer has encountered a previous fatal error or for any other unexpected error

         // terminal tx
         IllegalStateException – if no transactional.id has been configured or if initTransactions() has not yet been invoked
         UnsupportedVersionException – fatal error indicating the broker does not support transactions (i.e. if its version is lower than 0.11.0.0)
         ProducerFencedException – if another producer with the same transactional.id is active
         InvalidProducerEpochException – if the producer has attempted to produce with an old epoch to the partition leader. See the exception for more details

         // retriable tx
         none
         */
        producer().beginTransaction();
    }

    /**
     * Assumes the system is drained at this point, or draining is not desired.
     */
    public void close(Duration timeout) {
        log.debug("Closing producer, assuming no more in flight...");
        synchronized (availabilityMonitor) {
            availability = Availability.TERMINAL;
            availabilityMonitor.notifyAll(); // release any worker parked during an outage
        }
        ProducerWrapper<K, V> current = producerWrapper;
        if (current == null) {
            log.debug("No producer to close: it was discarded during recovery and no replacement was built");
            return;
        }
        if (options.isUsingTransactionalProducer() && !current.isTransactionReady()) {
            try {
                acquireCommitLock();
            } catch (java.util.concurrent.TimeoutException | InterruptedException e) {
                log.error("Exception acquiring commit lock, will try to abort anyway", e);
            }
            try {
                // close started after tx began, but before work was done, otherwise a tx wouldn't have been started
                abortTransaction();
            } catch (RuntimeException e) {
                // Inherited from astubbs#262: a fenced producer's abort always throws, and letting it escape here
                // skipped closeProducer and leaked a producer per fenced shutdown. The broker has already aborted
                // the transaction in that case, so there is nothing the throw protects.
                log.warn("Aborting the transaction on close failed; closing the producer regardless: {}", e.toString());
            } finally {
                releaseCommitLock();
            }
        }
        closeProducer(timeout);
    }

    private void closeProducer(Duration timeout) {
        producer().close(timeout);
    }

    private void abortTransaction() {
        producer().abortTransaction();
    }

    private void acquireCommitLock() throws java.util.concurrent.TimeoutException, InterruptedException {
        log.debug("Acquiring commit - checking lock state...");
        if (producerTransactionLock.isWriteLocked() && producerTransactionLock.isWriteLockedByCurrentThread()) {
            log.debug("Lock already held, returning with-out reentering to avoid write lock layers...");
            return;
        }

        ReentrantReadWriteLock.WriteLock writeLock = producerTransactionLock.writeLock();
        if (producerTransactionLock.isWriteLocked() && !producerTransactionLock.isWriteLockedByCurrentThread()) {
            throw new ConcurrentModificationException(this.getClass().getSimpleName() + " is not safe for multi-threaded access - write lock already held by another thread");
        }

        // acquire lock the commit lock
        var commitLockTimeout = options.getCommitLockAcquisitionTimeout();
        log.debug("Acquiring commit lock (timeout: {})...", commitLockTimeout);
        boolean gotLock = writeLock.tryLock(commitLockTimeout.toMillis(), TimeUnit.MILLISECONDS);

        if (gotLock) {
            log.debug("Commit lock acquired.");
        } else {
            var msg = msg("Timeout getting commit lock (which was set to {}). Slow processing or too many records being ack'd? " +
                            "Try increasing the commit lock timeout ({}), or reduce your record processing time.",
                    commitLockTimeout,
                    ParallelConsumerOptions.Fields.commitLockAcquisitionTimeout
            );
            throw new java.util.concurrent.TimeoutException(msg);
        }
    }

    private void releaseCommitLock() {
        log.debug("Releasing commit lock...");
        ReentrantReadWriteLock.WriteLock writeLock = producerTransactionLock.writeLock();
        if (!producerTransactionLock.isWriteLockedByCurrentThread())
            throw new IllegalStateException("Not held be me");
        writeLock.unlock();
        log.debug("Commit lock released.");
    }

    private void ensureCommitLockHeld() {
        if (!producerTransactionLock.isWriteLockedByCurrentThread())
            throw new IllegalStateException("Expected commit lock to be held");
    }

    /**
     * @return true if the commit lock has been acquired by any thread.
     */
    public boolean isTransactionCommittingInProgress() {
        return producerTransactionLock.isWriteLocked();
    }

    /**
     * Must call before sending records - acquires the lock on sending records, which blocks committing transactions)
     */
    public ProducingLock beginProducing(PollContextInternal<K, V> context) throws java.util.concurrent.TimeoutException {
        return acquireProduceLock(context);
    }

    /**
     * Must call after finishing sending records - unlocks the produce lock to potentially unblock transaction
     * committing.
     */
    public void finishProducing(@NonNull ProducingLock produceLock) {
        ensureProduceStarted();
        releaseProduceLock(produceLock);
    }

    /**
     * Sanity check to make sure the produce lock is held.
     */
    private void ensureProduceStarted() {
        if (options.isUsingTransactionCommitMode() && producerTransactionLock.getReadHoldCount() < 1) {
            throw new ProduceLockNotHeldException("Need to call #beginProducing first");
        }
    }

    /**
     * Readability wrapper on the {@link ReentrantReadWriteLock.ReadLock}s of our {@link #producerTransactionLock}.
     */
    @RequiredArgsConstructor
    public class ProducingLock {

        private final PollContextInternal<K, V> context;
        private final ReentrantReadWriteLock.ReadLock produceLock;

        /**
         * Unlocks the produce lock.
         * <p>
         * Public rather than protected because a rejected hand-over has to release the hold it is refusing:
         * {@link PollContextInternal#setProducingLock} throws when a context already owns a lock, and no caller
         * releases the hold it was passing in on that throw path. Without a release reachable from there, the guard
         * would swap a silently orphaned first hold for a loudly orphaned second one - the same permanent block on
         * the next commit's write-lock acquisition, which is exactly what that guard exists to prevent. Reported by
         * Codex review on astubbs#262.
         */
        public void unlock() {
            produceLock.unlock();
            log.debug("Unlocking produce lock (context: {}).", context.getOffsets());
        }
    }
}
