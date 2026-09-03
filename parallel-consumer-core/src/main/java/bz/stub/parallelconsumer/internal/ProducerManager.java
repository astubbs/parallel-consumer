package bz.stub.parallelconsumer.internal;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 * Modifications Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.*;
import bz.stub.parallelconsumer.metrics.PCMetrics;
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

import java.time.Duration;
import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Future;
import java.util.Optional;
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

    private final PCMetrics pcMetrics;

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
     * The replacement half of recovery: what the broker reported, whether a usable producer exists, and the two
     * steps that replace it. This manager keeps the locks, the producer and the commit, and consults it on every
     * produce and commit path; {@link ProducerRecoveryPass} drives it from the control thread.
     */
    private final ProducerRecovery<K, V> recovery;

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
        this.pcMetrics = wm.getPcMetrics();
        this.recovery = new ProducerRecovery<>(this, options, pcMetrics, replacementProducerSource);

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
            recovery.awaitProducerAvailable();
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
                            "it re-queues behind that work so ordered shards produce the earlier offset first", recovery.conditionUnderRecovery());
                }
                log.debug("Produce lock acquired (context: {}).", context.getOffsets());
                return new ProducingLock(context, readLock);
            }
            // the producer went away between the availability check and the lock: park again, without the lock
            readLock.unlock();
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
            throw new ProducerInvalidatedException("No usable producer while the replacement is being built", recovery.conditionUnderRecovery());
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
            throw new ProducerInvalidatedException("Commit skipped: no usable producer while the replacement is built", recovery.conditionUnderRecovery());
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
                recovery.commitSucceeded();
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
     * The replacement half of recovery, for the control thread's {@link ProducerRecoveryPass} and for tests that
     * drive a recovery step by step. What this manager's own paths need of it is delegated below.
     */
    public ProducerRecovery<K, V> recovery() {
        return recovery;
    }

    /** @see ProducerRecovery#recordIfRecoverable(Throwable) */
    public Optional<ProducerInvalidatedException> recordIfRecoverable(Throwable failure) {
        return recovery.recordIfRecoverable(failure);
    }

    /** @see ProducerRecovery#canRecover() */
    public boolean canRecover() {
        return recovery.canRecover();
    }

    /** @see ProducerRecovery#isProducerAvailable() */
    public boolean isProducerAvailable() {
        return recovery.isProducerAvailable();
    }

    /** @see ProducerRecovery#replayGeneration() */
    public long replayGeneration() {
        return recovery.replayGeneration();
    }

    /** @see ProducerRecovery#isReplacing() */
    public boolean isReplacing() {
        return recovery.isReplacing();
    }

    /** @see ProducerRecovery#getConsecutiveRecoveriesWithoutCommit() */
    public int getConsecutiveRecoveriesWithoutCommit() {
        return recovery.getConsecutiveRecoveriesWithoutCommit();
    }

    /** @see ProducerRecovery#setSuspensionEndsWhen(BooleanSupplier) */
    public void setSuspensionEndsWhen(BooleanSupplier shuttingDown) {
        recovery.setSuspensionEndsWhen(shuttingDown);
    }

    /**
     * Recovery's first step, under the write lock it holds: takes the invalidated producer away, leaving this manager
     * with none until {@link #publishProducer} - every path that needs one meanwhile parks or unwinds.
     *
     * @return the producer discarded, or null when none was in use
     */
    ProducerWrapper<K, V> discardProducer() {
        ProducerWrapper<K, V> discarded = producerWrapper;
        producerWrapper = null;
        return discarded;
    }

    /**
     * Recovery's last step, outside the lock: the replacement is initialised and in use from here on.
     */
    void publishProducer(ProducerWrapper<K, V> replacement) {
        producerWrapper = replacement;
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
        recovery.markTerminal(); // releases any worker parked during an outage
        ProducerWrapper<K, V> current = producerWrapper;
        if (current == null) {
            log.debug("No producer to close: it was discarded during recovery and no replacement was built");
            return;
        }
        if (options.isUsingTransactionCommitMode() && !current.isTransactionReady()) {
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

    /** Package-private for recovery, which enters the write lock directly and must leave it the same way. */
    void releaseCommitLock() {
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
