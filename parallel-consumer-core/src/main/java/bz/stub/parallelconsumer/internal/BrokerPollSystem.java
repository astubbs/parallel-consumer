package bz.stub.parallelconsumer.internal;

/*-
 * Copyright (C) 2020-2024 Confluent, Inc.
 * Modifications Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.ParallelConsumerOptions.CommitMode;
import bz.stub.parallelconsumer.metrics.PCMetrics;
import bz.stub.parallelconsumer.metrics.PCMetricsDef;
import bz.stub.parallelconsumer.state.WorkManager;
import io.micrometer.core.instrument.Gauge;
import lombok.Getter;
import lombok.Setter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.MDC;

import javax.naming.InitialContext;
import javax.naming.NamingException;
import java.time.Duration;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.*;

import static bz.stub.parallelconsumer.internal.utils.StringUtils.msg;
import static bz.stub.parallelconsumer.internal.AbstractParallelEoSStreamProcessor.DEFAULT_TIMEOUT;
import static bz.stub.parallelconsumer.internal.AbstractParallelEoSStreamProcessor.MDC_INSTANCE_ID;
import static bz.stub.parallelconsumer.internal.State.*;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Subsystem for polling the broker for messages.
 *
 * @param <K>
 * @param <V>
 */
@Slf4j
public class BrokerPollSystem<K, V> implements OffsetCommitter {

    private final ConsumerManager<K, V> consumerManager;

    /**
     * The single source of truth for this subsystem's lifecycle. Volatile: mutated by the control thread
     * (drain / close transitions) and read by the poll thread's loop and by {@link ConsumerManager}'s
     * {@link #isCloseInProgress()} signal (potentially from committer threads).
     */
    private volatile State runState = RUNNING;

    private Optional<Future<Boolean>> pollControlThreadFuture = Optional.empty();

    /**
     * Whether we currently have the consumer's partitions paused for back pressure - <b>derived from
     * Kafka, never mirrored</b>.
     * <p>
     * This used to be a {@code boolean} field kept in step with {@code consumer.pause()} /
     * {@code resume()} calls, and it could not be kept in step. Kafka clears its pause state for
     * every partition on an <b>eager</b> rebalance (the assignment map is replaced - see
     * {@code SubscriptionState.assignFromSubscribed}) but <b>keeps</b> it for partitions retained
     * across a <b>cooperative</b> one. A mirror therefore had to model both protocols correctly and
     * stay correct as Kafka changes; the version before this one reset the flag on every assignment,
     * which is right for eager and leaves cooperative permanently paused - consumption stops with no
     * error, which is confluentinc#857's own symptom, reintroduced by the code meant to fix it.
     * <p>
     * Asking Kafka makes the protocol irrelevant and the logic self-correcting: after a cooperative
     * rebalance the retained partitions are still paused, so this reports true and
     * {@link #resumeIfPaused()} resumes them; after an eager one Kafka has already cleared them, so
     * this reports false and there is nothing to do. No reset hook is needed, which is why
     * {@code onPartitionsAssigned()} is gone.
     * <p>
     * <b>Poll thread only.</b> {@code consumer.paused()} is a consumer call, and the consumer is
     * confined to the poll thread ({@code ThreadConfinedConsumer}). The control thread must use
     * {@link #isSubscriptionsPausedForBackPressure()} instead, which reads the per-poll cache.
     */
    private boolean subscriptionsArePausedForBackPressure() {
        return !consumerManager.paused().isEmpty();
    }

    /**
     * As {@link #subscriptionsArePausedForBackPressure()}, but safe to call from the <b>control</b>
     * thread, which may not touch the consumer.
     * <p>
     * Reads {@code ConsumerManager}'s paused-partition cache, refreshed once per poll. That staleness
     * is acceptable here and only here: its one caller, {@code maybeWakeupPoller()}, is a heuristic
     * that wakes a poller it believes is paused, and waking one that has already resumed costs
     * nothing. Do not use it to decide whether to pause or resume.
     */
    public boolean isSubscriptionsPausedForBackPressure() {
        return consumerManager.getPausedPartitionSize() > 0;
    }

    private final AbstractParallelEoSStreamProcessor<K, V> pc;

    private Optional<ConsumerOffsetCommitter<K, V>> committer = Optional.empty();

    /**
     * Note how this relates to {@link BrokerPollSystem#getLongPollTimeout()} - if longPollTimeout is high and loading
     * factor is low, there may not be enough messages queued up to satisfy demand.
     */
    @Setter
    @Getter
    private static Duration longPollTimeout = Duration.ofMillis(2000);

    private final WorkManager<K, V> wm;

    private final PCMetrics pcMetrics;

    private Gauge statusGauge;
    private Gauge numPausedPartitionsGauge;

    public BrokerPollSystem(ConsumerManager<K, V> consumerMgr, WorkManager<K, V> wm, AbstractParallelEoSStreamProcessor<K, V> pc, final ParallelConsumerOptions<K, V> options) {
        this.wm = wm;
        this.pc = pc;

        this.consumerManager = consumerMgr;

        // ConsumerManager holds no lifecycle state of its own - it derives "should abort retries/polling"
        // from this subsystem's runState (single source of truth). A duplicated shutdown flag here
        // previously desynced from the lifecycle (set at drain instead of close) and caused the
        // drain-path busy-spin / zombie-member defect.
        // See docs/solutions/test-flakiness/pc-silent-stall-under-contention-2026-07-29.md
        consumerMgr.setCloseInProgressSignal(this::isCloseInProgress);

        switch (options.getCommitMode()) {
            case PERIODIC_CONSUMER_SYNC, PERIODIC_CONSUMER_ASYNCHRONOUS -> {
                ConsumerOffsetCommitter<K, V> consumerCommitter = new ConsumerOffsetCommitter<>(consumerMgr, wm, options);
                committer = Optional.of(consumerCommitter);
            }
        }
        pcMetrics = pc.getModule().pcMetrics();
        initMetrics();
    }

    private void initMetrics() {
        statusGauge = pcMetrics.gaugeFromMetricDef(PCMetricsDef.PC_POLLER_STATUS, this, poller -> poller.runState.getValue());
        numPausedPartitionsGauge = pcMetrics.gaugeFromMetricDef(PCMetricsDef.NUM_PAUSED_PARTITIONS,
                this.consumerManager, ConsumerManager::getPausedPartitionSize);
    }

    public void start(String managedExecutorService) {
        ExecutorService executorService;
        try {
            executorService = InitialContext.doLookup(managedExecutorService);
        } catch (NamingException e) {
            log.debug("Couldn't look up an execution service, falling back to Java SE Thread", e);
            executorService = Executors.newSingleThreadExecutor();
        }
        Future<Boolean> submit = executorService.submit(this::controlLoop);
        this.pollControlThreadFuture = Optional.of(submit);
    }

    public void supervise() {
        if (pollControlThreadFuture.isPresent()) {
            Future<Boolean> booleanFuture = pollControlThreadFuture.get();
            if (booleanFuture.isCancelled() || booleanFuture.isDone()) {
                try {
                    booleanFuture.get();
                } catch (Exception e) {
                    throw new InternalRuntimeException("Error in " + BrokerPollSystem.class.getSimpleName() + " system.", e);
                }
            }
        }
    }

    /**
     * @return true if closed cleanly
     */
    private boolean controlLoop() throws TimeoutException, InterruptedException {
        Thread.currentThread().setName("pc-broker-poll");
        pc.getMyId().ifPresent(id -> {
            Thread.currentThread().setName("pc-broker-poll-" + id);
            MDC.put(MDC_INSTANCE_ID, id);
        });
        log.trace("Broker poll control loop start");
        consumerManager.claimConsumerOwnership();
        committer.ifPresent(ConsumerOffsetCommitter::claim);
        try {
            while (runState != CLOSED) {
                handlePoll();

                maybeDoCommit();

                switch (runState) {
                    case DRAINING -> {
                        doPause();
                    }
                    case CLOSING -> {
                        doClose();
                    }
                }
            }
            log.debug("Broker poller thread finished normally, returning OK (true) to future...");
            return true;
        } catch (Exception e) {
            log.error("Unknown error", e);
            throw e;
        } finally {
            // This thread will never touch the consumer again - release ownership so the thread
            // that performs the final consumer close (pc-control, in transactional mode) can take
            // over via tryClaimOwnership(). Must be in a finally: a poll loop that dies by
            // exception must also hand over, or the consumer can never be closed and no
            // LeaveGroup is ever sent. The release happens-before closeAndWait()'s Future.get()
            // returns, so the takeover is strictly sequential. See confluentinc#857.
            consumerManager.releaseConsumerOwnership();
        }
    }

    private void handlePoll() {
        // Guarded: the argument is a live consumer.paused() call now, not a volatile read, so an
        // unguarded log statement pays for it on every loop iteration whether or not trace is on.
        if (log.isTraceEnabled()) {
            log.trace("Loop: Broker poller: ({}), pausedForBackPressure={}", runState, subscriptionsArePausedForBackPressure());
        }
        if (runState == RUNNING || runState == DRAINING) { // if draining - subs will be paused, so use this to just sleep
            var polledRecords = pollBrokerForRecords();
            int count = polledRecords.count();
            log.debug("Got {} records in poll result", count);
            if (count == 0) {
                log.trace("Poll returned 0 records. assignment={}, paused={}",
                        consumerManager.getAssignmentSize(),
                        consumerManager.getPausedPartitionSize());
            }

            if (count > 0) {
                log.trace("Loop: Register work - {} records from {} partitions",
                        count, polledRecords.partitions().size());
                pc.registerWork(polledRecords);
            }
        } else {
            log.trace("Not polling - runState={}", runState);
        }
    }

    private void doClose() {
        log.debug("Doing close...");
        doPause();
        maybeCloseConsumerManager();
        runState = CLOSED;
    }

    /**
     * To keep things simple, make sure the correct thread which can make a commit, is the one to close the consumer.
     * This way, if partitions are revoked, the commit can be made inline.
     */
    private void maybeCloseConsumerManager() {
        if (isResponsibleForCommits()) {
            log.debug("Closing {}, first closing consumer...", this.getClass().getSimpleName());
            this.consumerManager.close(DEFAULT_TIMEOUT);
            log.debug("Consumer closed.");
        }
    }

    private boolean isResponsibleForCommits() {
        return committer.isPresent();
    }

    private EpochAndRecordsMap<K, V> pollBrokerForRecords() {

        checkStateForPausingSubscriptions();

        if (log.isDebugEnabled()) {
            log.debug("Subscriptions are paused: {}", subscriptionsArePausedForBackPressure());
        }

        boolean pollTimeoutNormally = runState == RUNNING || runState == DRAINING;
        Duration thisLongPollTimeout = pollTimeoutNormally ? BrokerPollSystem.longPollTimeout
                : Duration.ofMillis(1); // Can't use Duration.ZERO - this causes Object#wait to wait forever

        log.debug("Long polling broker with timeout {}, might appear to sleep here if subs are paused, or no data available on broker. Run state: {}", thisLongPollTimeout, runState);
        ConsumerRecords<K, V> poll = consumerManager.poll(thisLongPollTimeout);

        log.debug("Poll completed");

        // build records map
        return new EpochAndRecordsMap<>(poll, wm.getPm());
    }

    private void checkStateForPausingSubscriptions() {
        if (runState == DRAINING) {
            doPause();
        } else {
            managePauseOfSubscription();
        }
    }

    /**
     * Will begin the shutdown process, eventually closing itself once drained
     */
    public void drain() {
        // Deliberately does NOT stop the ConsumerManager: while DRAINING, the poller must keep calling
        // consumer.poll() - the paused long poll is this loop's sleep (see the comment in handlePoll),
        // and polling is what keeps this member rebalance-responsive. A member that stops polling
        // busy-spins the loop and zombie-holds its partition assignment (background heartbeats keep it
        // "alive") until max.poll.interval.ms - starving same-group siblings.
        // See docs/solutions/test-flakiness/pc-silent-stall-under-contention-2026-07-29.md
        // idempotent
        if (runState != State.DRAINING) {
            log.debug("Signaling poll system to drain, waking up consumer...");
            runState = State.DRAINING;
            consumerManager.wakeup();
        }
    }

    private final RateLimiter pauseLimiter = new RateLimiter(1);

    private void doPauseMaybe(Set<TopicPartition> pausedNow) {
        // idempotent
        if (!pausedNow.isEmpty()) {
            log.trace("Already paused");
        } else {
            if (pauseLimiter.couldPerform()) {
                pauseLimiter.performIfNotLimited(() -> {
                    doPause(pausedNow);
                });
            } else {
                if (log.isDebugEnabled()) {
                    log.debug("Should pause but pause rate limit exceeded {} vs {}.",
                            pauseLimiter.getElapsedDuration(),
                            pauseLimiter.getRate());
                }
            }
        }
    }

    /**
     * Pause all assignments
     */
    /**
     * For callers outside the throttle loop - drain and close - which have no pass-scoped view to
     * hand down. They run once per lifecycle rather than once per iteration, so fetching here costs
     * nothing; only the hot path threads its already-read set through.
     */
    private void doPause() {
        doPause(consumerManager.paused());
    }

    private void doPause(Set<TopicPartition> pausedNow) {
        if (pausedNow.isEmpty()) {
            log.debug("Pausing subs");
            Set<TopicPartition> assignment = consumerManager.assignment();
            consumerManager.pause(assignment);
        } else {
            log.debug("Already paused, skipping");
        }
    }

    public void closeAndWait() throws TimeoutException, ExecutionException {
        log.debug("Requesting broker polling system to close...");
        transitionToClosing();
        if (pollControlThreadFuture.isPresent()) {
            log.debug("Wait for loop to finish ending...");
            Future<Boolean> pollControlResult = pollControlThreadFuture.get();
            boolean interrupted = true;
            while (interrupted) {
                try {
                    Boolean pollShutdownSuccess = pollControlResult.get(DEFAULT_TIMEOUT.toMillis(), MILLISECONDS);
                    interrupted = false;
                    if (!pollShutdownSuccess) {
                        log.warn("Broker poll control thread not closed cleanly.");
                    }
                } catch (InterruptedException e) {
                    log.debug("Interrupted waiting for broker poller thread to finish", e);
                } catch (ExecutionException | TimeoutException e) {
                    log.error("Execution or timeout exception waiting for broker poller thread to finish", e);
                    throw e;
                }
            }
        }
        log.debug("Broker poll system finished closing");
    }

    private void transitionToClosing() {
        log.debug("Poller transitioning to closing, waking up consumer");
        // setting CLOSING is itself the stop signal - ConsumerManager's closeInProgressSignal reads this
        // runState; set it before the wakeup so an aborted poll observes it
        runState = State.CLOSING;
        consumerManager.wakeup();
    }

    /**
     * True once this poll system has begun its final close ({@link State#CLOSING} / {@link State#CLOSED}).
     * Wired into {@link ConsumerManager} as its abort signal, so that class needs no lifecycle state of
     * its own. NOT true while merely {@link State#DRAINING} - a draining consumer must keep polling.
     */
    private boolean isCloseInProgress() {
        return runState == CLOSING || runState == CLOSED;
    }

    /**
     * If we are currently processing too many records, we must stop polling for more from the broker. But we must also
     * make sure we maintain the keep alive with the broker so as not to cause a rebalance.
     */
    private void managePauseOfSubscription() {
        // Read Kafka's pause state ONCE per pass and hand it down, rather than each check asking
        // again. Deriving the answer from the consumer instead of mirroring it in a field is what
        // makes the rebalance protocols irrelevant (see subscriptionsArePausedForBackPressure), but
        // consumer.paused() is a real call on the poll thread's hot path, and this method plus the
        // three below were making three to five of them per loop iteration.
        //
        // A PARAMETER rather than a field on purpose: a local cannot outlive the pass, so there is
        // no invalidation to get wrong and nothing that can go stale between iterations - which is
        // exactly the failure mode of the mirror this replaced. It is also strictly fewer calls than
        // before in the resume path, which used to fetch the set a second time to act on it.
        Set<TopicPartition> pausedNow = consumerManager.paused();
        boolean throttle = shouldThrottle();
        if (log.isTraceEnabled()) {
            log.trace("Need to throttle: {}, pausedForBackPressure={}, assignment={}", throttle, !pausedNow.isEmpty(), consumerManager.getAssignmentSize());
        }
        if (throttle) {
            doPauseMaybe(pausedNow);
        } else {
            resumeIfPaused(pausedNow);
        }
    }

    /**
     * Has no flap limit, always resume if we need to
     */
    private void resumeIfPaused(Set<TopicPartition> pausedNow) {
        // idempotent, and self-correcting: whatever Kafka still has paused is what gets resumed,
        // whether we paused it or it survived a cooperative rebalance.
        if (!pausedNow.isEmpty()) {
            log.debug("Resuming consumer, waking up");
            consumerManager.resume(pausedNow);
            // trigger consumer to perform a new poll without the assignments paused, otherwise it will continue to long poll on nothing
            consumerManager.wakeup();
        }
    }

    private boolean shouldThrottle() {
        return wm.shouldThrottle();
    }

    /**
     * Optionally blocks. Threadsafe
     *
     * @see CommitMode
     */
    @SneakyThrows
    @Override
    public void retrieveOffsetsAndCommit() {
        if (runState == RUNNING || runState == DRAINING || runState == CLOSING) {
            // {@link Optional#ifPresentOrElse} only @since 9
            ConsumerOffsetCommitter<K, V> committer = this.committer.orElseThrow(() -> {
                // shouldn't be here
                throw new IllegalStateException("No committer configured");
            });
            committer.commit();
        } else {
            throw new IllegalStateException(msg("Can't commit - not running (state: {}", runState));
        }
    }

    /**
     * Will silently skip if not configured with a committer
     */
    private void maybeDoCommit() throws TimeoutException, InterruptedException {
        if (committer.isPresent()) {
            committer.get().maybeDoCommit();
        }
    }

    /**
     * Wake the poller if it is currently parked on a paused subscription.
     * <p>
     * Called from the CONTROL thread ({@code maybeWakeupPoller}), so it reads the per-poll cache via
     * {@link #isSubscriptionsPausedForBackPressure()} rather than asking the consumer directly - the
     * consumer is confined to the poll thread. Staleness is harmless here: waking a poller that has
     * already resumed costs one spurious wakeup, and {@code wakeup()} is the one consumer method
     * Kafka documents as thread-safe.
     */
    public void wakeupIfPaused() {
        if (isSubscriptionsPausedForBackPressure())
            consumerManager.wakeup();
    }

    /**
     * Pause polling from the underlying Kafka Broker.
     * <p>
     * Note: If the poll system is currently not in state
     * {@link bz.stub.parallelconsumer.internal.State#RUNNING running}, calling this method will be a no-op.
     * </p>
     */

    public void pausePollingAndWorkRegistrationIfRunning() {
        if (this.runState == RUNNING) {
            log.info("Transitioning broker poll system to state paused.");
            this.runState = PAUSED;
        } else {
            log.info("Skipping transition of broker poll system to state paused. Current state is {}.", this.runState);
        }
    }

    /**
     * Resume polling from the underlying Kafka Broker.
     * <p>
     * Note: If the poll system is currently not in state
     * {@link bz.stub.parallelconsumer.internal.State#PAUSED paused}, calling this method will be a no-op.
     * </p>
     */
    public void resumePollingAndWorkRegistrationIfPaused() {
        if (this.runState == PAUSED) {
            log.info("Transitioning broker poll system to state running.");
            this.runState = RUNNING;
        } else {
            log.info("Skipping transition of broker poll system to state running. Current state is {}.", this.runState);
        }
    }

    /**
     * Returns cached view of paused partition size. Useful for testing and monitoring by wrapping application / user
     * code.
     *
     * @return number of paused partitions
     */
    public int getPausedPartitionSize() {
        return consumerManager.getPausedPartitionSize();
    }
}
