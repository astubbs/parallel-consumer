package bz.stub.parallelconsumer.internal;

/*-
 * Copyright (C) 2020-2024 Confluent, Inc.
 * Modifications Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.internal.utils.SupplierUtils;
import bz.stub.parallelconsumer.internal.utils.TimeUtils;
import bz.stub.parallelconsumer.*;
import bz.stub.parallelconsumer.metrics.PCMetrics;
import bz.stub.parallelconsumer.metrics.PCMetricsDef;
import bz.stub.parallelconsumer.state.WorkContainer;
import bz.stub.parallelconsumer.state.WorkManager;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.binder.jvm.ExecutorServiceMetrics;
import lombok.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.internals.ConsumerCoordinator;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.MDC;

import javax.naming.InitialContext;
import javax.naming.NamingException;
import java.io.Closeable;
import java.lang.reflect.Field;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static bz.stub.parallelconsumer.internal.utils.BackportUtils.isEmpty;
import static bz.stub.parallelconsumer.internal.utils.BackportUtils.toSeconds;
import static bz.stub.parallelconsumer.internal.utils.StringUtils.msg;
import static bz.stub.parallelconsumer.internal.State.*;
import static bz.stub.parallelconsumer.metrics.PCMetricsDef.USER_FUNCTION_EXECUTOR_PREFIX;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static lombok.AccessLevel.PRIVATE;
import static lombok.AccessLevel.PROTECTED;

/**
 * @see ParallelConsumer
 */
@Slf4j
public abstract class AbstractParallelEoSStreamProcessor<K, V> implements ParallelConsumer<K, V>, ConsumerRebalanceListener, Closeable {

    public static final String MDC_INSTANCE_ID = "pcId";

    /**
     * Key for the work container descriptor that will be added to the {@link MDC diagnostic context} while inside a
     * user function.
     */
    private static final String MDC_WORK_CONTAINER_DESCRIPTOR = "offset";

    /**
     * Timeout used by various subsystems(BrokerPoller, Consumer) during shut down.
     */
    public static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(30);

    public static final Duration GRACE_PERIOD_FOR_OVERALL_SHUTDOWN = Duration.ofSeconds(10);

    /**
     * How long the control thread waits for a single {@link CommitFailureHandler} decision before proceeding
     * fail-safe as {@link CommitFailureHandler.CommitFailureDecision#SHUT_DOWN}. Deliberately a constant
     * rather than a user option - a handler that needs longer than this to decide is hung, not thinking. Static and
     * settable only as a test seam, the same shape as {@link BrokerPollSystem#getLongPollTimeout()}.
     */
    @Setter
    @Getter
    private static Duration commitFailureHandlerTimeBound = Duration.ofSeconds(30);


    @Getter(PROTECTED)
    protected final ParallelConsumerOptions<K, V> options;

    /**
     * Injectable clock for testing
     */
    @Setter(AccessLevel.PACKAGE)
    private Clock clock = TimeUtils.getClock();

    /**
     * Sets the time between commits. Using a higher frequency will put more load on the brokers.
     *
     * @deprecated use {@link  ParallelConsumerOptions.ParallelConsumerOptionsBuilder#commitInterval}} instead. This
     *         will be deleted in the next major version.
     */
    // todo delete in next major version
    @Deprecated
    public void setTimeBetweenCommits(final Duration timeBetweenCommits) {
        options.setCommitInterval(timeBetweenCommits);
    }

    /**
     * Gets the time between commits.
     *
     * @deprecated use {@link ParallelConsumerOptions#setCommitInterval} instead. This will be deleted in the next major
     *         version.
     */
    // todo delete in next major version
    @Deprecated
    public Duration getTimeBetweenCommits() {
        return options.getCommitInterval();
    }

    private Instant lastCommitCheckTime = Instant.now();

    @Getter(PROTECTED)
    private final Optional<ProducerManager<K, V>> producerManager;

    private final org.apache.kafka.clients.consumer.Consumer<K, V> consumer;

    /**
     * The pool which is used for running the users' supplied function
     */
    @Getter(PROTECTED)
    protected final Supplier<ThreadPoolExecutor> workerThreadPool;

    private Optional<Future<Boolean>> controlThreadFuture = Optional.empty();

    // todo make package level
    @Getter(AccessLevel.PUBLIC)
    protected WorkManager<K, V> wm;

    /**
     * Collection of work waiting to be
     */
    @Getter(PROTECTED)
    private final BlockingQueue<ControllerEventMessage<K, V>> workMailBox = new LinkedBlockingQueue<>(); // Thread safe, highly performant, non blocking

    private final AtomicBoolean isRebalanceInProgress = new AtomicBoolean(false);

    /**
     * An inbound message to the controller.
     * <p>
     * Currently, an Either type class, representing either newly polled records to ingest, or a work result.
     */
    @Value
    @RequiredArgsConstructor(access = PRIVATE)
    static class ControllerEventMessage<K, V> {

        WorkContainer<K, V> workContainer;

        EpochAndRecordsMap<K, V> consumerRecords;

        private boolean isWorkResult() {
            return workContainer != null;
        }

        private boolean isNewConsumerRecords() {
            return !isWorkResult();
        }

        private static <K, V> ControllerEventMessage<K, V> of(EpochAndRecordsMap<K, V> polledRecords) {
            return new ControllerEventMessage<>(null, polledRecords);
        }

        public static <K, V> ControllerEventMessage<K, V> of(WorkContainer<K, V> work) {
            return new ControllerEventMessage<K, V>(work, null);
        }
    }

    private final BrokerPollSystem<K, V> brokerPollSubsystem;

    /**
     * Useful for testing async code
     */
    private final List<Runnable> controlLoopHooks = new ArrayList<>();

    /**
     * Reference to the control thread, used for waking up a blocking poll ({@link BlockingQueue#poll}) against a
     * collection sooner.
     *
     * @see #processWorkCompleteMailBox
     */
    private Thread blockableControlThread;

    /**
     * @see #notifySomethingToDo
     * @see #processWorkCompleteMailBox
     */
    private final AtomicBoolean currentlyPollingWorkCompleteMailBox = new AtomicBoolean();

    /**
     * Indicates state of waiting while in-flight messages complete processing on shutdown. Used to prevent control
     * thread interrupt due to wakeup logic on rebalances
     */
    private final AtomicBoolean awaitingInflightProcessingCompletionOnShutdown = new AtomicBoolean();

    /**
     * Edge trigger for {@link #onPoolGoneWhileStateAllowsWork()}. The condition is sticky - a shut down pool
     * never comes back, so the disagreement lasts for the rest of this instance's life - and
     * {@link #retrieveAndDistributeNewWork} is on the
     * control loop, so an un-gated warn would repeat once per commit check interval forever and bury the signal it
     * exists to give. The moment the two first disagree is the whole diagnostic; every line after it says the same
     * thing. Deliberately not a {@link RateLimiter}: that would still repeat a message which can never change. The
     * {@code queueStatsLimiter} precedent is a periodic debug stat, which this is not.
     */
    private final AtomicBoolean handledPoolGoneWhileStateAllowsWork = new AtomicBoolean(false);

    private final OffsetCommitter committer;

    /**
     * Used to request a commit asap
     */
    private final AtomicBoolean commitCommand = new AtomicBoolean(false);

    /**
     * Multiple of {@link ParallelConsumerOptions#getMaxConcurrency()} to have in our processing queue, in order to make
     * sure threads always have work to do.
     */
    protected final DynamicLoadFactor dynamicExtraLoadFactor;

    /**
     * If the system failed with an exception, it is referenced here.
     */
    // volatile: the self-close path writes it on the control thread and getFailureCause is read by callers
    // and by the chaos harness's canary sweep from theirs
    private volatile Exception failureReason;

    /**
     * Time of last successful commit
     */
    private Instant lastCommitTime;

    // --- commit-failure seam accounting (astubbs#317), maintained beside lastCommitTime -------------------------
    // Written by the control thread's decision loop and by the (serialized) rebalance callbacks; volatile for
    // visibility. Each field's non-atomic increment has a single writer: the control thread for the consecutive
    // count, the consumer's callback thread for the epoch.

    /**
     * When a commit last completed without a terminal failure in the current assignment; {@code null} until one has.
     * Distinct from {@link #lastCommitTime}, which is the commit-cadence clock and is also advanced on a CONTINUE
     * decision to restore the cadence.
     */
    private volatile Instant lastSuccessfulCommitTime;

    /**
     * The epoch for {@link CommitFailureContext#getTimeSinceLastSuccessfulCommit()} when no commit has ever
     * succeeded in this assignment - so time-based handler bounds work from the first exhaustion.
     */
    private volatile Instant assignmentStartTime = Instant.now();

    /**
     * Feeds {@link CommitFailureContext#getAssignmentEpoch()}: bumped on every assignment callback, so a stateful
     * handler can notice that history predating the current assignment no longer applies. The rest of the
     * rebalance lane's history scoping happens alongside the bump in {@link #onPartitionsAssigned}.
     */
    private volatile long assignmentEpoch;

    /**
     * Feeds {@link CommitFailureContext#getConsecutiveExhaustedBudgets()}: budgets exhausted in a row with no
     * intervening successful commit. Reset on success and on assignment change.
     */
    private volatile int consecutiveExhaustedBudgets;

    /**
     * The commit-failure seam's OWN pause axis (astubbs#317): engaged by a CONTINUE decision under
     * {@link ParallelConsumerOptions.CommitFailureContinueMode#PAUSE_INTAKE}, released by the next successful
     * commit (or a fresh assignment). Deliberately NOT the user-visible {@link #state} machine: that field's
     * RUNNING&lt;-&gt;PAUSED pair carries exactly one pause reason, so borrowing it would let the seam's release
     * silently resume a user {@link #pauseIfRunning()} - or the user's {@link #resumeIfPaused()} silently cancel
     * the seam's protection. The two axes are composed in {@link #retrieveAndDistributeNewWork}: new work is drawn
     * only when BOTH allow it. During DRAINING the close path wins and this flag does not gate the drain.
     * <p>
     * Volatile: written by the control thread's decision loop and by whichever thread runs a successful commit
     * (the rebalance-path commit runs on the broker-poll thread); read by the control thread's distribution gate.
     */
    private volatile boolean commitFailurePauseActive;

    /**
     * Runs {@link CommitFailureHandler} decisions, so user code executes on neither the control thread nor the
     * broker-poll thread. Single daemon thread, created lazily on the first terminal commit failure.
     */
    private ExecutorService commitFailureHandlerExecutor;

    @Override
    public boolean isClosedOrFailed() {
        boolean closed = state == State.CLOSED;
        boolean doneOrCancelled = false;
        if (this.controlThreadFuture.isPresent()) {
            Future<Boolean> threadFuture = controlThreadFuture.get();
            doneOrCancelled = threadFuture.isDone() || threadFuture.isCancelled();
        }
        return closed || doneOrCancelled;
    }

    /**
     * @return if the system failed, returns the recorded reason.
     */
    public Exception getFailureCause() {
        return this.failureReason;
    }

    /**
     * The run state of the controller.
     *
     * @see State
     */
    // Neither half is public. Writing is package-only because this is the controller's own state machine: the
    // transitions are driven from inside this class, and a subclass setting it arbitrarily is the shape of bug this
    // class has spent astubbs#296 hardening against. Reading is protected because a subclass may legitimately want
    // to know whether it is still running. Both are used only by this package's tests today.
    @Setter(AccessLevel.PACKAGE)
    @Getter(PROTECTED)
    private State state = State.UNUSED;

    /**
     * Wrapped {@link ConsumerRebalanceListener} passed in by a user that we can also call on events
     */
    private Optional<ConsumerRebalanceListener> usersConsumerRebalanceListener = Optional.empty();

    @Getter
    private int numberOfAssignedPartitions;

    private final RateLimiter queueStatsLimiter = new RateLimiter();

    @Getter(PROTECTED)
    PCModule<K, V> module;

    /**
     * Control for stepping loading factor - shouldn't step if work requests can't be fulfilled due to restrictions.
     * (e.g. we may want 10, but maybe there's a single partition and we're in partition mode - stepping up won't
     * help).
     */
    private boolean lastWorkRequestWasFulfilled = false;

    private io.micrometer.core.instrument.Timer userProcessingTimer;
    private Gauge loadFactorGauge;
    private Gauge statusGauge;

    /**
     * The commit-failure seam's meters (astubbs#317): one increment per exhausted budget, plus gauges over the
     * accounting fields above - so a continuing-but-failing instance is visible from a dashboard, not only from the
     * ERROR log each exhaustion emits. Registered in {@link #initMetrics()}; the gauge fields hold the strong
     * references micrometer needs to keep observing.
     */
    private Counter commitFailureExhaustionsCounter;
    private Gauge commitFailureConsecutiveExhaustionsGauge;
    private Gauge commitTimeSinceLastSuccessGauge;
    private Gauge commitFailureSeamStateGauge;

    private Duration shutdownTimeout;

    private Duration drainTimeout;

    private PCMetrics pcMetrics;

    protected AbstractParallelEoSStreamProcessor(ParallelConsumerOptions<K, V> newOptions) {
        this(newOptions, new PCModule<>(newOptions));
    }

    /**
     * Construct the AsyncConsumer by wrapping this passed in conusmer and producer, which can be configured any which
     * way as per normal.
     *
     * @see ParallelConsumerOptions
     */
    protected AbstractParallelEoSStreamProcessor(ParallelConsumerOptions<K, V> newOptions, PCModule<K, V> module) {
        requireNonNull(newOptions, "Options must be supplied");
        this.module = module;
        options = newOptions;
        this.shutdownTimeout = options.getShutdownTimeout();
        this.drainTimeout = options.getDrainTimeout();
        this.consumer = options.getConsumer();

        validateConfiguration();

        module.setParallelEoSStreamProcessor(this);

        log.info("Confluent Parallel Consumer initialise... groupId: {}, Options: {}",
                newOptions.getConsumer().groupMetadata().groupId(),
                newOptions);
        //Initialize global metrics - should be initialized before any of the module objects are created so that meters can be bound in them.
        pcMetrics = module.pcMetrics();

        this.dynamicExtraLoadFactor = module.dynamicExtraLoadFactor();

        workerThreadPool = SupplierUtils.memoize(() -> requireRejectionIsVisible(setupWorkerPool(newOptions.getMaxConcurrency())));
        // Resolved here, not left to the first dispatch. The supplier is memoized and therefore lazy, but
        // #requireRejectionIsVisible is a precondition on a subclass's #setupWorkerPool, and a precondition that only
        // fires when the first batch is submitted is one a subclass can ship without ever meeting. Construction built
        // the pool anyway - #initMetrics binds meters to it a few lines below - so this changes no startup behaviour,
        // it only stops the precondition's timing from depending on that, and moves the failure ahead of the poller
        // and producer manager, so nothing half built has to be unwound.
        workerThreadPool.get();

        this.wm = module.workManager();

        this.brokerPollSubsystem = module.brokerPoller(this);

        if (options.isProducerSupplied()) {
            this.producerManager = Optional.of(module.producerManager());
            if (options.isUsingTransactionalProducer())
                this.committer = this.producerManager.get();
            else
                this.committer = this.brokerPollSubsystem;
        } else {
            this.producerManager = Optional.empty();
            this.committer = this.brokerPollSubsystem;
        }
        //Initialize metrics for this class once all the objects are created
        initMetrics();
    }

    private void initMetrics() {
        this.userProcessingTimer = pcMetrics.getTimerFromMetricDef(PCMetricsDef.USER_FUNCTION_PROCESSING_TIME);
        this.loadFactorGauge = pcMetrics.gaugeFromMetricDef(PCMetricsDef.DYNAMIC_EXTRA_LOAD_FACTOR,
                dynamicExtraLoadFactor, DynamicLoadFactor::getCurrentFactor);
        this.statusGauge = pcMetrics.gaugeFromMetricDef(PCMetricsDef.PC_STATUS, this, pc -> pc.state.getValue());
        this.commitFailureExhaustionsCounter =
                pcMetrics.getCounterFromMetricDef(PCMetricsDef.COMMIT_FAILURE_EXHAUSTIONS);
        this.commitFailureConsecutiveExhaustionsGauge = pcMetrics.gaugeFromMetricDef(
                PCMetricsDef.COMMIT_FAILURE_CONSECUTIVE_EXHAUSTIONS, this, pc -> pc.consecutiveExhaustedBudgets);
        this.commitTimeSinceLastSuccessGauge = pcMetrics.gaugeFromMetricDef(
                PCMetricsDef.COMMIT_TIME_SINCE_LAST_SUCCESS, this, pc -> pc.secondsSinceLastSuccessfulCommit());
        this.commitFailureSeamStateGauge = pcMetrics.gaugeFromMetricDef(
                PCMetricsDef.COMMIT_FAILURE_SEAM_STATE, this, pc -> pc.commitFailureSeamState().getValue());
        new ExecutorServiceMetrics(this.getWorkerThreadPool().get(), "pc-user-function-executor",
                USER_FUNCTION_EXECUTOR_PREFIX,
                pcMetrics.getCommonTags()).bindTo(pcMetrics.getMeterRegistry());
    }

    private void validateConfiguration() {
        options.validate();

        checkGroupIdConfigured(consumer);
        checkNotSubscribed(consumer);
        checkAutoCommitIsDisabled(consumer);
    }

    private void checkGroupIdConfigured(final org.apache.kafka.clients.consumer.Consumer<K, V> consumer) {
        try {
            consumer.groupMetadata();
        } catch (RuntimeException e) {
            throw new IllegalArgumentException("Error validating Consumer configuration - no group metadata - missing a " +
                    "configured GroupId on your Consumer?", e);
        }
    }

    protected ThreadPoolExecutor setupWorkerPool(int poolSize) {
        ThreadFactory defaultFactory;
        try {
            defaultFactory = InitialContext.doLookup(options.getManagedThreadFactory());
        } catch (NamingException e) {
            log.debug("Using Java SE Thread", e);
            defaultFactory = Executors.defaultThreadFactory();
        }
        ThreadFactory finalDefaultFactory = defaultFactory;
        ThreadFactory namingThreadFactory = r -> {
            Thread thread = finalDefaultFactory.newThread(r);
            String name = thread.getName();
            thread.setName("pc-" + name);
            this.getMyId().ifPresent(id -> thread.setName("pc-" + name + "-" + id));
            return thread;
        };
        ThreadPoolExecutor.AbortPolicy rejectionHandler = new ThreadPoolExecutor.AbortPolicy();
        LinkedBlockingQueue<Runnable> workQueue = new LinkedBlockingQueue<>();
        return new ThreadPoolExecutor(poolSize, poolSize, 0L, MILLISECONDS, workQueue,
                namingThreadFactory, rejectionHandler);
    }

    /**
     * A worker pool must announce a rejection by throwing, so any pool whose {@link RejectedExecutionHandler} is not an
     * {@link java.util.concurrent.ThreadPoolExecutor.AbortPolicy} is refused here, where it is built.
     * <p>
     * Refused at setup rather than detected later because there is nothing to detect. {@code submit} wraps the task in
     * a {@code FutureTask}, calls {@code execute}, and hands back that future whatever the handler then does with the
     * task. {@code DiscardPolicy}'s body is empty, so a discarded batch produces no exception, no log line and a
     * {@link Future} that simply never completes - at the call site in {@link #submitWorkToPoolInner} that is
     * indistinguishable from a batch a worker is still running. The pool's configuration is only visible here.
     * <p>
     * What each of the JDK's handlers would do to this subsystem:
     * <ul>
     *     <li>{@code AbortPolicy} - throws {@link RejectedExecutionException}, which {@code submitWorkToPoolInner}
     *         catches, hands the batch back for, and either rethrows or logs. Supported.</li>
     *     <li>{@code CallerRunsPolicy} - loses nothing, but runs the user's function on the caller, which is the
     *         control thread: polling, committing and work distribution all stop for its duration.</li>
     *     <li>{@code DiscardPolicy} - the submitted batch is lost, silently.</li>
     *     <li>{@code DiscardOldestPolicy} - a <em>different</em>, already queued batch is lost, silently.</li>
     *     <li>a custom handler - unknowable, so not supported.</li>
     * </ul>
     * Barring {@code CallerRunsPolicy}, every one of those leaves work that {@link WorkManager#getWorkIfAvailable(int)}
     * marked in flight and counted with no event that could ever clear it.
     * <p>
     * Reachable on this codebase's own default pool, which is why this check is not conditional on the queue.
     * {@code ThreadPoolExecutor#execute} rejects a task submitted to a **shut down** pool before it ever offers it to
     * the queue, so an unbounded queue does not make the handler unreachable - it only makes saturation unreachable.
     * Measured: an unbounded pool with {@code DiscardPolicy}, shut down, accepts {@code submit} without throwing and
     * returns a {@link Future} that never completes. That is precisely the close-race path
     * {@link #submitWorkToPoolInner} exists to survive, so narrowing this check to bounded queues would reopen the
     * hole on the one path that is definitely reached.
     * <p>
     * A subclass of {@code AbortPolicy} passes: the requirement is the throw, not the exact class, and a subclass that
     * logs or counts before calling {@code super} still throws.
     * <p>
     * This is a construction-time snapshot, not a lifetime guarantee - {@code setRejectedExecutionHandler} is public,
     * so a subclass holding the pool can swap the handler afterwards and this would not see it. It narrows
     * {@code submitWorkToPoolInner}'s catch of {@link RejectedExecutionException} alone from unsound to
     * unsound-only-under-deliberate-misuse; it does not make it total.
     *
     * @return the pool, unaltered - this is a precondition on what {@link #setupWorkerPool} returned, not a chance to
     *         substitute something else
     * @throws IllegalArgumentException if the pool would swallow a rejection
     */
    private ThreadPoolExecutor requireRejectionIsVisible(ThreadPoolExecutor pool) {
        RejectedExecutionHandler handler = pool.getRejectedExecutionHandler();
        if (!(handler instanceof ThreadPoolExecutor.AbortPolicy)) {
            throw new IllegalArgumentException(msg(
                    "Unsupported worker pool returned by {}#setupWorkerPool: its rejected execution handler is {}, " +
                            "but only {} (or a subclass of it) is supported. Rejection is only visible to this " +
                            "subsystem as a RejectedExecutionException. A handler that does not throw either drops the " +
                            "batch silently ({}, {}) - submit() still returns a Future, but that Future never " +
                            "completes, so those records stay in flight, numberRecordsOutForProcessing stays inflated " +
                            "for the life of this instance, and their offsets are never committed - or runs the user's " +
                            "function on the calling thread ({}), which is the control thread, stalling polling and " +
                            "committing while it runs. Return a pool built with AbortPolicy; if that pool then rejects " +
                            "work, its queue is too small for the configured maxConcurrency.",
                    getClass().getName(),
                    handler.getClass().getName(),
                    ThreadPoolExecutor.AbortPolicy.class.getName(),
                    ThreadPoolExecutor.DiscardPolicy.class.getSimpleName(),
                    ThreadPoolExecutor.DiscardOldestPolicy.class.getSimpleName(),
                    ThreadPoolExecutor.CallerRunsPolicy.class.getSimpleName()));
        }
        return pool;
    }

    private void checkNotSubscribed(org.apache.kafka.clients.consumer.Consumer<K, V> consumerToCheck) {
        if (consumerToCheck instanceof MockConsumer)
            // disabled for unit tests which don't test rebalancing
            return;
        Set<String> subscription = consumerToCheck.subscription();
        Set<TopicPartition> assignment = consumerToCheck.assignment();
        if (!subscription.isEmpty() || !assignment.isEmpty()) {
            throw new IllegalStateException("Consumer subscription must be managed by the Parallel Consumer. Use " + this.getClass().getName() + "#subcribe methods instead.");
        }
    }

    @Override
    public void subscribe(Collection<String> topics) {
        log.debug("Subscribing to {}", topics);
        consumer.subscribe(topics, this);
    }

    @Override
    public void subscribe(Pattern pattern) {
        log.debug("Subscribing to {}", pattern);
        consumer.subscribe(pattern, this);
    }

    @Override
    public void subscribe(Collection<String> topics, ConsumerRebalanceListener callback) {
        log.debug("Subscribing to {}", topics);
        usersConsumerRebalanceListener = Optional.of(callback);
        consumer.subscribe(topics, this);
    }

    @Override
    public void subscribe(Pattern pattern, ConsumerRebalanceListener callback) {
        log.debug("Subscribing to {}", pattern);
        usersConsumerRebalanceListener = Optional.of(callback);
        consumer.subscribe(pattern, this);
    }

    /**
     * Commit our offsets
     * <p>
     * Make sure the calling thread is the thread which performs commit - i.e. is the {@link OffsetCommitter}.
     */
    @SneakyThrows
    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        log.debug("Partitions revoked {}, state: {}", partitions, state);
        isRebalanceInProgress.set(true);
        while (isTransactionCommittingInProgress())
            Thread.sleep(100); //wait for the transaction to finish committing

        numberOfAssignedPartitions = numberOfAssignedPartitions - partitions.size();

        try {
            // commit any offsets from revoked partitions BEFORE truncation
            try {
                commitOffsetsThatAreReady();
            } catch (OffsetCommitBudgetExceededException revocationTimeExhaustion) {
                // The rebalance lane's fourth handler-free exit (astubbs#317): a commit whose budget - sync
                // or transactional - exhausts DURING revocation is a DEFERRAL, not a decision point. This runs
                // inside the rebalance callback, so there is no waiter to act on a handler's answer, and failing
                // the callback would turn one slow commit into a failed rebalance and a dead instance. So: no
                // handler, no kill - the offsets were never marked committed, the truncation below hands them to
                // the new assignee to resolve by reprocessing, and this thread carries on. Deliberately
                // adds no locking of any kind here - a rebalance callback may only ever tryLock.
                log.warn("Offset commit budget exhausted during partition revocation - deferring (postponed, not " +
                        "dropped): the revoked partitions' uncommitted offsets fall to their new assignee to " +
                        "reprocess. The commit-failure handler is not consulted for revocation-time failures - " +
                        "there is no commit cycle left in this assignment for a CONTINUE to resume.",
                        revocationTimeExhaustion);
            }

            // truncate the revoked partitions
            wm.onPartitionsRevoked(partitions);
        } catch (Exception e) {
            throw new InternalRuntimeException("onPartitionsRevoked event error", e);
        } finally {
            isRebalanceInProgress.set(false);
        }
        //
        try {
            usersConsumerRebalanceListener.ifPresent(listener -> listener.onPartitionsRevoked(partitions));
        } catch (Exception e) {
            throw new ExceptionInUserFunctionException("Error from rebalance listener function after #onPartitionsRevoked", e);
        }
    }

    /**
     * Delegate to {@link WorkManager}
     *
     * @see WorkManager#onPartitionsAssigned
     */
    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        numberOfAssignedPartitions = numberOfAssignedPartitions + partitions.size();
        log.info("Assigned {} total ({} new) partition(s) {}", numberOfAssignedPartitions, partitions.size(), partitions);

        // a new assignment starts a new commit-failure history epoch (astubbs#317): a stateful handler's
        // bounds must not graduate on failures that belonged to partitions this instance may no longer even hold
        assignmentEpoch++;
        assignmentStartTime = Instant.now();
        lastSuccessfulCommitTime = null;
        consecutiveExhaustedBudgets = 0;
        // the rebalance-deferral streak is scoped to its assignment for the same reason - the committer keeps
        // that accounting (it observes the deferrals), so the epoch change is forwarded to it
        brokerPollSubsystem.onPartitionsAssigned();
        // without this, an assignment arriving with nothing dirty would never attempt a commit, so a pause
        // engaged before the rebalance could never release - intake would be stuck for good
        releaseCommitFailurePauseIfActive("a fresh assignment started a new commit-failure history epoch");

        wm.onPartitionsAssigned(partitions);
        usersConsumerRebalanceListener.ifPresent(x -> x.onPartitionsAssigned(partitions));
        notifySomethingToDo();
    }

    /**
     * Cannot commit any offsets for partitions that have been `lost` (as opposed to revoked). Just delegate to
     * {@link WorkManager} for truncation.
     *
     * @see WorkManager#onPartitionsAssigned
     */
    @Override
    public void onPartitionsLost(Collection<TopicPartition> partitions) {
        numberOfAssignedPartitions = numberOfAssignedPartitions - partitions.size();
        wm.onPartitionsLost(partitions);
        usersConsumerRebalanceListener.ifPresent(x -> x.onPartitionsLost(partitions));
    }

    /**
     * Nasty reflection to check if auto commit is disabled.
     * <p>
     * Other way would be to politely request the user also include their consumer properties when construction, but
     * this is more reliable in a correctness sense, but brittle in terms of coupling to internal implementation.
     * Consider requesting ability to inspect configuration at runtime.
     */
    private void checkAutoCommitIsDisabled(org.apache.kafka.clients.consumer.Consumer<K, V> consumer) {
        final Optional<Boolean> isAutoCommitEnabled;
        try {
            isAutoCommitEnabled = getAutoCommitEnabled(consumer);
        } catch (ClassNotFoundException | IllegalAccessException | NoSuchFieldException | NullPointerException e) {
            if (!options.isIgnoreReflectiveAccessExceptionsForAutoCommitDisabledCheck()) {
                throw new ParallelConsumerException("Failed to check whether auto commit is enabled for consumer "
                        + "type " + consumer.getClass() + ". This exception can be ignored by enabling the "
                        + "ignoreReflectiveAccessExceptionsForAutoCommitDisabledCheck option.");
            }

            log.warn("Failed to check whether auto commit is enabled for consumer type {}. "
                    + "Ignoring because ignoreReflectiveAccessExceptionsForAutoCommitDisabledCheck is enabled.", consumer.getClass(), e);
            return;
        }

        if (isAutoCommitEnabled.isPresent() && isAutoCommitEnabled.get()) {
            throw new ParallelConsumerException("Consumer auto commit must be disabled, as commits are handled by the library.");
        }

        if (!isAutoCommitEnabled.isPresent()) {
            if (options.isIgnoreReflectiveAccessExceptionsForAutoCommitDisabledCheck()) {
                log.warn("Unable to check whether auto commit is enabled for consumer type {}. "
                        + "Ignoring because ignoreReflectiveAccessExceptionsForAutoCommitDisabledCheck is enabled.", consumer.getClass());
            } else {
                throw new ParallelConsumerException("Unable to check whether auto commit is enabled for consumer "
                        + "type " + consumer.getClass() + ". This exception can be ignored by enabling the "
                        + "ignoreReflectiveAccessExceptionsForAutoCommitDisabledCheck option.");
            }
        }
    }

    private static Optional<Boolean> getAutoCommitEnabled(final org.apache.kafka.clients.consumer.Consumer<?, ?> consumer) throws ClassNotFoundException, IllegalAccessException, NoSuchFieldException {
        if (consumer instanceof MockConsumer<?, ?>) {
            log.debug("Detected MockConsumer class which doesn't do auto commits");
            return Optional.of(false);
        } else if (!(consumer instanceof KafkaConsumer<?, ?>)) {
            log.warn("Consumer is neither a KafkaConsumer nor a MockConsumer - cannot check auto commit is disabled for consumer type: {}", consumer.getClass());
            return Optional.of(false); // Probably Mockito
        }

        final KafkaConsumer<?, ?> kafkaConsumer = (KafkaConsumer<?, ?>) consumer;

        Field delegateField;
        try {
            delegateField = KafkaConsumer.class.getDeclaredField("delegate");
            delegateField.setAccessible(true);
        } catch (NoSuchFieldException ignored) {
            delegateField = null;
        }

        if (delegateField != null) { // kafka-clients >= 3.7.0
            final org.apache.kafka.clients.consumer.Consumer<?, ?> delegate =
                    (org.apache.kafka.clients.consumer.Consumer<?, ?>) delegateField.get(kafkaConsumer);
            requireNonNull(delegate, "Consumer delegate must not be null");

            if ("org.apache.kafka.clients.consumer.internals.LegacyKafkaConsumer".equals(delegate.getClass().getName())
                    // kafka-clients >= 3.9.0
                    || "org.apache.kafka.clients.consumer.internals.ClassicKafkaConsumer".equals(delegate.getClass().getName())) {
                final boolean autoCommitEnabled = getAutoCommitEnabledFromCoordinator(delegate.getClass(), delegate);
                return Optional.of(autoCommitEnabled);
            } else if ("org.apache.kafka.clients.consumer.internals.AsyncKafkaConsumer".equals(delegate.getClass().getName())) {
                final Field autoCommitEnabledField = delegate.getClass().getDeclaredField("autoCommitEnabled"); //NoSuchFieldException
                autoCommitEnabledField.setAccessible(true);
                final boolean autoCommitEnabled = (boolean) autoCommitEnabledField.get(delegate); //IllegalAccessException
                return Optional.of(autoCommitEnabled);
            } else {
                log.warn("Encountered unknown consumer delegate {}", consumer.getClass());
                return Optional.empty();
            }
        } else { // kafka-clients < 3.7.0
            final boolean autoCommitEnabled = getAutoCommitEnabledFromCoordinator(kafkaConsumer.getClass(), kafkaConsumer);
            return Optional.of(autoCommitEnabled);
        }
    }

    @SuppressWarnings("rawtypes")
    private static <T extends org.apache.kafka.clients.consumer.Consumer, U extends org.apache.kafka.clients.consumer.Consumer<?, ?>> boolean getAutoCommitEnabledFromCoordinator(final Class<T> consumerClass, final U consumer) throws IllegalAccessException, NoSuchFieldException {
        final Field coordinatorField = consumerClass.getDeclaredField("coordinator"); //NoSuchFieldException
        coordinatorField.setAccessible(true);
        final ConsumerCoordinator coordinator = (ConsumerCoordinator) coordinatorField.get(consumer); //IllegalAccessException
        requireNonNull(coordinator, "Consumer coordinator must not be null. Ensure that group.id is configured for this consumer.");

        final Field autoCommitEnabledField = coordinator.getClass().getDeclaredField("autoCommitEnabled"); //NoSuchFieldException
        autoCommitEnabledField.setAccessible(true);
        return (boolean) autoCommitEnabledField.get(coordinator); //IllegalAccessException
    }

    /**
     * Close the system, without draining.
     *
     * @see State#DRAINING
     */
    @Override
    public void close() {
        closeDontDrainFirst();
    }

    /**
     * Close the system without draining and set failure reason
     * @param exception
     *
     * @see State#DRAINING
     */
    public void closeOnException(Exception exception){
        this.failureReason = exception;
        closeDontDrainFirst();
    }

    @Override
    public void close(Duration timeout, DrainingMode drainMode) {
        shutdownTimeout = timeout;
        close(drainMode);
    }

    @Override
    @SneakyThrows
    public void close(DrainingMode drainMode) {
        if (state == CLOSED) {
            log.info("Already closed, checking end state..");
        } else {
            log.info("Signaling to close...");

            switch (drainMode) {
                case DRAIN -> {
                    log.info("Will wait for all in flight to complete before");
                    transitionToDraining();
                    waitForClose(drainTimeout.plus(shutdownTimeout).plus(GRACE_PERIOD_FOR_OVERALL_SHUTDOWN));

                }
                case DONT_DRAIN -> {
                    log.info("Not waiting for remaining queued to complete, will finish in flight, then close...");
                    transitionToClosing();
                    waitForClose(shutdownTimeout.plus(GRACE_PERIOD_FOR_OVERALL_SHUTDOWN));
                }
            }
        }

        if (controlThreadFuture.isPresent()) {
            log.debug("Checking for control thread exception...");
            Future<?> future = controlThreadFuture.get();
            future.get(shutdownTimeout.toMillis(), MILLISECONDS); // throws exception if supervisor saw one
        }

        log.info("Close complete.");
    }

    /**
     * Returns cached view of paused partition size. Useful for testing and monitoring by wrapping application / user
     * code.
     *
     * @return number of paused partitions
     */
    public int getPausedPartitionSize() {
        return brokerPollSubsystem.getPausedPartitionSize();
    }

    private void waitForClose(Duration timeout) throws TimeoutException, ExecutionException {
        log.info("Waiting on closed state...");
        while (!state.equals(CLOSED)) {
            try {
                Future<Boolean> booleanFuture = this.controlThreadFuture.get();
                log.debug("Blocking on control future, for duration {} seconds", toSeconds(timeout));
                boolean signaled = booleanFuture.get(toSeconds(timeout), SECONDS);
                if (!signaled)
                    throw new TimeoutException("Timeout waiting for system to close (" + timeout + ")");
            } catch (InterruptedException e) {
                // ignore
                log.trace("Interrupted", e);
            } catch (ExecutionException | TimeoutException e) {
                log.error("Execution or timeout exception while waiting for the control thread to close cleanly " +
                        "(state was {}). Try increasing your time-out to allow the system to drain, or close without " +
                        "draining.", state, e);
                throw e;
            }
            log.trace("Still waiting for system to close...");
        }
    }

    private void doClose(Duration timeout) throws TimeoutException, ExecutionException, InterruptedException {
        // fixes github issue confluentinc#809 - ensure doClose() state transition to CLOSED
        // by catching unhandled exceptions in subsystems during close
        try {
            innerDoClose(timeout);
        } catch (Exception e) {
            log.error("exception during close", e);
            throw e;
        } finally {
            if (commitFailureHandlerExecutor != null) {
                // interrupts any still-running (already timed-out and forfeited) handler decision
                var ignoredNeverStartedTasks = commitFailureHandlerExecutor.shutdownNow();
            }
            deregisterMeters();
            pcMetrics.close();
            log.debug("Close complete.");
            this.state = CLOSED;
            if (this.getFailureCause() != null) {
                log.error("PC closed due to error: {}", getFailureCause(), null);
            }
        }
    }

    private void innerDoClose(Duration timeout) throws TimeoutException, ExecutionException, InterruptedException {
        log.debug("Starting close process (state: {})...", state);

        // Drain and pause polling - keeps consumer alive for later commit, but paused
        // drained messages will be sent to retry queue and not actually processed.
        brokerPollSubsystem.drain();

        log.debug("Shutting down execution pool...");
        //Clear scheduled but not started work in execution pool
        workerThreadPool.get().getQueue().clear();
        //request graceful shutdown
        workerThreadPool.get().shutdown();
        if (workerThreadPool.get().getActiveCount() > 0) {
            log.info("Inflight work in execution pool: {}, letting to finish on shutdown with timeout: {}", workerThreadPool.get().getActiveCount(), timeout);
        }

        log.debug("Awaiting worker pool termination...");
        awaitingInflightProcessingCompletionOnShutdown.getAndSet(true);
        boolean awaitingInflightCompletion = true;
        while (awaitingInflightCompletion) {
            log.debug("Still awaiting completion of inflight work");
            try {
                boolean terminationFinishedWithoutTimeout = workerThreadPool.get().awaitTermination(toSeconds(timeout), SECONDS);
                awaitingInflightCompletion = false;
                if (!terminationFinishedWithoutTimeout) {
                    log.warn("Thread execution pool termination await timeout ({})! Were any processing jobs dead locked (test latch locks?) or otherwise stuck? Forcing shutdown of workers.", timeout);
                    //Requesting threads shutdown immediately - inflight threads will be interrupted at this point.
                    workerThreadPool.get().shutdownNow();
                    //Give a second for any interrupt handling / resource cleanup in user functions
                    workerThreadPool.get().awaitTermination(toSeconds(Duration.ofSeconds(1)), SECONDS);
                }
            } catch (InterruptedException e) {
                log.error("InterruptedException", e);
                awaitingInflightCompletion = true;
            }
        }
        awaitingInflightProcessingCompletionOnShutdown.getAndSet(false);

        if (workerThreadPool.get().getActiveCount() > 0) {
            log.warn("Clean execution pool termination failed - some threads still active despite await and interrupt - is user function swallowing interrupted exception? Threads still not done count: {}", workerThreadPool.get().getActiveCount());
        }
        log.debug("Worker pool terminated.");

        // last check to see if after worker pool closed, has any new work arrived?
        processWorkCompleteMailBox(Duration.ZERO);

        //
        if (Thread.currentThread().isInterrupted()) {
            log.warn("control thread interrupted - may lead to issues with transactional commit lock acquisition");
        }
        try {
            commitOffsetsThatAreReady();
        } catch (Exception e) {
            log.warn("failed to commit during close sequence", e);
        }
        // only close consumer once producer has committed it's offsets (tx'l)
        log.debug("Closing and waiting for broker poll system...");
        try {
            brokerPollSubsystem.closeAndWait();
        } catch (Exception e) {
            log.warn("failed to close brokerPollSubsystem during close sequence", e);
        }

        try {
            maybeCloseConsumer();
        } catch (Exception e) {
            log.warn("failed to maybeCloseConsumer during close sequence", e);
        }

        producerManager.ifPresent(x -> x.close(timeout));
    }

    /**
     * De-registers and removes user function executor meters from meter registry on shutdown
     */
    private void deregisterMeters() {
        pcMetrics.removeMetersByPrefixAndCommonTags(USER_FUNCTION_EXECUTOR_PREFIX);
    }

    /**
     * To keep things simple, make sure the correct thread which can make a commit, is the one to close the consumer.
     * This way, if partitions are revoked, the commit can be made inline.
     */
    private void maybeCloseConsumer() {
        if (isResponsibleForCommits()) {
            consumer.close();
        }
    }

    private boolean isResponsibleForCommits() {
        return (committer instanceof ProducerManager);
    }

    private boolean isRecordsAwaitingProcessing() {
        boolean isRecordsAwaitingProcessing = wm.isRecordsAwaitingProcessing();
        boolean threadsDone = areMyThreadsDone();
        log.trace("isRecordsAwaitingProcessing {} || threadsDone {}", isRecordsAwaitingProcessing, threadsDone);
        return isRecordsAwaitingProcessing || threadsDone;
    }

    private void transitionToDraining() {
        log.debug("Transitioning to draining...");
        this.state = State.DRAINING;
        notifySomethingToDo();
    }

    /**
     * Control thread can be blocked waiting for work, but is interruptible. Interrupting it can be useful to inform
     * that work is available when there was none, to make tests run faster, or to move on to shutting down the
     * {@link BrokerPollSystem} so that less messages are downloaded and queued.
     */
    private void interruptControlThread() {
        if (blockableControlThread != null) {
            log.debug("Interrupting {} thread in case it's waiting for work", blockableControlThread.getName());
            blockableControlThread.interrupt();
        }
    }

    private boolean areMyThreadsDone() {
        if (isEmpty(controlThreadFuture)) {
            // not constructed yet, will become alive, unless #poll is never called
            return false;
        } else {
            return controlThreadFuture.get().isDone();
        }
    }

    /**
     * Optional ID of this instance. Useful for testing.
     */
    @Setter
    @Getter
    private Optional<String> myId = Optional.empty();

    /**
     * Kicks off the control loop in the executor, with supervision and returns.
     *
     * @see #supervisorLoop(Function, Consumer)
     */
    protected <R> void supervisorLoop(Function<PollContextInternal<K, V>, List<R>> userFunctionWrapped,
                                      Consumer<R> callback) {
        if (state != State.UNUSED) {
            throw new IllegalStateException(msg("Invalid state - you cannot call the poll* or pollAndProduce* methods " +
                    "more than once (they are asynchronous) (current state is {})", state));
        } else {
            state = RUNNING;
        }

        // broker poll subsystem
        brokerPollSubsystem.start(options.getManagedExecutorService());

        ExecutorService executorService;
        try {
            executorService = InitialContext.doLookup(options.getManagedExecutorService());
        } catch (NamingException e) {
            log.debug("Using Java SE Thread", e);
            executorService = Executors.newSingleThreadExecutor();
        }


        // run main pool loop in thread
        Callable<Boolean> controlTask = () -> {
            addInstanceMDC();
            log.info("Control loop starting up...");
            Thread controlThread = Thread.currentThread();
            controlThread.setName("pc-control");
            this.getMyId().ifPresent(id -> controlThread.setName("pc-control-" + id));
            this.blockableControlThread = controlThread;
            while (state != CLOSED) {
                log.debug("Control loop start");
                try {
                    controlLoop(userFunctionWrapped, callback);
                } catch (InterruptedException e) {
                    log.debug("Control loop interrupted, closing");
                    Thread.interrupted(); //clear interrupted flag as during close need to acquire commit locks and interrupted flag will cause it to throw another interrupted exception.
                    doClose(shutdownTimeout);
                } catch (Exception e) {
                    if (Thread.interrupted()) { //clear interrupted flag
                        log.debug("Thread interrupted flag cleared in control loop error handling");
                    }
                    log.error("Error from poll control thread, will attempt controlled shutdown, then rethrow. Error: " + e.getMessage(), e);
                    failureReason = new RuntimeException("Error from poll control thread: " + e.getMessage(), e);
                    doClose(shutdownTimeout); // attempt to close
                    throw failureReason;
                }
            }
            log.info("Control loop ending clean (state:{})...", state);
            return true;
        };
        Future<Boolean> controlTaskFutureResult = executorService.submit(controlTask);
        this.controlThreadFuture = Optional.of(controlTaskFutureResult);
    }

    /**
     * Useful when testing with more than one instance
     */
    private void addInstanceMDC() {
        this.myId.ifPresent(id -> MDC.put(MDC_INSTANCE_ID, id));
    }

    /**
     * Main control loop
     */
    protected <R> void controlLoop(Function<PollContextInternal<K, V>, List<R>> userFunction,
                                   Consumer<R> callback) throws TimeoutException, ExecutionException, InterruptedException {
        maybeWakeupPoller();

        final boolean shouldTryCommitNow = maybeAcquireCommitLock();

        // make sure all work that's been completed are arranged ready for commit
        Duration timeToBlockFor = shouldTryCommitNow ? Duration.ZERO : getTimeToBlockFor();
        processWorkCompleteMailBox(timeToBlockFor);

        //
        if (shouldTryCommitNow) {
            // offsets will be committed when the consumer has its partitions revoked
            commitOffsetsConsultingSeamOnTerminalFailure();
        }

        // distribute more work
        retrieveAndDistributeNewWork(userFunction, callback);

        // run call back
        log.trace("Loop: Running {} loop end plugin(s)", controlLoopHooks.size());
        this.controlLoopHooks.forEach(Runnable::run);

        log.trace("Current state: {}", state);
        switch (state) {
            case DRAINING -> {
                drain();
            }
            case CLOSING -> {
                // Clear immediately before the close, never earlier. doClose acquires commit locks and an interrupted
                // flag makes that throw, skipping the final commit - so those offsets go uncommitted and their records
                // are redelivered. Every route into CLOSING can arrive with the flag set: transitionToClosing wakes
                // this loop by interrupting it, and so does every other path through notifySomethingToDo. Do not
                // try to list them: this comment enumerated that set three times and was wrong three times. Every
                // state transition calls it, BOTH forms of close() reach it via transitionToClosing or
                // transitionToDraining, the rebalance listener reaches it on the broker poll thread, and the method
                // is public, so an embedding application can call it directly. The one notable non-source is the
                // worker threads, worth saying only because they are the first guess: addToMailbox enqueues, it
                // does not interrupt. Clearing here rather than at each of those sites keeps the window to one
                // statement, which is the same guarantee supervisorLoop's own pre-doClose clear gives.
                Thread.interrupted();
                doClose(shutdownTimeout);
            }
        }

        // sanity - supervise the poller
        brokerPollSubsystem.supervise();

        // thread yield for spin lock avoidance
        Duration duration = Duration.ofMillis(1);
        try {
            Thread.sleep(duration.toMillis());
        } catch (InterruptedException e) {
            log.trace("Woke up", e);
        }

        // end of loop
        if (log.isTraceEnabled()) {
            log.trace("End of control loop, waiting processing {}, remaining in partition queues: {}, out for processing: {}. In state: {}",
                    wm.getNumberOfWorkQueuedInShardsAwaitingSelection(), wm.getNumberOfIncompleteOffsets(), wm.getNumberRecordsOutForProcessing(), state);
        }
    }

    /**
     * Commit, and when that fails, report why the <em>poller</em> died rather than the symptom the
     * control thread happens to observe.
     * <p>
     * The broker-poll thread is the only producer of commit responses, so any exception that escapes
     * its control loop leaves every later sync commit unanswerable. Historically that surfaced as
     * {@code "Timeout waiting for commit response"} - a message that named neither the failing
     * subsystem nor the failure, was what users reported (astubbs#177, confluentinc#833), and pointed
     * nowhere near the cause; today the waiter is told of the death directly and fails fast with it.
     * Note that budget exhaustion no longer arrives this way at all: it stays on the poll thread's
     * response channel as a typed commit-failure outcome and is intercepted by
     * {@link #commitOffsetsConsultingSeamOnTerminalFailure()} - what reaches the poller-death paths
     * here is genuinely a dead poller.
     * <p>
     * {@link BrokerPollSystem#supervise()} holds the real exception, but the ordinary call at the end
     * of {@link #controlLoop} never reaches it in this scenario: the poller dies <em>while servicing
     * the commit this thread is already blocked on</em>, so the control thread is inside
     * {@code commitAndWait()} rather than at the top of the loop. Moving that supervise call earlier
     * in the loop does not help for the same reason - it was tried and measured. Supervising here, on
     * the failure path, is what actually reaches it.
     * <p>
     * When the poller is healthy the commit failure is the whole story and is rethrown untouched. When
     * it is not, the poller's exception becomes the cause and the commit failure is retained as
     * suppressed, so neither is lost.
     * <p>
     * This is now the <b>backstop</b>, not the primary path. A poller that dies while servicing a
     * commit publishes its own exception through
     * {@link ConsumerOffsetCommitter#notifyPollerDied(Throwable)}, which releases the waiter at that
     * moment with the right cause already attached. What is left for this to catch is a poller that
     * died without reaching that call - before the committer existed, or through a route that does not
     * run the poll thread's own exit path - and any commit failure in a mode that has no
     * {@code ConsumerOffsetCommitter} at all.
     */
    private void commitOffsetsReportingPollerDeath() throws TimeoutException, InterruptedException {
        try {
            commitOffsetsThatAreReady();
        } catch (InternalRuntimeException commitFailure) {
            try {
                brokerPollSubsystem.supervise();
            } catch (RuntimeException pollerFailure) {
                pollerFailure.addSuppressed(commitFailure);
                throw pollerFailure;
            }
            throw commitFailure;
        }
    }

    /**
     * The commit-failure seam's decision loop (astubbs#317, confluentinc#833): the scheduled commit's ONE
     * interception point for {@link OffsetCommitBudgetExceededException}, the typed outcome the broker-poll thread
     * publishes when a commit's retry budget is exhausted (see {@code ConsumerOffsetCommitter#maybeDoCommit} - the
     * poll thread stays alive, which is what makes CONTINUE possible at all).
     * <p>
     * The catch is deliberately OUTSIDE {@link #commitOffsetsThatAreReady()}'s {@code synchronized (commitCommand)}
     * block: the failure propagates out of the monitor as an exception, so everything below - above all the user's
     * {@link CommitFailureHandler} - runs monitor-free, and a slow handler can never stall a rebalance callback
     * contending for that monitor.
     * <p>
     * Everything else stays handler-free by construction: genuine poller death and non-retriable commit failures
     * arrive as other exception types and keep their fatal route through
     * {@link #commitOffsetsReportingPollerDeath()}; the close sequence's own commit and the revocation-time commit
     * call {@link #commitOffsetsThatAreReady()} at different sites, which this method does not wrap - the
     * revocation site catches the exhaustion itself and treats it as a deferral (see
     * {@link #onPartitionsRevoked}).
     */
    private void commitOffsetsConsultingSeamOnTerminalFailure() throws TimeoutException, InterruptedException {
        try {
            commitOffsetsReportingPollerDeath();
        } catch (OffsetCommitBudgetExceededException commitFailure) {
            decideCommitFailureOutcome(commitFailure);
        }
    }

    /**
     * Assemble the failure's history, consult the configured handler (time-bounded, off-thread), act on the
     * decision. Runs on the control thread, monitor-free - see
     * {@link #commitOffsetsConsultingSeamOnTerminalFailure()}.
     */
    private void decideCommitFailureOutcome(OffsetCommitBudgetExceededException commitFailure) {
        if (state == DRAINING || state == CLOSING || state == CLOSED) {
            // once close (or drain-to-close) has begun, the handler is never consulted - continuing is no longer
            // an option the application can meaningfully choose, so the failure keeps its historical fatal route
            log.warn("Commit budget exhausted while shutdown already in progress (state: {}) - not consulting the " +
                    "commit-failure handler", state, commitFailure);
            throw commitFailure;
        }

        consecutiveExhaustedBudgets++;
        // counted here, the seam's single interception point, so every lane's exhaustion - sync, transactional,
        // and an escalated rebalance-deferral streak - increments exactly once, whatever the decision below
        commitFailureExhaustionsCounter.increment();
        CommitFailureContext context = buildCommitFailureContext(commitFailure);

        // loud regardless of the decision: a continuing-but-failing instance must never be quiet
        log.error("Offset commit failed terminally - retry budget exhausted after {} attempt(s) in {}. Consecutive " +
                        "exhausted budgets: {}, time since last successful commit: {}. Consulting the configured " +
                        "commit-failure handler: {}",
                commitFailure.getAttemptsMade(), commitFailure.getElapsed(), context.getConsecutiveExhaustedBudgets(),
                context.getTimeSinceLastSuccessfulCommit(), options.getCommitFailureHandler(), commitFailure);

        CommitFailureHandler.CommitFailureDecision decision = invokeHandlerBounded(context, commitFailure);
        if (decision == CommitFailureHandler.CommitFailureDecision.CONTINUE) {
            log.error("Commit-failure handler decided CONTINUE: the failed offsets stay dirty and will be " +
                    "re-committed on the next commit cycle with a fresh budget; each further exhaustion re-consults " +
                    "the handler with updated history");
            maybeEngageCommitFailurePause();
            // restore the commit cadence - the failed cycle counts as this interval's attempt, so the retry
            // happens one commitInterval from now rather than immediately in a budget-long hot loop
            this.lastCommitTime = Instant.now();
            // ...and consume any commit command still pending from before the failure: the command is only
            // cleared on the success path, so left in place it re-fires the very next control-loop pass and
            // turns the cadence reset above into exactly that hot loop. The command's offsets are still dirty
            // and travel on the cadence retry.
            clearCommitCommand();
        } else {
            log.error("Commit-failure handler decided SHUT_DOWN - closing with the commit failure as the cause");
            // the same fatal route budget exhaustion always took: the supervisor records it as the failure
            // reason, runs the close, and getFailureCause() reaches it - byte-compatible with the pre-seam
            // default
            throw commitFailure;
        }
    }

    private CommitFailureContext buildCommitFailureContext(OffsetCommitBudgetExceededException commitFailure) {
        return CommitFailureContext.builder()
                .failure(commitFailure)
                .offsets(commitFailure.getOffsets())
                .attemptsMade((int) Math.min(Integer.MAX_VALUE, commitFailure.getAttemptsMade()))
                .elapsed(commitFailure.getElapsed())
                .consecutiveExhaustedBudgets(consecutiveExhaustedBudgets)
                .timeSinceLastSuccessfulCommit(timeSinceLastSuccessfulCommit())
                .commitMode(options.getCommitMode())
                .assignmentEpoch(assignmentEpoch)
                .build();
    }

    /**
     * The epoch rule (see {@link CommitFailureContext#getTimeSinceLastSuccessfulCommit()}): while no commit has
     * succeeded in the current assignment, measured from {@link #assignmentStartTime} - which is initialised at
     * construction, so there is always an epoch and never an absent-state sentinel (before the first assignment it
     * counts from construction). Time-based bounds are therefore reachable from the very first failure.
     */
    private Duration timeSinceLastSuccessfulCommit() {
        Instant successEpoch = lastSuccessfulCommitTime != null ? lastSuccessfulCommitTime : assignmentStartTime;
        return Duration.between(successEpoch, Instant.now());
    }

    /**
     * Seconds since the last successful commit, observed by {@link PCMetricsDef#COMMIT_TIME_SINCE_LAST_SUCCESS} -
     * {@link #timeSinceLastSuccessfulCommit()} carries the epoch rule.
     */
    private double secondsSinceLastSuccessfulCommit() {
        return timeSinceLastSuccessfulCommit().toMillis() / 1_000.0;
    }

    /**
     * The seam's observable state, observed by {@link PCMetricsDef#COMMIT_FAILURE_SEAM_STATE}. Derived from the
     * accounting fields on each observation rather than stored, so it can never drift from the behaviour it
     * reports.
     */
    private CommitFailureSeamState commitFailureSeamState() {
        if (commitFailurePauseActive) {
            return CommitFailureSeamState.FAILING_PAUSED;
        }
        if (consecutiveExhaustedBudgets > 0) {
            return CommitFailureSeamState.FAILING_CONTINUING;
        }
        return CommitFailureSeamState.HEALTHY;
    }

    /**
     * Engage the seam's pause if the configured {@link ParallelConsumerOptions#getCommitFailureContinueMode()} asks
     * for it. Called only from a CONTINUE decision (control thread), which the shutdown guard in
     * {@link #decideCommitFailureOutcome} already keeps out of DRAINING/CLOSING - and the distribution gate
     * additionally never lets this flag gate a drain, so the close path always wins.
     * <p>
     * INFO on the transition: an operator watching a continuing-but-failing instance needs to see intake stop.
     */
    private void maybeEngageCommitFailurePause() {
        boolean pauseIntakeMode = options.getCommitFailureContinueMode()
                == ParallelConsumerOptions.CommitFailureContinueMode.PAUSE_INTAKE;
        if (pauseIntakeMode && !commitFailurePauseActive) {
            commitFailurePauseActive = true;
            log.info("Commit-failure pause engaged (commitFailureContinueMode: PAUSE_INTAKE): no new work will be " +
                    "taken until a commit succeeds; in-flight work continues and completed offsets stay dirty for " +
                    "the next commit cycle");
        }
    }

    /**
     * Release the seam's pause: the condition it guarded against - work completing that may never be committed -
     * has passed. Called wherever the seam's failure history resets: a genuinely successful commit (a DEFERRED
     * cycle does not qualify - see {@link #commitOffsetsThatAreReady()}), and a fresh assignment, whose
     * commit-failure history no longer applies. Never called from the user's {@link #resumeIfPaused()}, which owns
     * only the {@link #state} axis.
     */
    private void releaseCommitFailurePauseIfActive(String reason) {
        if (commitFailurePauseActive) {
            commitFailurePauseActive = false;
            log.info("Commit-failure pause released ({}): resuming taking new work", reason);
        }
    }

    /**
     * Run the handler off-thread, bounded by {@link #getCommitFailureHandlerTimeBound()}. Fail-safe: a
     * handler that throws, hangs past the bound, or returns {@code null} decides nothing, and PC proceeds as
     * {@link CommitFailureHandler.CommitFailureDecision#SHUT_DOWN}. When the handler threw, its exception is
     * attached to {@code commitFailure} as suppressed, so the recorded failure names both.
     */
    private CommitFailureHandler.CommitFailureDecision invokeHandlerBounded(CommitFailureContext context,
                                                                            OffsetCommitBudgetExceededException commitFailure) {
        CommitFailureHandler handler = options.getCommitFailureHandler();
        Future<CommitFailureHandler.CommitFailureDecision> decisionFuture =
                getCommitFailureHandlerExecutor().submit(() -> handler.onCommitFailure(context));
        long deadlineNanos = System.nanoTime() + getCommitFailureHandlerTimeBound().toNanos();
        boolean interruptedWhileWaiting = false;
        try {
            while (true) {
                long remainingNanos = deadlineNanos - System.nanoTime();
                if (remainingNanos <= 0) {
                    var ignoredMayHaveCompleted = decisionFuture.cancel(true); // best effort - the decision is forfeit either way
                    log.error("Commit-failure handler did not decide within its time bound of {} - proceeding " +
                            "fail-safe as SHUT_DOWN", getCommitFailureHandlerTimeBound());
                    return CommitFailureHandler.CommitFailureDecision.SHUT_DOWN;
                }
                try {
                    var decision = decisionFuture.get(remainingNanos, TimeUnit.NANOSECONDS);
                    if (decision == null) {
                        log.error("Commit-failure handler returned null - proceeding fail-safe as SHUT_DOWN");
                        return CommitFailureHandler.CommitFailureDecision.SHUT_DOWN;
                    }
                    return decision;
                } catch (ExecutionException handlerThrew) {
                    Throwable handlerFailure = handlerThrew.getCause() != null ? handlerThrew.getCause() : handlerThrew;
                    log.error("Commit-failure handler itself threw - proceeding fail-safe as SHUT_DOWN, reporting " +
                            "both the commit failure and the handler's error", handlerFailure);
                    commitFailure.addSuppressed(handlerFailure);
                    return CommitFailureHandler.CommitFailureDecision.SHUT_DOWN;
                } catch (TimeoutException handlerTooSlow) {
                    // loop: the deadline check above turns this into the fail-safe SHUT_DOWN
                } catch (InterruptedException interrupted) {
                    // NOT a fail-safe trigger: interrupting the control thread is this class's ROUTINE wake-up
                    // mechanism (notifySomethingToDo - worker completions, requestCommitAsap, close all use it),
                    // so treating it as SHUT_DOWN would let any background wake-up convert a decision in
                    // progress into a shutdown - measured: a concurrent requestCommitAsap did exactly that.
                    // Remember it, keep waiting out the bound, and restore the flag for the control loop's own
                    // handling once the decision is in.
                    interruptedWhileWaiting = true;
                }
            }
        } finally {
            if (interruptedWhileWaiting) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private ExecutorService getCommitFailureHandlerExecutor() {
        if (commitFailureHandlerExecutor == null) {
            commitFailureHandlerExecutor = Executors.newSingleThreadExecutor(runnable -> {
                Thread thread = new Thread(runnable, "pc-commit-failure-handler");
                // daemon: an abandoned (timed-out) handler must not hold the JVM open after PC closes
                thread.setDaemon(true);
                return thread;
            });
        }
        return commitFailureHandlerExecutor;
    }

    /**
     * If we don't have enough work queued, and the poller is paused for throttling,
     * <p>
     * todo move into {@link WorkManager} as it's specific to WM having enough work?
     */
    private void maybeWakeupPoller() {
        if (state == RUNNING) {
            if (!wm.isSufficientlyLoaded() && brokerPollSubsystem.isPausedForThrottling()) {
                if (log.isDebugEnabled()) {
                    long inShards = wm.getNumberOfWorkQueuedInShardsAwaitingSelection();
                    long outForProcessing = wm.getNumberRecordsOutForProcessing();
                    log.debug("Found Poller paused with not enough front loaded messages, ensuring poller is awake (in buffers: {} vs target: {}), in shards: {}, outForProcessing: {}",
                            inShards + outForProcessing,
                            options.getTargetAmountOfRecordsInFlight(), inShards, outForProcessing);
                }
                brokerPollSubsystem.wakeupIfPaused();
            }
        }
    }

    /**
     * If it's time to commit, and using transactional system, tries to acquire the commit lock.
     * <p>
     * Call {@link ProducerManager#preAcquireOffsetsToCommit()} early, to initiate the record sending barrier for this
     * transaction (so no more records can be sent, before collecting offsets to commit).
     *
     * @return true if committing should either way be attempted now
     */
    private boolean maybeAcquireCommitLock() throws TimeoutException, InterruptedException {
        final boolean shouldTryCommitNow = isTimeToCommitNow() && wm.isDirty() && !isRebalanceInProgress.get();
        // could do this optimistically as well, and only get the lock if it's time to commit, so is not frequent
        if (shouldTryCommitNow && options.isUsingTransactionCommitMode()) {
            // get into write lock queue, so that no new work can be started from here on
            log.debug("Acquiring commit lock pessimistically, before we try to collect offsets for committing");
            //noinspection OptionalGetWithoutIsPresent - options will already be verified
            producerManager.get().preAcquireOffsetsToCommit();
        }
        return shouldTryCommitNow;
    }

    <R> int retrieveAndDistributeNewWork(final Function<PollContextInternal<K, V>, List<R>> userFunction, final Consumer<R> callback) {
        // check queue pressure first before addressing it
        checkPipelinePressure();

        int gotWorkCount = 0;

        // Two pause axes, composed: the user-visible state machine AND the commit-failure seam's own flag -
        // new work is drawn only when both allow it. During DRAINING the close path wins: the drain must be able to
        // finish even though the pause's release condition (a successful commit) may never arrive, so the seam flag
        // deliberately does not gate it - consistent with the shutdown guard in decideCommitFailureOutcome, which
        // keeps the handler (and so any new pause) out of the close entirely.
        boolean userStateAllowsNewWork = state == RUNNING || state == DRAINING;
        boolean seamPauseBlocksNewWork = commitFailurePauseActive && state != DRAINING;
        if (userStateAllowsNewWork && !seamPauseBlocksNewWork) {
            if (isWorkerPoolShutDown()) {
                // don't take work there is nowhere to run - taking it would only get it dropped at the submit,
                // uncommitted, for redelivery after rebalance
                onPoolGoneWhileStateAllowsWork();
            } else {
                int delta = calculateQuantityToRequest();
                var records = wm.getWorkIfAvailable(delta);

                gotWorkCount = records.size();
                lastWorkRequestWasFulfilled = gotWorkCount >= delta;

                log.trace("Loop: Submit to pool");
                submitWorkToPool(userFunction, callback, records);
            }
        }

        //
        queueStatsLimiter.performIfNotLimited(() -> {
            int queueSize = getNumberOfUserFunctionsQueued();
            log.debug("Stats: \n- pool active: {} queued:{} \n- queue size: {} target: {} loading factor: {}",
                    workerThreadPool.get().getActiveCount(), queueSize, queueSize, getPoolLoadTarget(), dynamicExtraLoadFactor.getCurrentFactor());
        });

        return gotWorkCount;
    }


    /**
     * Whether the worker pool can no longer run anything handed to it. Two places ask - before taking work, and when
     * a submission is rejected - and both mean the same thing by it: nothing this pool is given from now on will ever
     * run.
     */
    private boolean isWorkerPoolShutDown() {
        return workerThreadPool.get().isShutdown();
    }

    /**
     * The state says work may be submitted, but the pool it would be submitted to is shut down.
     * <p>
     * A single control thread cannot produce that on its own: {@link #innerDoClose} is the only caller of
     * {@code workerThreadPool.shutdown()}, it is only reached from {@link #doClose}, {@code doClose} is only called
     * from inside the control task, and its {@code finally} sets the state to {@link State#CLOSED} before the loop
     * guard re-reads it - one thread writes both. So this is defence against the subsystem being misused from
     * outside: a pool supplied through {@link #setupWorkerPool} and shut down by whoever owns it, or a driver that
     * gives one instance two control threads.
     * <p>
     * It narrows the window, it does not close it - the pool can be shut down just after this check passes - so
     * {@link #submitWorkToPoolInner} still has to tolerate a rejection for work already taken.
     */
    private void onPoolGoneWhileStateAllowsWork() {
        if (handledPoolGoneWhileStateAllowsWork.compareAndSet(false, true)) {
            // The condition is sticky - a shut down pool never comes back - so the diagnosis is said once. Only the
            // log is gated: transitioning is idempotent, and gating that too would spend the trigger on the first
            // detection and leave a later close(DRAIN) with no way out of DRAINING.
            log.error("Worker pool is shut down while the state is {}, so this instance can never process another " +
                        "record. It only shuts its own pool down as part of closing, which also moves the state, so " +
                        "this pool was shut down from outside. Closing, rather than looping with nothing to run. " +
                        "Records already taken are not committed, so they are redelivered after rebalance. " +
                        "Pool stats: {}",
                    state, workerThreadPool.get());
        }

        // Record why, even though nothing is thrown. On master a dead pool reached the supervisor catch, which set
        // this, so a caller could ask getFailureCause() what happened. Closing quietly without it would make a
        // destroyed pool indistinguishable from an ordinary close - to a user health check, and to the chaos
        // harness's canary sweep, which reads exactly this field.
        if (failureReason == null) {
            failureReason = new IllegalStateException(msg(
                    "Worker pool is shut down while the state is {} - this instance can never process another record, "
                            + "so it is closing itself", state));
        }

        // Closing rather than throwing. The instance is unusable either way, but an orderly close still commits
        // what completed, releases the group membership and lets close() return normally, where an exception out
        // of the control thread leaves the caller to discover the corpse. Loud in the log, calm in the shutdown.
        // The interrupt this causes is cleared where it matters, immediately before doClose in controlLoop's state
        // switch, not here. Clearing at the point of cause leaves the hooks callback and two log statements between
        // the clear and the close, and any thread reaching notifySomethingToDo can re-arm the flag in that gap.
        // Every state transition and both forms of close() reach it, and it is public - so the senders are not an
        // enumerable set. Worker threads are the exception: addToMailbox only enqueues.
        transitionToClosing();
    }

    /**
     * Submit a piece of work to the processing pool.
     * <p>
     * A batch this method declines to dispatch is dropped. Its containers stay marked in flight and counted against
     * {@link WorkManager#getNumberRecordsOutForProcessing()}, which would matter on an instance that kept running -
     * but every path that declines is a closing instance or one whose pool is already dead, and its
     * {@link WorkManager} does not outlive it. The offsets are not committed, so the records are redelivered.
     *
     * @param workToProcess the polled records to process
     */
    protected <R> void submitWorkToPool(Function<PollContextInternal<K, V>, List<R>> usersFunction,
                                        Consumer<R> callback,
                                        List<WorkContainer<K, V>> workToProcess) {
        if (state.equals(CLOSING) || state.equals(CLOSED)) {
            log.debug("Not submitting new work as Parallel Consumer is in {} state, incoming work: {}, Pool stats: {}", state, workToProcess.size(), workerThreadPool.get());
            return;
        }
        if (!workToProcess.isEmpty()) {
            log.debug("New work incoming: {}, Pool stats: {}", workToProcess.size(), workerThreadPool.get());

            // perf: could inline makeBatches
            var batches = makeBatches(workToProcess);

            // debugging
            if (log.isDebugEnabled()) {
                var sizes = batches.stream().map(List::size).sorted().collect(Collectors.toList());
                log.debug("Number batches: {}, smallest {}, sizes {}", batches.size(), sizes.stream().findFirst().get(), sizes);
                List<Integer> integerStream = sizes.stream().filter(x -> x < (int) options.getBatchSize()).collect(Collectors.toList());
                if (integerStream.size() > 1) {
                    log.warn("More than one batch isn't target size: {}. Input number of batches: {}", integerStream, batches.size());
                }
            }

            // submit
            for (var batch : batches) {
                if (!submitWorkToPoolInner(usersFunction, callback, batch)) {
                    // the pool is gone, so every remaining batch would reject too - and each would log its own
                    // stack trace. One warning per poll is the useful signal; N of them is noise.
                    break;
                }
            }
        }
    }

    /**
     * @return false if the pool rejected the batch because it is shut down, in which case the batch is dropped -
     *         uncommitted, so redelivered after rebalance - and no further batch can be submitted either.
     * @throws RejectedExecutionException if a live pool rejected the batch, which means saturation rather than
     *                                    shutdown and is not something this class can absorb
     */
    private <R> boolean submitWorkToPoolInner(final Function<PollContextInternal<K, V>, List<R>> usersFunction,
                                              final Consumer<R> callback,
                                              final List<WorkContainer<K, V>> batch) {
        // for each record, construct dispatch to the executor and capture a Future
        log.trace("Sending work ({}) to pool", batch);
        Future outputRecordFuture;
        try {
            outputRecordFuture = workerThreadPool.get().submit(() -> {
                addInstanceMDC();
                return runUserFunction(usersFunction, callback, batch);
            });
        } catch (RejectedExecutionException e) {
            // Narrow on purpose, and safe to be: #requireRejectionIsVisible refuses any pool whose handler is not an
            // AbortPolicy, so RejectedExecutionException is the only thing a rejection here can throw.
            if (!isWorkerPoolShutDown()) {
                // A live pool rejected, which means saturation rather than shutdown - #setupWorkerPool's queue is
                // unbounded, so this takes a subclass that bounds it. Absorbing that would drop work under healthy
                // load, which is the one thing this catch must never do, so it stays loud.
                throw e;
            }
            // The pool is shut down, so this is the close racing work distribution. The batch is dropped: a closing
            // instance does not commit these offsets, so the records are redelivered after rebalance.
            // Warn rather than debug: the state guard above absorbs the ordinary closing case, so reaching
            // here means the pool died while the state still said otherwise - rare, and worth noticing.
            // Count and a locator, not the batch itself: rendering the records makes this line grow with batch size
            // until log tooling truncates it, which is astubbs#169 and astubbs#170's complaint in a third place.
            var first = batch.get(0);
            log.warn("Worker pool is shut down, not submitting work ({} record(s), first {}:{}). Records will be redelivered.",
                    batch.size(), first.getTopicPartition(), first.offset(), e);
            return false;
        }
        // for a batch, each message in the batch shares the same result
        for (final WorkContainer<K, V> workContainer : batch) {
            workContainer.setFuture(outputRecordFuture);
        }
        return true;
    }

    private List<List<WorkContainer<K, V>>> makeBatches(List<WorkContainer<K, V>> workToProcess) {
        int maxBatchSize = options.getBatchSize();
        return partition(workToProcess, maxBatchSize);
    }

    private static <T> List<List<T>> partition(Collection<T> sourceCollection, int maxBatchSize) {
        List<List<T>> listOfBatches = new ArrayList<>();
        List<T> batchInConstruction = new ArrayList<>();

        //
        for (T item : sourceCollection) {
            batchInConstruction.add(item);

            //
            if (batchInConstruction.size() == maxBatchSize) {
                listOfBatches.add(batchInConstruction);
                batchInConstruction = new ArrayList<>();
            }
        }

        // add partial tail
        if (!batchInConstruction.isEmpty()) {
            listOfBatches.add(batchInConstruction);
        }

        if (log.isDebugEnabled()) {
            log.debug("sourceCollection.size() {}, batches: {}, batch sizes {}",
                    sourceCollection.size(),
                    listOfBatches.size(),
                    listOfBatches.stream().map(List::size).collect(Collectors.toList()));
        }
        return listOfBatches;
    }

    /**
     * @return number of {@link WorkContainer} to try to get
     */
    protected int calculateQuantityToRequest() {
        int target = getTargetOutForProcessing();
        int current = wm.getNumberRecordsOutForProcessing();
        int delta = target - current;

        // always round up to fill batches - get however extra are needed to fill a batch
        if (options.isUsingBatching()) {
            //noinspection OptionalGetWithoutIsPresent
            int batchSize = options.getBatchSize();
            int modulo = delta % batchSize;
            if (modulo > 0) {
                int extraToFillBatch = target - modulo;
                delta = delta + extraToFillBatch;
            }
        }

        log.debug("Will try to get work - target: {}, current queue size: {}, requesting: {}, loading factor: {}",
                target, current, delta, dynamicExtraLoadFactor.getCurrentFactor());
        return delta;
    }

    protected int getTargetOutForProcessing() {
        return getQueueTargetLoaded();
    }

    protected int getQueueTargetLoaded() {
        //noinspection unchecked
        return getPoolLoadTarget() * dynamicExtraLoadFactor.getCurrentFactor();
    }

    /**
     * Checks the system has enough pressure in the pipeline of work, if not attempts to step up the load factor.
     */
    protected void checkPipelinePressure() {
        if (log.isTraceEnabled())
            log.trace("Queue pressure check: (current size: {}, loaded target: {}, factor: {}) " +
                            "if (isPoolQueueLow() {} && lastWorkRequestWasFulfilled {}))",
                    getNumberOfUserFunctionsQueued(),
                    getQueueTargetLoaded(),
                    dynamicExtraLoadFactor.getCurrentFactor(),
                    isPoolQueueLow(),
                    lastWorkRequestWasFulfilled);

        if (isPoolQueueLow() && lastWorkRequestWasFulfilled) {
            boolean steppedUp = dynamicExtraLoadFactor.maybeStepUp();
            if (steppedUp) {
                log.debug("isPoolQueueLow(): Executor pool queue is not loaded with enough work (queue: {} vs target: {}), stepped up loading factor to {}",
                        getNumberOfUserFunctionsQueued(), getPoolLoadTarget(), dynamicExtraLoadFactor.getCurrentFactor());
            } else if (dynamicExtraLoadFactor.isMaxReached()) {
                log.warn("isPoolQueueLow(): Max loading factor steps reached: {}/{}", dynamicExtraLoadFactor.getCurrentFactor(), dynamicExtraLoadFactor.getMaxFactor());
            }
        }
    }

    /**
     * @return aim to never have the pool queue drop below this
     */
    private int getPoolLoadTarget() {
        return options.getTargetAmountOfRecordsInFlight();
    }

    private boolean isPoolQueueLow() {
        int queueSize = getNumberOfUserFunctionsQueued();
        int queueTarget = getPoolLoadTarget();
        boolean workAmountBelowTarget = queueSize <= queueTarget;
        log.debug("isPoolQueueLow()? workAmountBelowTarget {} {} vs {};",
                workAmountBelowTarget, queueSize, queueTarget);
        return workAmountBelowTarget;
    }

    private void drain() {
        log.debug("Signaling to drain...");
        brokerPollSubsystem.drain();
        if (!isRecordsAwaitingProcessing()) {
            transitionToClosing();
        } else {
            log.debug("Records still waiting processing, won't transition to closing.");
        }
    }

    private void transitionToClosing() {
        log.debug("Transitioning to closing...");
        if (state == State.UNUSED) {
            state = CLOSED;
        } else {
            state = State.CLOSING;
        }
        notifySomethingToDo();
    }

    /**
     * Check the work queue for work to be done, potentially blocking.
     * <p>
     * Can be interrupted if something else needs doing.
     * <p>
     * Visible for testing.
     */
    protected void processWorkCompleteMailBox(final Duration timeToBlockFor) {
        log.trace("Processing mailbox (might block waiting for results)...");
        Queue<ControllerEventMessage<K, V>> results = new ArrayDeque<>();

        if (timeToBlockFor.toMillis() > 0) {
            currentlyPollingWorkCompleteMailBox.getAndSet(true);
            if (log.isDebugEnabled()) {
                log.debug("Blocking poll on work until next scheduled offset commit attempt for {}. active threads: {}, queue: {}",
                        timeToBlockFor, workerThreadPool.get().getActiveCount(), getNumberOfUserFunctionsQueued());
            }
            // wait for work, with a timeToBlockFor for sanity
            log.trace("Blocking poll {}", timeToBlockFor);
            try {
                var firstBlockingPoll = workMailBox.poll(timeToBlockFor.toMillis(), MILLISECONDS);
                if (firstBlockingPoll == null) {
                    log.debug("Mailbox results returned null, indicating timeToBlockFor elapsed (which was set as {})", timeToBlockFor);
                } else {
                    log.debug("Work arrived in mailbox during blocking poll. (Timeout was set as {})", timeToBlockFor);
                    results.add(firstBlockingPoll);
                }
            } catch (InterruptedException e) {
                log.debug("Interrupted waiting on work results");
            } finally {
                currentlyPollingWorkCompleteMailBox.getAndSet(false);
            }
            log.trace("Blocking poll finish");
        }

        // check for more work to batch up, there may be more work queued up behind the head that we can also take
        // see how big the queue is now, and poll that many times
        int size = workMailBox.size();
        log.trace("Draining {} more, got {} already...", size, results.size());
        workMailBox.drainTo(results, size);

        log.trace("Processing drained work {}...", results.size());
        for (var action : results) {
            if (action.isNewConsumerRecords()) {
                wm.registerWork(action.getConsumerRecords());
            } else {
                WorkContainer<K, V> work = action.getWorkContainer();
                MDC.put(MDC_WORK_CONTAINER_DESCRIPTOR, work.toString());
                wm.handleFutureResult(work);
                MDC.remove(MDC_WORK_CONTAINER_DESCRIPTOR);
            }
        }
    }

    /**
     * The amount of time to block poll in this cycle
     *
     * @return either the duration until next commit, or next work retry
     * @see ParallelConsumerOptions#getTargetAmountOfRecordsInFlight()
     */
    private Duration getTimeToBlockFor() {
        // if less than target work already in flight, don't sleep longer than the next retry time for failed work, if it exists - so that we can wake up and maybe retry the failed work
        if (!wm.isWorkInFlightMeetingTarget()) {
            // though check if we have work awaiting retry
            var lowestScheduledOpt = wm.getLowestRetryTime();
            if (lowestScheduledOpt.isPresent()) {
                // todo can sleep for less than this time? is this lower bound required? given that if we're starved - the failed work will most likely be selected? And even if not selected - then we will no longer be starved.
                Duration retryDelay = options.getDefaultMessageRetryDelay();
                // at min block for the retry time - retry time is not exact
                Duration lowestScheduled = lowestScheduledOpt.get();
                Duration timeBetweenCommits = getTimeBetweenCommits();
                Duration effectiveRetryDelay = lowestScheduled.toMillis() < retryDelay.toMillis() ? retryDelay : lowestScheduled;
                Duration result = timeBetweenCommits.toMillis() < effectiveRetryDelay.toMillis() ? timeBetweenCommits : effectiveRetryDelay;
                log.debug("Not enough work in flight, while work is waiting to be retried - so will only sleep until next retry time of {} (lowestScheduled = {})", result, lowestScheduled);
                return result;
            }
        }

        //
        Duration effectiveCommitAttemptDelay = getTimeToNextCommitCheck();
        log.debug("Calculated next commit time in {}", effectiveCommitAttemptDelay);
        return effectiveCommitAttemptDelay;
    }

    private boolean isIdlingOrRunning() {
        return state == RUNNING || state == DRAINING || state == PAUSED;
    }

    protected boolean isTimeToCommitNow() {
        updateLastCommitCheckTime();

        Duration elapsedSinceLastCommit = this.lastCommitTime == null ? Duration.ofDays(1) : Duration.between(this.lastCommitTime, Instant.now());

        boolean commitFrequencyOK = elapsedSinceLastCommit.compareTo(getTimeBetweenCommits()) > 0;
        boolean isCommandedToCommit = isCommandedToCommit();

        boolean shouldCommitNow = commitFrequencyOK || isCommandedToCommit;

        if (log.isDebugEnabled()) {
            log.debug("Should commit this cycle? " +
                    "shouldCommitNow? " + shouldCommitNow + " : " +
                    "commitFrequencyOK? " + commitFrequencyOK + ", " +
                    "isCommandedToCommit? " + isCommandedToCommit
            );
        }

        return shouldCommitNow;
    }

    private int getNumberOfUserFunctionsQueued() {
        return workerThreadPool.get().getQueue().size();
    }


    private Duration getTimeToNextCommitCheck() {
        // draining is a normal running mode for the controller
        if (isIdlingOrRunning()) {
            Duration timeSinceLastCommit = getTimeSinceLastCheck();
            Duration timeBetweenCommits = getTimeBetweenCommits();
            @SuppressWarnings("UnnecessaryLocalVariable")
            Duration minus = timeBetweenCommits.minus(timeSinceLastCommit);
            return minus;
        } else {
            log.debug("System not {} (state: {}), so don't wait to commit, only a small thread yield time", RUNNING, state);
            return Duration.ZERO;
        }
    }

    private Duration getTimeSinceLastCheck() {
        Instant now = clock.instant();
        return Duration.between(lastCommitCheckTime, now);
    }

    /**
     * Visible for testing
     */
    protected void commitOffsetsThatAreReady() throws TimeoutException, InterruptedException {
        log.trace("Synchronizing on commitCommand...");
        synchronized (commitCommand) {
            log.debug("Committing offsets that are ready...");
            committer.retrieveOffsetsAndCommit();
            clearCommitCommand();
            this.lastCommitTime = Instant.now();
            if (committer.lastCommitWasDeferred()) {
                // A DEFERRED cycle (rebalance-class: postponed, not dropped) advances only the cadence clock
                // above - it reached no broker, so it is not a success (astubbs#317). Treating it as one
                // would advance lastSuccessfulCommitTime every deferred cycle, laundering the handler's
                // time-since-last-successful-commit story while nothing commits; wipe the consecutive count
                // mid-streak; and release the seam pause on the strength of a commit that never happened. The
                // deferral's own accounting - and its escalation to the handler once a streak outlives the
                // offsetCommitTimeout quantum - lives with the observer of the deferrals,
                // ConsumerOffsetCommitter#commitDeferringOnRebalance.
                log.debug("Commit cycle was deferred - commit cadence advances, but the commit-failure seam's " +
                        "success accounting deliberately does not");
            } else {
                // the commit cycle completed without a terminal failure, so the commit-failure seam's history
                // resets (astubbs#317)
                this.lastSuccessfulCommitTime = this.lastCommitTime;
                this.consecutiveExhaustedBudgets = 0;
                releaseCommitFailurePauseIfActive("a commit succeeded");
            }
        }
    }

    private void updateLastCommitCheckTime() {
        lastCommitCheckTime = Instant.now();
    }

    /**
     * Run the supplied function.
     */
    protected <R> List<ParallelConsumer.Tuple<ConsumerRecord<K, V>, R>> runUserFunction(Function<PollContextInternal<K, V>, List<R>> usersFunction,
                                                                                        Consumer<R> callback,
                                                                                        List<WorkContainer<K, V>> workContainerBatch) {
        if (log.isDebugEnabled()) {
            // first offset of the batch
            MDC.put(MDC_WORK_CONTAINER_DESCRIPTOR, workContainerBatch.get(0).offset() + "");
        }
        log.trace("Pool received: {}", workContainerBatch);

        /*
         *  Handle and filter stale work from the batch, before creating the internal context for running user function.
         *  The context created is used by the "wrapped" user function to inject transactional producer synchronization.
         */
        Map<Boolean, List<WorkContainer<K, V>>> splitContainersMap = workContainerBatch.stream()
                .collect(Collectors.groupingBy(wm::checkIfWorkIsStale));
        final List<WorkContainer<K, V>> staleWorkContainers = splitContainersMap.getOrDefault(Boolean.TRUE, new ArrayList<>());
        final List<WorkContainer<K, V>> activeWorkContainers = splitContainersMap.getOrDefault(Boolean.FALSE, new ArrayList<>());

        handleStaleWork(staleWorkContainers);

        final PollContextInternal<K, V> context = new PollContextInternal<>(activeWorkContainers);

        try {
            if (!activeWorkContainers.isEmpty()) {
                return runUserFunctionInternal(usersFunction, context, callback, activeWorkContainers);
            }
            return Collections.emptyList();
        } catch (Exception e) {
            // handle fail
            var cause = e.getCause();
            String msg = msg("Exception caught in user function running stage, registering WC as failed, returning to" +
                    " mailbox. Context: {}", context, e);
            if (cause instanceof PCRetriableException) {
                log.debug("Explicit " + PCRetriableException.class.getSimpleName() + " caught, logging at DEBUG only. " + msg, e);
            } else {
                log.error(msg, e);
            }

            for (var wc : workContainerBatch) {
                wc.onUserFunctionFailure(e);
                addToMailbox(context, wc); // always add on error
            }
            throw e; // trow again to make the future failed
        } finally {
            cleanUpContext(context);
        }
    }

    /**
     * Given the batch of work containers, publish stale work to feedback loop to be reduced from in progress work.
     *
     * @param workContainerBatch
     */
    protected void handleStaleWork(final List<WorkContainer<K, V>> staleWorkContainers) {
        final PollContextInternal<K, V> internalContext = new PollContextInternal<>(staleWorkContainers);
        try {
            if (!staleWorkContainers.isEmpty()) {
                // when epoch's change, we can't remove them from the executor pool queue, so we just have to skip them when we find them
                log.debug("Pool found work from old generation of assigned work, skipping message as epoch doesn't match current {}", staleWorkContainers);
                staleWorkContainers.forEach(wc -> addToMailbox(internalContext, wc));
            }
        } finally {
            cleanUpContext(internalContext);
        }
    }

    protected <R> ArrayList<Tuple<ConsumerRecord<K, V>, R>> runUserFunctionInternal(final Function<PollContextInternal<K, V>, List<R>> usersFunction,
                                                                                    final PollContextInternal<K, V> context,
                                                                                    final Consumer<R> callback,
                                                                                    final List<WorkContainer<K, V>> activeWorkContainers) {
        List<R> resultsFromUserFunction;
        resultsFromUserFunction = userProcessingTimer.record(() -> usersFunction.apply(context));


        for (final WorkContainer<K, V> kvWorkContainer : activeWorkContainers) {
            onUserFunctionSuccess(kvWorkContainer, resultsFromUserFunction);
        }

        // capture each result, against the input record
        var intermediateResults = new ArrayList<Tuple<ConsumerRecord<K, V>, R>>();
        for (R result : resultsFromUserFunction) {
            log.trace("Running users call back...");
            callback.accept(result);
        }

        // fail or succeed, either way we're done
        for (var kvWorkContainer : activeWorkContainers) {
            addToMailBoxOnUserFunctionSuccess(context, kvWorkContainer, resultsFromUserFunction);
        }
        log.trace("User function future registered");

        return intermediateResults;
    }

    private void cleanUpContext(final PollContextInternal<K, V> context) {
        context.getProducingLock().ifPresent(ProducerManager.ProducingLock::unlock);
    }

    protected void addToMailBoxOnUserFunctionSuccess(PollContextInternal<K, V> context, WorkContainer<K, V> wc, List<?> resultsFromUserFunction) {
        addToMailbox(context, wc);
    }

    protected void onUserFunctionSuccess(WorkContainer<K, V> wc, List<?> resultsFromUserFunction) {
        log.trace("User function success");
        wc.onUserFunctionSuccess();
    }

    protected void addToMailbox(PollContextInternal<K, V> pollContext, WorkContainer<K, V> wc) {
        String state = wc.isUserFunctionSucceeded() ? "succeeded" : "FAILED";
        log.trace("Adding {} {} to mailbox...", state, wc);
        workMailBox.add(ControllerEventMessage.of(wc));

        wc.onPostAddToMailBox(pollContext, producerManager);
    }

    public void registerWork(EpochAndRecordsMap<K, V> polledRecords) {
        log.trace("Adding {} to mailbox...", polledRecords);
        workMailBox.add(ControllerEventMessage.of(polledRecords));
    }

    /**
     * Early notify of work arrived.
     * <p>
     * Only wake up the thread if it's sleeping while polling the mail box.
     *
     * @see #processWorkCompleteMailBox
     * @see #blockableControlThread
     */
    public void notifySomethingToDo() {
        boolean noTransactionInProgress = !producerManager.map(ProducerManager::isTransactionCommittingInProgress).orElse(false);
        // do not interrupt while workerThreadPool is draining submitted / inflight tasks
        if (noTransactionInProgress && !awaitingInflightProcessingCompletionOnShutdown.get()) {
            log.trace("Interrupting control thread: Knock knock, wake up! You've got mail (tm)!");
            interruptControlThread();
        } else {
            log.trace("Would have interrupted control thread, but TX in progress");
        }
    }

    @Override
    public long workRemaining() {
        return wm.getNumberOfIncompleteOffsets();
    }

    /**
     * Plugin a function to run at the end of each main loop.
     * <p>
     * Useful for testing and controlling loop progression.
     */
    public void addLoopEndCallBack(Runnable r) {
        this.controlLoopHooks.add(r);
    }

    public void setLongPollTimeout(Duration ofMillis) {
        BrokerPollSystem.setLongPollTimeout(ofMillis);
    }

    /**
     * Request a commit as soon as possible (ASAP), overriding other constraints.
     */
    public void requestCommitAsap() {
        log.debug("Registering command to commit next chance");
        synchronized (commitCommand) {
            this.commitCommand.set(true);
        }
        notifySomethingToDo();
    }


    private boolean isTransactionCommittingInProgress() {
        return options.isUsingTransactionCommitMode() &&
                producerManager.map(ProducerManager::isTransactionCommittingInProgress).orElse(false);
    }

    @Override
    public void pauseIfRunning() {
        if (this.state == State.RUNNING) {
            log.info("Transitioning parallel consumer to state paused.");
            this.state = State.PAUSED;
        } else {
            log.debug("Skipping transition of parallel consumer to state paused. Current state is {}.", this.state);
        }
    }

    @Override
    public void resumeIfPaused() {
        if (this.state == State.PAUSED) {
            log.info("Transitioning parallel consumer to state running.");
            this.state = State.RUNNING;
            notifySomethingToDo();
        } else {
            log.debug("Skipping transition of parallel consumer to state running. Current state is {}.", this.state);
        }
    }

    private boolean isCommandedToCommit() {
        synchronized (commitCommand) {
            return this.commitCommand.get();
        }
    }

    private void clearCommitCommand() {
        synchronized (commitCommand) {
            if (commitCommand.get()) {
                log.debug("Command to commit asap received, clearing");
                this.commitCommand.set(false);
            }
        }
    }

}