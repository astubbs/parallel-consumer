package bz.stub.parallelconsumer.internal;

/*-
 * Copyright (C) 2020-2024 Confluent, Inc.
 * Modifications Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.internal.utils.ThrowableUtils;
import bz.stub.parallelconsumer.internal.utils.SupplierUtils;
import bz.stub.parallelconsumer.internal.utils.TimeUtils;
import bz.stub.parallelconsumer.*;
import bz.stub.parallelconsumer.metrics.PCMetrics;
import bz.stub.parallelconsumer.metrics.PCMetricsDef;
import bz.stub.parallelconsumer.state.WorkContainer;
import bz.stub.parallelconsumer.state.WorkManager;
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
import static bz.stub.parallelconsumer.internal.utils.ThrowableUtils.describeWithRootCause;
import static bz.stub.parallelconsumer.internal.utils.ThrowableUtils.logWithoutEscaping;
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

    /**
     * All consumer access goes through ConsumerManager (which wraps with ThreadConfinedConsumer).
     * No raw Consumer<K,V> reference is held — enforced by ArchUnit. See confluentinc#857.
     */
    private final ConsumerManager<K, V> consumerManager;

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
     * Useful for testing async code.
     * <p>
     * Concurrent because {@link #addLoopEndCallBack} is public and callable from any thread, while the control loop
     * iterates this list every cycle. A plain list breaks its own iteration when a registration lands mid-cycle, and
     * the resulting {@link java.util.ConcurrentModificationException} escapes the control loop and stops the consumer.
     * Writes are rare and iteration happens every loop, which is exactly what copy-on-write is for.
     */
    private final List<Runnable> controlLoopHooks = new CopyOnWriteArrayList<>();

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
     * Lock for offset commit operations. Replaces synchronized(commitCommand) for commit execution
     * to allow tryLock() semantics in rebalance callbacks, preventing the deadlock in confluentinc#857.
     */
    private final java.util.concurrent.locks.ReentrantLock commitLock = new java.util.concurrent.locks.ReentrantLock();

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
    // volatile because it is no longer read only by the thread that writes it: ExternalEngine's dispatch
    // thread blocks on the external dispatch ceiling and reads this to learn that a close has begun. That is a
    // plain flag with no lock to name, so `volatile` is the whole fix and there is no @GuardedBy to write - see
    // parallel-consumer-core/src/main/java/bz/stub/parallelconsumer/AGENTS.md.
    @Setter(AccessLevel.PACKAGE)
    @Getter(PROTECTED)
    private volatile State state = State.UNUSED;

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
     * Carries the caller's SLF4J diagnostic context across into the threads that run the user function.
     *
     * @see MdcPropagation
     */
    @Getter(PROTECTED)
    private final MdcPropagation mdcPropagation;

    /**
     * Snapshot of the diagnostic context of the thread that called {@code poll*}, taken in
     * {@link #supervisorLoop(Function, Consumer)}. {@code null} when that thread had no context, or when propagation is
     * disabled.
     * <p>
     * Volatile because it is written by the caller's thread and read by the controller and broker-poller threads it
     * then starts.
     *
     * @see MdcPropagation
     */
    volatile Map<String, String> callersDiagnosticContext;

    /**
     * Control for stepping loading factor - shouldn't step if work requests can't be fulfilled due to restrictions.
     * (e.g. we may want 10, but maybe there's a single partition and we're in partition mode - stepping up won't
     * help).
     */
    private boolean lastWorkRequestWasFulfilled = false;

    private io.micrometer.core.instrument.Timer userProcessingTimer;
    private Gauge loadFactorGauge;
    private Gauge statusGauge;

    private Duration shutdownTimeout;

    /**
     * How the user asked us to close. Recorded so the close path can tell whether an uncommitted
     * offset is the consequence of a choice they can change ({@link DrainingMode#DONT_DRAIN}) or
     * something that happened despite draining - the difference between advice worth printing and
     * noise.
     */
    private volatile DrainingMode requestedDrainMode = DrainingMode.DONT_DRAIN;

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
        this.mdcPropagation = module.mdcPropagation();
        options = newOptions;
        this.shutdownTimeout = options.getShutdownTimeout();
        this.drainTimeout = options.getDrainTimeout();
        this.consumerManager = module.consumerManager();

        validateConfiguration();

        module.setParallelEoSStreamProcessor(this);

        log.info("Confluent Parallel Consumer initialise... groupId: {}, Options: {}",
                consumerManager.groupMetadata().groupId(),
                newOptions);
        //Initialize global metrics - should be initialized before any of the module objects are created so that meters can be bound in them.
        pcMetrics = module.pcMetrics();

        this.dynamicExtraLoadFactor = module.dynamicExtraLoadFactor();

        workerThreadPool = SupplierUtils.memoize(() -> requireRejectionIsVisible(setupWorkerPool(newOptions.getMaxConcurrency())));
        forceWorkerPoolConstruction();

        this.wm = module.workManager();

        this.brokerPollSubsystem = module.brokerPoller(this);

        if (options.isProducerSupplied()) {
            this.producerManager = Optional.of(module.producerManager());
            // a worker parked on the produce lock during a producer outage is released as soon as this instance
            // leaves RUNNING or PAUSED, so a close during the outage does not wait out the shutdown timeout (R15)
            this.producerManager.get().setSuspensionEndsWhen(() -> state != RUNNING && state != State.PAUSED);
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
        new ExecutorServiceMetrics(this.getWorkerThreadPool().get(), "pc-user-function-executor",
                USER_FUNCTION_EXECUTOR_PREFIX,
                pcMetrics.getCommonTags()).bindTo(pcMetrics.getMeterRegistry());
    }

    private void validateConfiguration() {
        options.validate();

        checkGroupIdConfigured();
        checkNotSubscribed(options.getConsumer());
        checkAutoCommitIsDisabled(options.getConsumer());
    }

    private void checkGroupIdConfigured() {
        try {
            var metadata = consumerManager.groupMetadata();
            if (metadata == null) {
                throw new IllegalArgumentException("Error validating Consumer configuration - no group metadata - missing a " +
                        "configured GroupId on your Consumer?");
            }
        } catch (IllegalArgumentException e) {
            throw e; // rethrow our own
        } catch (RuntimeException e) {
            throw new IllegalArgumentException("Error validating Consumer configuration - no group metadata - missing a " +
                    "configured GroupId on your Consumer?", e);
        }
    }

    /**
     * Looks up a container-managed resource by JNDI name, falling back to the Java SE equivalent when there is no
     * container to ask.
     * <p>
     * This method exists to be the smallest possible thing carrying {@code @SuppressWarnings("BanJNDI")}. The
     * suppression used to sit on the callers, one of which is a fifty-line method - so a JNDI lookup added anywhere
     * in that body later would have been waved through by a suppression written for an unrelated line. A suppression
     * is a claim about one call, and its scope should say so.
     */
    // BanJNDI: the lookup name is this library's own managedThreadFactory / managedExecutorService option, set by
    // the embedding application, and the whole point is to use the container's executor when running inside one.
    // Suppressed here rather than demoted globally, so a new JNDI lookup anywhere else still fails the build.
    // docs/inflight/static-error-prone-rule-registry.md carries the reasoning and the re-enable trigger.
    @SuppressWarnings("BanJNDI")
    private static <T> T lookupManagedResource(String jndiName, Supplier<T> javaSeFallback) {
        try {
            return InitialContext.doLookup(jndiName);
        } catch (NamingException e) {
            log.debug("Using Java SE Thread", e);
            return javaSeFallback.get();
        }
    }

    protected ThreadPoolExecutor setupWorkerPool(int poolSize) {
        // Was a non-final local plus a `finalDefaultFactory` copy, because a try/catch cannot assign to a final.
        // The lookup returning a value rather than assigning one removes the need for both.
        final ThreadFactory defaultFactory =
                lookupManagedResource(options.getManagedThreadFactory(), Executors::defaultThreadFactory);
        ThreadFactory namingThreadFactory = r -> {
            Thread thread = defaultFactory.newThread(r);
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

    /**
     * Forces the memoized {@link #workerThreadPool} supplier at construction rather than leaving it to the first
     * dispatch. {@link #requireRejectionIsVisible} is a precondition on a subclass's {@link #setupWorkerPool}, and a
     * precondition that only fires when the first batch is submitted is one a subclass can ship without ever meeting.
     * Construction built the pool anyway - {@code initMetrics} binds meters to it - so this changes no startup
     * behaviour. It only stops the precondition's timing from depending on that, and moves the failure ahead of the
     * poller and producer manager, so nothing half built has to be unwound.
     * <p>
     * The pool is discarded on purpose: the supplier keeps it and every later reader goes through
     * {@link #workerThreadPool}. Extracted into its own method so the suppression covers exactly this one call - a
     * named local would satisfy Error Prone and then trip SpotBugs' {@code DLS_DEAD_LOCAL_STORE} instead, which is
     * trading one finding for another rather than saying what is meant.
     */
    @SuppressWarnings("ReturnValueIgnored")
    private void forceWorkerPoolConstruction() {
        workerThreadPool.get();
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
        consumerManager.subscribe(topics, this);
    }

    @Override
    public void subscribe(Pattern pattern) {
        log.debug("Subscribing to {}", pattern);
        consumerManager.subscribe(pattern, this);
    }

    @Override
    public void subscribe(Collection<String> topics, ConsumerRebalanceListener callback) {
        log.debug("Subscribing to {}", topics);
        usersConsumerRebalanceListener = Optional.of(callback);
        consumerManager.subscribe(topics, this);
    }

    @Override
    public void subscribe(Pattern pattern, ConsumerRebalanceListener callback) {
        log.debug("Subscribing to {}", pattern);
        usersConsumerRebalanceListener = Optional.of(callback);
        consumerManager.subscribe(pattern, this);
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
            // Try to commit offsets for revoked partitions, but don't block if the control
            // thread is already mid-commit. Blocking here deadlocks: the poll thread (us)
            // holds the rebalance callback, and the control thread's commitSync() needs the
            // poll thread to be responsive. If we can't commit, it's safe — the offsets will
            // be re-delivered to the new assignee. See confluentinc#857.
            tryCommitOffsetsOnRevoke();

            // truncate the revoked partitions
            wm.onPartitionsRevoked(partitions);
        } catch (Exception e) {
            throw new PCInternalRuntimeException("onPartitionsRevoked event error", e);
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
     * Non-blocking attempt to commit offsets during partition revocation. Uses tryLock semantics
     * on the commitCommand monitor to avoid deadlocking with the control thread.
     * <p>
     * If the lock is already held (control thread is mid-commit), we skip the commit. This is
     * safe because Kafka will re-deliver uncommitted records to the new partition assignee.
     * <p>
     * See <a href="https://github.com/confluentinc/parallel-consumer/issues/857">#857</a> —
     * the original synchronized(commitCommand) call in onPartitionsRevoked caused a deadlock
     * between the poll thread and the control thread under rebalance churn.
     */
    private void tryCommitOffsetsOnRevoke() {
        if (commitLock.tryLock()) {
            try {
                // INFO, not DEBUG, and deliberately paired with the decline below at the same level:
                // the two branches are the two outcomes of one fork, and logging only one of them
                // makes "the revoke path never contended" indistinguishable from "the revoke path
                // never ran". A seed replay cannot tell whether the deadlock window opened without
                // this line, which is what voided the 2026-08-31 cooperative replay.
                log.info("Acquired commitLock on revoke without contention - committing offsets " +
                        "inline. See confluentinc#857.");
                committer.retrieveOffsetsAndCommit();
                clearCommitCommand();
                this.lastCommitTime = Instant.now();
            } catch (Exception e) {
                // Restore the flag rather than swallowing the interrupt: this runs inside the poll
                // thread's rebalance callback, and dropping it strands whatever is waiting on it.
                if (e instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                }
                // Pass the throwable, never e.getMessage(): the message alone drops the type, the
                // cause chain and the stack, and an exception thrown without one renders as
                // "...: null" - the exact complaint behind astubbs#177. describeWithRootCause is
                // interpolated alongside it so a description survives even if the trace is elided.
                ThrowableUtils.logWithoutEscaping(e, () ->
                        log.warn("Failed to commit offsets during revoke: {}",
                                ThrowableUtils.describeWithRootCause(e), e));
            } finally {
                commitLock.unlock();
            }
        } else {
            log.info("Skipping offset commit during partition revocation — control thread is mid-commit. " +
                    "Uncommitted offsets will be re-delivered to the new assignee. See confluentinc#857.");
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
        wm.onPartitionsAssigned(partitions);
        // Reset the throttle flag — Kafka clears its internal pause state on reassignment,
        // so our flag must match. Without this, shouldThrottle() may re-pause the new
        // partitions immediately if stale shard counts make it think we're overloaded.
        // See confluentinc#857.
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

    /**
     * Terminates PC because a record could not be returned to the mailbox.
     * <p>
     * <b>This is not user code failing, it is PC's own bookkeeping failing</b>, and the consequence is that PC can
     * no longer account for that record: it is neither in flight nor completed, so nothing will retry it and
     * nothing will report it.
     * <p>
     * <b>The route this was written for has since been closed, and the guard is kept deliberately.</b> When
     * astubbs#267 added these guards, {@link #addToMailbox} also released the produce lock, so a double release
     * raised {@link ProduceLockNotHeldException} from {@code ProducerManager#finishProducing} straight through the
     * mailbox path - that was the named, suspected-live route. astubbs#257 made {@code cleanUpContext} the single
     * release point and removed the release from {@link #addToMailbox}, so core's {@link #addToMailbox} is now a
     * queue add and nothing else, and that route is gone.
     * <p>
     * <b>It is still reachable, which is why nothing here was deleted.</b> {@link #addToMailbox} is a
     * {@code protected} extension point and {@link ExternalEngine} overrides it to return the dispatch permit, so a
     * subclass can still throw here; and the queue add itself throws on {@code OutOfMemoryError}. What changed is
     * that PC no longer has a known invariant on this path, not that the path became infallible - and an
     * unmailboxable record is equally unaccountable whichever route produced it.
     * <p>
     * {@code Throwable} rather than {@code Exception} at the call sites, because an {@code Error} raised here would
     * otherwise pass straight through the very guards this path exists to be. Continuing from there risks committing past work that was never done - a silent
     * skip, with no error and no lag anomaly to find it by. Operator ruling on astubbs#267: a failure to post the
     * letter is a terminal system failure, and continued operation under a suspected skip is not permitted.
     * <p>
     * <b>It signals rather than closing, and that is load-bearing twice over.</b> {@link #closeOnException} waits
     * for the shutdown to finish, and every caller of this method is either a batch loop or an async completion
     * handler - blocking one would hold up the very records still waiting to be mailboxed, and on vert.x it would
     * hold the event loop. Throwing is equally unavailable: an exception escaping these sites skips every sibling
     * container behind it, which is the stall this whole path exists to prevent. So the reason is recorded, the
     * state is moved to CLOSING, and the control thread performs the shutdown on its own.
     * <p>
     * The reason is written BEFORE the state, because {@link #state} is volatile and its write is what publishes
     * the reason to the control thread. That ordering is exact.
     * <p>
     * <b>The first-failure preference is best-effort, and deliberately not more than that.</b> The
     * {@code failureReason == null} test and the write that follows it are not atomic, so two workers failing to
     * mailbox at once can race and the later diagnosis can win. Left as a plain check because the cost is which of
     * two genuine causes is reported, both of which are the same bug, while making it atomic would put a lock or a
     * field-type change on a path whose contract is that it never blocks. Raised by review on astubbs#267, where an
     * earlier version of this comment claimed an ordering it did not deliver.
     *
     * @param wc              the container that could not be returned
     * @param mailboxingThrew what {@link #addToMailbox} threw
     */
    protected void failFatallyOnUnmailboxableRecord(WorkContainer<K, V> wc, Throwable mailboxingThrew) {
        try {
            var failure = new UnmailboxableRecordException(msg(
                    "Could not return {} to the mailbox. PC can no longer account for this record, so it is "
                            + "shutting down rather than continuing with work it may silently skip.", wc),
                    mailboxingThrew);

            // ERROR with the consequence in it, because the consequence is the part that is not obvious: an
            // unretired record leaves no exception, no lag anomaly and correct-looking committed offsets, so
            // "could not mailbox" alone would not tell a reader why PC just stopped.
            logWithoutEscaping(failure, () -> log.error(
                    "Could not return {} to the mailbox - shutting down. This is PC's own bookkeeping, so it is a "
                            + "bug in PC. The record is neither in flight nor completed, so nothing retries it and "
                            + "nothing reports it, and continuing risks committing past work that was never done. "
                            + "Cause: {}",
                    wc, describeWithRootCause(mailboxingThrew)));

            // The call sites name PCInternalRuntimeException as the expected arm and keep a broad backstop, because
            // anything escaping them strands the sibling records behind it. This second line is what the arm buys
            // once it reaches here: a PC invariant break reads differently from an unenumerated route. The produce
            // lock was the known instance until astubbs#257 removed the release from addToMailbox; no named route
            // has replaced it, so the message says what the type means rather than naming a route that is gone.
            // docs/inflight/core-exception-hierarchy-cleanup.md owns the wider cleanup that this and
            // ProduceLockNotHeldException are two instances of.
            if (mailboxingThrew instanceof PCInternalRuntimeException) {
                log.error("The cause above is one of PC's own invariants breaking inside the mailbox path, not a "
                        + "user failure - so it is a bug in PC rather than an operating condition.");
            }

            if (this.failureReason == null) {
                this.failureReason = failure;
            }
            transitionToClosing();
        } catch (Throwable escalationItselfThrew) {
            // Never propagate: this runs on paths whose remaining work must still be returned to the mailbox, so
            // an escape here would strand exactly the records the shutdown is being raised to protect.
            try {
                log.error("Failed to escalate an unmailboxable record", escalationItselfThrew);
            } catch (Throwable ignored) {
                // logging is what just failed
            }
        }
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

            this.requestedDrainMode = drainMode;
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
                // e carries the control thread's failure - the reason the caller is here. Rendering it runs the
                // thrower's getCause/getMessage inside the logging binding, and an escape would replace that
                // diagnosis with a stack trace from inside the logger.
                logWithoutEscaping(e, () ->
                        log.error("Execution or timeout exception while waiting for the control thread to close cleanly " +
                                "(state was {}). Try increasing your time-out to allow the system to drain, or close without " +
                                "draining.", state, e));
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
            logWithoutEscaping(e, () -> log.error("exception during close", e));
            throw e;
        } finally {
            // Each step guarded separately, and NEITHER may escape. Both call into the MeterRegistry,
            // which is usually the USER'S - so this is third-party code running inside PC's close.
            // An exception thrown from a finally REPLACES the one already in flight, so an unguarded
            // failure here would destroy the real shutdown error, skip the remaining teardown, and
            // leave state short of CLOSED. Note the last one does NOT strand a caller polling
            // isClosedOrFailed(): that method also returns true once this thread's future completes,
            // which an escape from here does - exceptionally. The harm is the opposite and quieter,
            // a premature true meaning "the control thread finished, somehow" rather than "closed
            // cleanly", which no caller can tell apart. A metrics problem must not be able to do any
            // of that: it is reporting, and it cannot be allowed to break shutting down.
            try {
                deregisterMeters();
            } catch (Exception e) {
                ThrowableUtils.logWithoutEscaping(e, () ->
                        log.warn("Failed to de-register user-function meters during close - the metrics " +
                                "registry is the user's, so meters may be left behind in it. Shutdown " +
                                "continues; this cannot fail the close. Cause: {}",
                                ThrowableUtils.describeWithRootCause(e), e));
            }
            try {
                pcMetrics.close();
            } catch (Exception e) {
                ThrowableUtils.logWithoutEscaping(e, () ->
                        log.warn("Failed to close the metrics subsystem during close - meters may be " +
                                "left behind in the registry. Shutdown continues; this cannot fail the " +
                                "close. Cause: {}", ThrowableUtils.describeWithRootCause(e), e));
            }
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
        boolean interruptedWhileAwaitingTermination = false;
        while (awaitingInflightCompletion) {
            log.debug("Still awaiting completion of inflight work");
            try {
                boolean terminationFinishedWithoutTimeout = workerThreadPool.get().awaitTermination(toSeconds(timeout), SECONDS);
                awaitingInflightCompletion = false;
                if (!terminationFinishedWithoutTimeout) {
                    // The user's function is what runs in this pool, so the actionable cause is theirs.
                    log.warn("User functions did not finish within the shutdown timeout of {} - interrupting them. " +
                            "Records still in flight will not be committed and will be redelivered on restart.", timeout);
                    log.debug("Worker pool did not terminate in {}. Active: {}, queued: {}, state: {}. " +
                                    "A user function blocking uninterruptibly, or a test latch, will do this.",
                            timeout, workerThreadPool.get().getActiveCount(),
                            workerThreadPool.get().getQueue().size(), state);
                    //Requesting threads shutdown immediately - inflight threads will be interrupted at this point.
                    workerThreadPool.get().shutdownNow();
                    //Give a second for any interrupt handling / resource cleanup in user functions
                    workerThreadPool.get().awaitTermination(toSeconds(Duration.ofSeconds(1)), SECONDS);
                }
            } catch (InterruptedException e) {
                // Do NOT restore the flag here: awaitTermination throws IMMEDIATELY while the flag is
                // set, so restoring it inside this retry loop turns the loop into a 100% CPU livelock
                // that never reaches shutdownNow() - the user function is never interrupted, the pool
                // never terminates, and close() times out (executorThreadsInterruptedOnShutdownTimeout,
                // ~24s, any commit mode, under parallel-suite load). The throw has already cleared the
                // flag, so the retry below waits normally. Remember the interrupt and restore it once,
                // after the loop, so callers of this thread still observe it.
                interruptedWhileAwaitingTermination = true;
                log.debug("Interrupted while awaiting worker pool termination; will keep awaiting", e);
                awaitingInflightCompletion = true;
            }
        }
        if (interruptedWhileAwaitingTermination) {
            // Restore the flag - swallowing it entirely strands anything waiting on this thread.
            Thread.currentThread().interrupt();
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
            log.debug("Control thread carries an interrupt into the close sequence (state: {}, drain mode: {}). " +
                    "If the transactional commit lock cannot be acquired below, this is the likely reason.",
                    state, requestedDrainMode);
        }
        try {
            commitOffsetsThatAreReady();
        } catch (Exception e) {
            // One attempt only: ConsumerManager#commitSync stops retrying once the poll system is
            // closing ("allow to try to commit at least once during close"), because retrying would
            // stall shutdown while nothing is polling. Say so, rather than leaving the reader to
            // wonder whether we gave up early.
            if (requestedDrainMode == DrainingMode.DONT_DRAIN) {
                ThrowableUtils.logWithoutEscaping(e, () ->
                        log.warn("Could not commit offsets while closing, and close does not retry - these records " +
                                "will be redelivered to the next consumer of these partitions. If you need offsets " +
                                "committed before shutdown, close with closeDrainFirst() (or DrainingMode.DRAIN), " +
                                "which finishes and commits in-flight work first. Cause: {}",
                                ThrowableUtils.describeWithRootCause(e), e));
            } else {
                ThrowableUtils.logWithoutEscaping(e, () ->
                        log.warn("Could not commit offsets while closing, despite draining first, and close does not " +
                                "retry - these records will be redelivered to the next consumer of these " +
                                "partitions. Cause: {}", ThrowableUtils.describeWithRootCause(e), e));
            }
        }
        // only close consumer once producer has committed it's offsets (tx'l)
        log.debug("Closing and waiting for broker poll system...");
        try {
            brokerPollSubsystem.closeAndWait();
        } catch (Exception e) {
            // We continue to the consumer close regardless: stopping here would leak the consumer
            // entirely. But the poll loop may still be running, so the consumer close below may
            // legitimately refuse - see ThreadConfinedConsumer.
            ThrowableUtils.logWithoutEscaping(e, () ->
                    log.warn("The broker poll system did not shut down cleanly - the consumer may not be closed, " +
                            "in which case this member will not leave its consumer group promptly and the group's " +
                            "next rebalance will be delayed by up to session.timeout.ms. Cause: {}",
                            ThrowableUtils.describeWithRootCause(e), e));
        }

        try {
            maybeCloseConsumer();
        } catch (Exception e) {
            ThrowableUtils.logWithoutEscaping(e, () ->
                    log.warn("Failed to close the Kafka consumer - this member will not send a LeaveGroup request, " +
                            "so the group's next rebalance will be delayed by up to session.timeout.ms and these " +
                            "partitions will stay assigned to this dead member until then. Cause: {}",
                            ThrowableUtils.describeWithRootCause(e), e));
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
            // shutdownTimeout, not a literal: the user configures how long close may take, and a
            // hardcoded 10s both ignored a shorter budget and capped a longer one. master called
            // consumer.close() with no timeout at all, so this is also the first time the value is
            // the user's to set.
            consumerManager.close(shutdownTimeout);
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

        // snapshot the caller's diagnostic context before starting any thread, so every thread we go on to create -
        // controller, broker poller, and through them the worker pool - inherits it
        captureCallersDiagnosticContext();

        // broker poll subsystem
        brokerPollSubsystem.start(options.getManagedExecutorService());

        ExecutorService executorService =
                lookupManagedResource(options.getManagedExecutorService(), Executors::newSingleThreadExecutor);


        // run main pool loop in thread
        Callable<Boolean> controlTask = () -> {
            mdcPropagation.adopt(callersDiagnosticContext);
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
                    // Arm the failure, then log, then close - and close in a finally, because shutting down is the
                    // part that must happen. describeWithRootCause never throws, but the logger renders the same
                    // user-supplied throwable and its binding is the user's; if that throws, the consumer would
                    // otherwise be left running with an already-failed control future, which is the state this
                    // handler exists to avoid.
                    var described = describeWithRootCause(e);
                    failureReason = new RuntimeException("Error from poll control thread: " + described, e);
                    try {
                        // guarded, not just finally'd: an escaping logger failure would propagate INSTEAD of
                        // failureReason, so the control future would report "the logger blew up" rather than what
                        // actually killed the consumer
                        logWithoutEscaping(failureReason, () ->
                                log.error("Error from poll control thread, will attempt controlled shutdown, then rethrow. Error: " + described, e));
                    } finally {
                        doClose(shutdownTimeout); // attempt to close
                    }
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
     * Takes the snapshot of the caller's diagnostic context that all of PC's threads will run under.
     * <p>
     * Called from {@code poll*} (i.e. on the user's own thread) before any PC thread exists, because that is the only
     * moment at which the user's context is reachable - none of PC's threads inherit it, the SLF4J MDC is not
     * inheritable.
     *
     * @see MdcPropagation
     */
    private void captureCallersDiagnosticContext() {
        this.callersDiagnosticContext = mdcPropagation.capture();
        if (callersDiagnosticContext != null && !callersDiagnosticContext.isEmpty()) {
            // keys only - the values are the user's data, and may be large or sensitive. Logged so that a context
            // accidentally pinned for the life of the consumer (e.g. a request-scoped trace id) is discoverable.
            log.info("Propagating caller's diagnostic context (MDC) keys {} into the processing threads", callersDiagnosticContext.keySet());
        }
    }

    /**
     * Main control loop
     */
    protected <R> void controlLoop(Function<PollContextInternal<K, V>, List<R>> userFunction,
                                   Consumer<R> callback) throws TimeoutException, ExecutionException, InterruptedException {
        maybeRecoverProducer();

        maybeWakeupPoller();

        final boolean shouldTryCommitNow = maybeAcquireCommitLock();

        // make sure all work that's been completed are arranged ready for commit
        Duration timeToBlockFor = shouldTryCommitNow ? Duration.ZERO : getTimeToBlockFor();
        // Suppliers, not values: getNumberOfWorkQueuedInShardsAwaitingSelection() sums a counter across EVERY
        // processing shard, and this is the control loop - the hottest path there is. SLF4J defers formatting
        // but NOT argument evaluation, so passing it directly runs that scan on every pass at every log level,
        // including the levels production runs at. Under KEY ordering the shard map is keyed per record key, so
        // the scan grows with in-flight key cardinality exactly when the loop is spinning fastest.
        // atTrace() returns the NOP builder when trace is off, and NOP's addArgument(Supplier) never calls get()
        // - which is what makes this free rather than merely cheap. HotPathLogArgumentsAreDeferredTest pins it.
        log.atTrace()
                .addArgument(timeToBlockFor)
                .addArgument(shouldTryCommitNow)
                .addArgument(() -> wm.getNumberOfWorkQueuedInShardsAwaitingSelection())
                .addArgument(() -> wm.getNumberRecordsOutForProcessing())
                .log("Control loop: blocking on mailbox for {}, shouldCommit={}, queuedInShards={}, outForProcessing={}");
        processWorkCompleteMailBox(timeToBlockFor);

        //
        if (shouldTryCommitNow) {
            // offsets will be committed when the consumer has its partitions revoked
            commitOffsetsReportingPollerDeath();
        }

        // distribute more work
        retrieveAndDistributeNewWork(userFunction, callback);

        // run call back - counted from the iteration itself, because a separate size() read takes its own snapshot of
        // the copy-on-write array and can report a number this loop never ran
        int loopEndPluginsRun = 0;
        try {
            for (Runnable hook : this.controlLoopHooks) {
                // user code, wrapped as everywhere else - Runnable::run is the Consumer<Runnable> that runs it, so
                // a throwing hook is reported as user code rather than as an anonymous control-thread failure
                UserFunctions.carefullyRun(Runnable::run, hook);
                loopEndPluginsRun++;
            }
        } finally {
            // in a finally so a hook that throws still leaves a record of how far the phase got - that trace line is
            // the last breadcrumb before the control loop unwinds
            log.trace("Loop: Ran {} loop end plugin(s)", loopEndPluginsRun);
        }

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
     * its control loop turns every later sync commit into
     * {@code "Timeout waiting for commit response"} - a message that names neither the failing
     * subsystem nor the failure. That symptom is what users report (astubbs#177, confluentinc#833) and it points
     * nowhere near the cause.
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
        } catch (ProducerInvalidatedException producerInvalidated) {
            // not a failure of this thread and not a poller problem: the broker reported the producer invalid, the
            // condition is recorded, and the next pass of this loop recovers (KTD3, KTD4)
            log.debug("Commit unwound because the producer was reported invalid; recovery runs on the next control loop pass: {}",
                    producerInvalidated.getMessage());
        } catch (PCInternalRuntimeException commitFailure) {
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
     * If we don't have enough work queued, and the poller is paused for throttling,
     * <p>
     * todo move into {@link WorkManager} as it's specific to WM having enough work?
     */
    private void maybeWakeupPoller() {
        if (state == RUNNING) {
            if (!wm.isSufficientlyLoaded() && brokerPollSubsystem.isSubscriptionsPausedForBackPressure()) {
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
        final boolean shouldTryCommitNow = isTimeToCommitNow() && wm.isDirty() && !isRebalanceInProgress.get() && !isProducerBeingReplaced();
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

        //
        if ((state == RUNNING || state == DRAINING) && !isProducerBeingReplaced()) {
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
        // snapshot at submit time, on the controller thread - which is already running under the caller's context
        final Map<String, String> submittersDiagnosticContext = mdcPropagation.capture();
        Future outputRecordFuture;
        try {
            outputRecordFuture = workerThreadPool.get().submit(() -> {
                // scoped, so the context is torn off the pooled thread when the batch finishes - both what we put on it
                // and anything the user function added - rather than being inherited by the next, unrelated, batch
                try (var mdcScope = mdcPropagation.enter(submittersDiagnosticContext)) {
                    addInstanceMDC();
                    return runUserFunction(usersFunction, callback, batch);
                }
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
        if (state == CLOSED) {
            // CLOSED IS TERMINAL - you cannot un-close. Without this, a caller arriving after the control thread
            // has finished writes CLOSING over CLOSED, and nothing is left alive to write it back: `state` reaches
            // CLOSED only in doClose()'s finally, on a thread that exits immediately afterwards. A later close()
            // then misses its `state == CLOSED` fast path and enters waitForClose(), whose loop re-reads an
            // already-completed controlThreadFuture - so `get(timeout)` returns at once, every time, and the loop
            // becomes a hot spin with no exit rather than a wait that times out.
            //
            // Reachable because failFatallyOnUnmailboxableRecord can fire from an async engine completion, which
            // has no timing relationship to the control thread's lifetime: ExternalEngine's pool is sized 1 and
            // only DISPATCHES, so awaitTermination in doClose() knows nothing about the vert.x event loop or a
            // Reactor scheduler still holding an outstanding request. Raised by review on astubbs#267; the guard
            // is here rather than at that one caller because the invariant belongs to this method.
            log.debug("Already closed - not regressing the state");
            return;
        }
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
        return capAtNextRecoveryAttempt(effectiveCommitAttemptDelay);
    }

    /**
     * While the producer is being replaced nothing else wakes this loop - no work is distributed, so no results
     * arrive, and no commit is due - so the wait is capped at the time to the next recovery attempt (KTD7).
     */
    private Duration capAtNextRecoveryAttempt(Duration wait) {
        return producerManager
                .flatMap(pm -> pm.timeUntilNextRecoveryAttempt(Instant.now()))
                .filter(untilAttempt -> untilAttempt.compareTo(wait) < 0)
                .orElse(wait);
    }

    private boolean isProducerBeingReplaced() {
        return options.isUsingTransactionCommitMode() && producerManager.map(ProducerManager::isReplacing).orElse(false);
    }

    /**
     * Recovery from a producer the broker reported invalid, on this thread only and at the top of every pass
     * (KTD4): when a condition is recorded, take the producer write lock, abort and discard the producer, drain the
     * mailbox so every result of the aborted transaction is accounted for, replay the work that transaction
     * discarded (KTD5), release the lock, and then - outside it - build and initialise the replacement (KTD7). A
     * replacement that cannot be built yet is retried on a later pass with backoff; one that can never be built ends
     * the instance, naming the transactional id. Nothing thrown here may escape: the supervisor treats an exception
     * escaping {@link #controlLoop} as fatal, which is the outcome this exists to avoid.
     */
    private void maybeRecoverProducer() throws InterruptedException {
        if (!producerManager.isPresent() || !options.isUsingTransactionCommitMode()) {
            return;
        }
        ProducerManager<K, V> pm = producerManager.get();
        if (!pm.isRecoveryAttemptDue(Instant.now())) {
            return;
        }
        try {
            if (pm.pendingInvalidation().isPresent()) {
                boolean entered = pm.beginReplacement();
                if (!entered) {
                    return; // the wait elapsed; a retry is scheduled and the condition stays recorded
                }
                try {
                    // every worker that produced into the aborted transaction has already mailboxed its result
                    // (the write lock guarantees it); land those results before the replay so the ledger is complete
                    processWorkCompleteMailBox(Duration.ZERO);
                    wm.restoreWorkDiscardedByAbortedTransaction();
                } finally {
                    pm.releaseCommitLockAfterReplacement();
                }
            }
            ProducerManager.ReplacementOutcome outcome = pm.completeReplacement();
            if (outcome.isTerminal()) {
                if (this.failureReason == null) {
                    this.failureReason = outcome.getFailure();
                }
                transitionToClosing();
            }
        } catch (RuntimeException e) {
            log.error("Producer recovery pass failed unexpectedly; it will be attempted again on a later pass: {}", describeWithRootCause(e), e);
        }
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
        log.trace("Acquiring commitLock...");
        commitLock.lock();
        try {
            log.debug("Committing offsets that are ready...");
            committer.retrieveOffsetsAndCommit();
            clearCommitCommand();
            this.lastCommitTime = Instant.now();
        } finally {
            commitLock.unlock();
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
            // Record the failures BEFORE rendering them, and guard the render. This is the highest-traffic render
            // of a user-supplied throwable in the library - every user function failure passes here - and both
            // building the message (which interpolates e) and handing e to the logger run the thrower's
            // getMessage/getCause, the second inside the binding's own unbounded cause-chain walk. If either
            // throws, the loop below never runs: the batch is never marked failed and never returns to the
            // mailbox, so those records stay in flight forever - one stalls its shard under KEY ordering, and
            // maxConcurrency of them stall the consumer. Nothing is logged, because logging is what failed.
            // Per container, and each independent of the others. onUserFunctionFailure runs USER code -
            // updateFailureHistory asks getRetryDelayConfig, which calls the user's retryDelayProvider
            // unguarded - so one container's provider throwing used to abort this loop and strand every
            // container after it in flight forever. That is the same stall this batch is being mailboxed
            // to avoid, reached through a different door. addToMailbox is in a finally for the same
            // reason: returning the record is the part that must happen.
            Throwable bookkeepingFailed = null;
            for (var wc : workContainerBatch) {
                try {
                    wc.onUserFunctionFailure(e);
                } catch (Throwable userCodeThrew) {
                    bookkeepingFailed = firstOrSuppress(bookkeepingFailed, userCodeThrew);
                }
                try {
                    addToMailbox(context, wc); // always add on error
                } catch (PCInternalRuntimeException pcInvariantBroke) {
                    // The EXPECTED shape: one of PC's own invariants. It was reachable here as
                    // ProduceLockNotHeldException from the produce-lock release inside addToMailbox until
                    // astubbs#257 made cleanUpContext the single release point; the arm stays because
                    // addToMailbox is an extension point and ExternalEngine overrides it.
                    //
                    // NOT a finally around the call above: an exception from a finally supersedes everything and
                    // propagates straight out of this loop, so a single failure here would strand every container
                    // AFTER it - reintroducing exactly the bug this loop is shaped to prevent. This is PC's own
                    // code rather than the user's, so a throw is our bug, which is a reason to surface it, not a
                    // reason to let it take the rest of the batch with it.
                    bookkeepingFailed = firstOrSuppress(bookkeepingFailed, pcInvariantBroke);
                    // ...and a reason to stop. Surfacing it to this caller is not enough: the record is now
                    // unaccounted for, so PC shuts down rather than continue past a possible silent skip. The
                    // loop still finishes first, so the containers behind this one are returned.
                    failFatallyOnUnmailboxableRecord(wc, pcInvariantBroke);
                } catch (Throwable nothingElseIsExpected) {
                    // Backstop for a route nobody has enumerated - broad on purpose, for the same
                    // must-not-escape reason as the arm above.
                    bookkeepingFailed = firstOrSuppress(bookkeepingFailed, nothingElseIsExpected);
                    failFatallyOnUnmailboxableRecord(wc, nothingElseIsExpected);
                }
            }
            // attached rather than thrown: the user's own failure is what the caller needs to see, and
            // it is already on its way out below. Nothing is swallowed - it travels with e.
            if (bookkeepingFailed != null && bookkeepingFailed != e) {
                e.addSuppressed(bookkeepingFailed);
            }

            logWithoutEscaping(e, () -> {
                String msg = msg("Exception caught in user function running stage, registering WC as failed, returning to" +
                        " mailbox. Context: {}", context, e);
                if (PCRetriableException.isPresentIn(e)) {
                    log.debug("Explicit " + PCRetriableException.class.getSimpleName() + " caught, logging at DEBUG only. " + msg, e);
                } else {
                    log.error(msg, e);
                }
            });
            throw e; // trow again to make the future failed
        } finally {
            cleanUpContext(context);
        }
    }

    /**
     * Keeps the first failure and attaches every later one to it, so a second container failing the same way is not
     * dropped without trace - which is what keeping only the first, on its own, would do.
     */
    private static Throwable firstOrSuppress(Throwable first, Throwable next) {
        if (first == null) {
            return next;
        }
        if (first != next) {
            try {
                first.addSuppressed(next);
            } catch (Throwable ignored) {
                // suppression disabled, or an override; nothing further to do
            }
        }
        return first;
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

    /**
     * The single release point for a context's produce lock.
     * <p>
     * Only unlock our producing lock once every {@link WorkContainer} of this context has been safely returned to the
     * controller's inbound queue, so we know they'll all be included properly before the next commit as succeeded
     * offsets. As in order for the controller to perform the transaction commit, it will be blocked from acquiring its
     * commit lock until all produce locks have been returned, inbound queue processed, and thus their representative
     * offsets placed into the commit payload (offset map).
     * <p>
     * This runs in the {@code finally} of {@link #runUserFunction}, which is strictly after the whole batch has been
     * added to the mailbox on the success path and after the failure handler's re-add on the error path. Releasing
     * per-{@link WorkContainer} instead would release after the *first* record of a batch, leaving the rest of it
     * exposed to exactly the commit window this lock exists to close - and would owe one release per record against a
     * lock acquired once per context.
     * <p>
     * An {@link ExternalEngine} never reaches here holding one: its constructor rejects
     * {@link ParallelConsumerOptions.CommitMode#PERIODIC_TRANSACTIONAL_PRODUCER} outright, so there is no produce lock
     * in that path to release.
     * <p>
     * The lock is <b>taken</b> rather than read, so the context is left empty and a second release is a no-op instead
     * of an {@link IllegalMonitorStateException} on a read lock this thread no longer holds.
     */
    private void cleanUpContext(final PollContextInternal<K, V> context) {
        context.takeProducingLock().ifPresent(lock -> {
            try {
                // a lock can only exist because a ProducerManager handed it out
                producerManager
                        .orElseThrow(() -> new PCInternalRuntimeException(
                                "Produce lock held, but there is no producer manager to return it to"))
                        .finishProducing(lock);
            } catch (RuntimeException e) {
                // Reported, never rethrown. This runs in runUserFunction's finally, and an exception thrown from a
                // finally REPLACES the one the catch above is propagating - plain try/finally does not attach it as
                // suppressed the way try-with-resources would. Throwing here would therefore destroy the user
                // function's real failure and report this in its place, which is strictly worse: by this point the
                // lock is already unrecoverable either way, because takeProducingLock has claimed it out of the
                // context. So the log is the whole signal, and it has to carry everything - the more so because the
                // alternative destination, WorkContainer#future, is read by nothing in main
                // (docs/inflight/bug-worker-future-swallows-framework-exceptions.md).
                // Offsets, not the whole context - PollContextInternal's toString traverses the wrapped records
                // and includes their keys and values. PollContextInternal#setProducingLock avoids that for the
                // same reason, and this path was added without matching it; Codex review on astubbs#262 caught the
                // inconsistency.
                log.error("Could not return the produce lock for {} - it cannot be released now, and the next "
                        + "transaction commit will block on it", context.getOffsets(), e);
            }
        });
    }

    protected void addToMailBoxOnUserFunctionSuccess(PollContextInternal<K, V> context, WorkContainer<K, V> wc, List<?> resultsFromUserFunction) {
        addToMailbox(context, wc);
    }

    protected void onUserFunctionSuccess(WorkContainer<K, V> wc, List<?> resultsFromUserFunction) {
        log.trace("User function success");
        wc.onUserFunctionSuccess();
    }

    /**
     * Hands a finished record back to the control thread.
     * <p>
     * <b>It can throw, and which routes are real is not obvious - that is why this is written down.</b> Callers guard
     * it because a throw here leaves the record neither in flight nor completed; see
     * {@link #failFatallyOnUnmailboxableRecord}.
     * <p>
     * <b>The {@code throws} clause is deliberate on an unchecked type.</b> Java does not require it and the compiler
     * will not enforce it, but it is the difference between a caller that can reason about what goes wrong here and
     * one that can only assume anything might - which is how the guards around this call came to be
     * {@code catch (Throwable)} with nobody able to say what they were catching. Declared, a caller can narrow to
     * the expected arm honestly and keep a backstop for the rest, rather than treating the two as the same thing.
     *
     * @throws PCInternalRuntimeException a PC invariant broken by an override of this method. <b>Core's own body no
     *                                    longer has one.</b> Until astubbs#257 it also released the produce lock
     *                                    here, and {@code ProducerManager#finishProducing} rejecting a release it
     *                                    did not hold - {@link ProduceLockNotHeldException} - was the named
     *                                    reachable route. {@code cleanUpContext} is now the single release point, so
     *                                    core's body is a queue add and nothing else. The declaration stays because
     *                                    this is a {@code protected} extension point: {@link ExternalEngine}
     *                                    overrides it to return the dispatch permit, and a caller still needs to be
     *                                    able to narrow honestly rather than guard with {@code catch (Throwable)}
     *                                    and no idea what it is guarding.
     * @implNote The queue add is NOT a meaningful throw route here, which is worth stating because
     *         {@link java.util.Queue#add} documents four. {@link #workMailBox} is an unbounded
     *         {@link java.util.concurrent.LinkedBlockingQueue}, so its {@code IllegalStateException} capacity clause
     *         cannot fire; {@code ClassCastException} and {@code IllegalArgumentException} are for ordered and
     *         bounded queues and it raises neither; and the element is never null. Copying that contract here would
     *         document four exceptions that cannot happen while saying nothing about the one that can.
     */
    protected void addToMailbox(PollContextInternal<K, V> pollContext, WorkContainer<K, V> wc)
            throws PCInternalRuntimeException {
        String state = wc.isUserFunctionSucceeded() ? "succeeded" : "FAILED";
        log.trace("Adding {} {} to mailbox...", state, wc);
        workMailBox.add(ControllerEventMessage.of(wc));
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
     * A cheap, side-effect-free description of how much work this instance is holding, for a caller
     * that is waiting on it and needs to know WHY the wait is not ending.
     * <p>
     * <b>It reports both ends on purpose.</b> {@link #workRemaining()} alone cannot tell "nothing is
     * finishing" from "nothing is happening" - a fleet inside a slow user function reads as a flat
     * line while entirely busy, and a wedged one reads identically. Pairing outstanding work with the
     * number of records currently out for processing separates them: work outstanding AND records
     * out means the instance is occupied, while work outstanding and nothing out means it is not
     * trying. That distinction is the difference between a slow test and a defect, and without it a
     * timeout says only that a deadline passed.
     * <p>
     * <b>The out-for-processing figure is a diagnostic estimate, not a value to branch on.</b> It is
     * read without a fence - {@code WorkManager.numberRecordsOutForProcessing} is a plain
     * {@code int} mutated on controller-thread paths, which is a known defect in its own right
     * ({@code docs/inflight/bug-number-records-out-for-processing-is-a-plain-int.md}). A stale read
     * costs a slightly wrong diagnostic line; it must never decide control flow.
     * <p>
     * Public because the callers who need it are outside this package - integration tests and
     * applications diagnosing a stall - and because a diagnostic that requires reflection to reach
     * does not get used at the moment it is needed.
     *
     * @return a single line safe to put in an assertion message or a log
     */
    public String describeProgress() {
        long incomplete = workRemaining();
        int outForProcessing = wm.getNumberRecordsOutForProcessing();
        int queued = executorQueueDepth();
        return "workRemaining=" + incomplete
                + " recordsOutForProcessing=" + outForProcessing
                + " executorQueue=" + queued
                + " state=" + state
                + " closedOrFailed=" + isClosedOrFailed()
                // Splits an idle instance into its two opposite causes: paused (back-pressure held on)
                // or simply not being given work. Without it, both read as zero-and-zero.
                + " " + brokerPollSubsystem.describePauseObservation()
                + coherenceWarning(incomplete, outForProcessing, queued);
    }

    /**
     * Depth of the worker pool's queue, or {@code -1} when the pool is not up.
     *
     * <p>Included in the progress description because it is the ONLY one of these numbers sourced
     * from outside PC's own bookkeeping - it is the executor's own count. The others are things PC
     * believes; this is a thing that is true. When they disagree, that asymmetry is what tells you
     * which one to doubt.
     */
    private int executorQueueDepth() {
        var pool = workerThreadPool.get();
        return pool == null ? -1 : pool.getQueue().size();
    }

    /**
     * Flags a state PC should never be in: <b>work queued for execution while it believes no offsets
     * are incomplete and nothing is out for processing.</b>
     *
     * <p><b>A different KIND of check from the probes that already exist.</b> Those watch liveness -
     * is the system still progressing - and answer it by sampling one number over time. This watches
     * COHERENCE: whether PC's separate views of its own state can all be true at once. A liveness
     * probe cannot see this at all, because a system can be perfectly live while lying about what it
     * holds, and it will report healthy right up until the lie matters.
     *
     * <p><b>Why this exact triple.</b> {@code getNumberOfIncompleteOffsets()} sums over ASSIGNED
     * partitions, so it returns zero whenever that map is empty - regardless of how much work is
     * queued. That makes "queued but nothing incomplete" reachable without anything throwing, and it
     * was observed on three consecutive log lines during the confluentinc#857 throughput
     * investigation: an executor queue of 319 against a target of 320, with both counters reading
     * zero.
     *
     * <p><b>It reports; it does not assert.</b> The two reads are not atomic, so a queue that drains
     * between them produces a false positive, and a single sample is not evidence. Whoever turns this
     * into a gate must require the contradiction to PERSIST across samples and must show it firing on
     * a tree that should fail before trusting a quiet one - this repo's standing rule for detectors.
     * Recorded as an observation for now:
     * {@code docs/inflight/test-857-branch-red-lanes-cause-unestablished.md}.
     *
     * @return an empty string when coherent, so the common case adds nothing to the line
     */
    private String coherenceWarning(long incomplete, int outForProcessing, int queued) {
        boolean incoherent = queued > 0 && incomplete == 0 && outForProcessing == 0;
        return incoherent
                ? " INCOHERENT=work-queued-but-nothing-incomplete"
                : "";
    }

    /**
     * Plugin a function to run at the end of each main loop.
     * <p>
     * Useful for testing and controlling loop progression.
     * <p>
     * Safe to call from any thread, including while the consumer is running. The callback itself, however, runs on the
     * control thread - so it must not block, and must not call back into this consumer in a way that waits on the
     * control loop it is currently occupying.
     * <p>
     * <b>A callback that throws stops the consumer.</b> It is run through {@link UserFunctions}, as every other piece
     * of user-supplied code is, so the failure is reported as coming from user code - but it is not swallowed.
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

    /**
     * <b>Cleared suspicion, 2026-08-31 - this monitor is NOT a residual confluentinc#857 edge.</b>
     * Recorded here because the suspicion is the obvious one to form when reading this file: the
     * revoke path calls this method, so the poll thread does still enter a monitor inside a rebalance
     * callback, which is the shape of the deadlock that {@link #tryCommitOffsetsOnRevoke()} exists to
     * break.
     * <p>
     * It is not the same defect, and the discriminator is what a holder DOES rather than that it
     * holds. An AB-BA cycle needs one thread holding lock A while blocked on lock B. Every holder of
     * this monitor - {@link #requestCommitAsap()}, {@link #isCommandedToCommit()} and this method -
     * does one {@code AtomicBoolean} get or set and nothing else, so a hold here cannot span a wait
     * and there is no edge for a cycle to close on. The control thread's own commit takes
     * {@code commitLock} across the blocking {@code retrieveOffsetsAndCommit()} and enters this
     * monitor only after that call has returned.
     * <p>
     * <b>What would reopen it:</b> anything blocking added inside one of those three
     * {@code synchronized (commitCommand)} blocks, or a fourth holder that waits while holding. No
     * gate protects this - {@code ArchitectureTest.rebalanceCallbacksMustNotBlock} matches method
     * calls, and a {@code synchronized} block is a {@code MONITORENTER} instruction it cannot see, so
     * the rule is green here whether the invariant holds or not.
     */
    private void clearCommitCommand() {
        synchronized (commitCommand) {
            if (commitCommand.get()) {
                log.debug("Command to commit asap received, clearing");
                this.commitCommand.set(false);
            }
        }
    }

}