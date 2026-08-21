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
import static lombok.AccessLevel.PACKAGE;
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

    private final org.apache.kafka.clients.consumer.Consumer<K, V> consumer;

    /**
     * The pool which is used for running the users' supplied function.
     * <p>
     * Typed as {@link ExecutorService} rather than {@link ThreadPoolExecutor} because
     * {@link ParallelConsumerOptions#isUseVirtualThreads()} selects a virtual-thread-per-task executor, which is not
     * one. Nothing outside {@link #innerDoClose(Duration)} needs the wider type: the pool's queue depth and active
     * count - the two figures the pressure system and the shutdown diagnostics used to read off
     * {@link ThreadPoolExecutor} - are now counted by {@link #userFunctionTaskAccounting}.
     */
    @Getter(PROTECTED)
    protected final Supplier<ExecutorService> workerThreadPool;

    /**
     * Replaces {@code workerThreadPool.getQueue().size()} and {@code workerThreadPool.getActiveCount()}, which a
     * virtual-thread-per-task executor does not have.
     *
     * @see UserFunctionTaskAccounting
     */
    private final UserFunctionTaskAccounting userFunctionTaskAccounting = new UserFunctionTaskAccounting();

    private Optional<Future<Boolean>> controlThreadFuture = Optional.empty();

    /**
     * MEASUREMENT ONLY. Present when {@link ParallelConsumerOptions#isDirectPullEngine()} selects the direct-pull
     * engine, in which case the control loop stops distributing work altogether: the workers take it themselves.
     *
     * @see DirectPullWorkerPool
     */
    private Optional<DirectPullWorkerPool<K, V>> directPullPool = Optional.empty();

    // todo make package level
    @Getter(AccessLevel.PUBLIC)
    protected WorkManager<K, V> wm;

    /**
     * Collection of work waiting to be
     */
    @Getter(PROTECTED)
    // EXPERIMENT: CountedTransferQueue instead of LinkedBlockingQueue.
    //
    // Profiling put 17,785 of ~39,000 parks in five seconds right here - workers calling offer() and
    // taking LinkedBlockingQueue's putLock to report a completed record. It is the largest single park
    // site and the only one that is PC's own code; the rest are workers idle in getTask, which is the
    // pool working correctly.
    //
    // The shape suits a lock-free queue and, unlike the worker pool's queue, it suits THIS one:
    // LinkedTransferQueue spins before parking, which was catastrophic where a thousand threads
    // CONSUME, but here a thousand threads PRODUCE - and offer() on an unbounded transfer queue is a
    // CAS append that never spins. One consumer, the control loop, does the waiting.
    //
    // Counted, so size() stays O(1): the previous experiment changed the structure and its size()
    // behaviour together and could not be interpreted.
    private final BlockingQueue<ControllerEventMessage<K, V>> workMailBox = new CountedTransferQueue<>();

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
    private Exception failureReason;

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
    @Setter
    private State state = State.UNUSED;

    /**
     * Wrapped {@link ConsumerRebalanceListener} passed in by a user that we can also call on events
     */
    private Optional<ConsumerRebalanceListener> usersConsumerRebalanceListener = Optional.empty();

    @Getter
    private int numberOfAssignedPartitions;

    private final RateLimiter queueStatsLimiter = new RateLimiter();

    /**
     * Limits how often {@link #maybeReportLoadFactorCeiling()} speaks. Matches the interval used for the equivalent
     * steady-state warning in {@code ProcessingShard}'s slow-work check.
     */
    private final RateLimiter loadFactorCeilingLimiter = new RateLimiter(5);

    @Getter(PROTECTED)
    PCModule<K, V> module;

    /**
     * Control for stepping loading factor - shouldn't step if work requests can't be fulfilled due to restrictions.
     * (e.g. we may want 10, but maybe there's a single partition and we're in partition mode - stepping up won't
     * help).
     * <p>
     * Package-private setter so that pipeline-pressure tests can drive {@link #checkPipelinePressure()} directly,
     * without having to run a whole control loop to get the flag set.
     */
    @Setter(PACKAGE)
    private boolean lastWorkRequestWasFulfilled = false;

    private io.micrometer.core.instrument.Timer userProcessingTimer;
    private Gauge loadFactorGauge;
    private Gauge statusGauge;

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

        workerThreadPool = SupplierUtils.memoize(() -> setupWorkerPool(newOptions.getMaxConcurrency()));

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
        // Micrometer's ExecutorServiceMetrics only knows how to read a ThreadPoolExecutor (and a ForkJoinPool).
        // Handed a virtual-thread-per-task executor it binds no meters and reports nothing - a gauge set that
        // silently measures nothing is worse than an absent one, because a dashboard showing a flat zero reads as
        // "no work" rather than "not instrumented". Say so once, at INFO, and leave the meters unregistered.
        ExecutorService pool = this.getWorkerThreadPool().get();
        if (pool instanceof ThreadPoolExecutor || pool instanceof ForkJoinPool) {
            new ExecutorServiceMetrics(pool, "pc-user-function-executor",
                    USER_FUNCTION_EXECUTOR_PREFIX,
                    pcMetrics.getCommonTags()).bindTo(pcMetrics.getMeterRegistry());
        } else {
            log.info("Worker pool is a {}, which Micrometer's ExecutorServiceMetrics cannot introspect, so the " +
                            "{} meters are not registered. Parallel Consumer's own in-flight and queued figures are " +
                            "unaffected.",
                    pool.getClass().getName(), USER_FUNCTION_EXECUTOR_PREFIX);
        }
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

    /**
     * MEASUREMENT ONLY. Whether this engine can hand work selection over to the workers themselves.
     * <p>
     * {@link ExternalEngine} cannot: its "worker pool" is a single thread that only starts the asynchronous work,
     * with the concurrency living in the external runtime, so there are no worker threads to give the shards to.
     *
     * @see DirectPullWorkerPool
     */
    protected boolean supportsDirectPull() {
        return true;
    }

    /**
     * Whether this engine can run the user's function on virtual threads.
     * <p>
     * False for {@link ExternalEngine}, for the same reason {@link #supportsDirectPull()} is: its worker "pool" is
     * one thread that dispatches into an external runtime, and the concurrency lives out there. Replacing that one
     * thread with an unbounded virtual-thread executor would silently make the dispatch itself concurrent, which is
     * not what any of those engines are built on.
     *
     * @see ParallelConsumerOptions#isUseVirtualThreads()
     */
    protected boolean supportsVirtualThreads() {
        return true;
    }

    protected ExecutorService setupWorkerPool(int poolSize) {
        if (options.isUseVirtualThreads()) {
            if (supportsVirtualThreads()) {
                return setupVirtualThreadWorkerPool();
            }
            log.warn("useVirtualThreads is set, but {} dispatches into an external runtime rather than running the " +
                            "user's function on its own pool, so virtual threads have nothing to do here. Using the " +
                            "usual single dispatch thread. Concurrency for this engine is configured in the runtime " +
                            "it dispatches to.",
                    this.getClass().getSimpleName());
        }

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
     * DO NOT "SIMPLIFY" THIS INTO DIRECT CALLS. Every JDK 21 symbol here is reached reflectively on purpose: this
     * module compiles with {@code release.target=8} (see {@code pom.xml} and
     * {@code docs/features/java-compatibility.yaml}), so {@code Thread.ofVirtual()} and
     * {@code Executors.newThreadPerTaskExecutor(...)} are not on the compile-time API surface at all. Writing them
     * directly does not fail at review time - it fails the Java 8 build, for everyone, for a capability that is
     * opt-in and off by default.
     * <p>
     * {@link ParallelConsumerOptions#validate()} has already probed these same two methods, so reaching here on a
     * JVM without them means the runtime changed underneath a constructed instance. That is a bug, not a
     * configuration error, which is why it throws {@link IllegalStateException} rather than the
     * {@link UnsupportedOperationException} validation raises.
     * <p>
     * The threads are named rather than left anonymous: at a {@code maxConcurrency} where virtual threads are worth
     * having, a thread dump holds thousands of them, and unnamed ones make it unreadable.
     *
     * @see bz.stub.parallelconsumer.ParallelConsumerOptions#isUseVirtualThreads()
     */
    private ExecutorService setupVirtualThreadWorkerPool() {
        try {
            // Thread.ofVirtual().name("pc-vt-", 0).factory()
            Object builder = Class.forName("java.lang.Thread").getMethod("ofVirtual").invoke(null);
            Class<?> builderClass = Class.forName("java.lang.Thread$Builder");
            String prefix = getMyId().map(id -> "pc-vt-" + id + "-").orElse("pc-vt-");
            builderClass.getMethod("name", String.class, long.class).invoke(builder, prefix, 0L);
            ThreadFactory factory = (ThreadFactory) builderClass.getMethod("factory").invoke(builder);

            // Executors.newThreadPerTaskExecutor(factory)
            ExecutorService executor = (ExecutorService) Executors.class
                    .getMethod("newThreadPerTaskExecutor", ThreadFactory.class)
                    .invoke(null, factory);
            log.info("Running the user function on virtual threads. maxConcurrency ({}) is a target the control " +
                            "loop aims at, not a cap the pool enforces - the pool is unbounded.",
                    options.getMaxConcurrency());
            return executor;
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException(msg(
                    "useVirtualThreads passed validation but this JVM ({} {}) cannot create a virtual-thread " +
                            "executor - report a bug.",
                    System.getProperty("java.vm.name"), System.getProperty("java.version")), e);
        }
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
            commitOffsetsThatAreReady();

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
        // Direct-pull workers occupy their threads in a loop rather than sitting in the pool's queue, so
        // shutdown() alone would never terminate the pool - it only stops NEW tasks being accepted.
        directPullPool.ifPresent(DirectPullWorkerPool::stop);
        //Clear scheduled but not started work in execution pool
        discardQueuedWork();
        //request graceful shutdown
        workerThreadPool.get().shutdown();
        if (userFunctionTaskAccounting.getActive() > 0) {
            log.info("Inflight work in execution pool: {}, letting to finish on shutdown with timeout: {}", userFunctionTaskAccounting.getActive(), timeout);
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
                    // shutdownNow() hands back tasks it accepted but never ran; they have to be accounted for or
                    // the derived queue depth never returns to zero - see UserFunctionTaskAccounting.
                    userFunctionTaskAccounting.onTasksDiscarded(workerThreadPool.get().shutdownNow().size());
                    //Give a second for any interrupt handling / resource cleanup in user functions
                    workerThreadPool.get().awaitTermination(toSeconds(Duration.ofSeconds(1)), SECONDS);
                }
            } catch (InterruptedException e) {
                log.error("InterruptedException", e);
                awaitingInflightCompletion = true;
            }
        }
        awaitingInflightProcessingCompletionOnShutdown.getAndSet(false);

        if (userFunctionTaskAccounting.getActive() > 0) {
            log.warn("Clean execution pool termination failed - some threads still active despite await and interrupt - is user function swallowing interrupted exception? Threads still not done count: {}", userFunctionTaskAccounting.getActive());
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

        // MEASUREMENT ONLY: hand the worker pool over to direct pull, so nothing is ever submitted to its queue and
        // the pressure system that sizes that queue never runs.
        if (options.isDirectPullEngine() && supportsDirectPull()) {
            var pool = new DirectPullWorkerPool<K, V>(wm,
                    options.getBatchSize(),
                    () -> state == RUNNING || state == State.DRAINING,
                    batch -> {
                        addInstanceMDC();
                        runUserFunction(userFunctionWrapped, callback, batch);
                    });
            this.directPullPool = Optional.of(pool);
            pool.start(workerThreadPool.get(), options.getMaxConcurrency());
        }

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
            commitOffsetsThatAreReady();
        }

        // distribute more work - or, under direct pull, tell the workers there may be some and let them take it
        // themselves. The mailbox drain above is where new records are registered and where returned records become
        // selectable again, so one announcement per pass covers every way work appears.
        if (directPullPool.isPresent()) {
            var pool = directPullPool.get();
            // The direct-pull replacement for checkPipelinePressure(): a worker that was allowed to work and found
            // nothing means the buffer feeding the shards is too shallow, which is the same conclusion
            // isPoolQueueLow() reaches by reading the executor's queue depth. What has gone is the ThreadPoolExecutor
            // reading, not the load factor - see DirectPullWorkerPool#starvedSinceLastCheck.
            if (pool.consumeStarvationSignal()) {
                dynamicExtraLoadFactor.maybeStepUp();
            }
            pool.onWorkMaybeAvailable((int) Math.min(Integer.MAX_VALUE, wm.getUpperBoundOnSelectableWork()));
        } else {
            retrieveAndDistributeNewWork(userFunction, callback);
        }

        // run call back
        log.trace("Loop: Running {} loop end plugin(s)", controlLoopHooks.size());
        this.controlLoopHooks.forEach(Runnable::run);

        log.trace("Current state: {}", state);
        switch (state) {
            case DRAINING -> {
                drain();
            }
            case CLOSING -> {
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

    private <R> int retrieveAndDistributeNewWork(final Function<PollContextInternal<K, V>, List<R>> userFunction, final Consumer<R> callback) {
        // check queue pressure first before addressing it
        checkPipelinePressure();

        int gotWorkCount = 0;

        //
        if (state == RUNNING || state == DRAINING) {
            int delta = calculateQuantityToRequest();
            var records = wm.getWorkIfAvailable(delta);

            gotWorkCount = records.size();
            lastWorkRequestWasFulfilled = gotWorkCount >= delta;

            log.trace("Loop: Submit to pool");
            submitWorkToPool(userFunction, callback, records);
        }

        //
        queueStatsLimiter.performIfNotLimited(() -> {
            int queueSize = getNumberOfUserFunctionsQueued();
            log.debug("Stats: \n- pool active: {} queued:{} \n- queue size: {} target: {} loading factor: {}",
                    userFunctionTaskAccounting.getActive(), queueSize, queueSize, getPoolLoadTarget(), dynamicExtraLoadFactor.getCurrentFactor());
        });

        return gotWorkCount;
    }

    /**
     * Submit a piece of work to the processing pool.
     *
     * @param workToProcess the polled records to process
     */
    protected <R> void submitWorkToPool(Function<PollContextInternal<K, V>, List<R>> usersFunction,
                                        Consumer<R> callback,
                                        List<WorkContainer<K, V>> workToProcess) {
        if (state.equals(CLOSING) || state.equals(CLOSED)) {
            log.debug("Not submitting new work as Parallel Consumer is in {} state, incoming work: {}, Pool stats: {}", state, workToProcess.size(), userFunctionTaskAccounting);
        }
        if (!workToProcess.isEmpty()) {
            log.debug("New work incoming: {}, Pool stats: {}", workToProcess.size(), userFunctionTaskAccounting);

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
                submitWorkToPoolInner(usersFunction, callback, batch);
            }
        }
    }

    private <R> void submitWorkToPoolInner(final Function<PollContextInternal<K, V>, List<R>> usersFunction,
                                           final Consumer<R> callback,
                                           final List<WorkContainer<K, V>> batch) {
        // for each record, construct dispatch to the executor and capture a Future
        log.trace("Sending work ({}) to pool", batch);

        // Counted BEFORE the submit, not after. A virtual thread can be running the task before submit() has
        // returned, and an increment placed after the call would let the task's own onTaskStarted() land first -
        // making the derived queue depth transiently negative. See UserFunctionTaskAccounting.
        userFunctionTaskAccounting.onSubmitting();
        Future outputRecordFuture;
        try {
            outputRecordFuture = workerThreadPool.get().submit(() -> {
                userFunctionTaskAccounting.onTaskStarted();
                try {
                    addInstanceMDC();
                    return runUserFunction(usersFunction, callback, batch);
                } finally {
                    // Outermost, so it also covers an interrupt delivered by shutdownNow() and anything thrown
                    // out of addInstanceMDC(). finally runs for Error too; only JVM exit skips it.
                    userFunctionTaskAccounting.onTaskFinished();
                }
            });
        } catch (RuntimeException e) {
            // AbortPolicy on the platform pool, or any executor rejecting after shutdown: the task will never run,
            // so nothing downstream will ever account for it.
            userFunctionTaskAccounting.onSubmitRejected();
            throw e;
        }
        // for a batch, each message in the batch shares the same result
        for (final WorkContainer<K, V> workContainer : batch) {
            workContainer.setFuture(outputRecordFuture);
        }
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

    /**
     * How many records to hold out for processing.
     * <p>
     * <b>The load factor multiplies this only when there is an executor queue for the surplus to sit in.</b> Under
     * the default engine, everything past {@code maxConcurrency} waits in the {@link ThreadPoolExecutor}'s queue, so
     * a factor of N means "keep the workers N deep in queued work" - more records buffered, never more records
     * running. A virtual-thread executor has no queue: every task it accepts gets a thread immediately, so the same
     * multiplication would mean N times {@code maxConcurrency} records <em>running at once</em>, up to
     * {@link DynamicLoadFactor#DEFAULT_MAX_LOADING_FACTOR}. That is not a deeper buffer, it is a hundredfold breach
     * of the concurrency the user configured.
     * <p>
     * The factor is <em>not</em> disabled in this mode - it is still doing its other job. See
     * {@link #isVirtualThreadPool()}.
     */
    protected int getQueueTargetLoaded() {
        if (isVirtualThreadPool()) {
            return getPoolLoadTarget();
        }
        //noinspection unchecked
        return getPoolLoadTarget() * dynamicExtraLoadFactor.getCurrentFactor();
    }

    /**
     * Whether the worker pool hands every accepted task a thread at once, rather than queueing it.
     * <p>
     * Asked of the pool rather than of {@link ParallelConsumerOptions#isUseVirtualThreads()} because the two can
     * disagree: {@link ExternalEngine} declines virtual threads via {@link #supportsVirtualThreads()} and keeps its
     * platform dispatch thread even when the option is set.
     */
    private boolean isVirtualThreadPool() {
        return !(workerThreadPool.get() instanceof ThreadPoolExecutor);
    }

    /**
     * Checks the system has enough pressure in the pipeline of work, if not attempts to step up the load factor.
     * <p>
     * <b>This still runs under virtual threads, deliberately.</b> The obvious move when the executor queue goes away
     * is to no-op the pressure system with it, and that move has a measured price: {@link ExternalEngine} does
     * exactly that, and it is the identified cause of that family's throughput regression
     * ({@code docs/inflight/next-core-async-user-function.md}). The load factor has two jobs and only one of them is
     * about the executor's queue. The other is sizing the buffer of records polled from the broker and held in the
     * shards - {@code WorkManager#isSufficientlyLoaded()}, which pauses the poller when that buffer is full - and
     * virtual threads do not remove that buffer. Left unstepped, a virtual-thread run would be fed from a buffer two
     * deep while the default engine ran with one up to a hundred deep, and any comparison between them would be of
     * buffer depths rather than of thread types.
     * <p>
     * What the mode does change is where the surplus goes, which is {@link #getQueueTargetLoaded()}'s problem, not
     * this method's.
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
                maybeReportLoadFactorCeiling();
            }
        }
    }

    /**
     * Reports that the extra load factor has nothing left to give: the pool queue is running dry, but the factor is
     * already at its ceiling, so PC will not queue any more work than it already is.
     * <p>
     * Rate limited, because the calling {@link #checkPipelinePressure()} runs once per control-loop pass and the
     * condition it describes is a steady state, not an event - unlimited, it repeats for as long as the pipeline stays
     * hungry.
     */
    private void maybeReportLoadFactorCeiling() {
        if (isVirtualThreadPool()) {
            // There is no dispatch queue to run dry, so "the queue is low" is this pool's permanent resting state
            // rather than a symptom - the factor reaches its ceiling immediately and stays there. The advice the
            // WARN gives would be actively wrong here: raising maximumLoadFactor buys a deeper record buffer, not
            // more concurrency, because getQueueTargetLoaded() deliberately stops multiplying in this mode. An
            // operator whose throughput is short under virtual threads should raise maxConcurrency.
            return;
        }
        loadFactorCeilingLimiter.performIfNotLimited(() -> {
            int factor = dynamicExtraLoadFactor.getCurrentFactor();
            if (dynamicExtraLoadFactor.isStatic()) {
                // Demoted rather than suppressed. There is nothing here for an operator to act on - they pinned the
                // buffer themselves, and the factor has been "at its maximum" since startup - so it must not be a
                // WARN. It is not deleted, because it is still the direct answer to "why isn't PC fetching more?",
                // which is a question people turn debug logging on to answer.
                log.debug("Work queue is running low, but the load factor is pinned at {} by the configured " +
                                "messageBufferSize, so PC won't queue more than {} records. This is the requested " +
                                "behaviour - raise ParallelConsumerOptions#messageBufferSize to hold more in flight.",
                        factor, getQueueTargetLoaded());
            } else {
                log.warn("Work queue is running low, but the extra load factor has reached its ceiling ({} of a " +
                                "maximum {}), so PC won't queue more than {} records ({} in-flight target x {}). " +
                                "Processing is keeping up with everything PC is allowed to buffer; if you want more " +
                                "in flight, raise ParallelConsumerOptions#maximumLoadFactor, or the concurrency and " +
                                "batch size that set the in-flight target.",
                        factor, dynamicExtraLoadFactor.getMaxFactor(), getQueueTargetLoaded(), getPoolLoadTarget(), factor);
            }
        });
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
                        timeToBlockFor, userFunctionTaskAccounting.getActive(), getNumberOfUserFunctionsQueued());
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
        return userFunctionTaskAccounting.getQueued();
    }

    /**
     * Package-private so tests can assert the derived figures against the executor's own, which is the only
     * independent oracle they have - and only on the platform path, because that is the only path where the
     * executor knows.
     * <p>
     * Deliberately not {@code getUserFunctionTaskAccounting()}: the Truth subject generator treats any
     * {@code get}-prefixed method as a property and emits an assertion calling it from
     * {@code bz.stub.parallelconsumer}, where a package-private member of {@code ...parallelconsumer.internal} is
     * not visible - which fails test compilation, not main compilation. The no-prefix form also matches
     * {@link PCModule}'s accessors.
     */
    UserFunctionTaskAccounting userFunctionTaskAccounting() {
        return userFunctionTaskAccounting;
    }

    /**
     * Drops tasks the executor accepted but has not begun, on the way down.
     * <p>
     * The only place left that needs the concrete {@link ThreadPoolExecutor}: draining a pending-task queue has no
     * {@link ExecutorService} equivalent. It is a shutdown path rather than a hot one, and a virtual-thread
     * executor has no such queue to drain - every task it accepted already has a thread.
     * <p>
     * Package-private for the same reason {@link #setLastWorkRequestWasFulfilled(boolean)} is: its only caller is
     * {@link #innerDoClose(Duration)}, which a never-started processor never reaches (an UNUSED instance goes
     * straight to CLOSED), so a test could not otherwise drive it without standing up a whole control loop. It has
     * to be driveable, because removing the accounting call inside it left the entire suite green.
     */
    void discardQueuedWork() {
        ExecutorService pool = workerThreadPool.get();
        if (pool instanceof ThreadPoolExecutor) {
            List<Runnable> discarded = new ArrayList<>();
            ((ThreadPoolExecutor) pool).getQueue().drainTo(discarded);
            userFunctionTaskAccounting.onTasksDiscarded(discarded.size());
        }
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
        // DELIBERATELY STILL `synchronized`, and not migrated with the other monitors on the virtual-thread path.
        //
        // This one is held across retrieveOffsetsAndCommit() - a network commit - so on a JDK before 24 (JEP 491) it
        // is the textbook pinning site, and converting it to a plain ReentrantLock is a two-line change that would
        // look like an improvement.
        //
        // It is also the exact monitor in the AB-BA deadlock between the poll thread's onPartitionsRevoked and this
        // method, diagnosed but NOT yet fixed - astubbs#29, and
        // docs/solutions/runtime-errors/revoke-path-commit-deadlock-between-poll-and-control-threads.md. That fix
        // needs a specific lock POLICY (tryLock with a timeout, so the cycle cannot close), not merely a lock. A
        // plain lock() migration landing first either silently forecloses that fix or, worse, makes the file look
        // as though it already has it.
        //
        // The cost of waiting is bounded: pinning here only bites on JDK 21-23, only with virtual threads enabled,
        // and only on the control thread - which is one thread, not one per record. Whoever takes astubbs#29 should
        // make it a ReentrantLock with the tryLock policy, and note that clearCommitCommand() is called from
        // INSIDE this block, so reentrancy is load-bearing.
        log.trace("Synchronizing on commitCommand...");
        synchronized (commitCommand) {
            log.debug("Committing offsets that are ready...");
            committer.retrieveOffsetsAndCommit();
            clearCommitCommand();
            this.lastCommitTime = Instant.now();
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