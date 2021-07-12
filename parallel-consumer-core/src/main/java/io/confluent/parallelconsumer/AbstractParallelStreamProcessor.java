package io.confluent.parallelconsumer;

import io.confluent.csid.utils.WallClock;
import io.confluent.parallelconsumer.internal.*;
import io.confluent.parallelconsumer.state.WorkContainer;
import io.confluent.parallelconsumer.state.WorkManager;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.SneakyThrows;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.internals.ConsumerCoordinator;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Time;
import org.slf4j.MDC;

import java.io.Closeable;
import java.lang.reflect.Field;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.regex.Pattern;

import static io.confluent.csid.utils.BackportUtils.isEmpty;
import static io.confluent.csid.utils.BackportUtils.toSeconds;
import static io.confluent.csid.utils.StringUtils.msg;
import static io.confluent.parallelconsumer.internal.State.*;
import static java.time.Duration.ofMillis;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Base class
 *
 * @param <K>
 * @param <V>
 */
public class AbstractParallelStreamProcessor<K, V> implements ParallelStreamProcessor<K, V>, ConsumerRebalanceListener, Closeable {
    public static final String MDC_INSTANCE_ID = "pcId";
    /**
     * Kafka's default auto commit frequency
     */
    private static final int KAFKA_DEFAULT_AUTO_COMMIT_FREQUENCY = 5000;
    protected final ParallelConsumerOptions options;
    protected final Optional<ProducerManager<K, V>> producerManager;
    protected final org.apache.kafka.clients.consumer.Consumer<K, V> consumer;
    /**
     * The pool which is used for running the users's supplied function
     */
    protected final ThreadPoolExecutor workerPool;
    protected final BrokerPollSystem<K, V> brokerPollSubsystem;
    protected final OffsetCommitter committer;
    /**
     * Multiple of {@link ParallelConsumerOptions#getMaxConcurrency()} to have in our processing queue, in order to make
     * sure threads always have work to do.
     */
    protected final DynamicLoadFactor dynamicExtraLoadFactor = new DynamicLoadFactor();
    /**
     * Collection of work waiting to be
     */
    private final BlockingQueue<WorkContainer<K, V>> workMailBox = new LinkedBlockingQueue<>(); // Thread safe, highly performant, non blocking
    /**
     * Useful for testing async code
     */
    private final List<Runnable> controlLoopHooks = new ArrayList<>();
    /**
     * @see #notifyNewWorkRegistered
     * @see #processWorkCompleteMailBox
     */
    private final AtomicBoolean currentlyPollingWorkCompleteMailBox = new AtomicBoolean();
    /**
     * Used to request a commit asap
     */
    private final AtomicBoolean commitCommand = new AtomicBoolean(false);
    private final RateLimiter queueStatsLimiter = new RateLimiter();
    // todo make package level
    @Getter(AccessLevel.PUBLIC)
    protected WorkManager<K, V> wm;
    /**
     * Injectable clock for testing
     */
    @Setter(AccessLevel.PACKAGE)
    private WallClock clock = new WallClock();
    /**
     * Time between commits. Using a higher frequency will put more load on the brokers.
     */
    @Setter
    @Getter
    private Duration timeBetweenCommits = ofMillis(AbstractParallelStreamProcessor.KAFKA_DEFAULT_AUTO_COMMIT_FREQUENCY);
    private Instant lastCommitCheckTime = Instant.now();
    private Optional<Future<Boolean>> controlThreadFuture = Optional.empty();
    /**
     * Reference to the control thread, used for waking up a blocking poll ({@link BlockingQueue#poll}) against a
     * collection sooner.
     *
     * @see #processWorkCompleteMailBox
     */
    private Thread blockableControlThread;
    /**
     * If the system failed with an exception, it is referenced here.
     */
    private Exception failureReason;
    /**
     * The run state of the controller.
     *
     * @see State
     */
    private State state = State.unused;
    /**
     * Wrapped {@link ConsumerRebalanceListener} passed in by a user that we can also call on events
     */
    private Optional<ConsumerRebalanceListener> usersConsumerRebalanceListener = Optional.empty();
    @Getter
    private int numberOfAssignedPartitions;
    /**
     * Control for stepping loading factor - shouldn't step if work requests can't be fulfilled due to restrictions.
     * (e.g. we may want 10, but maybe there's a single partition and we're in partition mode - stepping up won't
     * help).
     */
    private boolean lastWorkRequestWasFulfilled = false;
    /**
     * Optioanl ID of this instance. Useful for testing.
     */
    @Setter
    @Getter
    private Optional<String> myId = Optional.empty();

    public boolean isClosedOrFailed() {
        boolean closed = state == State.closed;
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

    protected void checkGroupIdConfigured(final org.apache.kafka.clients.consumer.Consumer<K, V> consumer) {
        try {
            consumer.groupMetadata();
        } catch (RuntimeException e) {
            throw new IllegalArgumentException("Error validating Consumer configuration - no group metadata - missing a " +
                    "configured GroupId on your Consumer?", e);
        }
    }

    protected ThreadPoolExecutor setupWorkerPool(int poolSize) {
        ThreadFactory defaultFactory = Executors.defaultThreadFactory();
        ThreadFactory namingThreadFactory = r -> {
            Thread thread = defaultFactory.newThread(r);
            String name = thread.getName();
            thread.setName("pc-" + name);
            return thread;
        };
        ThreadPoolExecutor.AbortPolicy rejectionHandler = new ThreadPoolExecutor.AbortPolicy();
        LinkedBlockingQueue<Runnable> workQueue = new LinkedBlockingQueue<>();
        return new ThreadPoolExecutor(poolSize, poolSize, 0L, MILLISECONDS, workQueue,
                namingThreadFactory, rejectionHandler);
    }

    protected void checkNotSubscribed(org.apache.kafka.clients.consumer.Consumer<K, V> consumerToCheck) {
        if (consumerToCheck instanceof MockConsumer)
            // disabled for unit tests which don't test rebalancing
            return;
        Set<String> subscription = consumerToCheck.subscription();
        Set<TopicPartition> assignment = consumerToCheck.assignment();
        if (!subscription.isEmpty() || !assignment.isEmpty()) {
            throw new IllegalStateException("Consumer subscription must be managed by this class. Use " + this.getClass().getName() + "#subcribe methods instead.");
        }
    }

    @Override
    public void subscribe(Collection<String> topics) {
        AbstractParallelStreamProcessor.log.debug("Subscribing to {}", topics);
        consumer.subscribe(topics, this);
    }

    @Override
    public void subscribe(Pattern pattern) {
        AbstractParallelStreamProcessor.log.debug("Subscribing to {}", pattern);
        consumer.subscribe(pattern, this);
    }

    @Override
    public void subscribe(Collection<String> topics, ConsumerRebalanceListener callback) {
        AbstractParallelStreamProcessor.log.debug("Subscribing to {}", topics);
        usersConsumerRebalanceListener = Optional.of(callback);
        consumer.subscribe(topics, this);
    }

    @Override
    public void subscribe(Pattern pattern, ConsumerRebalanceListener callback) {
        AbstractParallelStreamProcessor.log.debug("Subscribing to {}", pattern);
        usersConsumerRebalanceListener = Optional.of(callback);
        consumer.subscribe(pattern, this);
    }

    /**
     * Commit our offsets
     * <p>
     * Make sure the calling thread is the thread which performs commit - i.e. is the {@link OffsetCommitter}.
     */
    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        AbstractParallelStreamProcessor.log.debug("Partitions revoked {}, state: {}", partitions, state);
        numberOfAssignedPartitions = numberOfAssignedPartitions - partitions.size();
        try {
            commitOffsetsThatAreReady();
            wm.onPartitionsRevoked(partitions);
            usersConsumerRebalanceListener.ifPresent(x -> x.onPartitionsRevoked(partitions));
        } catch (Exception e) {
            throw new InternalRuntimeError("onPartitionsRevoked event error", e);
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
        AbstractParallelStreamProcessor.log.info("Assigned {} total ({} new) partition(s) {}", numberOfAssignedPartitions, partitions.size(), partitions);
        wm.onPartitionsAssigned(partitions);
        usersConsumerRebalanceListener.ifPresent(x -> x.onPartitionsAssigned(partitions));
        notifyNewWorkRegistered();
    }

    /**
     * Delegate to {@link WorkManager}
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
     * this is more reliable in a correctness sense, but britle in terms of coupling to internal implementation.
     * Consider requesting ability to inspect configuration at runtime.
     */
    @SneakyThrows
    protected void checkAutoCommitIsDisabled(org.apache.kafka.clients.consumer.Consumer<K, V> consumer) {
        if (consumer instanceof KafkaConsumer) {
            // Commons lang FieldUtils#readField - avoid needing commons lang
            Field coordinatorField = consumer.getClass().getDeclaredField("coordinator"); //NoSuchFieldException
            coordinatorField.setAccessible(true);
            ConsumerCoordinator coordinator = (ConsumerCoordinator) coordinatorField.get(consumer); //IllegalAccessException

            if (coordinator == null)
                throw new IllegalStateException("Coordinator for Consumer is null - missing GroupId? Reflection broken?");

            Field autoCommitEnabledField = coordinator.getClass().getDeclaredField("autoCommitEnabled");
            autoCommitEnabledField.setAccessible(true);
            Boolean isAutoCommitEnabled = (Boolean) autoCommitEnabledField.get(coordinator);

            if (isAutoCommitEnabled)
                throw new IllegalArgumentException("Consumer auto commit must be disabled, as commits are handled by the library.");
        } else {
            // noop - probably MockConsumer being used in testing - which doesn't do auto commits
        }
    }

    /**
     * Close the system, without draining.
     *
     * @see State#draining
     */
    @Override
    public void close() {
        // use a longer timeout, to cover for evey other step using the default
        Duration timeout = DrainingCloseable.DEFAULT_TIMEOUT.multipliedBy(2);
        closeDontDrainFirst(timeout);
    }

    @Override
    @SneakyThrows
    public void close(Duration timeout, DrainingMode drainMode) {
        if (state == closed) {
            AbstractParallelStreamProcessor.log.info("Already closed, checking end state..");
        } else {
            AbstractParallelStreamProcessor.log.info("Signaling to close...");

            switch (drainMode) {
                case DRAIN -> {
                    AbstractParallelStreamProcessor.log.info("Will wait for all in flight to complete before");
                    transitionToDraining();
                }
                case DONT_DRAIN -> {
                    AbstractParallelStreamProcessor.log.info("Not waiting for in flight to complete, will transition directly to closing");
                    transitionToClosing();
                }
            }

            waitForClose(timeout);
        }

        if (controlThreadFuture.isPresent()) {
            AbstractParallelStreamProcessor.log.debug("Checking for control thread exception...");
            Future<?> future = controlThreadFuture.get();
            future.get(toSeconds(timeout), SECONDS); // throws exception if supervisor saw one
        }

        AbstractParallelStreamProcessor.log.info("Close complete.");
    }

    private void waitForClose(Duration timeout) throws TimeoutException, ExecutionException {
        AbstractParallelStreamProcessor.log.info("Waiting on closed state...");
        while (!state.equals(closed)) {
            try {
                Future<Boolean> booleanFuture = this.controlThreadFuture.get();
                AbstractParallelStreamProcessor.log.debug("Blocking on control future");
                boolean signaled = booleanFuture.get(toSeconds(timeout), SECONDS);
                if (!signaled)
                    throw new TimeoutException("Timeout waiting for system to close (" + timeout + ")");
            } catch (InterruptedException e) {
                // ignore
                AbstractParallelStreamProcessor.log.trace("Interrupted", e);
            } catch (ExecutionException | TimeoutException e) {
                AbstractParallelStreamProcessor.log.error("Execution or timeout exception while waiting for the control thread to close cleanly " +
                        "(state was {}). Try increasing your time-out to allow the system to drain, or close withing " +
                        "draining.", state, e);
                throw e;
            }
            AbstractParallelStreamProcessor.log.trace("Still waiting for system to close...");
        }
    }

    private void doClose(Duration timeout) throws TimeoutException, ExecutionException {
        AbstractParallelStreamProcessor.log.debug("Doing closing state: {}...", state);

        // only close consumer once producer has committed it's offsets (tx'l)
        AbstractParallelStreamProcessor.log.debug("Closing and waiting for broker poll system...");
        brokerPollSubsystem.closeAndWait();

        maybeCloseConsumer();

        producerManager.ifPresent(x -> x.close(timeout));

        AbstractParallelStreamProcessor.log.debug("Shutting down execution pool...");
        List<Runnable> unfinished = workerPool.shutdownNow();
        if (!unfinished.isEmpty()) {
            AbstractParallelStreamProcessor.log.warn("Threads not done count: {}", unfinished.size());
        }

        AbstractParallelStreamProcessor.log.trace("Awaiting worker pool termination...");
        boolean interrupted = true;
        while (interrupted) {
            AbstractParallelStreamProcessor.log.debug("Still interrupted");
            try {
                boolean terminationFinishedWithoutTimeout = workerPool.awaitTermination(toSeconds(timeout), SECONDS);
                interrupted = false;
                if (!terminationFinishedWithoutTimeout) {
                    AbstractParallelStreamProcessor.log.warn("Thread execution pool termination await timeout ({})! Were any processing jobs dead locked or otherwise stuck?", timeout);
                    boolean shutdown = workerPool.isShutdown();
                    boolean terminated = workerPool.isTerminated();
                }
            } catch (InterruptedException e) {
                AbstractParallelStreamProcessor.log.error("InterruptedException", e);
                interrupted = true;
            }
        }

        AbstractParallelStreamProcessor.log.debug("Close complete.");
        this.state = closed;
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

    /**
     * Block the calling thread until no more messages are being processed.
     */
    @SneakyThrows
    public void waitForProcessedNotCommitted(Duration timeout) {
        AbstractParallelStreamProcessor.log.debug("Waiting processed but not committed...");
        var timer = Time.SYSTEM.timer(timeout);
        while (wm.isRecordsAwaitingToBeCommitted()) {
            AbstractParallelStreamProcessor.log.trace("Waiting for no in processing...");
            Thread.sleep(100);
            timer.update();
            if (timer.isExpired()) {
                throw new TimeoutException("Waiting for no more records in processing");
            }
        }
        AbstractParallelStreamProcessor.log.debug("No longer anything in flight.");
    }

    private boolean isRecordsAwaitingProcessing() {
        boolean isRecordsAwaitingProcessing = wm.isRecordsAwaitingProcessing();
        boolean threadsDone = areMyThreadsDone();
        AbstractParallelStreamProcessor.log.trace("isRecordsAwaitingProcessing {} || threadsDone {}", isRecordsAwaitingProcessing, threadsDone);
        return isRecordsAwaitingProcessing || threadsDone;
    }

    private void transitionToDraining() {
        AbstractParallelStreamProcessor.log.debug("Transitioning to draining...");
        this.state = State.draining;
        interruptControlThread();
    }

    /**
     * Control thread can be blocked waiting for work, but is interruptible. Interrupting it can be useful to make tests
     * run faster, or to move on to shutting down the {@link BrokerPollSystem} so that less messages are downloaded and
     * queued.
     */
    private void interruptControlThread() {
        if (blockableControlThread != null) {
            AbstractParallelStreamProcessor.log.debug("Interrupting {} thread in case it's waiting for work", blockableControlThread.getName());
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

    protected <R> void supervisorLoop(Function<ConsumerRecord<K, V>, List<R>> userFunction,
                                      Consumer<R> callback) {
        AbstractParallelStreamProcessor.log.info("Control loop starting up...");

        if (state != State.unused) {
            throw new IllegalStateException(msg("Invalid state - the consumer cannot be used more than once (current " +
                    "state is {})", state));
        } else {
            state = running;
        }

        // run main pool loop in thread
        Callable<Boolean> controlTask = () -> {
            Thread controlThread = Thread.currentThread();
            addInstanceMDC();
            controlThread.setName("pc-control");
            AbstractParallelStreamProcessor.log.trace("Control task scheduled");
            this.blockableControlThread = controlThread;
            while (state != closed) {
                try {
                    controlLoop(userFunction, callback);
                } catch (Exception e) {
                    AbstractParallelStreamProcessor.log.error("Error from poll control thread, will attempt controlled shutdown, then rethrow. Error: " + e.getMessage(), e);
                    doClose(DrainingCloseable.DEFAULT_TIMEOUT); // attempt to close
                    failureReason = new RuntimeException("Error from poll control thread: " + e.getMessage(), e);
                    throw failureReason;
                }
            }
            AbstractParallelStreamProcessor.log.info("Control loop ending clean (state:{})...", state);
            return true;
        };

        brokerPollSubsystem.start();

        ExecutorService executorService = Executors.newSingleThreadExecutor();
        Future<Boolean> controlTaskFutureResult = executorService.submit(controlTask);
        this.controlThreadFuture = Optional.of(controlTaskFutureResult);
    }

    /**
     * Useful when testing with more than one instance
     */
    private void addInstanceMDC() {
        this.myId.ifPresent(id -> MDC.put(AbstractParallelStreamProcessor.MDC_INSTANCE_ID, id));
    }

    /**
     * Main control loop
     */
    private <R> void controlLoop(Function<ConsumerRecord<K, V>, List<R>> userFunction,
                                 Consumer<R> callback) throws TimeoutException, ExecutionException, InterruptedException {

        //
        int newWork = handleWork(userFunction, callback);

        if (state == running) {
            if (!wm.isSufficientlyLoaded() & brokerPollSubsystem.isPaused()) {
                // can occur
                AbstractParallelStreamProcessor.log.debug("Found Poller paused with not enough front loaded messages, ensuring poller is awake (mail: {} vs concurrency: {})",
                        wm.getWorkQueuedInMailboxCount(),
                        options.getMaxConcurrency());
                brokerPollSubsystem.wakeupIfPaused();
            }
        }

        AbstractParallelStreamProcessor.log.trace("Loop: Process mailbox");
        processWorkCompleteMailBox();

        if (state == running) {
            // offsets will be committed when the consumer has its partitions revoked
            AbstractParallelStreamProcessor.log.trace("Loop: Maybe commit");
            commitOffsetsMaybe();
        }

        // run call back
        AbstractParallelStreamProcessor.log.trace("Loop: Running {} loop end plugin(s)", controlLoopHooks.size());
        this.controlLoopHooks.forEach(Runnable::run);

        AbstractParallelStreamProcessor.log.trace("Current state: {}", state);
        switch (state) {
            case draining -> {
                drain();
            }
            case closing -> {
                doClose(DrainingCloseable.DEFAULT_TIMEOUT);
            }
        }

        // sanity - supervise the poller
        brokerPollSubsystem.supervise();

        // thread yield for spin lock avoidance
        Duration duration = Duration.ofMillis(1);
        try {
            Thread.sleep(duration.toMillis());
        } catch (InterruptedException e) {
            AbstractParallelStreamProcessor.log.trace("Woke up", e);
        }

        // end of loop
        AbstractParallelStreamProcessor.log.trace("End of control loop, waiting processing {}, remaining in partition queues: {}, out for processing: {}. In state: {}",
                wm.getTotalWorkWaitingProcessing(), wm.getNumberOfEntriesInPartitionQueues(), wm.getNumberRecordsOutForProcessing(), state);
    }

    private <R> int handleWork(final Function<ConsumerRecord<K, V>, List<R>> userFunction, final Consumer<R> callback) {
        // check queue pressure first before addressing it
        checkPressure();

        int gotWorkCount = 0;

        //
        if (state == running || state == draining) {
            int target = getQueueTargetLoaded();
            BlockingQueue<Runnable> queue = workerPool.getQueue();
            int current = queue.size();
            int delta = target - current;

            AbstractParallelStreamProcessor.log.debug("Loop: Will try to get work - target: {}, current queue size: {}, requesting: {}, loading factor: {}",
                    target, current, delta, dynamicExtraLoadFactor.getCurrentFactor());
            var records = wm.maybeGetWork(delta);
            gotWorkCount = records.size();
            lastWorkRequestWasFulfilled = gotWorkCount >= delta;

            AbstractParallelStreamProcessor.log.trace("Loop: Submit to pool");
            submitWorkToPool(userFunction, callback, records);
        }

        //
        queueStatsLimiter.performIfNotLimited(() -> {
            int queueSize = getWorkerQueueSize();
            AbstractParallelStreamProcessor.log.debug("Stats: \n- pool active: {} queued:{} \n- queue size: {} target: {} loading factor: {}",
                    workerPool.getActiveCount(), queueSize, queueSize, getPoolQueueTarget(), dynamicExtraLoadFactor.getCurrentFactor());
        });

        return gotWorkCount;
    }

    private int getQueueTargetLoaded() {
        return getPoolQueueTarget() * dynamicExtraLoadFactor.getCurrentFactor();
    }

    /**
     * Checks the system has enough pressure, if not attempts to step up the load factor.
     */
    private void checkPressure() {
        boolean moreWorkInQueuesAvailableThatHaveNotBeenPulled = wm.getWorkQueuedInMailboxCount() > options.getMaxConcurrency();
        if (AbstractParallelStreamProcessor.log.isTraceEnabled())
            AbstractParallelStreamProcessor.log.trace("Queue pressure check: (current size: {}, loaded target: {}, factor: {}) if (isPoolQueueLow() {} && dynamicExtraLoadFactor.isWarmUpPeriodOver() {} && moreWorkInQueuesAvailableThatHaveNotBeenPulled {})",
                    getWorkerQueueSize(),
                    getQueueTargetLoaded(),
                    dynamicExtraLoadFactor.getCurrentFactor(),
                    isPoolQueueLow(),
                    dynamicExtraLoadFactor.isWarmUpPeriodOver(),
                    moreWorkInQueuesAvailableThatHaveNotBeenPulled);
        if (isPoolQueueLow() && dynamicExtraLoadFactor.isWarmUpPeriodOver() && moreWorkInQueuesAvailableThatHaveNotBeenPulled && lastWorkRequestWasFulfilled) {
            boolean steppedUp = dynamicExtraLoadFactor.maybeStepUp();
            if (steppedUp) {
                AbstractParallelStreamProcessor.log.debug("isPoolQueueLow(): Executor pool queue is not loaded with enough work (queue: {} vs target: {}), stepped up loading factor to {}",
                        getWorkerQueueSize(), getPoolQueueTarget(), dynamicExtraLoadFactor.getCurrentFactor());
            } else if (dynamicExtraLoadFactor.isMaxReached()) {
                AbstractParallelStreamProcessor.log.warn("isPoolQueueLow(): Max loading factor steps reached: {}/{}", dynamicExtraLoadFactor.getCurrentFactor(), dynamicExtraLoadFactor.getMaxFactor());
            }
        }
    }

    /**
     * @return aim to never have the pool queue drop below this
     */
    private int getPoolQueueTarget() {
        return options.getMaxConcurrency();
    }

    private boolean isPoolQueueLow() {
        int queueSize = getWorkerQueueSize();
        int queueTarget = getPoolQueueTarget();
        boolean workAmountBelowTarget = queueSize <= queueTarget;
        boolean hasWorkInMailboxes = wm.hasWorkInMailboxes();
        AbstractParallelStreamProcessor.log.debug("workAmountBelowTarget {} {} vs {} && wm.hasWorkInMailboxes() {};",
                workAmountBelowTarget, queueSize, queueTarget, hasWorkInMailboxes);
        return workAmountBelowTarget && hasWorkInMailboxes;
    }

    private void drain() {
        AbstractParallelStreamProcessor.log.debug("Signaling to drain...");
        brokerPollSubsystem.drain();
        if (!isRecordsAwaitingProcessing()) {
            transitionToClosing();
        } else {
            AbstractParallelStreamProcessor.log.debug("Records still waiting processing, won't transition to closing.");
        }
    }

    private void transitionToClosing() {
        AbstractParallelStreamProcessor.log.debug("Transitioning to closing...");
        if (state == State.unused) {
            state = closed;
        } else {
            state = State.closing;
        }
        interruptControlThread();
    }

    /**
     * Check the work queue for work to be done, potentially blocking.
     * <p>
     * Can be interrupted if something else needs doing.
     */
    private void processWorkCompleteMailBox() {
        AbstractParallelStreamProcessor.log.trace("Processing mailbox (might block waiting for results)...");
        Set<WorkContainer<K, V>> results = new HashSet<>();
        final Duration timeout = getTimeToNextCommitCheck(); // don't sleep longer than when we're expected to maybe commit

        // blocking get the head of the queue
        WorkContainer<K, V> firstBlockingPoll = null;
        try {
            boolean noWorkToDoAndStillRunning = workMailBox.isEmpty() && state.equals(running);
            if (noWorkToDoAndStillRunning) {
                if (AbstractParallelStreamProcessor.log.isDebugEnabled()) {
                    AbstractParallelStreamProcessor.log.debug("Blocking poll on work until next scheduled offset commit attempt for {}. active threads: {}, queue: {}",
                            timeout, workerPool.getActiveCount(), getWorkerQueueSize());
                }
                currentlyPollingWorkCompleteMailBox.getAndSet(true);
                // wait for work, with a timeout for sanity
                AbstractParallelStreamProcessor.log.trace("Blocking poll {}", timeout);
                firstBlockingPoll = workMailBox.poll(timeout.toMillis(), MILLISECONDS);
                AbstractParallelStreamProcessor.log.trace("Blocking poll finish");
                currentlyPollingWorkCompleteMailBox.getAndSet(false);
            } else {
                // don't set the lock or log anything
                firstBlockingPoll = workMailBox.poll();
            }
        } catch (InterruptedException e) {
            AbstractParallelStreamProcessor.log.debug("Interrupted waiting on work results");
        }
        if (firstBlockingPoll == null) {
            AbstractParallelStreamProcessor.log.debug("Mailbox results returned null, indicating timeout (which was set as {}) or interruption during a blocking wait for returned work results", timeout);
        } else {
            results.add(firstBlockingPoll);
        }

        // check for more work to batch up, there may be more work queued up behind the head that we can also take
        // see how big the queue is now, and poll that many times
        int size = workMailBox.size();
        AbstractParallelStreamProcessor.log.trace("Draining {} more, got {} already...", size, results.size());
        workMailBox.drainTo(results, size);

        AbstractParallelStreamProcessor.log.trace("Processing drained work {}...", results.size());
        for (var work : results) {
            MDC.put("offset", work.toString());
            wm.handleFutureResult(work);
            MDC.clear();
        }
    }

    /**
     * Conditionally commit offsets to broker
     */
    private void commitOffsetsMaybe() {
        Duration elapsedSinceLast = getTimeSinceLastCommit();
        boolean commitFrequencyOK = elapsedSinceLast.compareTo(timeBetweenCommits) > 0;
        boolean lingerBeneficial = lingeringOnCommitWouldBeBeneficial();
        boolean commitCommand = isCommandedToCommit();
        boolean shouldCommitNow = commitFrequencyOK || !lingerBeneficial || commitCommand;
        if (shouldCommitNow) {
            AbstractParallelStreamProcessor.log.debug("commitFrequencyOK {} || !lingerBeneficial {} || commitCommand {}",
                    commitFrequencyOK,
                    !lingerBeneficial,
                    commitCommand
            );
//            if (poolQueueLow) {
//                /*
//                Shouldn't be needed if pressure system is working, unless commit frequency target too high or too much
//                information to encode into offset payload.
//                // TODO should be able to change this so that it checks with the work manager if a commit would help - i.e. are partitions blocked by offset encoding?
//                */
//                log.warn("Pool queue too low ({}), may be because of back pressure from encoding offsets, so committing", getWorkerQueueSize());
//            }
            commitOffsetsThatAreReady();
        } else {
            if (AbstractParallelStreamProcessor.log.isDebugEnabled()) {
                if (wm.hasCommittableOffsets()) {
                    AbstractParallelStreamProcessor.log.debug("Have offsets to commit, but not enough time elapsed ({}), waiting for at least {}...", elapsedSinceLast, timeBetweenCommits);
                } else {
                    AbstractParallelStreamProcessor.log.trace("Could commit now, but no offsets committable");
                }
            }
        }
    }

    private int getWorkerQueueSize() {
        return workerPool.getQueue().size();
    }

    /**
     * Under some conditions, waiting longer before committing can be faster
     *
     * @return true if waiting to commit would help performance
     */
    private boolean lingeringOnCommitWouldBeBeneficial() {
        // work is waiting to be done
        boolean workIsWaitingToBeCompletedSuccessfully = wm.workIsWaitingToBeProcessed();
        // no work is currently being done
        boolean workInFlight = wm.hasWorkInFlight();
        // work mailbox is empty
        boolean workWaitingInMailbox = !workMailBox.isEmpty();
        boolean workWaitingToCommit = wm.hasWorkInCommitQueues();
        AbstractParallelStreamProcessor.log.trace("workIsWaitingToBeCompletedSuccessfully {} || workInFlight {} || workWaitingInMailbox {} || !workWaitingToCommit {};",
                workIsWaitingToBeCompletedSuccessfully, workInFlight, workWaitingInMailbox, !workWaitingToCommit);
        return workIsWaitingToBeCompletedSuccessfully || workInFlight || workWaitingInMailbox || !workWaitingToCommit;
    }

    private Duration getTimeToNextCommitCheck() {
        // draining is a normal running mode for the controller
        if (state == running || state == draining) {
            Duration timeSinceLastCommit = getTimeSinceLastCommit();
            Duration timeBetweenCommits = getTimeBetweenCommits();
            Duration minus = timeBetweenCommits.minus(timeSinceLastCommit);
            return minus;
        } else {
            AbstractParallelStreamProcessor.log.debug("System not {} (state: {}), so don't wait to commit, only a small thread yield time", running, state);
            return Duration.ZERO;
        }
    }

    private Duration getTimeSinceLastCommit() {
        Instant now = clock.getNow();
        return Duration.between(lastCommitCheckTime, now);
    }

    private void commitOffsetsThatAreReady() {
        if (wm.isClean()) {
            AbstractParallelStreamProcessor.log.debug("Nothing changed since last commit, skipping");
            return;
        }
        committer.retrieveOffsetsAndCommit();
        updateLastCommitCheckTime();
    }

    private void updateLastCommitCheckTime() {
        lastCommitCheckTime = Instant.now();
    }

    /**
     * Submit a piece of work to the processing pool.
     *
     * @param workToProcess the polled records to process
     */
    private <R> void submitWorkToPool(Function<ConsumerRecord<K, V>, List<R>> usersFunction,
                                      Consumer<R> callback,
                                      List<WorkContainer<K, V>> workToProcess) {
        if (!workToProcess.isEmpty()) {
            AbstractParallelStreamProcessor.log.debug("New work incoming: {}, Pool stats: {}", workToProcess.size(), workerPool);
            for (var work : workToProcess) {
                // for each record, construct dispatch to the executor and capture a Future
                AbstractParallelStreamProcessor.log.trace("Sending work ({}) to pool", work);
                Future<List<?>> outputRecordFuture = workerPool.submit(() -> {
                    addInstanceMDC();
                    return userFunctionRunner(usersFunction, callback, work);
                });
                work.setFuture(outputRecordFuture);
            }
        }
    }

    /**
     * Run the supplied function.
     */
    protected <R> List<Tuple<ConsumerRecord<K, V>, R>> userFunctionRunner(Function<ConsumerRecord<K, V>, List<R>> usersFunction,
                                                                          Consumer<R> callback,
                                                                          WorkContainer<K, V> wc) {
        // call the user's function
        List<R> resultsFromUserFunction;
        try {
            MDC.put("offset", wc.toString());

            //
            boolean epochIsStale = wm.checkEpochIsStale(wc);
            if (epochIsStale) {
                // when epoch's change, we can't remove them from the executor pool queue, so we just have to skip them when we find them
                AbstractParallelStreamProcessor.log.debug("Pool found work from old generation of assigned work, skipping message as epoch doesn't match current {}", wc);
                return null;
            }

            AbstractParallelStreamProcessor.log.trace("Pool received: {}", wc);

            ConsumerRecord<K, V> rec = wc.getCr();
            resultsFromUserFunction = usersFunction.apply(rec);

            onUserFunctionSuccess(wc, resultsFromUserFunction);

            // capture each result, against the input record
            var intermediateResults = new ArrayList<Tuple<ConsumerRecord<K, V>, R>>();
            for (R result : resultsFromUserFunction) {
                AbstractParallelStreamProcessor.log.trace("Running users call back...");
                callback.accept(result);
            }
            AbstractParallelStreamProcessor.log.trace("User function future registered");
            // fail or succeed, either way we're done
            addToMailBoxOnUserFunctionSuccess(wc, resultsFromUserFunction);
            return intermediateResults;
        } catch (Exception e) {
            // handle fail
            AbstractParallelStreamProcessor.log.error("Error processing record", e);
            wc.onUserFunctionFailure();
            addToMailbox(wc); // always add on error
            throw e; // trow again to make the future failed
        }
    }

    protected void addToMailBoxOnUserFunctionSuccess(WorkContainer<K, V> wc, List<?> resultsFromUserFunction) {
        addToMailbox(wc);
    }

    protected void onUserFunctionSuccess(WorkContainer<K, V> wc, List<?> resultsFromUserFunction) {
        AbstractParallelStreamProcessor.log.trace("User function success");
        wc.onUserFunctionSuccess();
    }

    protected void addToMailbox(WorkContainer<K, V> wc) {
        AbstractParallelStreamProcessor.log.trace("Adding {} to mailbox...", wc);
        workMailBox.add(wc);
    }

    /**
     * Early notify of work arrived.
     * <p>
     * Only wake up the thread if it's sleeping while polling the mail box.
     *
     * @see #processWorkCompleteMailBox
     * @see #blockableControlThread
     */
    public void notifyNewWorkRegistered() {
        if (currentlyPollingWorkCompleteMailBox.get()) {
            boolean noTransactionInProgress = !producerManager.map(ProducerManager::isTransactionInProgress).orElse(false);
            if (noTransactionInProgress) {
                AbstractParallelStreamProcessor.log.trace("Interrupting control thread: Knock knock, wake up! You've got mail (tm)!");
                this.blockableControlThread.interrupt();
            } else {
                AbstractParallelStreamProcessor.log.trace("Would have interrupted control thread, but TX in progress");
            }
        } else {
            AbstractParallelStreamProcessor.log.trace("Work box not being polled currently, so thread not blocked, will come around to the bail box in the next looop.");
        }
    }

    @Override
    public long workRemaining() {
        return wm.getNumberOfEntriesInPartitionQueues();
    }

    /**
     * Plugin a function to run at the end of each main loop.
     * <p>
     * Useful for testing and controlling loop progression.
     */
    void addLoopEndCallBack(Runnable r) {
        this.controlLoopHooks.add(r);
    }

    public void setLongPollTimeout(Duration ofMillis) {
        BrokerPollSystem.setLongPollTimeout(ofMillis);
    }

    /**
     * Request a commit as soon as possible (ASAP), overriding other constraints.
     */
    public void requestCommitAsap() {
        AbstractParallelStreamProcessor.log.debug("Registering command to commit next chance");
        synchronized (commitCommand) {
            this.commitCommand.set(true);
        }
    }

    private boolean isCommandedToCommit() {
        synchronized (commitCommand) {
            boolean commitAsap = this.commitCommand.get();
            if (commitAsap) {
                AbstractParallelStreamProcessor.log.debug("Command to commit asap received, clearing");
                this.commitCommand.set(false);
            }
            return commitAsap;
        }
    }

    void setClock(io.confluent.csid.utils.WallClock clock) {
        this.clock = clock;
    }

    public void setTimeBetweenCommits(Duration timeBetweenCommits) {
        this.timeBetweenCommits = timeBetweenCommits;
    }

    public void setMyId(Optional<String> myId) {
        this.myId = myId;
    }

    public Duration getTimeBetweenCommits() {
        return this.timeBetweenCommits;
    }

    public WorkManager<K, V> getWm() {
        return this.wm;
    }

    public int getNumberOfAssignedPartitions() {
        return this.numberOfAssignedPartitions;
    }

    public Optional<String> getMyId() {
        return this.myId;
    }
}
