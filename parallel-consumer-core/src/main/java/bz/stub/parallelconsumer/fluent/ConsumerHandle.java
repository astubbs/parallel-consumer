package bz.stub.parallelconsumer.fluent;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelEoSStreamProcessor;
import bz.stub.parallelconsumer.state.WorkContainer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.annotation.InterfaceStability;

import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static bz.stub.parallelconsumer.internal.utils.StringUtils.msg;

/**
 * A running definition: what {@link ParallelConsumerDefinition#start()} hands back.
 * <p>
 * It is {@link AutoCloseable} so a definition reads as a try-with-resources block, and its close follows the
 * instance's declared {@link ClosePath} - draining by default, which is where it differs from the classic API,
 * whose plain close does not drain (R17).
 *
 * <h2>The three ways an instance ends</h2>
 * {@link #awaitShutdown()} returns for exactly one of them, and each leaves a different trace:
 * <ol>
 *     <li><b>Somebody closed it</b> - this handle's {@link #close()}, or the engine's own close. Await returns and
 *     {@link #stopRequest()} and {@link #failureCause()} are both empty.</li>
 *     <li><b>A route asked it to stop</b> (R24) - await returns normally and {@link #stopRequest()} names the
 *     record and the reason. It is not an exception because nothing failed: the definition's author asked for
 *     this.</li>
 *     <li><b>It failed</b> - a definition fault the facade raised (KTD3) is rethrown as it was thrown, and a
 *     control-thread failure is rethrown wrapped in an {@link InstanceFailedException}. Silence is not an option
 *     here: the instance is gone and a caller who only waited would read that as a clean shutdown.</li>
 * </ol>
 *
 * <h2>What is observable while it runs</h2>
 * {@link #topic(String)} reaches one route's parked records and {@link #parkedAllTopics()} the instance-wide
 * roll-up, both read from the engine's own retry queue (R28). The same figures are published as meters (R19).
 *
 * <h2>What this milestone does not do</h2>
 * The parked view's payload fraction, time-to-export estimate and held-behind count read empty - each needs an
 * engine accessor that does not exist yet, see {@link ParkedView} - and {@link ParkedView#resume} and
 * {@link ParkedView#dlq} refuse.
 */
@Slf4j
@InterfaceStability.Unstable
public class ConsumerHandle implements AutoCloseable, InstanceControl {

    /**
     * How often {@link #awaitShutdown()} looks up from its latch to ask whether the engine ended without telling
     * it. Short enough not to be noticed by a caller, long enough to be free.
     */
    private static final Duration FAILURE_POLL_INTERVAL = Duration.ofMillis(200);

    private final ParallelEoSStreamProcessor<byte[], byte[]> processor;

    private final RouteDispatcher dispatcher;

    /**
     * Every routed topic mapped to the topics of the route that claims it, so
     * {@code handle.topic("audit-replay")} answers with the whole route a set-declared topic belongs to (R5).
     */
    private final Map<String, Set<String>> routeTopicsByTopic;

    private final ClosePath closePath;

    private final FluentMeters meters;

    private final CountDownLatch shutdown = new CountDownLatch(1);

    private final AtomicBoolean closing = new AtomicBoolean();

    /**
     * The definition fault that ended this instance, if one did. First writer wins: the fault is what stopped the
     * instance, and every record after it would report the same thing.
     */
    private final AtomicReference<Throwable> fault = new AtomicReference<>();

    /**
     * The stop a route asked for, if one did. First writer wins - see {@link StopRequest}.
     */
    private final AtomicReference<StopRequest> stopRequest = new AtomicReference<>();

    /**
     * Said once, after the first assignment: which routes were assigned no partition at all (R28).
     */
    private final AtomicBoolean assignmentGapsLogged = new AtomicBoolean();

    ConsumerHandle(ParallelEoSStreamProcessor<byte[], byte[]> processor,
                   RouteDispatcher dispatcher,
                   Map<String, Set<String>> routeTopicsByTopic,
                   ClosePath closePath,
                   FluentMeters meters) {
        this.processor = processor;
        this.dispatcher = dispatcher;
        this.routeTopicsByTopic = routeTopicsByTopic;
        this.closePath = closePath;
        this.meters = meters;
    }

    /**
     * The parked containers the engine is holding, which is where the parked set lives (R28, KTD14).
     * <p>
     * There is no snapshot and no reconciliation, because there are no longer two answers to reconcile: a parked
     * record is a record the engine will never make due again, so the engine's retry queue <em>is</em> the parked
     * set, and nothing here has to be kept in step with it. The read takes the queue's read lock and walks it, so
     * it is a query rather than something to do per record.
     * <p>
     * <b>What leaves this view, and what does not.</b> A revoked record leaves it because
     * {@code ShardManager.getParkedWorkContainers()} excludes a stale container - the rebalance callbacks do not
     * touch the retry queue itself, so the entry outlives the revocation and the reader is what has to know. There
     * is no <em>resume</em> path in this release at all: nothing in the engine unparks a record, and a parked
     * record can never reach the later ordinary failure that would clear its reason, because it is never due
     * again. So a park is released by a restart or a rebalance and by nothing else, which is what the README's
     * park section says under "What releases a parked record today"; {@code resume} and {@code dlq} on this handle
     * refuse for that reason and arrive with the engine commands they need.
     */
    private List<WorkContainer<?, ?>> parkedContainers() {
        return processor.getWm().getSm().getParkedWorkContainers();
    }

    /**
     * The hook the control loop runs at the end of every pass: bring the parked gauges into line with the
     * assignment, and say once which routes were assigned nothing.
     * <p>
     * <b>It never throws.</b> The control loop runs its hooks as user code and a throw takes the instance down, so
     * every fault here is contained and logged - observability must not be able to stop consuming.
     */
    void onControlLoopEnd() {
        try {
            Set<TopicPartition> assigned = new LinkedHashSet<>(processor.getWm().getPm()
                    .getAssignedPartitions().keySet());
            meters.syncPartitionGauges(assigned);
            logRoutesWithNoAssignment(assigned);
        } catch (Throwable hookFailed) { //NOSONAR - a throw from here is fatal to the control loop
            log.warn("The fluent API's loop-end hook failed and is contained - the parked view and its meters may "
                    + "be stale. This cannot stop the instance.", hookFailed);
        }
    }

    /**
     * A route whose topic was assigned no partition processes nothing and says nothing, which reads as a broken
     * function rather than as a subscription that matched nothing - a misspelled topic name, or a topic this group
     * shares with another instance that took every partition (R28).
     */
    private void logRoutesWithNoAssignment(Set<TopicPartition> assigned) {
        if (assigned.isEmpty() || assignmentGapsLogged.get()) {
            // Nothing assigned yet is not a gap - it is an instance whose first rebalance has not landed.
            return;
        }
        Set<String> assignedTopics = new LinkedHashSet<>();
        for (TopicPartition partition : assigned) {
            assignedTopics.add(partition.topic());
        }
        Set<String> unassigned = new LinkedHashSet<>(routeTopicsByTopic.keySet());
        unassigned.removeAll(assignedTopics);
        if (!assignmentGapsLogged.compareAndSet(false, true)) {
            return;
        }
        if (!unassigned.isEmpty()) {
            log.warn("These routed topics were assigned no partition by this instance's first assignment, so their "
                            + "routes will process nothing: {}. Either the topic does not exist, or another member "
                            + "of the group holds every partition of it. Assigned: {}",
                    unassigned, assigned);
        }
    }

    /**
     * Closes on the instance's declared {@link ClosePath} - draining by default, bounded by the options' drain
     * timeout, with the shutdown timeout bounding the close that follows (R17). Idempotent: a second call returns
     * once the first has finished.
     */
    @Override
    public void close() {
        if (!closing.compareAndSet(false, true)) {
            awaitShutdown();
            return;
        }
        try {
            shutTheEngineDown(closePath);
        } finally {
            shutdown.countDown();
        }
    }

    /**
     * The close itself, shared by the caller's {@link #close()} and by the self-close the stop and fault paths
     * start. What is <b>not</b> shared is what happens when it fails: a caller who asked for the close is told, and
     * the self-close has nobody to tell, so it logs.
     * <p>
     * The meters go before the engine's close, so they are gone at a moment this handle chooses rather than only if
     * the engine's shutdown reaches its own metrics step.
     */
    private void shutTheEngineDown(ClosePath path) {
        meters.deregister();
        processor.close(path.drainingMode());
    }

    /**
     * Blocks until this instance has shut down - closed, stopped by request, or failed. See the three ways an
     * instance ends, on this class.
     *
     * @throws RuntimeException the definition fault that stopped the instance, or an
     *                          {@link InstanceFailedException} wrapping a control-thread failure
     */
    public void awaitShutdown() {
        try {
            // The latch is counted down by whoever closes through this handle. An engine that ended on its own -
            // a control-thread failure, or a close that went round this handle - counts nothing down, so the wait
            // asks the engine as well rather than blocking for ever on a latch nobody will touch.
            while (!shutdown.await(FAILURE_POLL_INTERVAL.toMillis(), TimeUnit.MILLISECONDS)) {
                if (processor.isClosedOrFailed()) {
                    break;
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.debug("Interrupted while awaiting shutdown", e);
        }
        surfaceAnyFault();
    }

    /**
     * @return true when the instance shut down within the bound
     * @throws RuntimeException the fault that stopped the instance, if one did and it shut down within the bound
     * @see #awaitShutdown()
     */
    public boolean awaitShutdown(Duration timeout) {
        boolean shutDown = false;
        Instant deadline = Instant.now().plus(timeout);
        try {
            while (Instant.now().isBefore(deadline)) {
                long remaining = Math.min(FAILURE_POLL_INTERVAL.toMillis(),
                        Math.max(1, Duration.between(Instant.now(), deadline).toMillis()));
                if (shutdown.await(remaining, TimeUnit.MILLISECONDS) || processor.isClosedOrFailed()) {
                    shutDown = true;
                    break;
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
        if (shutDown) {
            surfaceAnyFault();
        }
        return shutDown;
    }

    /**
     * Why this instance stopped, when a route asked it to (R24). Empty for an instance that is running, or that was
     * closed by its caller.
     */
    public Optional<StopRequest> stopRequest() {
        return Optional.ofNullable(stopRequest.get());
    }

    /**
     * @return the fault that stopped this instance, if one did - the facade's own definition fault, or the engine's
     * recorded control-thread failure
     */
    public Optional<Throwable> failureCause() {
        Throwable definitionFault = fault.get();
        return Optional.ofNullable(definitionFault != null ? definitionFault : processor.getFailureCause());
    }

    // ---------------------------------------------------------------- the parked set

    /**
     * One route of this instance, by any of its topics (R28).
     *
     * @throws IllegalArgumentException naming the routed topics, when nothing routes this one - a misspelled topic
     *                                  answered with an empty parked set would read as good news
     */
    public RouteHandle topic(String topic) {
        Set<String> routeTopics = routeTopicsByTopic.get(topic);
        if (routeTopics == null) {
            throw new IllegalArgumentException(msg("No route claims topic {} - this instance routes {}", topic,
                    routeTopicsByTopic.keySet()));
        }
        return new RouteHandle(this, routeTopics);
    }

    /**
     * Every parked record on this instance, whichever route it belongs to - the roll-up, named apart so the
     * per-route accessor is never overloaded (R28).
     */
    public ParkedView parkedAllTopics() {
        return parkedView("all topics", routeTopicsByTopic.keySet(), null);
    }

    ParkedView parkedView(String name, Set<String> topics, Integer partition) {
        // Read the set first and stamp it second, so the view's age can only overstate how stale it is.
        List<ParkedRecord> parked = dispatcher.parkedAcrossAllRoutes();
        return new ParkedView(name, topics, partition, parked, Instant.now());
    }

    // ---------------------------------------------------------------- InstanceControl

    /**
     * <b>Closes from a thread of its own, and that is the whole point.</b> This is called from a worker thread,
     * inside the user function's failure path, and the engine's close awaits the worker pool - so a worker that
     * closed inline would be waiting for itself until the shutdown timeout expired (KTD6).
     */
    @Override
    public void fatal(Throwable definitionFault) {
        if (!fault.compareAndSet(null, definitionFault)) {
            return;
        }
        log.error("Stopping this instance: a definition fault reached a record, and every record would meet it",
                definitionFault);
        // Not a drain, whatever the declared path: there is nothing worth draining, and every record in flight
        // meets the same fault.
        closeFromOurOwnThread("pc-fluent-fault-closer", ClosePath.DONT_DRAIN_FIRST);
    }

    /**
     * A route asked the instance to stop (R24, KTD6). The wrapper has already marked the stopping record never-due,
     * so a drain will not re-invoke it; what is left is the part a worker thread cannot do.
     * <p>
     * Three things happen, in this order and for different reasons. The reason is <b>recorded first</b>, so that a
     * caller woken by the close that follows can already read why. The engine is then <b>paused</b>, which is
     * non-blocking and stops the control thread handing out any more work - the window between this call and the
     * close is the whole reason the pause exists. Only then is the <b>close</b> started, on a thread of its own,
     * because this one is a worker and the close awaits the worker pool.
     */
    @Override
    public void stopRequested(ConsumerRecord<byte[], byte[]> record, String reason) {
        if (!stopRequest.compareAndSet(null, new StopRequest(record, reason, Instant.now()))) {
            log.debug("A second stop was requested at {}-{}@{} ({}); the first one is already closing this instance",
                    record.topic(), record.partition(), record.offset(), reason);
            return;
        }
        log.warn("Stopping this instance: the route for {} asked at {}-{}@{}: {}. The record is left incomplete, so "
                        + "a restart delivers it again - and will stop again unless the definition changes.",
                record.topic(), record.topic(), record.partition(), record.offset(), reason);
        meters.recordOutcome(record.topic(), FluentMeters.STOPPED);
        try {
            // Non-blocking: it moves the controller's state and wakes it, it does not wait for anything. The
            // controller then stops handing out new work AND takes the batches already queued in the worker pool
            // back out of it, which is what bounds what can still run after this line (KTD14).
            processor.pauseIfRunning();
        } catch (RuntimeException pauseFailed) {
            log.warn("Could not pause the instance while stopping it - the close below still stops it", pauseFailed);
        }
        closeFromOurOwnThread("pc-fluent-stop-closer", closePath);
    }

    private void closeFromOurOwnThread(String threadName, ClosePath path) {
        Thread closer = new Thread(() -> closeOnPath(path), threadName);
        closer.setDaemon(true);
        closer.start();
    }

    private void closeOnPath(ClosePath path) {
        if (!closing.compareAndSet(false, true)) {
            return;
        }
        try {
            shutTheEngineDown(path);
        } catch (RuntimeException closeFailed) {
            // Nobody asked for this close, so there is nobody to throw to.
            log.error("Closing this instance on the {} path failed", path, closeFailed);
        } finally {
            shutdown.countDown();
        }
    }

    private void surfaceAnyFault() {
        Throwable cause = fault.get();
        if (cause instanceof RuntimeException) {
            throw (RuntimeException) cause;
        }
        if (cause instanceof Error) {
            throw (Error) cause;
        }
        if (cause != null) {
            throw new IllegalStateException("This instance stopped because of a definition fault", cause);
        }
        surfaceAnyControlThreadFailure();
    }

    /**
     * The engine recorded a control-thread failure. Wrapped rather than rethrown: it is a checked exception the
     * awaiting caller never declared, and wrapping is also what tells it apart from a definition fault.
     */
    private void surfaceAnyControlThreadFailure() {
        Exception engineFailure = processor.getFailureCause();
        if (engineFailure == null) {
            return;
        }
        throw new InstanceFailedException("This instance's control thread failed, so it is no longer consuming: "
                + engineFailure, engineFailure);
    }

    // ---------------------------------------------------------------- seams

    /**
     * The engine underneath, for the units that grow this handle. Not part of the fluent surface.
     */
    ParallelEoSStreamProcessor<byte[], byte[]> processor() {
        return processor;
    }



    /**
     * Points the wrapper's parked view at the engine, and registers the control-thread hook. Called by the
     * definition once the engine is running, not from the constructor: this handle has to exist before the wrapper
     * can be told about it.
     */
    void startObserving() {
        dispatcher.parkedContainers(this::parkedContainers);
        processor.addLoopEndCallBack(this::onControlLoopEnd);
    }
}
