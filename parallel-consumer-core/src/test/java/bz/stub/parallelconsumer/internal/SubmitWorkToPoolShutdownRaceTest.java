package bz.stub.parallelconsumer.internal;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.PollContextInternal;
import bz.stub.parallelconsumer.state.WorkContainer;
import bz.stub.parallelconsumer.state.WorkManager;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import pl.tlinkowski.unij.api.UniLists;
import pl.tlinkowski.unij.api.UniMaps;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static bz.stub.parallelconsumer.internal.DrainingCloseable.DrainingMode;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Submitting work must not blow up the control thread when the worker pool is already gone.
 * <p>
 * Reproduces astubbs/parallel-consumer#209, where {@code submitWorkToPoolInner} submitted to a
 * {@code ThreadPoolExecutor[Terminated]} and the resulting {@link java.util.concurrent.RejectedExecutionException}
 * escaped {@code controlLoop} to be reported as an unclassified "Error from poll control thread".
 * <p>
 * There are two independent defences, and one test each, because neither subsumes the other:
 * <ol>
 *     <li>{@code submitWorkToPool} returns when the state is already {@code CLOSING}/{@code CLOSED}.</li>
 *     <li>{@code submitWorkToPoolInner} tolerates a rejected submit, which is the only defence that
 *         covers the window where the state still reads {@code RUNNING} but the pool has gone.</li>
 * </ol>
 * Both paths drop the batch, and each is asserted not to have dispatched it. Dropping leaves the containers in
 * flight and counted against {@code numberRecordsOutForProcessing}, which is safe only because every caller that
 * declines to dispatch is a closing instance or one whose pool is already dead - the offsets are not committed, so
 * the records are redelivered, and the leaked accounting does not outlive the instance that leaked it.
 * <p>
 * These drive {@code submitWorkToPool} directly rather than starting the control loop, so no broker
 * and no {@code supervisorLoop} are involved. The work, though, is taken exactly as the control loop takes it - through
 * {@link WorkManager#getWorkIfAvailable(int)} - because a hand built {@link WorkContainer} is not in flight and was
 * never counted, so it could not show a leak of either.
 */
@Slf4j
class SubmitWorkToPoolShutdownRaceTest {

    static final String TOPIC = "topic";
    static final int PARTITION = 0;

    /**
     * The stable part of the pool-gone line. Asserted on rather than the whole line so wording can move, but the line
     * cannot vanish.
     */
    static final String POOL_GONE_MESSAGE = "Worker pool is shut down while the state is";

    /**
     * The stable part of the per-submission rejection warning, counted to prove the batch loop stops rather than
     * offering every remaining batch to a pool that will refuse each one.
     */
    static final String REJECTED_SUBMISSION_MESSAGE = "Worker pool is shut down, not submitting work";

    final TopicPartition tp = new TopicPartition(TOPIC, PARTITION);
    final PCModuleTestEnv module = new PCModuleTestEnv();

    final Function<PollContextInternal<String, String>, List<String>> userFunction = context -> new ArrayList<>();
    final Consumer<String> callback = result -> {
    };

    TestParallelEoSStreamProcessor<String, String> pc;
    ExecutorService pool;
    WorkManager<String, String> wm;

    /**
     * Offsets are never reused across a test, so a container returned in one assertion cannot be confused with a fresh
     * take in the next.
     */
    long nextOffset = 0;

    @BeforeEach
    void setup() {
        var options = ParallelConsumerOptions.<String, String>builder()
                .consumer(new MockConsumer<String, String>(OffsetResetStrategy.LATEST))
                .build();
        pc = new TestParallelEoSStreamProcessor<>(options);

        wm = module.workManager();
        wm.onPartitionsAssigned(UniLists.of(tp));
        pc.setWm(wm);

        // the pool is a lazy supplier - resolve it up front so each test acts on the same instance
        pool = pc.workerThreadPool.get();
    }

    @AfterEach
    void tearDown() {
        // these tests never start the control loop, so the normal close handshake - which blocks on the
        // control thread's future - does not apply. Mark it closed and release the pool directly.
        pc.setState(State.CLOSED);
        pc.close();
        pool.shutdownNow();
    }

    /**
     * The state guard must actually stop the submission. Before the fix it logged "Not submitting new
     * work..." and then fell through and submitted anyway.
     */
    @Test
    void closingStateStopsSubmission() {
        var work = takeWork(1);
        pc.setState(State.CLOSING);

        pc.submitWorkToPool(userFunction, callback, work);

        // asserted on the container, not pool.getTaskCount(): that counter is documented approximate - a
        // worker's first task goes uncounted until runWorker takes its lock - so it can read 0 even when the
        // submit happened, which would let this test pass with the guard's return removed. setFuture is
        // synchronous inside submitWorkToPoolInner, so the future is exact.
        assertWithMessage("no work may be submitted once closing has begun")
                .that(work.get(0).getFuture()).isNull();
        assertNotDispatched(work);
    }

    /**
     * The window that actually produced astubbs/parallel-consumer#209: the pool has been terminated, but the state has not caught
     * up, so the guard above cannot help. The submit must fail soft rather than escape to the control loop.
     * <p>
     * This is also what keeps the return path proven live code now that {@code retrieveAndDistributeNewWork} checks
     * pool liveness before it takes anything. The check narrows the window; it cannot close it, because the pool can
     * be shut down in the instant after the check passes. What this test shows is that the path still works when
     * driven - not that {@code retrieveAndDistributeNewWork} can reach it. That reachability rests on an externally
     * shut-down pool or a second control thread, which no unit test can force from inside one instance.
     */
    @Test
    void terminatedPoolDoesNotEscapeToTheControlThread() throws InterruptedException {
        pool.shutdown();
        assertThat(pool.awaitTermination(5, SECONDS)).isTrue();

        var work = takeWork(1);
        // the state deliberately still reads RUNNING - this is the race, not a tidy shutdown
        pc.setState(State.RUNNING);

        assertThatCode(() -> pc.submitWorkToPool(userFunction, callback, work))
                .as("a terminated pool must not throw out of the control loop")
                .doesNotThrowAnyException();
        // pins the narrow behaviour: swallowing alone would also satisfy the assertion above, including from a
        // blanket catch. The batch must be undispatched whole, never left half-submitted.
        assertWithMessage("the rejected batch must not be dispatched")
                .that(work.get(0).getFuture()).isNull();
        assertNotDispatched(work);
    }

    /**
     * One batch is rejected, but the whole take is at stake: the {@code break} that stops the pointless resubmits
     * leaves every later batch taken and never dispatched, so those must come back too.
     */
    @Test
    void aShutdownRejectionStopsTheWholeSubmission() throws InterruptedException {
        pool.shutdown();
        assertThat(pool.awaitTermination(5, SECONDS)).isTrue();

        // batchSize is 1 by default, so three records are three batches - only the first is ever offered to the pool
        var work = takeWork(3);
        pc.setState(State.RUNNING);

        var processorLogger = (Logger) LoggerFactory.getLogger(AbstractParallelEoSStreamProcessor.class);
        var appender = new ListAppender<ILoggingEvent>();
        appender.start();
        processorLogger.addAppender(appender);
        try {
            pc.submitWorkToPool(userFunction, callback, work);
        } finally {
            processorLogger.detachAppender(appender);
        }

        assertNotDispatched(work);

        // The count is the whole point. Without the break, every remaining batch is offered to a pool that refuses
        // each one, and each refusal logs its own warning with a stack trace - so asserting only that nothing was
        // dispatched passes whether the loop stops or not. Scoped to this test's own pool by identity, because the
        // appender is on the class logger and surefire runs these concurrently.
        var ownPool = "@" + Integer.toHexString(System.identityHashCode(pool));
        var rejections = appender.list.stream()
                .filter(event -> event.getLevel() == Level.WARN)
                .filter(event -> event.getFormattedMessage().contains(REJECTED_SUBMISSION_MESSAGE))
                .filter(event -> event.getThrowableProxy() != null
                        && event.getThrowableProxy().getMessage() != null
                        && event.getThrowableProxy().getMessage().contains(ownPool))
                .collect(Collectors.toList());
        assertWithMessage("the batch loop must stop at the first rejection - three batches offered to a dead pool "
                + "would log three warnings, each with its own stack trace")
                .that(rejections).hasSize(1);
    }

    /**
     * Guards against the fix over-reaching: a healthy, running instance must still submit, and must not hand the work
     * back while a worker is holding it.
     */

    @Test
    void runningStateStillSubmits() {
        var work = takeWork(1);
        pc.setState(State.RUNNING);

        pc.submitWorkToPool(userFunction, callback, work);

        assertWithMessage("normal operation must be unaffected")
                .that(work.get(0).getFuture()).isNotNull();
        assertWithMessage("dispatched work stays in flight")
                .that(work.get(0).isInFlight()).isTrue();
        assertWithMessage("dispatched work stays counted as out for processing")
                .that(wm.getNumberRecordsOutForProcessing()).isEqualTo(1);
    }

    /**
     * The discard must be reachable ONLY by shutdown. A live pool that rejects is saturated, and swallowing
     * that would drop work under load - invisibly, which is the one outcome the catch must never produce.
     * <p>
     * {@code setupWorkerPool} is {@code protected}, so a subclass really can hand the processor a bounded
     * pool; this test is that subclass. It saturates a one-thread pool with a {@link SynchronousQueue} - no
     * queue slot, no free worker, so the next submit is rejected immediately while {@code isShutdown()} is
     * still false.
     * <p>
     * The throw is deliberately not absorbed: a live pool rejecting means saturation, and swallowing that would drop
     * work under healthy load - the one thing this catch must never do. It unwinds straight out of the batch loop, so
     * the batches queued behind the rejected one are dropped with it.
     */
    @Test
    void aLivePoolThatRejectsIsNotSwallowed() throws Exception {
        var workerRunning = new CountDownLatch(1);
        var releaseWorker = new CountDownLatch(1);
        var saturated = new ThreadPoolExecutor(1, 1, 0L, MILLISECONDS, new SynchronousQueue<>());

        var pcOnASaturatedPool = pcBackedBy(saturated);
        // two records is two batches: the first is rejected, the second never reaches the pool at all
        var work = takeWork(2);
        try {
            // occupy the only worker, so the SynchronousQueue has nowhere to hand off
            saturated.submit(() -> {
                workerRunning.countDown();
                releaseWorker.await();
                return null;
            });
            assertThat(workerRunning.await(5, SECONDS)).isTrue();
            assertWithMessage("precondition: the pool must be alive, not shut down")
                    .that(saturated.isShutdown()).isFalse();

            pcOnASaturatedPool.setState(State.RUNNING);

            assertThatThrownBy(() -> pcOnASaturatedPool.submitWorkToPool(userFunction, callback, work))
                    .as("a saturated live pool must fail loudly, never be discarded as if it were a shutdown")
                    .isInstanceOf(RejectedExecutionException.class);

            assertNotDispatched(work);
        } finally {
            releaseWorker.countDown();
            saturated.shutdownNow();
            pcOnASaturatedPool.setState(State.CLOSED);
            pcOnASaturatedPool.close();
        }
    }



    /**
     * The control loop must not take work it has nowhere to run. The state and the pool can disagree - a pool handed in
     * through {@code setupWorkerPool} and shut down by its owner, or a second control thread driving one instance -
     * and while they do, every take is a round trip out of the {@link WorkManager} and straight back in.
     */
    @Test
    void aShutDownPoolMeansNoWorkIsTakenAtAll() throws InterruptedException {
        registerWork(2);
        pool.shutdown();
        assertThat(pool.awaitTermination(5, SECONDS)).isTrue();
        // the disagreement: a live-looking state over a dead pool
        pc.setState(State.RUNNING);

        int got = pc.retrieveAndDistributeNewWork(userFunction, callback);

        assertWithMessage("no work may be taken when the pool cannot run it")
                .that(got).isEqualTo(0);
        assertWithMessage("nothing may be counted as out for processing")
                .that(wm.getNumberRecordsOutForProcessing()).isEqualTo(0);
        assertWithMessage("the work must be left where it was, still selectable")
                .that(wm.getNumberOfWorkQueuedInShardsAwaitingSelection()).isEqualTo(2);
    }

    /**
     * The skip has to be visible, or a consumer that has silently stopped processing looks identical to one with
     * nothing to do.
     * <p>
     * Once, though - not once per loop. The condition is sticky: a shut down pool never comes back, so the
     * disagreement lasts for the life of the instance. {@code controlLoop} blocks in {@code processWorkCompleteMailBox}
     * between passes, so this would be one line per commit-check interval rather than a spin - but repeated forever it
     * still buries the signal it exists to give. The edge is the diagnostic: the moment the state and the pool first
     * disagreed.
     */
    @Test
    void aPoolGoneUnderARunningInstanceClosesItOnceAndSaysWhy() throws InterruptedException {
        registerWork(3);
        pool.shutdown();
        assertThat(pool.awaitTermination(5, SECONDS)).isTrue();
        var processorLogger = (Logger) LoggerFactory.getLogger(AbstractParallelEoSStreamProcessor.class);
        var appender = new ListAppender<ILoggingEvent>();
        appender.start();
        processorLogger.addAppender(appender);
        try {
            for (int loop = 0; loop < 3; loop++) {
                // Re-arm the state every iteration, and note WHY: the first call transitions to CLOSING, and CLOSING
                // does not pass the state gate above onPoolGoneWhileStateAllowsWork. Without this the later calls
                // never reach the guarded block at all, so the count below would be 1 whether or not the edge
                // trigger exists - the assertion would pass against a deleted guard. DRAINING after the first pass
                // is the real re-entry route the trigger exists for: a close(DRAIN) arriving after the self-close
                // puts the state back to DRAINING, which is exactly what the guard's own comment describes.
                pc.setState(loop == 0 ? State.RUNNING : State.DRAINING);
                pc.retrieveAndDistributeNewWork(userFunction, callback);
            }
        } finally {
            processorLogger.detachAppender(appender);
        }

        // The appender is on the class logger, and surefire runs these tests concurrently, so it also catches the
        // warning from any other instance skipping at the same moment. Narrow to this test's own pool by its identity,
        // which the warning prints as part of the pool's toString - otherwise this passes or fails on scheduling.
        var ownPool = "@" + Integer.toHexString(System.identityHashCode(pool));
        var skipWarnings = appender.list.stream()
                .filter(event -> event.getLevel() == Level.ERROR)
                .filter(event -> event.getFormattedMessage().contains(POOL_GONE_MESSAGE))
                .filter(event -> event.getFormattedMessage().contains(ownPool))
                .collect(Collectors.toList());
        assertWithMessage("the pool-gone line must be logged once for this instance across %s loops - the condition "
                + "is sticky, so repeating it would bury the one line worth reading", 3)
                .that(skipWarnings).hasSize(1);
        assertWithMessage("a running instance whose pool was killed must close itself, not loop forever with work "
                + "it can never run")
                .that(pc.getState()).isEqualTo(State.CLOSING);
        assertWithMessage("the close must say why - a caller asking getFailureCause() should not see a destroyed "
                + "pool as an ordinary close, and the chaos harness's canary sweep reads exactly this")
                .that(pc.getFailureCause()).isNotNull();
        // Not asserted here, deliberately: transitionToClosing wakes the loop through interruptControlThread, which
        // no-ops unless blockableControlThread is set - and that is only assigned by supervisorLoop, which these
        // tests do not start. An interrupt assertion would pass because nothing interrupted, not because the flag was
        // cleared. The clear is still required in production, where that field is set; it is verified by reading, and
        // by the precedent supervisorLoop sets before its own doClose.
    }

    /**
     * Guards against the check over-reaching: a live pool must still be given work, exactly as before.
     */
    @Test
    void aLivePoolStillTakesAndDispatches() throws InterruptedException {
        registerWork(1);
        pc.setState(State.RUNNING);

        var ranOnAWorker = new CountDownLatch(1);
        Function<PollContextInternal<String, String>, List<String>> latchedUserFunction = context -> {
            ranOnAWorker.countDown();
            return new ArrayList<>();
        };

        int got = pc.retrieveAndDistributeNewWork(latchedUserFunction, callback);

        assertWithMessage("a live pool must still have work taken for it")
                .that(got).isEqualTo(1);
        assertWithMessage("and the work must actually reach a worker")
                .that(ranOnAWorker.await(5, SECONDS)).isTrue();
    }

    /**
     * A drain mode close must still finish over a dead pool.
     * <p>
     * The drain gate would not let it. {@code drain()} only transitions to {@code CLOSING} once
     * {@code isRecordsAwaitingProcessing()} is false, which during a live control loop reduces to the shards'
     * awaiting-selection count - and over a dead pool nothing consumes that count, because the pre-take check
     * declines to take work the pool can never run. Left to the gate alone, this would sit until
     * {@link java.util.concurrent.TimeoutException} out of {@code waitForClose}.
     * <p>
     * It terminates because the gate is never reached. {@code retrieveAndDistributeNewWork} runs before
     * {@code controlLoop}'s state switch, so its pool-liveness check calls {@code transitionToClosing} first, on the
     * same reasoning that makes a dead pool terminal in {@code RUNNING}: nobody asked this instance to close, and it
     * can never process another record. Draining is just the state it happened to be in.
     */
    @Test
    void drainModeCloseStillTerminatesOverADeadPool() {
        // work sat in the shards is what the drain gate reads, so this is the case that can hold it open
        registerWork(2);

        var draining = new TestParallelEoSStreamProcessor<String, String>(
                ParallelConsumerOptions.<String, String>builder()
                        .consumer(new MockConsumer<String, String>(OffsetResetStrategy.LATEST))
                        // short, so a gate that never opens fails fast rather than sitting on the 30s default
                        .drainTimeout(Duration.ofSeconds(2))
                        .shutdownTimeout(Duration.ofSeconds(1))
                        .commitInterval(Duration.ofMillis(50))
                        .build());
        draining.setWm(wm);
        // shut down by something other than this instance's close - the only way the state can still read live
        var deadPool = draining.workerThreadPool.get();
        deadPool.shutdown();

        try {
            draining.supervisorLoop(userFunction, callback);

            assertThatCode(() -> draining.close(DrainingMode.DRAIN))
                    .as("a drain mode close must reach CLOSED even when the pool it is draining into is gone")
                    .doesNotThrowAnyException();
            assertWithMessage("the instance must actually be closed, not merely un-thrown")
                    .that(draining.isClosedOrFailed()).isTrue();
        } finally {
            deadPool.shutdownNow();
        }
    }

    /**
     * None of this work reached the pool. It is dropped rather than handed back, which is only safe because every
     * caller that declines to dispatch is a closing instance or one whose pool is already dead: the offsets are not
     * committed, so the records are redelivered, and the in flight accounting it leaves behind does not outlive the
     * instance that leaked it.
     */
    private void assertNotDispatched(List<WorkContainer<String, String>> work) {
        for (var wc : work) {
            assertWithMessage("this work must not have reached the pool: %s", wc)
                    .that(wc.getFuture()).isNull();
        }
    }

    /**
     * A processor whose worker pool is the given one. {@code setupWorkerPool} is {@code protected} precisely so a
     * subclass can do this, which is also how a user could hand the real thing a bounded pool.
     */
    private TestParallelEoSStreamProcessor<String, String> pcBackedBy(ThreadPoolExecutor boundedPool) {
        var processor = new TestParallelEoSStreamProcessor<String, String>(
                ParallelConsumerOptions.<String, String>builder()
                        .consumer(new MockConsumer<String, String>(OffsetResetStrategy.LATEST))
                        .build()) {
            @Override
            protected ThreadPoolExecutor setupWorkerPool(int poolSize) {
                return boundedPool;
            }
        };
        processor.setWm(wm);
        return processor;
    }

    /**
     * Registers {@code count} records and takes them as work the way the control loop does, so every container really
     * is in flight and really is counted against {@code numberRecordsOutForProcessing}.
     * <p>
     * A key per record: the default ordering is KEY, so sharing one would serialise them and only the first would ever
     * be selectable.
     */
    private List<WorkContainer<String, String>> takeWork(int count) {
        registerWork(count);

        var taken = wm.getWorkIfAvailable(count);
        assertWithMessage("fixture: all %s records must be taken as work", count)
                .that(taken).hasSize(count);
        assertWithMessage("fixture: the take must be counted as out for processing")
                .that(wm.getNumberRecordsOutForProcessing()).isEqualTo(count);
        return taken;
    }

    /**
     * As {@link #takeWork(int)}, but stops at registration - the records are left sitting in the shards, selectable but
     * not taken. That is the starting state {@code retrieveAndDistributeNewWork} is given: it does its own take.
     */
    private void registerWork(int count) {
        var records = new ArrayList<ConsumerRecord<String, String>>();
        for (int i = 0; i < count; i++) {
            long offset = nextOffset++;
            records.add(new ConsumerRecord<>(TOPIC, PARTITION, offset, "key-" + offset, "value"));
        }
        wm.registerWork(new EpochAndRecordsMap<>(new ConsumerRecords<>(UniMaps.of(tp, records)), wm.getPm()));
    }
}
