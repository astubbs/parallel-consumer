package bz.stub.parallelconsumer.internal;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.PollContextInternal;
import bz.stub.parallelconsumer.state.ModelUtils;
import bz.stub.parallelconsumer.state.WorkContainer;
import bz.stub.parallelconsumer.state.WorkManager;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import pl.tlinkowski.unij.api.UniLists;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

/**
 * A pause has to reach the worker pool, not only the controller (KTD14).
 *
 * <h2>The gap this closes</h2>
 * {@code pauseIfRunning()} moves the controller's state, and the controller then stops handing out work. It can do
 * nothing, on its own, about the batches it has <em>already</em> handed to the pool: those sit in the pool's queue
 * and start whenever a worker frees up, running the user's function for an instance that has been told to stop
 * processing. The controller now takes them back - it pulls the queued batches out of the pool's queue and abandons
 * their claims, so the records return to awaiting selection with nothing having happened to them.
 *
 * <h2>Why the controller and not the task</h2>
 * Work that may not start should not be started. Checking inside the pool task means the batch runs, finds the
 * instance paused, and hands itself back through the failure path - the only hand-back a running task has - which
 * then needs an exception that says "this was not really a failure". Taking it out of the queue needs none of that,
 * and it is the same seam a worker-pull engine would use: a worker that may not start simply does not take.
 *
 * <h2>What is asserted, and what is deliberately not</h2>
 * The guarantee is an <b>upper bound on what runs</b>, not zero. The pause is set by whoever called it and the
 * controller acts on its next pass, so a batch that starts in between runs to completion - that window is accepted,
 * and documented on {@code purgeQueuedWorkNotAllowedToStart}. What must hold is that the queue is emptied by the
 * controller rather than drained by the workers, that a record taken back spent nothing, and that it is handed out
 * again when the instance resumes.
 */
@Timeout(120)
class PausedWorkIsTakenBackOutOfThePoolTest {

    private static final String TOPIC = "topic";

    private static final int PARTITION = 0;

    private final TopicPartition tp = new TopicPartition(TOPIC, PARTITION);

    private final PCModuleTestEnv module = new PCModuleTestEnv();

    private final AtomicInteger userFunctionRuns = new AtomicInteger();

    /**
     * Held by every batch that starts, so the pool's one worker stays occupied and its queue stays full.
     */
    private final CountDownLatch releaseTheWorker = new CountDownLatch(1);

    private final CountDownLatch aBatchHasStarted = new CountDownLatch(1);

    private final Function<PollContextInternal<String, String>, List<String>> blockingUserFunction = context -> {
        userFunctionRuns.incrementAndGet();
        aBatchHasStarted.countDown();
        try {
            if (!releaseTheWorker.await(60, TimeUnit.SECONDS)) {
                throw new IllegalStateException("the test never released the worker");
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
        return new ArrayList<>();
    };

    private final Consumer<String> callback = result -> {
    };

    private TestParallelEoSStreamProcessor<String, String> pc;

    private ThreadPoolExecutor pool;

    /**
     * Only the batching test builds one, because only it needs a batch of more than one record. Torn down beside
     * {@link #pc} so its pool and its threads do not outlive the test.
     */
    private TestParallelEoSStreamProcessor<String, String> batchingPc;

    private WorkManager<String, String> wm;

    private long nextOffset = 0;

    @BeforeEach
    void setup() {
        var options = ParallelConsumerOptions.<String, String>builder()
                .consumer(new MockConsumer<String, String>(OffsetResetStrategy.LATEST))
                // One worker, so a batch that is queued stays queued for as long as the test needs it to.
                .maxConcurrency(1)
                .build();
        pc = new TestParallelEoSStreamProcessor<>(options);
        wm = module.workManager();
        wm.onPartitionsAssigned(UniLists.of(tp));
        pc.setWm(wm);
        pool = pc.workerThreadPool.get();
    }

    @AfterEach
    void tearDown() {
        releaseTheWorker.countDown();
        if (batchingPc != null) {
            var batchingPool = batchingPc.workerThreadPool.get();
            batchingPc.setState(State.CLOSED);
            batchingPc.close();
            batchingPool.shutdownNow();
        }
        pc.setState(State.CLOSED);
        pc.close();
        pool.shutdownNow();
    }

    /**
     * The control arm. Everything below is measured against this: the same submission, the same work, the only
     * difference being the state the controller is in when it makes its pass.
     */
    @Test
    void aRunningInstanceLeavesItsQueuedWorkAlone() throws InterruptedException {
        // The containers are not needed here - this arm asserts about the QUEUE, not about either record - so the
        // fixture's return is dropped, as the other queue-shaped arms below do.
        submitOneBlockingBatchAndQueueAnother();
        pc.setState(State.RUNNING);

        assertThat(pc.purgeQueuedWork()).isEqualTo(0);

        assertThat(pool.getQueue()).hasSize(1);
        assertThat(pc.getRecordsPurgedWhilePaused()).isEqualTo(0);
    }

    /**
     * The change itself: the batch that had not started is taken back out of the pool's queue.
     */
    @Test
    void aQueuedBatchIsTakenBackOutOfTheQueueWhilePaused() throws InterruptedException {
        submitOneBlockingBatchAndQueueAnother();
        pc.setState(State.PAUSED);

        assertThat(pc.purgeQueuedWork()).isEqualTo(1);

        assertWithMessage("the queue must be emptied by the controller, not drained by the workers")
                .that(pool.getQueue()).isEmpty();
        assertThat(pc.getRecordsPurgedWhilePaused()).isEqualTo(1);
        // One run, and it is the batch that had already started - the queued one never reached the function.
        assertThat(userFunctionRuns.get()).isEqualTo(1);
    }

    /**
     * Nothing happened to the record, which is the whole point of abandoning the claim rather than failing it: no
     * attempt spent, no failure recorded, no retry delay to wait out, and selectable again at once.
     */
    @Test
    void anAbandonedRecordSpentNothingAndIsHandedOutAgainOnResume() throws InterruptedException {
        var work = submitOneBlockingBatchAndQueueAnother();
        pc.setState(State.PAUSED);

        // Asserted, not dropped: it is this test's own PREMISE - that the record was taken back rather than left
        // queued. Every assertion below is about a container the purge was supposed to have touched, so a purge
        // that took back nothing would be diagnosed from the wrong one of them.
        assertThat(pc.purgeQueuedWork()).isEqualTo(1);

        WorkContainer<String, String> abandoned = work.queued;
        assertThat(abandoned.getNumberOfFailedAttempts()).isEqualTo(0);
        assertThat(abandoned.getLastFailedAt().isPresent()).isFalse();
        assertThat(abandoned.isParked()).isFalse();
        assertThat(abandoned.isUserFunctionComplete()).isFalse();
        assertWithMessage("the claim ended with no verdict, so the record is available again")
                .that(abandoned.isAvailableToTakeAsWork()).isTrue();

        // ...and the controller hands it out again the moment it is allowed to, which is what "processed on resume"
        // means for a record that is back in its shard rather than in the retry queue.
        pc.setState(State.RUNNING);
        assertThat(wm.getWorkIfAvailable(10)).contains(abandoned);
    }

    /**
     * A worker that gets to a batch first has <em>started</em> it, which makes it in-flight work rather than queued
     * work - and in-flight work is finished. The queue's own removal is what decides which of the two happened, so
     * no batch can be both run and abandoned.
     */
    @Test
    void aBatchAWorkerHasAlreadyTakenIsNotAbandoned() throws InterruptedException {
        var work = submitOneBlockingBatchAndQueueAnother();
        pc.setState(State.PAUSED);

        // The started batch is not in the queue at all, so the purge cannot see it - one record taken back, not two.
        assertThat(pc.purgeQueuedWork()).isEqualTo(1);

        assertThat(work.started.isNotInFlight()).isFalse();
        assertThat(work.started.isUserFunctionComplete()).isFalse();
    }

    /**
     * The dont-drain close says "will finish in flight, then close". A batch still queued in the pool is not in
     * flight, so starting it is starting new work during a close that promised not to.
     */
    @Test
    void aDontDrainCloseTakesItsQueuedWorkBackToo() throws InterruptedException {
        submitOneBlockingBatchAndQueueAnother();
        pc.setState(State.CLOSING);

        assertThat(pc.purgeQueuedWork()).isEqualTo(1);
        assertThat(pool.getQueue()).isEmpty();
    }

    /**
     * And the drain-first close is the arm that must NOT take work back: it exists to dispatch the records already
     * buffered, so its queued batches have to run. Without this the two closes would be the same close.
     */
    @Test
    void aDrainingInstanceLeavesItsQueuedWorkToRun() throws InterruptedException {
        submitOneBlockingBatchAndQueueAnother();
        pc.setState(State.DRAINING);

        assertThat(pc.purgeQueuedWork()).isEqualTo(0);
        assertThat(pool.getQueue()).hasSize(1);
    }

    /**
     * <b>A batch is dequeued before its hand-backs run, so one hand-back must not take the rest with it.</b>
     * {@code queue.remove()} has already succeeded by the time {@code onAbandonedBeforeStarting} is called, and it
     * cannot be undone - so a throw escaping that loop leaves every container behind the failing one marked in
     * flight forever: never returned to selection, never run, and under KEY ordering stalling its shard. That is
     * the same stall the purge exists to prevent, reached through a different door. Found by the review of
     * astubbs/parallel-consumer#506, and it needs a batch of more than one to be visible at all - every other
     * test here uses a single-record batch.
     * <p>
     * The failure is still surfaced, because a container whose bookkeeping threw is unaccounted for and PC does not
     * continue past one. What changes is that it is surfaced <em>after</em> the loop rather than out of the middle
     * of it.
     * <p>
     * <b>Proved by sabotage:</b> removing the per-container try/catch from the purge loop reddens this test alone,
     * at the "still have been handed back" assertion, and leaves the other six here green - so the isolation is
     * detected here and nowhere else.
     */
    @Test
    void oneHandbackThrowingDoesNotStrandTheRestOfItsBatch() throws InterruptedException {
        // Its own instance, because every other test here wants single-record batches and this one cannot be written
        // with them: makeBatches splits by batchSize, so two records at the default size are two queued batches and
        // a throw in the first simply leaves the second in the queue. The stranding needs them in ONE batch.
        batchingPc = new TestParallelEoSStreamProcessor<>(ParallelConsumerOptions.<String, String>builder()
                .consumer(new MockConsumer<String, String>(OffsetResetStrategy.LATEST))
                .maxConcurrency(1)
                .batchSize(2)
                .build());
        batchingPc.setWm(wm);
        var batchingPool = batchingPc.workerThreadPool.get();
        batchingPc.setState(State.RUNNING);
        batchingPc.submitWorkToPool(blockingUserFunction, callback, UniLists.of(takeWork(), takeWork()));
        assertWithMessage("the first batch must be inside the function before the second is queued behind it")
                .that(aBatchHasStarted.await(30, TimeUnit.SECONDS)).isTrue();
        var queuedBatch = UniLists.of(takeWork(), takeWork());
        batchingPc.submitWorkToPool(blockingUserFunction, callback, queuedBatch);
        Awaitility.await().atMost(Duration.ofSeconds(30)).until(() -> batchingPool.getQueue().size() == 1);

        WorkManager<String, String> handbackFailsOnTheFirst = spy(wm);
        doThrow(new IllegalStateException("deliberate - PC's own bookkeeping throws for one container"))
                .when(handbackFailsOnTheFirst).onAbandonedBeforeStarting(queuedBatch.get(0));
        batchingPc.setWm(handbackFailsOnTheFirst);
        batchingPc.setState(State.PAUSED);

        assertWithMessage("a record PC can no longer account for is not something it continues past")
                .that(assertThrows(IllegalStateException.class, batchingPc::purgeQueuedWork))
                .hasMessageThat().contains("deliberate");

        WorkContainer<String, String> behindTheFailure = queuedBatch.get(1);
        assertWithMessage("the container behind the failing one must still have been handed back - stranded, it "
                + "stays in flight forever and holds its shard")
                .that(behindTheFailure.isNotInFlight()).isTrue();
        assertWithMessage("...and it spent nothing, so it is selectable again")
                .that(behindTheFailure.isAvailableToTakeAsWork()).isTrue();
        assertThat(wm.getWorkIfAvailable(10)).contains(behindTheFailure);
    }

    /**
     * Fills the one worker with a batch that blocks, and leaves a second batch sitting in the queue behind it -
     * which is the situation every test here is about. Returns both containers so a test can say which is which.
     */
    private SubmittedWork submitOneBlockingBatchAndQueueAnother() throws InterruptedException {
        pc.setState(State.RUNNING);
        var started = takeWork();
        pc.submitWorkToPool(blockingUserFunction, callback, UniLists.of(started));
        assertWithMessage("the first batch must be inside the function before the second is queued behind it")
                .that(aBatchHasStarted.await(30, TimeUnit.SECONDS)).isTrue();

        var queued = takeWork();
        pc.submitWorkToPool(blockingUserFunction, callback, UniLists.of(queued));
        Awaitility.await().atMost(Duration.ofSeconds(30)).until(() -> pool.getQueue().size() == 1);
        return new SubmittedWork(started, queued);
    }

    private static class SubmittedWork {

        final WorkContainer<String, String> started;

        final WorkContainer<String, String> queued;

        SubmittedWork(WorkContainer<String, String> started, WorkContainer<String, String> queued) {
            this.started = started;
            this.queued = queued;
        }
    }

    /**
     * One record, taken the way the controller takes it - through the work manager - so the container really is in
     * flight and really is counted, which a hand-built one would not be.
     */
    private WorkContainer<String, String> takeWork() {
        // ModelUtils.pollOf keys each record by its offset, which is what this needs: the default ordering is KEY,
        // so one shared key would put every record in one shard and the shard would hand out only its head.
        long offset = nextOffset++;
        wm.registerWork(new EpochAndRecordsMap<>(ModelUtils.pollOf(tp, offset), wm.getPm()));
        var work = wm.getWorkIfAvailable(1);
        assertWithMessage("the fixture must hand the test the work it asked for").that(work).hasSize(1);
        return work.get(0);
    }
}
