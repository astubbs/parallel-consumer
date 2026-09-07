package bz.stub.parallelconsumer;

/*-
 * Copyright (C) 2020-2024 Confluent, Inc.
 * Modifications Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.internal.PCModule;
import bz.stub.parallelconsumer.internal.PCModuleTestEnv;
import bz.stub.parallelconsumer.internal.TestParallelEoSStreamProcessor;
import bz.stub.parallelconsumer.offsets.OffsetMapCodecManager;
import bz.stub.parallelconsumer.state.ModelUtils;
import bz.stub.parallelconsumer.state.PartitionState;
import bz.stub.parallelconsumer.state.WorkContainer;
import bz.stub.parallelconsumer.state.WorkManager;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import pl.tlinkowski.unij.api.UniLists;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests to verify the protected and internal methods of
 * {@link bz.stub.parallelconsumer.internal.AbstractParallelEoSStreamProcessor} work as expected.
 * <p>
 *
 * @author Jonathon Koyle
 */
@Slf4j
class AbstractParallelEoSStreamProcessorConfigurationTest {
    final MockConsumer<String, String> consumer = new MockConsumer<>(OffsetResetStrategy.LATEST);
    final ParallelConsumerOptions<String, String> testOptions = ParallelConsumerOptions.<String, String>builder()
            .consumer(consumer)
            .build();

    ModelUtils mu = new ModelUtils();
    PartitionState<String, String> state;
    WorkManager<String, String> wm;

    String topic = "myTopic";
    int partition = 0;

    TopicPartition tp = new TopicPartition(topic, partition);
    PCModule module = new PCModuleTestEnv();

    @BeforeEach
    public void setup() {
        state = new PartitionState<>(0, mu.getModule(), tp, OffsetMapCodecManager.HighestOffsetAndIncompletes.of());
        wm = mu.getModule().workManager();
        wm.onPartitionsAssigned(UniLists.of(tp));
    }

    /**
     * Test that the {@link bz.stub.parallelconsumer.internal.AbstractParallelEoSStreamProcessor#getQueueTargetLoaded}
     */
    @Test
    void queueTargetLoad() {
        final int batchSize = 10;
        final int concurrency = 2;
        final MockConsumer<String, String> consumer = new MockConsumer<>(OffsetResetStrategy.LATEST);
        final ParallelConsumerOptions<String, String> testOptions = ParallelConsumerOptions.<String, String>builder()
                .batchSize(batchSize)
                .maxConcurrency(concurrency)
                .consumer(consumer)
                .build();
        try (final TestParallelEoSStreamProcessor<String, String> testInstance = new TestParallelEoSStreamProcessor<>(testOptions)) {
            final int defaultLoad = 2;
            final int expectedTargetLoad = batchSize * concurrency * defaultLoad;

            final int actualTargetLoad = testInstance.getTargetLoad();

            Assertions.assertEquals(expectedTargetLoad, actualTargetLoad);
        }
    }

    @Test
    void testHandleStaleWorkSplit() {
        List<WorkContainer<String, String>> workContainers = new ArrayList<>();

        workContainers.add(new WorkContainer<String, String>(0, new ConsumerRecord<>(topic, partition, 0, "test_k", "test_v1"), module));
        workContainers.add(new WorkContainer<String, String>(1, new ConsumerRecord<>(topic, partition, 1, "test_k", "test_v2"), module));

        try (final TestParallelEoSStreamProcessor<String, String> testInstance = new TestParallelEoSStreamProcessor<>(testOptions)) {
            testInstance.setWm(wm);
            Function<PollContextInternal<String, String>, List<String>> dummyFunction = (contextInternal) -> new ArrayList<>();
            Consumer<String> callback = (res) -> {
            };

            testInstance.runUserFunc(dummyFunction, callback, workContainers);


            Assertions.assertEquals(testInstance.getMailBoxSuccessCnt(), 1);
            Assertions.assertEquals(testInstance.getMailBoxFailedCnt(), 1);
        }
    }

    @Test
    void testHandleStaleWorkNoSplit() {
        List<WorkContainer<String, String>> workContainers = new ArrayList<>();

        workContainers.add(new WorkContainer<String, String>(0, new ConsumerRecord<>(topic, partition, 0, "test_k", "test_v1"), module));
        workContainers.add(new WorkContainer<String, String>(0, new ConsumerRecord<>(topic, partition, 1, "test_k", "test_v2"), module));

        try (final TestParallelEoSStreamProcessor<String, String> testInstance = new TestParallelEoSStreamProcessor<>(testOptions)) {
            testInstance.setWm(wm);
            Function<PollContextInternal<String, String>, List<String>> dummyFunction = (contextInternal) -> new ArrayList<>();
            Consumer<String> callback = (res) -> {
            };

            testInstance.runUserFunc(dummyFunction, callback, workContainers);


            Assertions.assertEquals(testInstance.getMailBoxSuccessCnt(), 2);
            Assertions.assertEquals(testInstance.getMailBoxFailedCnt(), 0);
        }
    }

    /**
     * {@code setupWorkerPool} is a {@code protected} extension point, so a subclass chooses the pool's queue bounds and
     * its {@link RejectedExecutionHandler}. Only a handler that throws is supported, and the pool is checked when it is
     * built rather than when it first rejects something - by then the work is already gone.
     * <p>
     * These construct the processor and assert on construction itself, which is the point: a precondition that only
     * fired on the first dispatch would let a subclass ship a work-losing pool and only discover it in production,
     * under the load that makes a bounded queue reject in the first place.
     */
    @Test
    void aPoolThatSilentlyDiscardsRejectedWorkIsRefusedAtSetup() {
        var discarding = boundedPoolWith(new ThreadPoolExecutor.DiscardPolicy());
        try {
            assertThatThrownBy(() -> processorBackedBy(discarding))
                    .as("a pool whose rejection handler drops the batch without a sound must not be accepted, and the " +
                            "refusal must happen at construction, not at the first dispatch")
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining(ThreadPoolExecutor.DiscardPolicy.class.getName())
                    .hasMessageContaining(ThreadPoolExecutor.AbortPolicy.class.getName())
                    // the consequence, not just the rule: a message that only says "unsupported" leaves the reader to
                    // guess whether this is a style preference or a data loss
                    .hasMessageContaining("never completes")
                    .hasMessageContaining("numberRecordsOutForProcessing");
        } finally {
            discarding.shutdownNow();
        }
    }

    /**
     * {@link ThreadPoolExecutor.DiscardOldestPolicy} loses a <em>different</em> batch than the one being submitted - it
     * evicts the head of the queue and retries - so the submit appears to succeed while some earlier batch is gone.
     */
    @Test
    void aPoolThatEvictsTheOldestQueuedWorkIsRefusedAtSetup() {
        var evicting = boundedPoolWith(new ThreadPoolExecutor.DiscardOldestPolicy());
        try {
            assertThatThrownBy(() -> processorBackedBy(evicting))
                    .as("evicting a queued batch loses work just as surely as discarding the submitted one")
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining(ThreadPoolExecutor.DiscardOldestPolicy.class.getName());
        } finally {
            evicting.shutdownNow();
        }
    }

    /**
     * {@link ThreadPoolExecutor.CallerRunsPolicy} loses nothing, but it runs the user's function on whichever thread
     * called {@code submit} - the control thread - so the control loop stops polling, committing and distributing work
     * for as long as that function takes. Refused for that, not for loss.
     */
    @Test
    void aPoolThatRunsRejectedWorkOnTheControlThreadIsRefusedAtSetup() {
        var callerRuns = boundedPoolWith(new ThreadPoolExecutor.CallerRunsPolicy());
        try {
            assertThatThrownBy(() -> processorBackedBy(callerRuns))
                    .as("running the user's function on the control thread stalls the control loop")
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining(ThreadPoolExecutor.CallerRunsPolicy.class.getName());
        } finally {
            callerRuns.shutdownNow();
        }
    }

    /**
     * Guards against the check over-reaching: the pool this class builds for itself must sail through unaltered.
     * <p>
     * Asserted on the instance {@code setupWorkerPool} returned, so this fails if the check ever starts substituting a
     * pool or a handler of its own rather than refusing one.
     */
    @Test
    void theDefaultPoolIsAcceptedUnchanged() {
        var built = new AtomicReference<ThreadPoolExecutor>();
        try (var testInstance = new TestParallelEoSStreamProcessor<String, String>(testOptions) {
            @Override
            protected ThreadPoolExecutor setupWorkerPool(int poolSize) {
                var pool = super.setupWorkerPool(poolSize);
                built.set(pool);
                return pool;
            }
        }) {
            assertThat(built.get())
                    .as("the default pool must be built during construction - the check cannot be deferred to first use")
                    .isNotNull();
            assertThat(built.get().getRejectedExecutionHandler())
                    .as("the pool must be handed on exactly as setupWorkerPool built it")
                    .isInstanceOf(ThreadPoolExecutor.AbortPolicy.class);
        }
    }

    /**
     * The pool supplier is memoized, so nothing about its type stops the pool - and therefore the check on it - from
     * being deferred to the first dispatch. It is not: the constructor resolves it immediately.
     * <p>
     * Pinned through a landmark rather than a clock, because "when" is a position in the constructor: {@code wm} is
     * assigned on the statement after the pool is resolved, so a pool built at that point sees a null one, and a pool
     * built any later - by the metrics binding at the end of the constructor, or by the first dispatch - does not. The
     * distinction matters beyond timing: a refusal here happens before the broker poller and producer manager exist,
     * so there is no half built subsystem to unwind.
     */
    @Test
    void theWorkerPoolIsResolvedBeforeTheRestOfTheSubsystemIsBuilt() {
        var landmarkWhenPoolWasBuilt = new AtomicReference<Object>("the pool was never built");
        try (var testInstance = new TestParallelEoSStreamProcessor<String, String>(testOptions) {
            @Override
            protected ThreadPoolExecutor setupWorkerPool(int poolSize) {
                landmarkWhenPoolWasBuilt.set(getWm());
                return super.setupWorkerPool(poolSize);
            }
        }) {
            assertThat(landmarkWhenPoolWasBuilt.get())
                    .as("the pool - and the precondition on it - must be resolved before the WorkManager is assigned, " +
                            "not left for the first dispatch, which a subclass could ship without ever reaching")
                    .isNull();
        }
    }

    /**
     * The requirement is behavioural - reject by throwing {@link java.util.concurrent.RejectedExecutionException} - so
     * an {@link ThreadPoolExecutor.AbortPolicy} subclass that adds logging or a metric still satisfies it. Checking for
     * the exact class would ban that for no reason.
     */
    @Test
    void aSubclassOfAbortPolicyIsAccepted() {
        var custom = boundedPoolWith(new CountingAbortPolicy());
        var accepted = new AtomicReference<TestParallelEoSStreamProcessor<String, String>>();
        try {
            assertThatCode(() -> accepted.set(processorBackedBy(custom)))
                    .as("an AbortPolicy subclass still throws on rejection, which is all the submission path needs")
                    .doesNotThrowAnyException();
            assertThat(((CountingAbortPolicy) custom.getRejectedExecutionHandler()).rejections)
                    .as("the check must read the pool's configuration, not probe it by pushing a task through")
                    .hasValue(0);
        } finally {
            if (accepted.get() != null) {
                accepted.get().close();
            }
            custom.shutdownNow();
        }
    }

    /**
     * Still an {@link ThreadPoolExecutor.AbortPolicy} - it throws - it just counts first. Stands in for the wrapping a
     * real subclass would do.
     */
    static class CountingAbortPolicy extends ThreadPoolExecutor.AbortPolicy {
        final AtomicInteger rejections = new AtomicInteger();

        @Override
        public void rejectedExecution(Runnable r, ThreadPoolExecutor e) {
            rejections.incrementAndGet();
            super.rejectedExecution(r, e);
        }
    }

    /**
     * The check is deliberately NOT conditional on the queue being bounded, and this is the test that says why.
     * <p>
     * It is tempting to argue that an unbounded queue never rejects, so a losing handler on one is harmless. That is
     * wrong: {@code ThreadPoolExecutor#execute} rejects a task submitted to a <em>shut down</em> pool before it ever
     * offers it to the queue. Measured on this exact configuration - unbounded queue, {@code DiscardPolicy}, pool shut
     * down - {@code submit} returns without throwing and hands back a {@code Future} that never completes.
     * <p>
     * That is precisely the close-race {@code submitWorkToPoolInner} exists to survive, so narrowing this refusal to
     * bounded queues would reopen the hole on the one path that is definitely reached. An unbounded queue removes
     * saturation, not rejection.
     */
    @Test
    void anUnboundedQueueDoesNotMakeALosingHandlerHarmless() {
        var unboundedDiscarding = new ThreadPoolExecutor(1, 1, 0L, MILLISECONDS,
                new LinkedBlockingQueue<>(), new ThreadPoolExecutor.DiscardPolicy());
        try {
            assertThatThrownBy(() -> processorBackedBy(unboundedDiscarding))
                    .as("an unbounded queue only removes saturation - a shut down pool still routes through the "
                            + "handler, so a discarding one still loses the batch silently")
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining(ThreadPoolExecutor.DiscardPolicy.class.getName());
        } finally {
            unboundedDiscarding.shutdownNow();
        }
    }

    /**
     * A pool that can actually reject under load: one thread, one queue slot. The bound is what makes the
     * handler reachable by SATURATION - an unbounded queue never saturates - which is why these tests bound it.
     * Note this is not the only way to reach the handler: a shut down pool rejects before it ever offers to the
     * queue, bounded or not, which is what {@code anUnboundedQueueDoesNotMakeALosingHandlerHarmless} measures and
     * why the refusal is on the handler rather than on the bound.
     */
    private ThreadPoolExecutor boundedPoolWith(RejectedExecutionHandler handler) {
        return new ThreadPoolExecutor(1, 1, 0L, MILLISECONDS, new LinkedBlockingQueue<>(1), handler);
    }

    /**
     * A processor whose worker pool is the given one, exactly as a user's subclass would supply it.
     */
    private TestParallelEoSStreamProcessor<String, String> processorBackedBy(ThreadPoolExecutor pool) {
        return new TestParallelEoSStreamProcessor<String, String>(testOptions) {
            @Override
            protected ThreadPoolExecutor setupWorkerPool(int poolSize) {
                return pool;
            }
        };
    }
}
