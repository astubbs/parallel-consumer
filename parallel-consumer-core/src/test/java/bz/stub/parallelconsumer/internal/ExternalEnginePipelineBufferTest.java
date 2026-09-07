package bz.stub.parallelconsumer.internal;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.PollContextInternal;
import bz.stub.parallelconsumer.state.WorkContainer;
import bz.stub.parallelconsumer.state.WorkManager;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import pl.tlinkowski.unij.api.UniLists;
import pl.tlinkowski.unij.api.UniMaps;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Pins the two halves of {@link ExternalEngine}'s pipeline buffer, which are only safe together: the request formula
 * it deliberately inherits rather than overrides - an external engine asks the work manager for the PIPELINED target,
 * not merely the shortfall against records in flight - and the hard dispatch ceiling that keeps that surplus BEHIND
 * {@code maxConcurrency} instead of through it. Why each is the right shape, and the regression each direction
 * caused, is on {@link ExternalEngine} itself - not restated here, so the two cannot drift.
 */
@Slf4j
class ExternalEnginePipelineBufferTest {

    static final String TOPIC = "topic";
    static final int PARTITION = 0;

    /**
     * The smallest possible concrete engine - the abstract surface, and nothing else.
     * <p>
     * {@code async} models what every real external engine does: the user function returns as soon as the async work
     * is handed to the engine, so nothing retires itself and retirement is driven by whatever signals completion.
     * Here that is the test, through {@link #retire}, which is the same {@code addToMailbox} call the Reactor,
     * Mutiny and Vert.x completion handlers make.
     */
    static class BareExternalEngine extends ExternalEngine<String, String> {

        private final boolean async;

        BareExternalEngine(PCModuleTestEnv module, boolean async) {
            super(module.options());
            this.async = async;
        }

        @Override
        protected boolean isAsyncFutureWork(List<?> resultsFromUserFunction) {
            return async;
        }

        void setWm(WorkManager<String, String> wm) {
            super.wm = wm;
        }

        void retire(WorkContainer<String, String> wc) {
            addToMailbox(new PollContextInternal<>(UniLists.of(wc)), wc);
        }
    }

    private static BareExternalEngine engineWithMaxConcurrency(int maxConcurrency) {
        return new BareExternalEngine(new PCModuleTestEnv(ParallelConsumerOptions.<String, String>builder()
                .maxConcurrency(maxConcurrency)
                .build()), false);
    }

    @Test
    void anExternalEngineRequestsThePipelineBufferNotJustTheShortfall() {
        var engine = engineWithMaxConcurrency(100);

        int inFlightTarget = engine.getOptions().getTargetAmountOfRecordsInFlight();
        int requested = engine.calculateQuantityToRequest();

        assertWithMessage("with nothing in flight, the request must be the loaded queue target - the in-flight "
                + "target times the load factor - so a buffer of ready work sits behind the ceiling")
                .that(requested).isEqualTo(engine.getQueueTargetLoaded());
        assertWithMessage("the buffer must actually be a buffer: strictly more than the in-flight target, which "
                + "is all the 0.4.0.0..0.6.0.0 override ever requested")
                .that(requested).isGreaterThan(inFlightTarget);
    }

    @Test
    void thePressureSystemStaysDisabledSoTheBufferIsBoundedAtTheInitialFactor() {
        var engine = engineWithMaxConcurrency(100);

        int before = engine.getQueueTargetLoaded();
        // core steps the factor here when the pool queue runs low; external engines keep it a no-op, so the
        // request target must not grow however often pressure is checked
        for (int i = 0; i < 50; i++) {
            engine.checkPipelinePressure();
        }
        assertThat(engine.getQueueTargetLoaded()).isEqualTo(before);
    }

    /**
     * The half the request formula cannot supply on its own.
     * <p>
     * An external engine hands every record it is given straight to the user's async engine and returns, so without
     * this gate the pipelined request IS the concurrency and {@code maxConcurrency} is breached by the load factor -
     * measured at 1.52-1.95x on Reactor and 1.80-1.93x on Mutiny, three runs each, before the gate existed. The
     * buffer must still be real,
     * so this asserts both directions at once: strictly more records taken and counted out for processing than the
     * ceiling, and never more than the ceiling actually dispatched.
     */
    @Test
    void dispatchIsCappedAtTheCeilingWhileTheBufferWaitsBehindIt() throws InterruptedException {
        int ceiling = 4;
        int taken = 3 * ceiling;

        var module = new PCModuleTestEnv(ParallelConsumerOptions.<String, String>builder()
                .maxConcurrency(ceiling)
                .build());
        var wm = module.workManager();
        var tp = new TopicPartition(TOPIC, PARTITION);
        wm.onPartitionsAssigned(UniLists.of(tp));

        var engine = new BareExternalEngine(module, true);
        engine.setWm(wm);
        engine.setState(State.RUNNING);

        var entered = new AtomicInteger();
        // one latch per interesting count, so every wait is bounded and no test needs a sleep to be meaningful
        var reachedCeiling = new CountDownLatch(ceiling);
        var wentOverCeiling = new CountDownLatch(ceiling + 1);
        var reachedCeilingAgain = new CountDownLatch(ceiling + 2);
        Function<PollContextInternal<String, String>, List<Object>> dispatchProbe = context -> {
            entered.incrementAndGet();
            reachedCeiling.countDown();
            wentOverCeiling.countDown();
            reachedCeilingAgain.countDown();
            // async work: the engine keeps it in flight until something signals completion
            return UniLists.of(new Object());
        };
        Consumer<Object> callback = ignore -> {
        };

        try {
            var work = takeWork(wm, taken);
            assertWithMessage("fixture: the buffered take must be bigger than the ceiling, or this proves nothing")
                    .that(taken).isGreaterThan(ceiling);

            engine.submitWorkToPool(dispatchProbe, callback, work);

            assertWithMessage("the engine must dispatch up to its ceiling without waiting for a control loop pass")
                    .that(reachedCeiling.await(10, SECONDS)).isTrue();
            assertWithMessage("dispatch must stop AT the ceiling - the buffered surplus waits, it is not extra "
                    + "concurrency handed to the user's engine")
                    .that(wentOverCeiling.await(500, MILLISECONDS)).isFalse();
            assertWithMessage("and the surplus really is buffered: taken from the work manager and counted out for "
                    + "processing, which is what the pipelined request bought")
                    .that(wm.getNumberRecordsOutForProcessing()).isEqualTo(taken);

            // a completion is what frees a slot - not the next control loop pass, which is where the throughput is
            engine.retire(work.get(0));
            engine.retire(work.get(1));

            assertWithMessage("a retired record must hand its slot straight to the next buffered one")
                    .that(reachedCeilingAgain.await(10, SECONDS)).isTrue();
            assertWithMessage("two slots freed means exactly two more dispatched, never the whole backlog")
                    .that(entered.get()).isEqualTo(ceiling + 2);
        } finally {
            engine.setState(State.CLOSED);
            engine.close();
            engine.workerThreadPool.get().shutdownNow();
        }
    }

    /**
     * Takes work exactly as the control loop does, so every container really is in flight and really is counted.
     * <p>
     * A key per record: the default ordering is KEY, so sharing one would serialise them and only the first would
     * ever be selectable.
     */
    private List<WorkContainer<String, String>> takeWork(WorkManager<String, String> wm, int count) {
        var records = new ArrayList<ConsumerRecord<String, String>>();
        for (int i = 0; i < count; i++) {
            records.add(new ConsumerRecord<>(TOPIC, PARTITION, i, "key-" + i, "value"));
        }
        var tp = new TopicPartition(TOPIC, PARTITION);
        wm.registerWork(new EpochAndRecordsMap<>(new ConsumerRecords<>(UniMaps.of(tp, records)), wm.getPm()));

        var taken = wm.getWorkIfAvailable(count);
        assertWithMessage("fixture: all %s records must be taken as work", count)
                .that(taken).hasSize(count);
        return taken;
    }
}
