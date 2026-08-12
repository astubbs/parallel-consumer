package bz.stub.parallelconsumer.dashboard.snapshot;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import bz.stub.parallelconsumer.internal.AbstractParallelEoSStreamProcessor;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * Proves the two properties the publisher exists for: a dashboard fault cannot reach the control loop, and a reader
 * on any other thread always sees a whole, consistent pair of snapshots.
 */
class SnapshotPublisherTest {

    /**
     * A sampler that can be told to fail, so the publisher's fault isolation can be exercised without needing a
     * broken registry.
     */
    private static class ControllableSampler extends StateSampler {

        private final AtomicBoolean shouldThrow = new AtomicBoolean(false);

        /**
         * Unique per instance, so a test inspecting the shared class logger can tell its own failures from those of
         * another test running concurrently - surefire runs this module's tests in parallel.
         */
        private final String faultMessage;

        ControllableSampler(String faultMessage) {
            super(new MeterSource(PcMeterFixture.fullyPopulated().getRegistry()), new DirectStateSource(null));
            this.faultMessage = faultMessage;
        }

        @Override
        public PcSnapshot sample() {
            if (shouldThrow.get()) {
                throw new IllegalStateException(faultMessage);
            }
            return super.sample();
        }
    }

    private static SnapshotPublisher publisherOverFullyPopulatedRegistry() {
        return new SnapshotPublisher(new StateSampler(
                new MeterSource(PcMeterFixture.fullyPopulated().getRegistry()), new DirectStateSource(null)));
    }

    @Test
    void beforeTheFirstSampleBothSnapshotsAreAbsent() {
        SnapshotPublisher publisher = publisherOverFullyPopulatedRegistry();

        assertThat(publisher.getCurrent()).isNull();
        assertThat(publisher.getPrevious()).isNull();
        assertThat(publisher.getSampleFailureCount()).isZero();
    }

    @Test
    void twoSuccessivePublishesMakeCurrentAndPreviousReadable() {
        SnapshotPublisher publisher = publisherOverFullyPopulatedRegistry();

        publisher.sampleOnce();
        PcSnapshot first = publisher.getCurrent();
        assertThat(first).isNotNull();
        assertThat(publisher.getPrevious()).isNull();

        publisher.sampleOnce();
        PcSnapshot second = publisher.getCurrent();

        assertThat(second).isNotSameAs(first);
        // previous is exactly the prior current, not a re-sample
        assertThat(publisher.getPrevious()).isSameAs(first);
        assertThat(second.getSampleSequence()).isEqualTo(first.getSampleSequence() + 1);

        publisher.sampleOnce();
        assertThat(publisher.getPrevious()).isSameAs(second);
    }

    /**
     * The pair accessor is what a reader needing both ends must use, and it must always be internally consistent -
     * previous exactly one sample behind current, with a measured elapsed time between them.
     */
    @Test
    void thePairIsAlwaysOneSampleApartAndCarriesMeasuredElapsedTime() throws Exception {
        SnapshotPublisher publisher = publisherOverFullyPopulatedRegistry();

        assertThat(publisher.getSnapshots().hasDelta()).isFalse();
        assertThat(publisher.getSnapshots().elapsedMillis()).isNull();

        publisher.sampleOnce();
        assertThat(publisher.getSnapshots().hasDelta()).isFalse();

        Thread.sleep(5);
        publisher.sampleOnce();

        SnapshotPublisher.Snapshots pair = publisher.getSnapshots();
        assertThat(pair.hasDelta()).isTrue();
        assertThat(pair.getCurrent().getSampleSequence() - pair.getPrevious().getSampleSequence()).isEqualTo(1);
        assertThat(pair.elapsedMillis()).isNotNull().isGreaterThanOrEqualTo(1L);
    }

    /**
     * A dashboard bug must not be able to stop a consumer. The callback swallows the fault, the last good snapshot
     * stays on the page, and the failure is counted.
     */
    @Test
    void aThrowingSamplerLeavesThePreviousSnapshotIntactAndDoesNotPropagate() {
        ControllableSampler sampler = new ControllableSampler("fault-isolation-test");
        SnapshotPublisher publisher = new SnapshotPublisher(sampler);

        publisher.sampleOnce();
        publisher.sampleOnce();
        PcSnapshot goodCurrent = publisher.getCurrent();
        PcSnapshot goodPrevious = publisher.getPrevious();
        assertThat(goodCurrent).isNotNull();
        assertThat(goodPrevious).isNotNull();

        sampler.shouldThrow.set(true);
        for (int i = 0; i < 50; i++) {
            // must not throw - this is the control loop callback
            publisher.sampleOnce();
        }

        assertThat(publisher.getCurrent()).isSameAs(goodCurrent);
        assertThat(publisher.getPrevious()).isSameAs(goodPrevious);
        assertThat(publisher.getSampleFailureCount()).isEqualTo(50);

        // and it recovers once the fault clears
        sampler.shouldThrow.set(false);
        publisher.sampleOnce();
        assertThat(publisher.getCurrent()).isNotSameAs(goodCurrent);
        assertThat(publisher.getPrevious()).isSameAs(goodCurrent);
    }

    /**
     * The control loop runs continuously, so an unsuppressed WARN per failed sample is itself an outage. One line,
     * then silence until the fault changes or the interval elapses.
     */
    @Test
    void repeatedIdenticalFailuresAreLoggedOnceNotOncePerLoopIteration() {
        Logger publisherLogger = (Logger) LoggerFactory.getLogger(SnapshotPublisher.class);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        publisherLogger.addAppender(appender);
        try {
            String faultMessage = "log-suppression-test-fault";
            ControllableSampler sampler = new ControllableSampler(faultMessage);
            SnapshotPublisher publisher = new SnapshotPublisher(sampler);
            sampler.shouldThrow.set(true);

            for (int i = 0; i < 500; i++) {
                publisher.sampleOnce();
            }

            assertThat(publisher.getSampleFailureCount()).isEqualTo(500);
            // surefire runs this module's tests in parallel and the appender is on the shared class logger, so
            // count only the events carrying THIS test's fault
            List<ILoggingEvent> mine = new ArrayList<>();
            for (ILoggingEvent event : new ArrayList<>(appender.list)) {
                if (event.getThrowableProxy() != null
                        && faultMessage.equals(event.getThrowableProxy().getMessage())) {
                    mine.add(event);
                }
            }
            assertThat(mine)
                    .as("500 identical failures must produce exactly one WARN, not 500")
                    .hasSize(1);
            assertThat(mine.get(0).getLevel()).isEqualTo(Level.WARN);
            assertThat(mine.get(0).getFormattedMessage()).contains("Dashboard state sampling failed");
        } finally {
            publisherLogger.detachAppender(appender);
        }
    }

    /**
     * An {@link Error} is still a dashboard fault as far as the control loop is concerned - catching only
     * {@link Exception} would let one through and kill the consumer.
     */
    @Test
    void anErrorFromTheSamplerIsAlsoIsolated() {
        SnapshotPublisher publisher = new SnapshotPublisher(new StateSampler(
                new MeterSource(new SimpleMeterRegistry()), new DirectStateSource(null)) {
            @Override
            public PcSnapshot sample() {
                throw new StackOverflowError("deliberate");
            }
        });

        publisher.sampleOnce();

        assertThat(publisher.getCurrent()).isNull();
        assertThat(publisher.getSampleFailureCount()).isEqualTo(1);
    }

    /**
     * The property the event stream is built on: a subscriber is told at publication time, not on a timer of its
     * own, and by the time it is told the snapshot it was told about is already readable.
     */
    @Test
    void aListenerIsToldOnPublicationAndFindsTheSampleItWasToldAbout() {
        SnapshotPublisher publisher = publisherOverFullyPopulatedRegistry();
        List<Long> sequencesVisibleWhenTold = new ArrayList<>();
        publisher.addListener(() -> sequencesVisibleWhenTold.add(publisher.getCurrent().getSampleSequence()));

        publisher.sampleOnce();
        publisher.sampleOnce();
        publisher.sampleOnce();

        assertThat(sequencesVisibleWhenTold)
                .as("told once per publication, and never before the publication it is about")
                .containsExactly(1L, 2L, 3L);
    }

    @Test
    void aListenerThatThrowsIsIsolatedFromTheControlLoopAndFromTheOtherListeners() {
        SnapshotPublisher publisher = publisherOverFullyPopulatedRegistry();
        AtomicBoolean survivorRan = new AtomicBoolean();
        publisher.addListener(() -> {
            throw new IllegalStateException("a subscriber's problem is not the consumer's problem");
        });
        publisher.addListener(() -> survivorRan.set(true));

        publisher.sampleOnce();

        assertThat(survivorRan).as("one broken listener must not cost the others their notification").isTrue();
        assertThat(publisher.getCurrent()).as("the snapshot was published regardless").isNotNull();
        assertThat(publisher.getListenerFailureCount()).isEqualTo(1);
        assertThat(publisher.getSampleFailureCount())
                .as("a broken subscriber is not a sampling failure - the reading itself was fine")
                .isZero();
    }

    /**
     * A publisher outlives the dashboard that subscribed to it, so a subscriber that shuts down without
     * deregistering is not merely untidy: it keeps being called on every control loop iteration for the rest of the
     * consumer's life.
     */
    @Test
    void aRemovedListenerIsNotCalledAgain() {
        SnapshotPublisher publisher = publisherOverFullyPopulatedRegistry();
        AtomicBoolean called = new AtomicBoolean();
        SnapshotPublisher.SnapshotListener listener = () -> called.set(true);

        publisher.addListener(listener);
        assertThat(publisher.getListenerCount()).isEqualTo(1);
        publisher.removeListener(listener);
        assertThat(publisher.getListenerCount()).isZero();
        publisher.sampleOnce();

        assertThat(called).isFalse();
    }

    @Test
    void registeringWiresSamplingIntoTheControlLoopEndCallback() {
        AbstractParallelEoSStreamProcessor<?, ?> pc = mock(AbstractParallelEoSStreamProcessor.class);
        SnapshotPublisher publisher = publisherOverFullyPopulatedRegistry();

        publisher.registerWith(pc);

        ArgumentCaptor<Runnable> captor = ArgumentCaptor.forClass(Runnable.class);
        verify(pc).addLoopEndCallBack(captor.capture());

        assertThat(publisher.getCurrent()).isNull();
        // running the captured callback is what the control loop does
        captor.getValue().run();
        assertThat(publisher.getCurrent()).isNotNull();
    }

    @Test
    void createAndRegisterBuildsASamplerAndRegistersItInOneStep() {
        AbstractParallelEoSStreamProcessor<?, ?> pc = mock(AbstractParallelEoSStreamProcessor.class);

        SnapshotPublisher publisher =
                SnapshotPublisher.createAndRegister(pc, PcMeterFixture.fullyPopulated().getRegistry());

        ArgumentCaptor<Runnable> captor = ArgumentCaptor.forClass(Runnable.class);
        verify(pc).addLoopEndCallBack(captor.capture());
        captor.getValue().run();

        assertThat(publisher.getCurrent()).isNotNull();
        assertThat(publisher.getCurrent().getPartitions()).hasSize(3);
    }

    /**
     * The callback cannot be removed - core has no {@code removeLoopEndCallBack} - so the only thing that can stop a
     * closed dashboard from sampling forever on the user's control loop is this flag.
     * <p>
     * Asserted by running the CAPTURED callback, which is exactly what the control loop does with it. Without the
     * guard, every iteration after a close would still walk the whole meter registry and allocate a snapshot, and
     * every start/stop cycle would add another sampler to the loop.
     */
    @Test
    void stoppingSamplingMakesTheControlLoopCallbackFree() {
        AbstractParallelEoSStreamProcessor<?, ?> pc = mock(AbstractParallelEoSStreamProcessor.class);
        SnapshotPublisher publisher = publisherOverFullyPopulatedRegistry();
        publisher.registerWith(pc);
        ArgumentCaptor<Runnable> captor = ArgumentCaptor.forClass(Runnable.class);
        verify(pc).addLoopEndCallBack(captor.capture());
        Runnable controlLoopCallback = captor.getValue();

        controlLoopCallback.run();
        controlLoopCallback.run();
        long sequenceBeforeStopping = publisher.getCurrent().getSampleSequence();
        assertThat(sequenceBeforeStopping).isEqualTo(2L);

        publisher.stopSampling();
        for (int i = 0; i < 20; i++) {
            controlLoopCallback.run();
        }

        assertThat(publisher.isSamplingStopped()).isTrue();
        assertThat(publisher.getCurrent().getSampleSequence())
                .as("no further sample may be taken once sampling is stopped")
                .isEqualTo(sequenceBeforeStopping);
        assertThat(publisher.getCurrent())
                .as("the last good reading is left in place, so a reader sees it ageing rather than a null")
                .isNotNull();
    }

    @Test
    void stoppingSamplingIsIdempotentAndDoesNotUnstop() {
        SnapshotPublisher publisher = publisherOverFullyPopulatedRegistry();

        publisher.stopSampling();
        publisher.stopSampling();
        publisher.sampleOnce();

        assertThat(publisher.isSamplingStopped()).isTrue();
        assertThat(publisher.getCurrent()).isNull();
    }

    /**
     * Readers on other threads must never see a half-built snapshot, and must never see a current from one tick
     * beside a previous from another - every rate the page derives is computed from that pair, so an inconsistent
     * pair is a wrong chart rather than a crash.
     */
    @Test
    void concurrentReadersNeverObserveAPartiallyConstructedOrMismatchedSnapshot() throws Exception {
        SnapshotPublisher publisher = publisherOverFullyPopulatedRegistry();
        publisher.sampleOnce();

        int readerCount = 6;
        int publishCount = 5_000;
        ExecutorService pool = Executors.newFixedThreadPool(readerCount + 1);
        CountDownLatch start = new CountDownLatch(1);
        AtomicBoolean publishing = new AtomicBoolean(true);
        AtomicReference<Throwable> firstFailure = new AtomicReference<>();
        List<java.util.concurrent.Future<?>> futures = new ArrayList<>();

        try {
            for (int i = 0; i < readerCount; i++) {
                futures.add(pool.submit(() -> {
                    try {
                        start.await();
                        long reads = 0;
                        while (publishing.get() || reads < 1_000) {
                            // ONE volatile read for both - see SnapshotPublisher#getSnapshots
                            SnapshotPublisher.Snapshots pair = publisher.getSnapshots();
                            PcSnapshot current = pair.getCurrent();
                            PcSnapshot previous = pair.getPrevious();
                            reads++;
                            if (current == null) {
                                throw new AssertionError("current went back to null after the first publish");
                            }
                            // fully constructed: every field the sampler sets is set
                            if (current.getPartitions().size() != 3) {
                                throw new AssertionError("partially constructed snapshot: "
                                        + current.getPartitions().size() + " partitions");
                            }
                            if (current.getCaptureEpochMillis() <= 0 || current.getSampleSequence() <= 0) {
                                throw new AssertionError("partially constructed snapshot: no capture stamp");
                            }
                            for (PartitionSnapshot row : current.getPartitions()) {
                                if (!row.isOffsetOrderingConsistent()) {
                                    throw new AssertionError("torn partition row: " + row.getKey());
                                }
                                if (row.getHighestSeenOffset() == null || row.getProcessedRecords() == null) {
                                    throw new AssertionError("partially constructed row: " + row.getKey());
                                }
                            }
                            // the pair is published atomically, so previous is always exactly one sample behind
                            if (previous != null
                                    && previous.getSampleSequence() != current.getSampleSequence() - 1) {
                                throw new AssertionError("mismatched pair: current="
                                        + current.getSampleSequence() + " previous=" + previous.getSampleSequence());
                            }
                        }
                    } catch (Throwable t) {
                        firstFailure.compareAndSet(null, t);
                    }
                }));
            }

            futures.add(pool.submit(() -> {
                try {
                    start.await();
                    for (int i = 0; i < publishCount; i++) {
                        publisher.sampleOnce();
                    }
                } catch (Throwable t) {
                    firstFailure.compareAndSet(null, t);
                } finally {
                    publishing.set(false);
                }
            }));

            start.countDown();
            for (java.util.concurrent.Future<?> future : futures) {
                future.get(60, TimeUnit.SECONDS);
            }
        } finally {
            pool.shutdownNow();
        }

        if (firstFailure.get() != null) {
            throw new AssertionError("concurrent reader observed an inconsistent snapshot", firstFailure.get());
        }
        assertThat(publisher.getCurrent().getSampleSequence()).isEqualTo(publishCount + 1);
        assertThat(publisher.getSampleFailureCount()).isZero();
    }
}
