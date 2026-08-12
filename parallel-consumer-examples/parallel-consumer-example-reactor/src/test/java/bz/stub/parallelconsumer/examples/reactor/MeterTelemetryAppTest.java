package io.confluent.parallelconsumer.examples.reactor;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.confluent.csid.utils.KafkaTestUtils;
import io.confluent.csid.utils.LongPollingMockConsumer;
import io.confluent.parallelconsumer.examples.support.DemoRecords;
import io.confluent.parallelconsumer.examples.support.ExampleMockConsumers;
import io.confluent.parallelconsumer.examples.support.RunSummary;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static pl.tlinkowski.unij.api.UniLists.of;

/**
 * Drives {@link MeterTelemetryApp} against a mock consumer - no broker, no Testcontainers. The behaviour
 * on show is Parallel Consumer's concurrency over a user function plus Reactive Streams demand inside one
 * record's publisher, both of which a mock consumer exercises exactly as well as a real broker.
 * <p>
 * <b>The assertion this test exists for is the demand one.</b> {@code doOnRequest} only reports real
 * demand while it sits UPSTREAM of {@code limitRate}; move it downstream and it watches Parallel
 * Consumer's own unbounded subscription instead, the example still runs, still logs, and demonstrates
 * nothing. {@link #assertDemandWasBoundedByLimitRate()} captures the log and fails on exactly that drift.
 * <p>
 * <b>Concurrency is proved by a {@link CyclicBarrier}, never by a wall clock.</b> A {@code peak >= N}
 * assertion is an assertion about the scheduler's mood on a loaded two-core runner. Timings are printed
 * and never asserted.
 * <p>
 * <b>The barrier's placement is the one that does not deadlock.</b> Parallel Consumer forces this
 * engine's dispatch pool to a single thread, so a barrier in the user function body hangs on every
 * machine; {@code delayElement} continues on {@code Schedulers.parallel()}, sized to the core count,
 * where a barrier deadlocks on a two-core runner. It therefore goes inside the returned publisher on the
 * {@code boundedElastic} step - {@link MeterTelemetryApp#beginReading} - and
 * {@link #assertNothingThatCanParkRanOnTheParallelScheduler()} asserts it stayed there.
 * <p>
 * <b>The barrier's failure mode is specified, or a shortfall hangs instead of failing.</b> A bare
 * {@code await()} parks the workers forever. A bare {@code await(timeout)} permanently BREAKS the
 * barrier, so every later arrival throws {@link BrokenBarrierException} - which Parallel Consumer reads
 * as user-function failure and retries on the one second default delay forever, so the developer sees
 * "offsets never commit" instead of "only N-1 readings were concurrent". Every arrival here calls
 * {@code await(timeout, unit)} inside a try/catch that records the shortfall and lets the reading
 * complete normally; the test body asserts on the recorded value.
 */
@Slf4j
class MeterTelemetryAppTest {

    private static final int METERS = 4;

    private static final int READINGS = 8;

    /**
     * One partition, so the concurrency the barrier proves cannot be explained by partition count.
     */
    private static final int PARTITIONS = 1;

    /**
     * Strictly below {@link #METERS}: under KEY ordering the distinct meter count is the concurrency
     * ceiling, and a barrier sized exactly at the ceiling needs every meter to have work available at the
     * same instant.
     */
    private static final int CONCURRENT_COHORT = 3;

    /**
     * Reactor's {@code Schedulers.parallel()} thread names. Nothing that can park a thread may run here:
     * the pool is sized to {@link Runtime#availableProcessors()}, so a barrier on it deadlocks on a
     * two-core runner.
     */
    private static final String PARALLEL_SCHEDULER_PREFIX = "parallel-";

    /**
     * The logger the app and Reactor's own {@code log()} trace both write under.
     */
    private static final String APP_LOGGER = "io.confluent.parallelconsumer.examples.reactor";

    /**
     * Generous upper bound, not a timing assumption: on a healthy run the barrier trips in milliseconds.
     */
    private static final Duration BARRIER_TIMEOUT = Duration.ofSeconds(30);

    private static final Duration RUN_TIMEOUT = Duration.ofSeconds(120);

    private static final Duration POLL_INTERVAL = Duration.ofMillis(100);

    private static final LocalDate FIRST_SETTLEMENT_DATE = LocalDate.of(2026, 1, 1);

    private MeterTelemetryAppUnderTest app;

    private Logger appLogger;

    private ListAppender<ILoggingEvent> capturedLog;

    @BeforeEach
    void captureTheDemandTrace() {
        capturedLog = new ListAppender<>();
        capturedLog.start();
        appLogger = (Logger) LoggerFactory.getLogger(APP_LOGGER);
        appLogger.addAppender(capturedLog);
    }

    @AfterEach
    void releaseTheDemandTrace() {
        if (appLogger != null) {
            appLogger.detachAppender(capturedLog);
        }
        if (capturedLog != null) {
            capturedLog.stop();
        }
    }

    @AfterEach
    void closeApp() {
        if (app != null) {
            app.closeOnce();
        }
    }

    @Test
    void meterReadingsStreamConcurrentlyWhileLimitRateBoundsDemandWithinEachReading() {
        app = MeterTelemetryAppUnderTest.create();
        List<ConsumerRecord<String, String>> readings = generateReadings(app.getInputTopic());

        app.run();
        for (ConsumerRecord<String, String> reading : readings) {
            app.mockConsumer.addRecord(reading);
        }

        // NON-VACUOUS precondition. This is false before the app starts consuming, so awaiting it proves
        // work actually began. Awaiting only the terminal condition would pass for the wrong reason on a
        // fast machine if the app never ran at all.
        Awaitility.await("the first meter reading opens its stream")
                .atMost(RUN_TIMEOUT).pollInterval(POLL_INTERVAL)
                .until(() -> app.getObserver().hasObservations());

        // The concurrency proof. The barrier cannot trip unless CONCURRENT_COHORT readings were being
        // streamed simultaneously, on one partition. A shortfall is recorded rather than thrown, so this
        // await ends either way instead of hanging.
        Awaitility.await("three meter readings are in flight at the same instant")
                .atMost(RUN_TIMEOUT).pollInterval(POLL_INTERVAL)
                .until(() -> app.cohortCleared.get() >= CONCURRENT_COHORT || app.cohortShortfalls.get() > 0);

        assertThat(app.cohortShortfalls.get())
                .as("readings that reached the barrier but never found %d peers there - fewer than %d "
                        + "readings were ever concurrent", CONCURRENT_COHORT, CONCURRENT_COHORT)
                .isZero();
        assertThat(app.cohortCleared.get())
                .as("all %d readings of the cohort cleared the barrier together", CONCURRENT_COHORT)
                .isEqualTo(CONCURRENT_COHORT);

        Awaitility.await("every meter reading's offset is committed")
                .atMost(RUN_TIMEOUT).pollInterval(POLL_INTERVAL)
                .untilAsserted(() -> KafkaTestUtils.assertLastCommitIs(app.mockConsumer, READINGS));

        assertEverySettlementPeriodWasIngested();
        assertDemandWasBoundedByLimitRate();
        assertOrderingHeldWithinEachMeter();
        assertNothingThatCanParkRanOnTheParallelScheduler();
        assertConcurrencyStayedWithinTheOrderingCeiling();

        app.closeOnce();
        assertRunSummaryExplainsTheRun();
    }

    /**
     * Without this the demand assertions could be satisfied by a run that requested periods and never
     * delivered them.
     */
    private void assertEverySettlementPeriodWasIngested() {
        assertThat(app.getSamplesIngested())
                .as("every settlement period of every reading is written - %d readings x %d periods",
                        READINGS, MeterTelemetryApp.SETTLEMENT_PERIODS_PER_DAY)
                .isEqualTo((long) READINGS * MeterTelemetryApp.SETTLEMENT_PERIODS_PER_DAY);
    }

    /**
     * <b>The assertion this test exists for.</b> {@code doOnRequest} upstream of {@code limitRate} sees
     * the throttle's own demand - {@value MeterTelemetryApp#DEMAND_WINDOW} and then its replenishments.
     * Downstream it sees Parallel Consumer's unbounded subscription instead, or {@code concatMap}'s
     * prefetch, and the example teaches the reader the wrong mechanism while still passing every other
     * assertion here.
     */
    private void assertDemandWasBoundedByLimitRate() {
        List<String> messages = capturedMessages();
        String boundedSignal = "demand signal: request(" + MeterTelemetryApp.DEMAND_WINDOW + ")";
        String unboundedSignal = "demand signal: request(" + MeterTelemetryApp.UNBOUNDED + ")";

        assertThat(messages)
                .as("the demand doOnRequest observed must be limitRate's, i.e. '%s'", boundedSignal)
                .anyMatch(message -> message.contains(boundedSignal));
        assertThat(messages)
                .as("doOnRequest must sit UPSTREAM of limitRate - downstream it reports '%s' forever, "
                        + "which is Parallel Consumer's subscription rather than the throttle", unboundedSignal)
                .noneMatch(message -> message.contains(unboundedSignal));

        // Reactor's own trace of the same subscription, from log() placed beside doOnRequest
        assertThat(messages)
                .as("Reactor's log() trace shows the subscription and the bounded request it received")
                .anyMatch(message -> message.contains("onSubscribe"))
                .anyMatch(message -> message.contains("request(" + MeterTelemetryApp.DEMAND_WINDOW + ")"));

        // Structural counterpart to the log assertions: every signal bounded, none unbounded
        assertThat(app.getDemandSignals())
                .as("every demand signal doOnRequest saw was bounded by limitRate(%d)",
                        MeterTelemetryApp.DEMAND_WINDOW)
                .isNotEmpty()
                .allSatisfy(demand -> assertThat(demand)
                        .isPositive()
                        .isLessThanOrEqualTo((long) MeterTelemetryApp.DEMAND_WINDOW));
    }

    /**
     * The ordering guarantee, and the assertion that fails if someone switches the example to
     * {@code UNORDERED}: Parallel Consumer never has two readings of the same meter in flight, so a
     * meter's settlement days cannot be applied out of order.
     */
    private void assertOrderingHeldWithinEachMeter() {
        assertThat(app.peakInFlightByMeter)
                .as("every meter must have been read at least once")
                .hasSize(METERS);
        assertThat(app.peakInFlightByMeter)
                .as("KEY ordering means at most ONE reading per meter is ever in flight - this is the "
                        + "assertion that fails if the example is changed to UNORDERED")
                .allSatisfy((meterId, peak) ->
                        assertThat(peak.get()).as("peak in flight for meter %s", meterId).isEqualTo(1));

        assertThat(app.settlementDatesByMeter)
                .as("a meter's settlement days are applied in offset order, oldest first")
                .allSatisfy((meterId, dates) -> assertThat(dates)
                        .as("settlement days seen for meter %s", meterId)
                        .isNotEmpty()
                        .isSorted());
    }

    /**
     * KTD2 and KTD3 in one assertion pair.
     * <p>
     * The first half is the deadlock: {@code Schedulers.parallel()} is sized to the core count, so a
     * barrier awaited there hangs on a two-core runner. The barrier belongs on {@code boundedElastic}.
     * <p>
     * The second half is the {@code Thread.sleep} check, and it is a real detector rather than a promise:
     * the simulated latency is {@link reactor.core.publisher.Mono#delayElement(Duration)}, which is
     * timer-driven and hands the continuation to {@code Schedulers.parallel()}. A {@code Thread.sleep}
     * would keep the continuation on whatever thread called it, so these names would never be
     * {@code parallel-*} - and a sleep would additionally be occupying a thread from that very pool,
     * which is what BlockHound exists to catch.
     */
    private void assertNothingThatCanParkRanOnTheParallelScheduler() {
        assertThat(app.barrierThreads)
                .as("the barrier must be awaited somewhere it cannot deadlock")
                .isNotEmpty()
                .allSatisfy(threadName -> assertThat(threadName)
                        .as("a barrier awaited on Reactor's parallel scheduler deadlocks on a two-core "
                                + "runner - it belongs on boundedElastic, inside the returned publisher")
                        .doesNotStartWith(PARALLEL_SCHEDULER_PREFIX));

        assertThat(app.getIngestContinuationThreads())
                .as("the simulated latency must be Mono.delayElement, whose continuation runs on "
                        + "Schedulers.parallel() - a Thread.sleep would keep it on the calling thread and "
                        + "would never produce a '%s' name", PARALLEL_SCHEDULER_PREFIX)
                .isNotEmpty()
                .allSatisfy(threadName -> assertThat(threadName).startsWith(PARALLEL_SCHEDULER_PREFIX));
    }

    /**
     * Structural, not timing: the barrier already forced the lower bound, and KEY ordering over
     * {@value #METERS} meters makes the upper bound arithmetic rather than luck.
     */
    private void assertConcurrencyStayedWithinTheOrderingCeiling() {
        assertThat(app.getObserver().getPeakInFlight())
                .as("the barrier forced at least %d readings to be in flight together", CONCURRENT_COHORT)
                .isGreaterThanOrEqualTo(CONCURRENT_COHORT)
                .as("KEY ordering caps concurrency at the distinct meter count, whatever max concurrency says")
                .isLessThanOrEqualTo(METERS);
    }

    private void assertRunSummaryExplainsTheRun() {
        Optional<RunSummary> emitted = app.getEmittedSummary();
        assertThat(emitted).as("closing the app emits its run summary").isPresent();

        RunSummary summary = emitted.get();
        assertThat(summary.getRecordCount()).isEqualTo(READINGS);
        assertThat(summary.getPartitionCount())
                .as("the summary names the partition count, because 'peak in-flight' means nothing without it")
                .isEqualTo(PARTITIONS);
        assertThat(summary.getDistinctKeys()).isEqualTo(METERS);
        assertThat(summary.getOrderingCeiling())
                .as("under KEY ordering the ceiling is the distinct meter count")
                .hasValue(METERS);
        assertThat(summary.getSimulatedLatency())
                .as("one record is a whole settlement day, so its simulated cost is %d periods x %s",
                        MeterTelemetryApp.SETTLEMENT_PERIODS_PER_DAY, MeterTelemetryApp.SAMPLE_INGEST_LATENCY)
                .isEqualTo(MeterTelemetryApp.SAMPLE_INGEST_LATENCY
                        .multipliedBy(MeterTelemetryApp.SETTLEMENT_PERIODS_PER_DAY));

        assertThat(summary.render()).contains(
                "input partitions",
                "ordering ceiling",
                METERS + " concurrent records (distinct keys)");

        // The closing line the summary itself cannot carry: a reader who takes "peak in-flight" for the
        // whole story has learnt the wrong lesson, because it is only the OUTER of two bounds
        assertThat(capturedMessages())
                .as("the run states both bounds plainly - maxConcurrency across readings, limitRate within one")
                .anyMatch(message -> message.contains("OUTER: maxConcurrency")
                        && message.contains("INNER: limitRate"));

        // PRINTED, NEVER ASSERTED: on a loaded two-core runner a concurrent run can be slower than its
        // own implied serial baseline, so any wall-clock assertion here would be a flake generator.
        log.info("Observed processing window {} for {} readings across {} meter(s), peak in flight {}, "
                        + "demand signals {}",
                app.getObserver().getObservedWindow(), summary.getRecordCount(), summary.getDistinctKeys(),
                app.getObserver().getPeakInFlight(), app.getDemandSignals());
    }

    private List<String> capturedMessages() {
        List<String> messages = new ArrayList<>();
        for (ILoggingEvent event : new ArrayList<>(capturedLog.list)) {
            messages.add(event.getFormattedMessage());
        }
        assertThat(messages).as("the app's log was captured at all").isNotEmpty();
        return messages;
    }

    /**
     * Eight settlement days over four meters on one partition. The day advances with the record index, so
     * within a meter the days arrive oldest first and the ordering assertion has something to check.
     */
    private static List<ConsumerRecord<String, String>> generateReadings(String topic) {
        List<String> meters = DemoRecords.keys("MPAN", METERS);
        return DemoRecords.generate(topic, READINGS, PARTITIONS, meters, index ->
                MeterTelemetryApp.readingValue(FIRST_SETTLEMENT_DATE.plusDays(index)));
    }

    /**
     * The app as the test drives it: the mock consumer goes in through the constructor (the seam that
     * survives a test moving package), and the barrier goes in {@code beginReading} - which runs inside
     * the returned publisher, on {@code boundedElastic}, which is the only placement that does not
     * deadlock.
     */
    static class MeterTelemetryAppUnderTest extends MeterTelemetryApp {

        final LongPollingMockConsumer<String, String> mockConsumer;

        final CyclicBarrier cohort = new CyclicBarrier(CONCURRENT_COHORT);

        /**
         * Only the first {@link #CONCURRENT_COHORT} arrivals wait. Later readings must not wait on a
         * barrier that nobody else is going to reach.
         */
        final AtomicInteger cohortArrivals = new AtomicInteger();

        final AtomicInteger cohortCleared = new AtomicInteger();

        final AtomicInteger cohortShortfalls = new AtomicInteger();

        /**
         * Which threads actually awaited the barrier - the evidence that it never ran on
         * {@code Schedulers.parallel()}.
         */
        final Set<String> barrierThreads = ConcurrentHashMap.newKeySet();

        final Map<String, AtomicInteger> inFlightByMeter = new ConcurrentHashMap<>();

        final Map<String, AtomicInteger> peakInFlightByMeter = new ConcurrentHashMap<>();

        final Map<String, List<String>> settlementDatesByMeter = new ConcurrentHashMap<>();

        /**
         * Reading ids already closed out, so {@link #finishReading(MeterReading)} stays idempotent.
         */
        final Set<String> finishedReadings = ConcurrentHashMap.newKeySet();

        private boolean closed;

        private MeterTelemetryAppUnderTest(LongPollingMockConsumer<String, String> mockConsumer) {
            super(mockConsumer);
            this.mockConsumer = mockConsumer;
        }

        static MeterTelemetryAppUnderTest create() {
            return new MeterTelemetryAppUnderTest(ExampleMockConsumers.spiedLongPollingMockConsumer());
        }

        @Override
        protected void postSetup() {
            mockConsumer.subscribeWithRebalanceAndAssignment(of(getInputTopic()), PARTITIONS);
        }

        @Override
        protected void beginReading(MeterReading reading) {
            super.beginReading(reading);

            String meterId = reading.getMeterId();
            int nowInFlight = inFlightByMeter
                    .computeIfAbsent(meterId, key -> new AtomicInteger()).incrementAndGet();
            peakInFlightByMeter
                    .computeIfAbsent(meterId, key -> new AtomicInteger())
                    .accumulateAndGet(nowInFlight, Math::max);
            settlementDatesByMeter
                    .computeIfAbsent(meterId, key -> new CopyOnWriteArrayList<>())
                    .add(reading.getSettlementDate());

            joinCohort();
        }

        /**
         * Idempotent for the same reason the superclass is: a second terminal signal must not decrement
         * the per-meter count twice, or the ordering assertion would go negative and pass vacuously.
         */
        @Override
        protected void finishReading(MeterReading reading) {
            try {
                super.finishReading(reading);
            } finally {
                if (finishedReadings.add(reading.getId())) {
                    AtomicInteger inFlight = inFlightByMeter.get(reading.getMeterId());
                    if (inFlight != null) {
                        inFlight.decrementAndGet();
                    }
                }
            }
        }

        private void joinCohort() {
            if (cohortArrivals.getAndIncrement() >= CONCURRENT_COHORT) {
                return;
            }
            barrierThreads.add(Thread.currentThread().getName());
            try {
                cohort.await(BARRIER_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                cohortCleared.incrementAndGet();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                cohortShortfalls.incrementAndGet();
            } catch (TimeoutException | BrokenBarrierException e) {
                // Recorded, NOT rethrown. Rethrowing would look to Parallel Consumer like the user
                // function failing, and it would retry this reading on the one second default delay
                // forever - the test would then report "offsets never commit" instead of "the cohort
                // never formed".
                cohortShortfalls.incrementAndGet();
                log.warn("Only {} of {} readings reached the concurrency barrier within {}",
                        cohortCleared.get(), CONCURRENT_COHORT, BARRIER_TIMEOUT, e);
            }
        }

        /**
         * The test closes the app to assert on the summary it emitted; {@code @AfterEach} closes it again
         * if an assertion threw first.
         */
        void closeOnce() {
            if (!closed) {
                closed = true;
                close();
            }
        }
    }
}
