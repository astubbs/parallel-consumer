package io.confluent.parallelconsumer.examples.core;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import io.confluent.csid.utils.KafkaTestUtils;
import io.confluent.csid.utils.LongPollingMockConsumer;
import io.confluent.parallelconsumer.examples.support.DemoRecords;
import io.confluent.parallelconsumer.examples.support.ExampleMockConsumers;
import io.confluent.parallelconsumer.examples.support.RunSummary;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.Serdes;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static pl.tlinkowski.unij.api.UniLists.of;

/**
 * Drives {@link ParcelTrackingApp} against a mock consumer and a {@link MockProducer} - no broker, no
 * Testcontainers. The behaviour on show is Parallel Consumer's concurrency over a user function, which a
 * mock consumer exercises exactly as well as a real broker while staying in the fast unit lane.
 * <p>
 * <b>Concurrency is proved by a {@link CyclicBarrier}, never by a wall clock.</b> A {@code peak >= N}
 * assertion is an assertion about the scheduler's mood on a loaded two-core runner. A barrier that trips
 * is a fact: N scans were provably inside the user function at the same instant. Timings are printed and
 * never asserted - on a loaded runner a concurrent run can be slower than its own serial baseline.
 * <p>
 * <b>The barrier is sized {@value #CONCURRENT_COHORT}, one BELOW the {@value #CONSIGNMENTS} distinct
 * consignments, deliberately.</b> A barrier equal to the key count needs every key to have work available
 * at the same instant; but the injected geocode failure parks one consignment's shard on the retry
 * backoff while the others are already blocked and cannot release, starving the cohort and reintroducing
 * exactly the timing dependence the barrier exists to remove.
 * <p>
 * <b>The barrier's failure mode is specified, or a shortfall hangs instead of failing.</b> A bare
 * {@code await()} parks Parallel Consumer's workers forever. A bare {@code await(timeout)} permanently
 * BREAKS the barrier, so every later arrival throws {@link BrokenBarrierException} - which Parallel
 * Consumer reads as user-function failure and retries on the one second default delay forever, so the
 * developer sees "offsets never commit" instead of "only N-1 scans were concurrent". Every arrival here
 * calls {@code await(timeout, unit)} inside a try/catch that records the shortfall and lets the scan
 * complete normally; the test body asserts on the recorded value.
 */
@Slf4j
class ParcelTrackingAppTest {

    private static final int CONSIGNMENTS = 4;

    private static final int SCANS = 12;

    /**
     * One partition, so the concurrency the barrier proves cannot be explained by partition count.
     */
    private static final int PARTITIONS = 1;

    /**
     * Strictly below {@link #CONSIGNMENTS} - see the class javadoc.
     */
    private static final int CONCURRENT_COHORT = 3;

    /**
     * Generous upper bound, not a timing assumption: on a healthy run the barrier trips in milliseconds.
     */
    private static final Duration BARRIER_TIMEOUT = Duration.ofSeconds(30);

    private static final Duration RUN_TIMEOUT = Duration.ofSeconds(120);

    private static final Duration POLL_INTERVAL = Duration.ofMillis(100);

    private ParcelTrackingAppUnderTest app;

    @AfterEach
    void closeApp() {
        if (app != null) {
            app.closeOnce();
        }
    }

    @Test
    void parcelScansRunConcurrentlyAcrossConsignmentsButNeverWithinOne() {
        app = ParcelTrackingAppUnderTest.create();
        List<ConsumerRecord<String, String>> scans = generateScans(app.getInputTopic());

        app.run();
        for (ConsumerRecord<String, String> scan : scans) {
            app.mockConsumer.addRecord(scan);
        }

        // NON-VACUOUS precondition. This is false before the app starts consuming, so awaiting it proves
        // work actually began. Awaiting only the terminal condition would pass for the wrong reason on a
        // fast machine if the app never ran at all.
        Awaitility.await("the first parcel scan enters the user function")
                .atMost(RUN_TIMEOUT).pollInterval(POLL_INTERVAL)
                .until(() -> app.getObserver().hasObservations());

        // The concurrency proof. The barrier cannot trip unless CONCURRENT_COHORT scans were inside the
        // user function simultaneously, on one partition. A shortfall is recorded rather than thrown, so
        // this await ends either way instead of hanging.
        Awaitility.await("three scans are inside the user function at the same instant")
                .atMost(RUN_TIMEOUT).pollInterval(POLL_INTERVAL)
                .until(() -> app.cohortCleared.get() >= CONCURRENT_COHORT || app.cohortShortfalls.get() > 0);

        assertThat(app.cohortShortfalls.get())
                .as("scans that reached the barrier but never found %d peers there - fewer than %d scans "
                        + "were ever concurrent", CONCURRENT_COHORT, CONCURRENT_COHORT)
                .isZero();
        assertThat(app.cohortCleared.get())
                .as("all %d scans of the cohort cleared the barrier together", CONCURRENT_COHORT)
                .isEqualTo(CONCURRENT_COHORT);

        Awaitility.await("every scan's offset is committed")
                .atMost(RUN_TIMEOUT).pollInterval(POLL_INTERVAL)
                .untilAsserted(() -> KafkaTestUtils.assertLastCommitIs(app.mockConsumer, SCANS));

        assertOrderingHeldWithinEachConsignment();
        assertFailedGeocodesWereRetriedRatherThanDropped();
        assertConcurrencyStayedWithinTheOrderingCeiling();

        app.closeOnce();
        assertRunSummaryExplainsTheRun();
    }

    /**
     * The ordering guarantee, and the assertion that fails if someone switches the example to
     * {@code UNORDERED}: Parallel Consumer never has two scans of the same consignment in flight, so a
     * consignment's status cannot go backwards.
     */
    private void assertOrderingHeldWithinEachConsignment() {
        assertThat(app.peakInFlightByConsignment)
                .as("every consignment must have been scanned at least once")
                .hasSize(CONSIGNMENTS);
        assertThat(app.peakInFlightByConsignment)
                .as("KEY ordering means at most ONE scan per consignment is ever in flight - this is the "
                        + "assertion that fails if the example is changed to UNORDERED")
                .allSatisfy((consignmentId, peak) ->
                        assertThat(peak.get()).as("peak in flight for consignment %s", consignmentId).isEqualTo(1));
    }

    private void assertFailedGeocodesWereRetriedRatherThanDropped() {
        // Without this, the retry assertion below would be vacuously true on a run where nothing failed
        assertThat(app.getGeocodeService().getFailureCount())
                .as("the deterministic geocode failure must actually have fired")
                .isPositive();

        List<Long> retriedOffsets = new ArrayList<>();
        for (Map.Entry<Long, AtomicInteger> attempts : app.attemptsByOffset.entrySet()) {
            if (attempts.getValue().get() > 1) {
                retriedOffsets.add(attempts.getKey());
            }
        }
        assertThat(retriedOffsets)
                .as("a failed geocode must be RETRIED, not dropped - and the commit asserted above shows "
                        + "the retried scans went on to commit")
                .isNotEmpty();
        assertThat(app.attemptsByOffset)
                .as("every scan was attempted, none skipped")
                .hasSize(SCANS);
        log.info("Geocode calls: {} ({} failed, offsets retried: {})",
                app.getGeocodeService().getCallCount(), app.getGeocodeService().getFailureCount(), retriedOffsets);
    }

    /**
     * Structural, not timing: the barrier already forced the lower bound, and KEY ordering over
     * {@value #CONSIGNMENTS} keys makes the upper bound arithmetic rather than luck.
     */
    private void assertConcurrencyStayedWithinTheOrderingCeiling() {
        assertThat(app.getObserver().getPeakInFlight())
                .as("the barrier forced at least %d scans to be in flight together", CONCURRENT_COHORT)
                .isGreaterThanOrEqualTo(CONCURRENT_COHORT)
                .as("KEY ordering caps concurrency at the distinct key count, whatever max concurrency says")
                .isLessThanOrEqualTo(CONSIGNMENTS);
    }

    private void assertRunSummaryExplainsTheRun() {
        Optional<RunSummary> emitted = app.getEmittedSummary();
        assertThat(emitted).as("closing the app emits its run summary").isPresent();

        RunSummary summary = emitted.get();
        assertThat(summary.getRecordCount()).isEqualTo(SCANS);
        assertThat(summary.getPartitionCount())
                .as("the summary names the partition count, because 'peak in-flight' means nothing without it")
                .isEqualTo(PARTITIONS);
        assertThat(summary.getDistinctKeys()).isEqualTo(CONSIGNMENTS);
        assertThat(summary.getOrderingCeiling())
                .as("under KEY ordering the ceiling is the distinct consignment count")
                .hasValue(CONSIGNMENTS);
        assertThat(summary.getSimulatedLatency()).isEqualTo(GeocodeService.DEFAULT_LATENCY);

        String rendered = summary.render();
        assertThat(rendered).contains(
                "input partitions",
                "ordering ceiling",
                CONSIGNMENTS + " concurrent records (distinct keys)",
                GeocodeService.DEFAULT_LATENCY.toMillis() + " ms simulated latency");

        // PRINTED, NEVER ASSERTED: on a loaded two-core runner a concurrent run can be slower than its
        // own implied serial baseline, so any wall-clock assertion here would be a flake generator.
        log.info("Observed processing window {} for {} scans across {} consignment(s), peak in flight {}",
                app.getObserver().getObservedWindow(), summary.getRecordCount(),
                summary.getDistinctKeys(), app.getObserver().getPeakInFlight());
    }

    /**
     * Twelve scans over four consignments on one partition, rotating through the real statuses and depots
     * so the logs read like a courier network rather than like test fixtures.
     */
    private static List<ConsumerRecord<String, String>> generateScans(String topic) {
        List<String> consignments = DemoRecords.keys("GB-CONS", CONSIGNMENTS);
        List<String> depots = GeocodeService.knownDepots();
        return DemoRecords.generate(topic, SCANS, PARTITIONS, consignments, index ->
                ParcelTrackingApp.scanValue(
                        ParcelTrackingApp.SCAN_STATUSES.get(index % ParcelTrackingApp.SCAN_STATUSES.size()),
                        depots.get(index % depots.size())));
    }

    /**
     * The app as the test drives it: the mocks go in through the constructor (the seam that survives a
     * test moving package), and the instrumentation goes around {@code trackScan} - which is where the
     * user function's real work happens, and is reached BEFORE any simulated geocode failure can throw.
     */
    static class ParcelTrackingAppUnderTest extends ParcelTrackingApp {

        final LongPollingMockConsumer<String, String> mockConsumer;

        final CyclicBarrier cohort = new CyclicBarrier(CONCURRENT_COHORT);

        /**
         * Only the first {@link #CONCURRENT_COHORT} arrivals wait. Later scans must not wait on a barrier
         * that nobody else is going to reach.
         */
        final AtomicInteger cohortArrivals = new AtomicInteger();

        final AtomicInteger cohortCleared = new AtomicInteger();

        final AtomicInteger cohortShortfalls = new AtomicInteger();

        final Map<String, AtomicInteger> inFlightByConsignment = new ConcurrentHashMap<>();

        final Map<String, AtomicInteger> peakInFlightByConsignment = new ConcurrentHashMap<>();

        final Map<Long, AtomicInteger> attemptsByOffset = new ConcurrentHashMap<>();

        private boolean closed;

        private ParcelTrackingAppUnderTest(LongPollingMockConsumer<String, String> mockConsumer,
                                           MockProducer<String, String> mockProducer) {
            super(mockConsumer, mockProducer);
            this.mockConsumer = mockConsumer;
        }

        static ParcelTrackingAppUnderTest create() {
            Serializer<String> stringSerializer = Serdes.String().serializer();
            return new ParcelTrackingAppUnderTest(
                    ExampleMockConsumers.spiedLongPollingMockConsumer(),
                    new MockProducer<>(true, stringSerializer, stringSerializer));
        }

        @Override
        protected void postSetup() {
            mockConsumer.subscribeWithRebalanceAndAssignment(of(getInputTopic()), PARTITIONS);
        }

        @Override
        protected String trackScan(ConsumerRecord<String, String> scan) {
            attemptsByOffset.computeIfAbsent(scan.offset(), offset -> new AtomicInteger()).incrementAndGet();

            String consignmentId = scan.key();
            int nowInFlight = inFlightByConsignment
                    .computeIfAbsent(consignmentId, key -> new AtomicInteger()).incrementAndGet();
            peakInFlightByConsignment
                    .computeIfAbsent(consignmentId, key -> new AtomicInteger())
                    .accumulateAndGet(nowInFlight, Math::max);
            try {
                // Before super.trackScan(), so every arrival reaches the barrier BEFORE the simulated
                // geocode failure gets a chance to throw
                joinCohort();
                return super.trackScan(scan);
            } finally {
                inFlightByConsignment.get(consignmentId).decrementAndGet();
            }
        }

        private void joinCohort() {
            if (cohortArrivals.getAndIncrement() >= CONCURRENT_COHORT) {
                return;
            }
            try {
                cohort.await(BARRIER_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                cohortCleared.incrementAndGet();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                cohortShortfalls.incrementAndGet();
            } catch (TimeoutException | BrokenBarrierException e) {
                // Recorded, NOT rethrown. Rethrowing would look to Parallel Consumer like the user
                // function failing, and it would retry this scan on the one second default delay forever -
                // the test would then report "offsets never commit" instead of "the cohort never formed".
                cohortShortfalls.incrementAndGet();
                log.warn("Only {} of {} scans reached the concurrency barrier within {}",
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
