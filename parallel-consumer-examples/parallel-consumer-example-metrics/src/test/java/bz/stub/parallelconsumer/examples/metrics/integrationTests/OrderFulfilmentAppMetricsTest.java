package bz.stub.parallelconsumer.examples.metrics.integrationTests;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.internal.utils.KafkaTestUtils;
import bz.stub.parallelconsumer.internal.utils.LongPollingMockConsumer;
import bz.stub.parallelconsumer.examples.metrics.FulfilmentService;
import bz.stub.parallelconsumer.examples.metrics.OrderFulfilmentApp;
import bz.stub.parallelconsumer.examples.support.DemoRecords;
import bz.stub.parallelconsumer.examples.support.ExampleMockConsumers;
import bz.stub.parallelconsumer.examples.support.RunSummary;
import bz.stub.parallelconsumer.metrics.PCMetricsDef;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.LongTaskTimer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.assertj.core.api.Condition;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static pl.tlinkowski.unij.api.UniLists.of;

/**
 * Drives {@link OrderFulfilmentApp} against a mock consumer, with a real Prometheus scraping its
 * endpoint - which is why this is a failsafe integration test rather than a unit test like the other
 * industry examples' tests. What is on trial here is the <em>signals</em>, and a signal that is never
 * scraped has not been demonstrated.
 * <p>
 * <b>Concurrency is proved by a {@link CyclicBarrier}, never by a wall clock.</b> "{@code
 * pc_inflight_records} rose above 1 under load" is an assertion about the scheduler's mood on a loaded
 * two-core runner. Here the barrier <em>forces</em> {@value #CONCURRENT_COHORT} allocations to be in
 * flight at the same instant, and the gauge is read while they are held there, so the number it reports
 * is a fact rather than a sample. Timings are printed and never asserted.
 * <p>
 * <b>The test thread is the barrier's last party.</b> The gate is sized
 * {@value #CONCURRENT_COHORT}{@code  + 1}: the handlers park, the test asserts on the meters while they
 * are parked, and only then joins the gate to release them. A barrier the handlers could trip on their
 * own would have let them all finish before anything could be read off the registry.
 * <p>
 * <b>The barrier's failure mode is specified, or a shortfall hangs instead of failing.</b> A bare
 * {@code await()} parks Parallel Consumer's workers forever. A bare {@code await(timeout)} permanently
 * BREAKS the barrier, so every later arrival throws {@link BrokenBarrierException} - which Parallel
 * Consumer reads as user-function failure and retries on the one second default delay forever, so the
 * developer sees "offsets never commit" instead of "only N-1 orders were concurrent". Every arrival here
 * calls {@code await(timeout, unit)} inside a try/catch that records the shortfall and lets the order
 * complete normally; the test body asserts on the recorded value.
 * <p>
 * <b>Exactly {@value #CONCURRENT_COHORT} records are fed before the gate opens.</b>
 * {@code pc_inflight_records} counts records handed to the processing pool, so the equality assertion
 * only means something if the cohort is the entire available backlog at that moment. The rest of the
 * orders go in afterwards.
 */
@Slf4j
@Testcontainers
class OrderFulfilmentAppMetricsTest {

    @Container
    private static final PrometheusContainer PROMETHEUS_CONTAINER = new PrometheusContainer();

    /**
     * The port {@link bz.stub.parallelconsumer.examples.metrics.CoreApp} binds, as a literal because
     * that example hardcodes it and exposes no constant. Named here so the port-collision assertion says
     * what it is guarding.
     */
    private static final int MINIMAL_EXAMPLE_METRICS_PORT = 7001;

    /**
     * The Prometheus family name of the allocation {@link io.micrometer.core.instrument.Timer}. Narrower
     * than the meter's base name on purpose: {@code order_allocation} would also match the long task
     * timer's and the outcome counter's series, which carry different tags.
     */
    private static final String ALLOCATION_TIMER_SERIES =
            OrderFulfilmentApp.ALLOCATION_TIMER.replace('.', '_') + "_seconds";

    private static final int CUSTOMERS = 4;

    /**
     * One order stage per record. Twelve is enough to reach the warehouse system's deterministic
     * out-of-stock call (every tenth) exactly once, so the failure counter's value is reproducible.
     */
    private static final int ORDER_STAGES = 12;

    /**
     * One partition, so the concurrency the barrier proves cannot be explained by partition count.
     */
    private static final int PARTITIONS = 1;

    /**
     * Every customer at once. The whole cohort is fed before the gate opens, so both
     * {@code pc_inflight_records} and the long task timer must report exactly this.
     */
    private static final int CONCURRENT_COHORT = CUSTOMERS;

    /**
     * Generous upper bound, not a timing assumption - and deliberately longer than {@link #RUN_TIMEOUT},
     * so an assertion that is waiting on the gauges times out and FAILS before the parked handlers give
     * up and quietly release themselves.
     */
    private static final Duration BARRIER_TIMEOUT = Duration.ofSeconds(180);

    private static final Duration RUN_TIMEOUT = Duration.ofSeconds(120);

    private static final Duration POLL_INTERVAL = Duration.ofMillis(100);

    private OrderFulfilmentAppUnderTest app;

    @AfterEach
    void closeApp() {
        if (app != null) {
            app.closeOnce();
        }
    }

    @Test
    void orderFulfilmentPublishesTheSignalsADashboardIsBuiltFrom() {
        org.testcontainers.Testcontainers.exposeHostPorts(OrderFulfilmentApp.METRICS_PORT);

        app = OrderFulfilmentAppUnderTest.create();
        List<ConsumerRecord<String, String>> orders = generateOrders(app.getInputTopic());

        app.run();

        // ---- the concurrency proof, with the meters read WHILE the cohort is held ------------------
        for (ConsumerRecord<String, String> order : orders.subList(0, CONCURRENT_COHORT)) {
            app.mockConsumer.addRecord(order);
        }
        awaitTheWholeCohortParkedAtTheGate();
        assertTheMetersAgreeOnHowManyAreInFlight();
        app.openTheGate();
        assertThat(app.cohortShortfalls.get())
                .as("orders that reached the gate but never found %d peers there - fewer than %d were "
                        + "ever concurrent", CONCURRENT_COHORT, CONCURRENT_COHORT)
                .isZero();

        // ---- the rest of the backlog, including the deterministic out-of-stock allocation -----------
        for (ConsumerRecord<String, String> order : orders.subList(CONCURRENT_COHORT, ORDER_STAGES)) {
            app.mockConsumer.addRecord(order);
        }
        Awaitility.await("every order stage's offset is committed")
                .atMost(RUN_TIMEOUT).pollInterval(POLL_INTERVAL)
                .untilAsserted(() -> KafkaTestUtils.assertLastCommitIs(app.mockConsumer, ORDER_STAGES));

        assertOutOfStockWasCountedByOutcomeAndRetried();
        assertLatencyIsABucketHistogramAndNotClientSidePercentiles();
        assertNoOrderIdEverBecameATagValue();
        assertPrometheusScrapedTheApplicationsOwnMeters();
        assertBothExamplesHaveTheirOwnScrapeTarget();

        app.closeOnce();
        assertTheClosingSummaryExplainsTheRun();
    }

    /**
     * NON-VACUOUS precondition, and the thing that makes the equality assertions below safe: it is false
     * until every one of the cohort is inside {@link FulfilmentService#allocate} at the same moment. The
     * gate cannot trip on its own - the test is its last party - so nothing can slip past while this is
     * being awaited.
     */
    private void awaitTheWholeCohortParkedAtTheGate() {
        Awaitility.await("all " + CONCURRENT_COHORT + " orders are inside the warehouse allocation call")
                .atMost(RUN_TIMEOUT).pollInterval(POLL_INTERVAL)
                .until(() -> app.gate.getNumberWaiting() == CONCURRENT_COHORT
                        || app.cohortShortfalls.get() > 0);
        assertThat(app.cohortShortfalls.get())
                .as("an order gave up at the gate before the cohort formed")
                .isZero();
    }

    /**
     * The two ways of asking "how many right now", read while the answer is pinned by the gate.
     * {@code pc_inflight_records} is Parallel Consumer's own view; the {@link LongTaskTimer} is the
     * application's. They are asserted together because agreeing is the point - a dashboard that showed
     * them diverging would be showing an instrumentation bug, not a system under load.
     */
    private void assertTheMetersAgreeOnHowManyAreInFlight() {
        assertThat(app.getObserver().getInFlight())
                .as("the example's own observer sees the whole cohort inside the user function")
                .isEqualTo(CONCURRENT_COHORT);
        assertThat(activeAllocations())
                .as("the LongTaskTimer reports the orders currently inside the allocation call - which a "
                        + "plain Timer could not, because it only records on completion")
                .isEqualTo(CONCURRENT_COHORT);
        // Awaited rather than read once: the gauge's backing counter is maintained by Parallel Consumer's
        // control thread, so the test thread can observe it a beat late. The gate holds indefinitely, so
        // waiting costs nothing and removes the only race here.
        Awaitility.await("pc_inflight_records reports the held cohort")
                .atMost(RUN_TIMEOUT).pollInterval(POLL_INTERVAL)
                .untilAsserted(() -> assertThat(inflightRecords())
                        .as("Parallel Consumer's own in-flight gauge, with the cohort forced open and no "
                                + "other records available to it")
                        .isEqualTo(CONCURRENT_COHORT));
    }

    private void assertOutOfStockWasCountedByOutcomeAndRetried() {
        long outOfStockCalls = app.getFulfilmentService().getFailureCount();
        // Without this the counter assertion below would be vacuously satisfied by a run in which the
        // warehouse never ran out of stock at all.
        assertThat(outOfStockCalls)
                .as("the deterministic out-of-stock allocation must actually have fired")
                .isPositive();

        assertThat(counterFor(OrderFulfilmentApp.OUTCOME_OUT_OF_STOCK).count())
                .as("the failure counter carries an outcome tag and agrees with the warehouse system")
                .isEqualTo((double) outOfStockCalls);
        assertThat(counterFor(OrderFulfilmentApp.OUTCOME_ALLOCATED).count())
                .as("every stage that was fulfilled is counted under the success outcome")
                .isEqualTo((double) ORDER_STAGES);
        assertThat(counterFor(OrderFulfilmentApp.OUTCOME_ERROR).count())
                .as("nothing failed for a reason other than stock")
                .isZero();

        // The retry, not just the failure: more allocation calls than records means the out-of-stock
        // order was re-processed rather than dropped, and the commit awaited above shows it went on to
        // commit.
        assertThat(app.getFulfilmentService().getCallCount())
                .as("an out-of-stock order is RETRIED, not dropped")
                .isEqualTo(ORDER_STAGES + outOfStockCalls);
    }

    /**
     * The choice made in {@link OrderFulfilmentApp}, asserted rather than asserted-in-a-comment.
     * <p>
     * Both halves of it bite. Swapping {@code publishPercentileHistogram()} for
     * {@code publishPercentiles(...)} fails the bucket assertion; ADDING {@code publishPercentiles(...)}
     * alongside the histogram fails the quantile assertion, because this registry publishes both
     * families rather than dropping one - measured, not assumed.
     */
    private void assertLatencyIsABucketHistogramAndNotClientSidePercentiles() {
        List<String> allocationSeries = seriesNamed(ALLOCATION_TIMER_SERIES);

        assertThat(allocationSeries)
                .as("the allocation timer publishes server-side histogram buckets, which "
                        + "histogram_quantile() can aggregate across instances")
                .anyMatch(line -> line.contains("_bucket{") && line.contains("le=\""));
        assertThat(allocationSeries)
                .as("...and publishes NO client-side quantiles, which are computed per process and so "
                        + "cannot be aggregated into a fleet-wide percentile at all")
                .noneMatch(line -> line.contains("quantile=\""));
        assertThat(allocationSeries)
                .as("throughput comes from the timer's own _count - a separate counter would be redundant")
                .anyMatch(line -> line.startsWith(ALLOCATION_TIMER_SERIES + "_count{"));

        Set<String> warehouseTags = new LinkedHashSet<>();
        for (String line : allocationSeries) {
            warehouseTags.add(tagValue(line, "warehouse"));
        }
        assertThat(warehouseTags)
                .as("the timer is tagged by warehouse, whose value set is fixed at startup")
                .isSubsetOf(FulfilmentService.knownWarehouses());
    }

    /**
     * The cardinality guard. Tagging by order or customer id is the mistake this example exists to avoid,
     * and it would be invisible in a four-customer demo - so it is asserted instead of trusted.
     */
    private void assertNoOrderIdEverBecameATagValue() {
        String scrape = app.getMeterRegistry().scrape();
        for (String customerId : DemoRecords.keys(CUSTOMER_PREFIX, CUSTOMERS)) {
            assertThat(scrape)
                    .as("no meter may carry '%s' as a tag value - one time series per order is the "
                            + "cardinality mistake this example exists to not make", customerId)
                    .doesNotContain(customerId);
        }
    }

    /**
     * The signals reached Prometheus, which is the part a unit test cannot show. Asserted against the
     * series names Prometheus itself holds, not against the app's own scrape output.
     */
    private void assertPrometheusScrapedTheApplicationsOwnMeters() {
        List<String> expected = of(
                "order_allocation_seconds_bucket",
                "order_allocation_seconds_count",
                "order_allocation_active_seconds_active_count",
                "order_allocation_outcomes_total",
                "pc_inflight_records",
                "pc_status");

        Awaitility.await("Prometheus has scraped the order fulfilment example's meters")
                .atMost(RUN_TIMEOUT).pollInterval(POLL_INTERVAL)
                .untilAsserted(() -> assertThat(scrapedSeriesNames()).containsAll(expected));
    }

    /**
     * Scenario: both examples in this module are scrapeable at once.
     * <p>
     * Asserted structurally here - two distinct targets on two distinct ports, with this example's target
     * healthy - rather than by also starting the minimal example, because both tests share one forked JVM
     * and {@code CoreApp} never stops its {@link com.sun.net.httpserver.HttpServer}.
     * <p>
     * That last fact is what makes the runtime half of the guarantee the <em>suite's</em> job rather than
     * this method's, and it has been measured: setting {@link OrderFulfilmentApp#METRICS_PORT} to
     * {@value #MINIMAL_EXAMPLE_METRICS_PORT} makes this test die on "could not bind the metrics endpoint
     * on port 7001" as soon as the other integration test runs first.
     */
    @SuppressWarnings("unchecked")
    private void assertBothExamplesHaveTheirOwnScrapeTarget() {
        assertThat(OrderFulfilmentApp.METRICS_PORT)
                .as("the two examples must not share a port")
                .isNotEqualTo(MINIMAL_EXAMPLE_METRICS_PORT);

        Map<String, Object> targets = PROMETHEUS_CONTAINER.queryApi("/api/v1/targets", Map.class);
        List<Map<String, Object>> active = (List<Map<String, Object>>) targets.get("activeTargets");

        Set<String> instances = new LinkedHashSet<>();
        String ownHealth = null;
        for (Map<String, Object> target : active) {
            Map<String, Object> labels = (Map<String, Object>) target.get("labels");
            String instance = String.valueOf(labels.get("instance"));
            instances.add(instance);
            if (instance.endsWith(":" + OrderFulfilmentApp.METRICS_PORT)) {
                ownHealth = String.valueOf(target.get("health"));
            }
        }

        assertThat(instances)
                .as("prometheus.yml carries a target for each example - a merged one would mean the two "
                        + "had been put back on the same port")
                .haveAtLeastOne(matching(":" + MINIMAL_EXAMPLE_METRICS_PORT))
                .haveAtLeastOne(matching(":" + OrderFulfilmentApp.METRICS_PORT));
        assertThat(ownHealth)
                .as("this example's endpoint is being scraped successfully")
                .isEqualTo("up");
    }

    private void assertTheClosingSummaryExplainsTheRun() {
        Optional<RunSummary> emitted = app.getEmittedSummary();
        assertThat(emitted).as("closing the app emits its run summary").isPresent();

        RunSummary summary = emitted.get();
        assertThat(summary.getRecordCount()).isEqualTo(ORDER_STAGES);
        assertThat(summary.getPartitionCount())
                .as("the summary names the partition count, because 'peak in-flight' means nothing "
                        + "without it")
                .isEqualTo(PARTITIONS);
        assertThat(summary.getDistinctKeys()).isEqualTo(CUSTOMERS);
        assertThat(summary.getOrderingCeiling())
                .as("under KEY ordering the ceiling is the distinct customer count")
                .hasValue(CUSTOMERS);
        assertThat(summary.getSimulatedLatency()).isEqualTo(FulfilmentService.DEFAULT_ALLOCATION_LATENCY);
        assertThat(app.getObserver().getPeakInFlight())
                .as("the gate forced the whole cohort in flight together")
                .isGreaterThanOrEqualTo(CONCURRENT_COHORT)
                .as("KEY ordering caps concurrency at the distinct key count, whatever max concurrency says")
                .isLessThanOrEqualTo(CUSTOMERS);

        Optional<String> families = app.getEmittedMetricFamilies();
        assertThat(families).as("closing the app prints the pc_* exposition text").isPresent();
        assertThat(families.get())
                .as("real exposition text, so a reader sees what a dashboard is built from without "
                        + "standing up Grafana - and sampled BEFORE the processor closed, since closing "
                        + "it deregisters Parallel Consumer's own meters and would leave this empty")
                .contains("# TYPE pc_inflight_records gauge")
                .contains("pc_status{")
                .doesNotContain("order_allocation");

        // PRINTED, NEVER ASSERTED: on a loaded two-core runner a concurrent run can be slower than its
        // own implied serial baseline, so any wall-clock assertion here would be a flake generator.
        log.info("Observed processing window {} for {} order stages across {} customer(s), peak in flight {}",
                app.getObserver().getObservedWindow(), summary.getRecordCount(),
                summary.getDistinctKeys(), app.getObserver().getPeakInFlight());
    }

    // ---- reading the meters -------------------------------------------------------------------------

    private double inflightRecords() {
        return app.getMeterRegistry().get(PCMetricsDef.INFLIGHT_RECORDS.getName()).gauge().value();
    }

    private int activeAllocations() {
        return app.getMeterRegistry()
                .get(OrderFulfilmentApp.ALLOCATION_ACTIVE_TIMER).longTaskTimer().activeTasks();
    }

    private Counter counterFor(String outcome) {
        return app.getMeterRegistry()
                .get(OrderFulfilmentApp.ALLOCATION_OUTCOMES_COUNTER).tag("outcome", outcome).counter();
    }

    /**
     * The exposition lines of one metric family, straight out of the app's own endpoint over HTTP - the
     * same bytes Prometheus scrapes, rather than a second rendering built for the test.
     */
    private List<String> seriesNamed(String prometheusName) {
        List<String> matching = new ArrayList<>();
        for (String line : scrapeOverHttp().split("\\R")) {
            if (line.startsWith(prometheusName)) {
                matching.add(line);
            }
        }
        assertThat(matching).as("the scrape contains the '%s' family at all", prometheusName).isNotEmpty();
        return matching;
    }

    private String scrapeOverHttp() {
        try {
            URL url = new URL("http://localhost:" + OrderFulfilmentApp.METRICS_PORT
                    + OrderFulfilmentApp.METRICS_ENDPOINT);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            assertThat(connection.getResponseCode())
                    .as("the example's own scrape endpoint answers").isEqualTo(200);
            StringBuilder body = new StringBuilder();
            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(connection.getInputStream(), StandardCharsets.UTF_8))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    body.append(line).append(System.lineSeparator());
                }
            }
            return body.toString();
        } catch (IOException e) {
            throw new IllegalStateException("could not scrape the example's own metrics endpoint", e);
        }
    }

    @SuppressWarnings("unchecked")
    private List<String> scrapedSeriesNames() {
        return PROMETHEUS_CONTAINER.queryApi("/api/v1/label/__name__/values", List.class);
    }

    private static String tagValue(String expositionLine, String tag) {
        String needle = tag + "=\"";
        int start = expositionLine.indexOf(needle);
        if (start < 0) {
            return null;
        }
        start += needle.length();
        return expositionLine.substring(start, expositionLine.indexOf('"', start));
    }

    private static Condition<String> matching(String portSuffix) {
        return new Condition<>(instance -> instance.endsWith(portSuffix),
                "a scrape target on %s", portSuffix);
    }

    // ---- the input ----------------------------------------------------------------------------------

    private static final String CUSTOMER_PREFIX = "GB-CUST";

    /**
     * Twelve order stages over four customers on one partition, rotating through the real fulfilment
     * stages so the logs read like a fulfilment centre rather than like test fixtures. The first four
     * records take the four customers in turn (see {@link DemoRecords}), which is what lets the cohort be
     * exactly one order per customer.
     */
    private static List<ConsumerRecord<String, String>> generateOrders(String topic) {
        List<String> customers = DemoRecords.keys(CUSTOMER_PREFIX, CUSTOMERS);
        return DemoRecords.generate(topic, ORDER_STAGES, PARTITIONS, customers, index ->
                OrderFulfilmentApp.FULFILMENT_STAGES.get(index % OrderFulfilmentApp.FULFILMENT_STAGES.size()));
    }

    /**
     * The app as the test drives it: the mock consumer goes in through the constructor (the seam that
     * survives a test moving package), and the starting gate goes inside the
     * {@link FulfilmentService} - which is where the blocking call is, so a parked handler is parked
     * exactly where the long task timer is counting.
     */
    static class OrderFulfilmentAppUnderTest extends OrderFulfilmentApp {

        final LongPollingMockConsumer<String, String> mockConsumer;

        /**
         * The gate the handlers park at, owned by the {@link GatedFulfilmentService} that parks them.
         */
        final CyclicBarrier gate;

        final AtomicInteger cohortShortfalls;

        private boolean closed;

        private OrderFulfilmentAppUnderTest(LongPollingMockConsumer<String, String> mockConsumer,
                                            GatedFulfilmentService fulfilmentService) {
            super(mockConsumer, fulfilmentService);
            this.mockConsumer = mockConsumer;
            this.gate = fulfilmentService.gate;
            this.cohortShortfalls = fulfilmentService.shortfalls;
        }

        static OrderFulfilmentAppUnderTest create() {
            return new OrderFulfilmentAppUnderTest(
                    ExampleMockConsumers.spiedLongPollingMockConsumer(), new GatedFulfilmentService());
        }

        @Override
        protected void postSetup() {
            super.postSetup();
            mockConsumer.subscribeWithRebalanceAndAssignment(of(getInputTopic()), PARTITIONS);
        }

        /**
         * The test's own arrival at the gate, which is what releases the held cohort.
         */
        void openTheGate() {
            try {
                gate.await(BARRIER_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                cohortShortfalls.incrementAndGet();
            } catch (TimeoutException | BrokenBarrierException e) {
                cohortShortfalls.incrementAndGet();
                log.warn("The test thread could not join the gate within {}", BARRIER_TIMEOUT, e);
            }
        }

        /**
         * The test asserts on the summary it emitted; {@code @AfterEach} closes it again if an assertion
         * threw first.
         */
        void closeOnce() {
            if (!closed) {
                closed = true;
                close();
            }
        }
    }

    /**
     * The warehouse system with a starting gate in front of it. The first {@value #CONCURRENT_COHORT}
     * allocations park here; later ones must not wait on a gate nobody else will reach, so arrivals are
     * counted and only the cohort awaits.
     */
    static class GatedFulfilmentService extends FulfilmentService {

        /**
         * Sized for the cohort PLUS the test thread, so the handlers stay parked until the test has read
         * the meters off the registry and joined.
         */
        final CyclicBarrier gate = new CyclicBarrier(CONCURRENT_COHORT + 1);

        final AtomicInteger arrivals = new AtomicInteger();

        final AtomicInteger shortfalls = new AtomicInteger();

        @Override
        public String allocate(String customerId, String stage, String warehouse) {
            joinCohort();
            return super.allocate(customerId, stage, warehouse);
        }

        private void joinCohort() {
            if (arrivals.getAndIncrement() >= CONCURRENT_COHORT) {
                return;
            }
            try {
                gate.await(BARRIER_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                shortfalls.incrementAndGet();
            } catch (TimeoutException | BrokenBarrierException e) {
                // Recorded, NOT rethrown. Rethrowing would look to Parallel Consumer like the user
                // function failing, and it would retry this order on the one second default delay
                // forever - the test would then report "offsets never commit" instead of "the cohort
                // never formed".
                shortfalls.incrementAndGet();
                log.warn("Fewer than {} orders reached the concurrency gate within {}",
                        CONCURRENT_COHORT, BARRIER_TIMEOUT, e);
            }
        }
    }
}
