package bz.stub.parallelconsumer.examples.vertx;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.common.FileSource;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.extension.Parameters;
import com.github.tomakehurst.wiremock.extension.ResponseTransformer;
import com.github.tomakehurst.wiremock.http.QueryParameter;
import com.github.tomakehurst.wiremock.http.Request;
import com.github.tomakehurst.wiremock.http.Response;
import com.github.tomakehurst.wiremock.verification.LoggedRequest;
import bz.stub.parallelconsumer.internal.utils.KafkaTestUtils;
import bz.stub.parallelconsumer.internal.utils.LongPollingMockConsumer;
import bz.stub.parallelconsumer.examples.support.DemoRecords;
import bz.stub.parallelconsumer.examples.support.ExampleMockConsumers;
import bz.stub.parallelconsumer.examples.support.RunSummary;
import bz.stub.parallelconsumer.examples.vertx.FlightSearchApp.FareProvider;
import bz.stub.parallelconsumer.examples.vertx.FlightSearchApp.FareQuote;
import bz.stub.parallelconsumer.examples.vertx.FlightSearchApp.FlightSearch;
import io.vertx.core.AsyncResult;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

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
 * Drives {@link FlightSearchApp} against a mock consumer and WireMock stubs standing in for the fare
 * providers - no broker, no Testcontainers. What is on show is Parallel Consumer's concurrency across
 * records plus a Vert.x fan-out within one, and a mock consumer exercises both exactly as well as a real
 * broker while staying in the fast unit lane.
 * <p>
 * <b>Concurrency is proved two different ways here, because there are two kinds of it.</b>
 * <ul>
 *     <li><b>Legs within one search</b> - proved from WireMock's own request timestamps: the three
 *     provider requests all arrive within the shortest leg's delay of each other, which a sequential
 *     fan-out could not manage.</li>
 *     <li><b>Records across searches</b> - proved by a {@link CyclicBarrier} in the stub handler.</li>
 * </ul>
 * <p>
 * <b>The barrier is gated to ONE ARRIVAL PER SEARCH, and that gating is the whole test.</b> All three
 * legs of a single search hit the stub handler, so an ungated barrier sized three trips on <em>one</em>
 * record - and the test would then pass on a build that dispatched records strictly one at a time, which
 * is the exact regression the barrier exists to catch. {@link FirstLegOfEachSearchGate} therefore admits
 * a search to the barrier only once, via {@link ConcurrentHashMap#putIfAbsent}.
 * <p>
 * <b>The barrier's failure mode is specified, or a shortfall hangs instead of failing.</b> A bare
 * {@code await()} parks a WireMock container thread forever. A bare {@code await(timeout)} permanently
 * BREAKS the barrier, so every later arrival throws {@link BrokenBarrierException} - the stub would then
 * fail, Parallel Consumer would read that as user-function failure and retry on the one second default
 * delay forever, and the developer would see "offsets never commit" instead of "only N-1 searches were
 * concurrent". Every arrival calls {@code await(timeout, unit)} inside a try/catch that records the
 * shortfall and lets the request complete normally; the test body asserts on the recorded value.
 * <p>
 * <b>Wall-clock durations are printed, never asserted</b> - on a loaded runner a concurrent run can be
 * slower than its own implied serial baseline.
 */
@Slf4j
class FlightSearchAppTest {

    /**
     * Real route codes, so the logs read like a travel system's. Four distinct keys, which under KEY
     * ordering is also the concurrency ceiling.
     */
    private static final List<String> ROUTES = of("LHR-JFK", "CDG-SIN", "AMS-GRU", "DUB-BOS");

    private static final int SEARCHES = 8;

    /**
     * One partition, so the concurrency the barrier proves cannot be explained by partition count.
     */
    private static final int PARTITIONS = 1;

    /**
     * Strictly below the {@link #ROUTES} count: under KEY ordering the distinct route count is the
     * concurrency ceiling, and a barrier sized exactly at the ceiling needs every route to have work
     * available at the same instant.
     * <p>
     * It is also sized against {@link #WIREMOCK_CONTAINER_THREADS}. Because the gate admits one leg per
     * search, only this many container threads are ever parked, leaving the rest of WireMock's pool free
     * to serve the sibling legs whose completion is what releases the barrier. Ungated, all three legs of
     * every in-flight search would park and the two numbers would be in a race.
     */
    private static final int CONCURRENT_COHORT = 3;

    /**
     * WireMock's own Jetty default, stated explicitly so the margin over {@link #CONCURRENT_COHORT} is
     * deliberate rather than inherited - if either number moves, the other is right beside it.
     */
    private static final int WIREMOCK_CONTAINER_THREADS = 10;

    /**
     * The two fast legs and the one slow leg of the fan-out timing test. A sequential fan-out could not
     * issue its second request until the first leg's {@value #FAST_LEG_DELAY_MILLIS} ms had elapsed, so
     * a spread below that is only reachable by issuing all three at once.
     * <p>
     * <b>Why these are deliberately large.</b> The arrival spread this test measures comes from dispatch
     * jitter, not from the leg delay - so it stays in the single-digit milliseconds however long the legs
     * are, while the bound it is compared against moves with {@code FAST_LEG_DELAY_MILLIS}. Raising the
     * delay therefore widens the safety margin without weakening what the assertion discriminates: a
     * sequential fan-out still cannot come in under one leg. At 30ms the observed spread was 0-11ms - a
     * roughly threefold margin, which is thin for a timing comparison on a loaded 2-core CI runner. The
     * cost of the larger figures is well under a second of test time.
     */
    private static final int FAST_LEG_DELAY_MILLIS = 200;

    private static final int SLOW_LEG_DELAY_MILLIS = 600;

    /**
     * Generous upper bound, not a timing assumption: on a healthy run the barrier trips in milliseconds.
     */
    private static final Duration BARRIER_TIMEOUT = Duration.ofSeconds(30);

    private static final Duration RUN_TIMEOUT = Duration.ofSeconds(120);

    private static final Duration POLL_INTERVAL = Duration.ofMillis(100);

    private static final LocalDate FIRST_DEPARTURE = LocalDate.of(2026, 9, 1);

    /**
     * Distinct per provider so "the cheapest won" is a statement about which provider's answer was
     * chosen, not just about a number.
     */
    private static final int[] PROVIDER_PRICES_MINOR_UNITS = {48999, 31950, 52400};

    /**
     * The index into {@link #PROVIDER_PRICES_MINOR_UNITS} that is genuinely the lowest.
     */
    private static final int CHEAPEST_PROVIDER_INDEX = 1;

    private FirstLegOfEachSearchGate gate;

    private WireMockServer fareProviders;

    private FlightSearchAppUnderTest app;

    @AfterEach
    void closeApp() {
        if (app != null) {
            app.closeOnce();
        }
        if (fareProviders != null) {
            fareProviders.stop();
        }
    }

    @Test
    void flightSearchesFanOutToEveryFareProviderAndRunConcurrentlyAcrossRoutes() {
        startFareProviders();
        for (int i = 0; i < FlightSearchApp.FARE_PROVIDERS.size(); i++) {
            stubProvider(FlightSearchApp.FARE_PROVIDERS.get(i), PROVIDER_PRICES_MINOR_UNITS[i],
                    (int) FlightSearchApp.FARE_LOOKUP_LATENCY.toMillis());
        }
        gate.arm(CONCURRENT_COHORT);

        app = FlightSearchAppUnderTest.create(fareProviders.port());
        List<ConsumerRecord<String, String>> searches = generateSearches(app.getInputTopic(), ROUTES, SEARCHES);

        long startedAt = System.nanoTime();
        app.run();
        for (ConsumerRecord<String, String> search : searches) {
            app.mockConsumer.addRecord(search);
        }

        // NON-VACUOUS precondition. This is false before the app starts consuming, so awaiting it proves
        // work actually began. Awaiting only the terminal condition would pass for the wrong reason on a
        // fast machine if the app never ran at all.
        Awaitility.await("the first flight search enters the user function")
                .atMost(RUN_TIMEOUT).pollInterval(POLL_INTERVAL)
                .until(() -> app.getObserver().hasObservations());

        // The record-concurrency proof. The barrier cannot trip unless CONCURRENT_COHORT DISTINCT
        // searches had a request outstanding at the same instant, on one partition. A shortfall is
        // recorded rather than thrown, so this await ends either way instead of hanging.
        Awaitility.await("three distinct searches have a fare request in flight at the same instant")
                .atMost(RUN_TIMEOUT).pollInterval(POLL_INTERVAL)
                .until(() -> gate.cleared.get() >= CONCURRENT_COHORT || gate.shortfalls.get() > 0);

        assertThat(gate.shortfalls.get())
                .as("searches that reached the barrier but never found %d peers there - fewer than %d "
                        + "SEARCHES were ever in flight together", CONCURRENT_COHORT, CONCURRENT_COHORT)
                .isZero();
        assertThat(gate.cleared.get())
                .as("all %d searches of the cohort cleared the barrier together", CONCURRENT_COHORT)
                .isEqualTo(CONCURRENT_COHORT);
        assertThat(gate.distinctSearchesAdmitted())
                .as("the barrier admitted one leg PER SEARCH - admitting three legs of ONE search would "
                        + "let this test pass on a build that dispatched records one at a time")
                .isGreaterThanOrEqualTo(CONCURRENT_COHORT);

        Awaitility.await("every flight search's offset is committed")
                .atMost(RUN_TIMEOUT).pollInterval(POLL_INTERVAL)
                .untilAsserted(() -> KafkaTestUtils.assertLastCommitIs(app.mockConsumer, SEARCHES));

        assertEverySearchWasPricedByTheCheapestProvider();
        assertEveryProviderWasAskedOncePerSearch();
        assertOrderingHeldWithinEachRoute();
        assertConcurrencyStayedWithinTheOrderingCeiling();

        app.closeOnce();
        assertRunSummaryExplainsTheRun(Duration.ofNanos(System.nanoTime() - startedAt));
    }

    /**
     * The leg-concurrency proof, and the one scenario that is about a single record.
     * <p>
     * Three stubs at {@value #FAST_LEG_DELAY_MILLIS}/{@value #FAST_LEG_DELAY_MILLIS}/
     * {@value #SLOW_LEG_DELAY_MILLIS} ms. A sequential fan-out would not send its second request until
     * the first leg had answered, so the spread between the first and last request WireMock logged would
     * be at least {@value #FAST_LEG_DELAY_MILLIS} ms. Issued together it is a handful of milliseconds.
     * <p>
     * The elapsed wall clock is printed and never asserted - it is the reader's evidence, not the test's.
     */
    @Test
    void theThreeFareProviderCallsAreIssuedConcurrentlyRatherThanOneAfterAnother() {
        startFareProviders();
        stubProvider(FlightSearchApp.FARE_PROVIDERS.get(0), PROVIDER_PRICES_MINOR_UNITS[0], FAST_LEG_DELAY_MILLIS);
        stubProvider(FlightSearchApp.FARE_PROVIDERS.get(1), PROVIDER_PRICES_MINOR_UNITS[1], FAST_LEG_DELAY_MILLIS);
        stubProvider(FlightSearchApp.FARE_PROVIDERS.get(2), PROVIDER_PRICES_MINOR_UNITS[2], SLOW_LEG_DELAY_MILLIS);

        app = FlightSearchAppUnderTest.create(fareProviders.port());
        List<String> oneRoute = of(ROUTES.get(0));
        List<ConsumerRecord<String, String>> searches = generateSearches(app.getInputTopic(), oneRoute, 1);

        long startedAt = System.nanoTime();
        app.run();
        app.mockConsumer.addRecord(searches.get(0));

        // NON-VACUOUS precondition - the search must actually have entered the user function before the
        // terminal condition can mean anything.
        Awaitility.await("the flight search enters the user function")
                .atMost(RUN_TIMEOUT).pollInterval(POLL_INTERVAL)
                .until(() -> app.getObserver().hasObservations());

        Awaitility.await("the search's offset is committed")
                .atMost(RUN_TIMEOUT).pollInterval(POLL_INTERVAL)
                .untilAsserted(() -> KafkaTestUtils.assertLastCommitIs(app.mockConsumer, 1));
        Duration elapsed = Duration.ofNanos(System.nanoTime() - startedAt);

        List<LoggedRequest> requests = allFareRequests();
        assertThat(requests)
                .as("one request per fare provider for the single search")
                .hasSize(FlightSearchApp.FARE_PROVIDERS.size());

        long earliest = Long.MAX_VALUE;
        long latest = Long.MIN_VALUE;
        for (LoggedRequest request : requests) {
            long loggedAt = request.getLoggedDate().getTime();
            earliest = Math.min(earliest, loggedAt);
            latest = Math.max(latest, loggedAt);
        }
        long spreadMillis = latest - earliest;

        // PRINTED, NEVER ASSERTED. The wall clock is what a reader wants to see; it is not what the test
        // turns on, because a loaded runner can make any duration figure say anything.
        log.info("Fan-out of {} fare providers ({} / {} / {} ms stub delays) - requests spread over {} ms, "
                        + "record round trip {} ms",
                FlightSearchApp.FARE_PROVIDERS.size(), FAST_LEG_DELAY_MILLIS, FAST_LEG_DELAY_MILLIS,
                SLOW_LEG_DELAY_MILLIS, spreadMillis, elapsed.toMillis());

        assertThat(spreadMillis)
                .as("all %d fare requests must be on the wire together: issued one after another, the "
                                + "second could not leave until the first leg's %d ms had elapsed",
                        FlightSearchApp.FARE_PROVIDERS.size(), FAST_LEG_DELAY_MILLIS)
                .isLessThan(FAST_LEG_DELAY_MILLIS);

        assertThat(app.getSearchesPriced())
                .as("the search was priced from the joined fan-out, not abandoned")
                .isEqualTo(1);
    }

    /**
     * The {@code recover}-before-join behaviour, and the reason it is not optional.
     * <p>
     * {@code Future.all} fails fast on the first failure. Delete the per-leg {@code .recover(...)} from
     * {@link FlightSearchApp} and this test fails: the search's Future fails, Parallel Consumer retries
     * it on the default delay forever, and the offsets never move.
     */
    @Test
    void anUnavailableFareProviderDoesNotFailTheSearch() {
        startFareProviders();
        FareProvider brokenProvider = FlightSearchApp.FARE_PROVIDERS.get(0);
        stubBrokenProvider(brokenProvider);
        stubProvider(FlightSearchApp.FARE_PROVIDERS.get(1), PROVIDER_PRICES_MINOR_UNITS[1], FAST_LEG_DELAY_MILLIS);
        stubProvider(FlightSearchApp.FARE_PROVIDERS.get(2), PROVIDER_PRICES_MINOR_UNITS[2], FAST_LEG_DELAY_MILLIS);

        app = FlightSearchAppUnderTest.create(fareProviders.port());
        List<ConsumerRecord<String, String>> searches = generateSearches(app.getInputTopic(), ROUTES, SEARCHES);

        app.run();
        for (ConsumerRecord<String, String> search : searches) {
            app.mockConsumer.addRecord(search);
        }

        // NON-VACUOUS precondition - proves the searches started before anything is claimed about them
        // surviving a dead provider.
        Awaitility.await("the first flight search enters the user function")
                .atMost(RUN_TIMEOUT).pollInterval(POLL_INTERVAL)
                .until(() -> app.getObserver().hasObservations());
        Awaitility.await("the broken fare provider has actually been called")
                .atMost(RUN_TIMEOUT).pollInterval(POLL_INTERVAL)
                .until(() -> !fareRequestsTo(brokenProvider).isEmpty());

        Awaitility.await("every flight search still commits its offset, with one provider down")
                .atMost(RUN_TIMEOUT).pollInterval(POLL_INTERVAL)
                .untilAsserted(() -> KafkaTestUtils.assertLastCommitIs(app.mockConsumer, SEARCHES));

        assertThat(app.getUnavailableLegs())
                .as("exactly one leg per search was unusable - the %s provider's 500", brokenProvider.getName())
                .isEqualTo(SEARCHES);
        assertThat(app.getCheapestBySearch())
                .as("every search still produced a fare, from the providers that were up")
                .hasSize(SEARCHES)
                .allSatisfy((searchId, quote) -> {
                    assertThat(quote.isAvailable()).as("fare for %s is a real quote", searchId).isTrue();
                    assertThat(quote.getProviderName())
                            .as("the fare for %s cannot have come from the provider that returned 500", searchId)
                            .isNotEqualTo(brokenProvider.getName());
                });
    }

    /**
     * Without this the fan-out could be satisfied by a run that asked one provider and made the numbers
     * up.
     */
    private void assertEverySearchWasPricedByTheCheapestProvider() {
        FareProvider cheapestProvider = FlightSearchApp.FARE_PROVIDERS.get(CHEAPEST_PROVIDER_INDEX);
        assertThat(app.getCheapestBySearch())
                .as("every one of the %d searches settled on a fare", SEARCHES)
                .hasSize(SEARCHES)
                .allSatisfy((searchId, quote) -> assertThat(quote.getProviderName())
                        .as("the cheapest of the %d provider quotes for %s wins",
                                FlightSearchApp.FARE_PROVIDERS.size(), searchId)
                        .isEqualTo(cheapestProvider.getName()));
        assertThat(app.getUnavailableLegs())
                .as("no provider was down in this scenario, so nothing should have been recovered")
                .isZero();
    }

    private void assertEveryProviderWasAskedOncePerSearch() {
        for (FareProvider provider : FlightSearchApp.FARE_PROVIDERS) {
            assertThat(fareRequestsTo(provider))
                    .as("provider %s is asked exactly once per search - the fan-out is %d wide, every time",
                            provider.getName(), FlightSearchApp.FARE_PROVIDERS.size())
                    .hasSize(SEARCHES);
        }
    }

    /**
     * The ordering guarantee, and the assertion that fails if someone switches the example to
     * {@code UNORDERED}: Parallel Consumer never has two searches for the same route in flight, so a
     * route's cached fares cannot be written out of order.
     */
    private void assertOrderingHeldWithinEachRoute() {
        assertThat(app.peakInFlightByRoute)
                .as("every route must have been searched at least once")
                .hasSize(ROUTES.size());
        assertThat(app.peakInFlightByRoute)
                .as("KEY ordering means at most ONE search per route is ever in flight - this is the "
                        + "assertion that fails if the example is changed to UNORDERED")
                .allSatisfy((route, peak) ->
                        assertThat(peak.get()).as("peak in flight for route %s", route).isEqualTo(1));

        assertThat(app.departureDatesByRoute)
                .as("a route's searches are priced in offset order, earliest departure first")
                .allSatisfy((route, dates) -> assertThat(dates)
                        .as("departure dates seen for route %s", route)
                        .isNotEmpty()
                        .isSorted());
    }

    /**
     * Structural, not timing: the barrier already forced the lower bound, and KEY ordering over
     * {@link #ROUTES} makes the upper bound arithmetic rather than luck.
     */
    private void assertConcurrencyStayedWithinTheOrderingCeiling() {
        assertThat(app.getObserver().getPeakInFlight())
                .as("the barrier forced at least %d searches to be in flight together", CONCURRENT_COHORT)
                .isGreaterThanOrEqualTo(CONCURRENT_COHORT)
                .as("KEY ordering caps concurrency at the distinct route count, whatever max concurrency says")
                .isLessThanOrEqualTo(ROUTES.size());
    }

    private void assertRunSummaryExplainsTheRun(Duration elapsed) {
        Optional<RunSummary> emitted = app.getEmittedSummary();
        assertThat(emitted).as("closing the app emits its run summary").isPresent();

        RunSummary summary = emitted.get();
        assertThat(summary.getRecordCount()).isEqualTo(SEARCHES);
        assertThat(summary.getPartitionCount())
                .as("the summary names the partition count, because 'peak in-flight' means nothing without it")
                .isEqualTo(PARTITIONS);
        assertThat(summary.getDistinctKeys()).isEqualTo(ROUTES.size());
        assertThat(summary.getOrderingCeiling())
                .as("under KEY ordering the ceiling is the distinct route count")
                .hasValue(ROUTES.size());
        assertThat(summary.getSimulatedLatency())
                .as("the legs are concurrent, so one search costs ONE provider's latency, not %d of them",
                        FlightSearchApp.FARE_PROVIDERS.size())
                .isEqualTo(FlightSearchApp.FARE_LOOKUP_LATENCY);

        assertThat(summary.render()).contains(
                "input partitions",
                "ordering ceiling",
                ROUTES.size() + " concurrent records (distinct keys)");

        assertThat(app.getCompletionThreads())
                .as("the fan-out completes on Vert.x event loops, not on a pool of Parallel Consumer "
                        + "workers - this engine dispatches on a single thread by design")
                .isNotEmpty();

        // PRINTED, NEVER ASSERTED: on a loaded runner a concurrent run can be slower than its own
        // implied serial baseline, so any wall-clock assertion here would be a flake generator.
        log.info("Observed processing window {} (whole run {} ms) for {} searches across {} route(s), "
                        + "peak in flight {}, completion threads {}",
                app.getObserver().getObservedWindow(), elapsed.toMillis(), summary.getRecordCount(),
                summary.getDistinctKeys(), app.getObserver().getPeakInFlight(), app.getCompletionThreads());
    }

    private void startFareProviders() {
        gate = new FirstLegOfEachSearchGate();
        WireMockConfiguration options = WireMockConfiguration.wireMockConfig()
                .dynamicPort()
                // Stated rather than inherited, so the margin over CONCURRENT_COHORT is visible: the gate
                // parks one thread per DISTINCT search, so at most CONCURRENT_COHORT of these threads are
                // ever held, and the rest stay free to serve the sibling legs.
                .containerThreads(WIREMOCK_CONTAINER_THREADS)
                .extensions(gate);
        fareProviders = new WireMockServer(options);
        fareProviders.start();
    }

    /**
     * Per-provider stubs with their own delay. Note that {@code WireMockUtils} - which
     * {@link VertxAppTest} uses - only stubs {@code GET /} and {@code /api} with no delay at all, so it
     * cannot express a fan-out whose legs differ in cost.
     */
    private void stubProvider(FareProvider provider, int priceMinorUnits, int delayMillis) {
        fareProviders.stubFor(WireMock.get(WireMock.urlPathEqualTo(provider.getPath()))
                .willReturn(WireMock.aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "text/plain")
                        .withFixedDelay(delayMillis)
                        .withBody(FlightSearchApp.DEFAULT_CURRENCY
                                + FlightSearchApp.FARE_FIELD_SEPARATOR + priceMinorUnits)));
    }

    /**
     * A provider that is up enough to answer and broken enough to be useless - the case the per-leg
     * {@code recover} exists for.
     */
    private void stubBrokenProvider(FareProvider provider) {
        fareProviders.stubFor(WireMock.get(WireMock.urlPathEqualTo(provider.getPath()))
                .willReturn(WireMock.aResponse()
                        .withStatus(500)
                        .withBody("fare engine unavailable")));
    }

    private List<LoggedRequest> fareRequestsTo(FareProvider provider) {
        return fareProviders.findAll(WireMock.getRequestedFor(WireMock.urlPathEqualTo(provider.getPath())));
    }

    private List<LoggedRequest> allFareRequests() {
        List<LoggedRequest> all = new ArrayList<>();
        for (FareProvider provider : FlightSearchApp.FARE_PROVIDERS) {
            all.addAll(fareRequestsTo(provider));
        }
        return all;
    }

    /**
     * Searches over the given routes on one partition. The departure date advances with the record index,
     * so within a route the dates arrive earliest first and the ordering assertion has something to
     * check.
     */
    private static List<ConsumerRecord<String, String>> generateSearches(String topic,
                                                                         List<String> routes,
                                                                         int searchCount) {
        return DemoRecords.generate(topic, searchCount, PARTITIONS, routes, index ->
                FlightSearchApp.searchValue(FIRST_DEPARTURE.plusDays(index).toString()));
    }

    /**
     * The barrier, living in the fare providers' response path because that is the only place in this
     * example where more than one record's work is simultaneously observable: the user function returns
     * a {@link io.vertx.core.Future} without blocking, and Parallel Consumer's Vert.x engine dispatches
     * on a single thread, so a barrier in the user function body would deadlock on every machine.
     * <p>
     * <b>Gated to one arrival per search.</b> Three legs of one search reach here; without the gate a
     * barrier of {@link #CONCURRENT_COHORT} would trip on a single record and this test would pass on a
     * build that dispatched records one at a time.
     */
    static class FirstLegOfEachSearchGate extends ResponseTransformer {

        /**
         * Searches that have already sent a leg through the gate. The value is unused - this is a
         * concurrent set built from {@link ConcurrentHashMap#putIfAbsent}, whose atomicity is what makes
         * "first leg of this search" a decision and not a race.
         */
        private final Map<String, Boolean> searchesAdmitted = new ConcurrentHashMap<>();

        final AtomicInteger cleared = new AtomicInteger();

        final AtomicInteger shortfalls = new AtomicInteger();

        /**
         * Only the first cohort of distinct searches waits. Later searches must not wait on a barrier
         * that nobody else is going to reach.
         */
        private final AtomicInteger arrivals = new AtomicInteger();

        private volatile CyclicBarrier cohort;

        private volatile int cohortSize;

        /**
         * Arms the gate. Until this is called the transformer is a pass-through, so the scenarios that
         * are not about record concurrency pay nothing for it.
         */
        void arm(int size) {
            this.cohortSize = size;
            this.cohort = new CyclicBarrier(size);
        }

        int distinctSearchesAdmitted() {
            return searchesAdmitted.size();
        }

        @Override
        public String getName() {
            return "first-leg-of-each-search-gate";
        }

        @Override
        public boolean applyGlobally() {
            return true;
        }

        @Override
        public Response transform(Request request, Response response, FileSource files, Parameters parameters) {
            CyclicBarrier barrier = cohort;
            if (barrier == null) {
                return response;
            }
            String searchId = searchIdOf(request);
            if (searchId == null || searchesAdmitted.putIfAbsent(searchId, Boolean.TRUE) != null) {
                // a sibling leg of a search already admitted - it must NOT occupy the barrier
                return response;
            }
            if (arrivals.getAndIncrement() >= cohortSize) {
                return response;
            }
            awaitCohort(barrier, searchId);
            return response;
        }

        private void awaitCohort(CyclicBarrier barrier, String searchId) {
            try {
                barrier.await(BARRIER_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                cleared.incrementAndGet();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                shortfalls.incrementAndGet();
            } catch (TimeoutException | BrokenBarrierException e) {
                // Recorded, NOT rethrown. Rethrowing would fail the stub, which Parallel Consumer would
                // read as the user function failing, and it would retry the search on the one second
                // default delay forever - the test would then report "offsets never commit" instead of
                // "the cohort never formed".
                shortfalls.incrementAndGet();
                log.warn("Only {} of {} distinct searches reached the fan-out barrier within {} (waiting for {})",
                        cleared.get(), cohortSize, BARRIER_TIMEOUT, searchId, e);
            }
        }

        /**
         * Identifies the originating record from the query parameters the fan-out carries - which is why
         * {@link FlightSearchApp} sends the route and the date on every leg.
         */
        private static String searchIdOf(Request request) {
            QueryParameter route = request.queryParameter(FlightSearchApp.ROUTE_PARAM);
            QueryParameter date = request.queryParameter(FlightSearchApp.DATE_PARAM);
            if (!route.isPresent() || !date.isPresent()) {
                return null;
            }
            return route.firstValue() + "/" + date.firstValue();
        }
    }

    /**
     * The app as the test drives it: the mock consumer goes in through the constructor (the seam that
     * survives a test moving package), the fan-out is pointed at the stub server's ephemeral port, and
     * the per-route in-flight counts are tracked so the ordering guarantee can be asserted.
     */
    static class FlightSearchAppUnderTest extends FlightSearchApp {

        final LongPollingMockConsumer<String, String> mockConsumer;

        final Map<String, AtomicInteger> inFlightByRoute = new ConcurrentHashMap<>();

        final Map<String, AtomicInteger> peakInFlightByRoute = new ConcurrentHashMap<>();

        final Map<String, List<String>> departureDatesByRoute = new ConcurrentHashMap<>();

        /**
         * Search ids already closed out, so {@link #finishSearch(FlightSearch, AsyncResult)} stays
         * idempotent in this subclass too.
         */
        final Set<String> finishedSearches = ConcurrentHashMap.newKeySet();

        private final int fareApiPort;

        private boolean closed;

        private FlightSearchAppUnderTest(LongPollingMockConsumer<String, String> mockConsumer, int fareApiPort) {
            super(mockConsumer);
            this.mockConsumer = mockConsumer;
            this.fareApiPort = fareApiPort;
        }

        static FlightSearchAppUnderTest create(int fareApiPort) {
            return new FlightSearchAppUnderTest(ExampleMockConsumers.spiedLongPollingMockConsumer(), fareApiPort);
        }

        @Override
        protected void postSetup() {
            mockConsumer.subscribeWithRebalanceAndAssignment(of(getInputTopic()), PARTITIONS);
        }

        @Override
        protected String getFareApiHost() {
            return "localhost";
        }

        @Override
        protected int getFareApiPort() {
            return fareApiPort;
        }

        @Override
        protected void beginSearch(FlightSearch search) {
            super.beginSearch(search);

            String route = search.getRoute();
            int nowInFlight = inFlightByRoute
                    .computeIfAbsent(route, key -> new AtomicInteger()).incrementAndGet();
            peakInFlightByRoute
                    .computeIfAbsent(route, key -> new AtomicInteger())
                    .accumulateAndGet(nowInFlight, Math::max);
            departureDatesByRoute
                    .computeIfAbsent(route, key -> new CopyOnWriteArrayList<>())
                    .add(search.getDepartureDate());
        }

        @Override
        protected void finishSearch(FlightSearch search, AsyncResult<FareQuote> outcome) {
            try {
                super.finishSearch(search, outcome);
            } finally {
                if (finishedSearches.add(search.getId())) {
                    AtomicInteger inFlight = inFlightByRoute.get(search.getRoute());
                    if (inFlight != null) {
                        inFlight.decrementAndGet();
                    }
                }
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
