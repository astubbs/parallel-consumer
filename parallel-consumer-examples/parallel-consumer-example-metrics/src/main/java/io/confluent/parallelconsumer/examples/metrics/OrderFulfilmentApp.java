package io.confluent.parallelconsumer.examples.metrics;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import com.sun.net.httpserver.HttpServer;
import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.ParallelStreamProcessor;
import io.confluent.parallelconsumer.examples.support.ConcurrencyObserver;
import io.confluent.parallelconsumer.examples.support.RunSummary;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.LongTaskTimer;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import pl.tlinkowski.unij.api.UniLists;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import static io.confluent.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.KEY;

/**
 * E-commerce order fulfilment: what a dashboard shows while orders are picked, packed and dispatched.
 * <p>
 * A retailer's order stream is keyed by customer, and every order moves through
 * {@link #FULFILMENT_STAGES} in that order - a customer's order must never be dispatched before it has
 * been packed. Each stage needs stock allocating in a warehouse management system, which blocks for
 * {@link FulfilmentService#DEFAULT_ALLOCATION_LATENCY} and sometimes has nothing to give.
 * <p>
 * That is the shape this example exists for. Processing the partition one order at a time makes peak
 * trading throughput a function of the warehouse system's latency. Processing it with no ordering at all
 * is faster and <em>wrong</em>. {@link ParallelConsumerOptions.ProcessingOrder#KEY} is the third option -
 * many customers in flight at once, never two stages of the same customer's order.
 * <p>
 * <b>The deliverable here is the signals, not the throughput.</b> This example sits in the metrics
 * module, so what it demonstrates is what an operator sees on Grafana while the above is happening:
 * <ul>
 *     <li><b>Latency distribution</b> - {@link #allocationTimer(String)}, a {@link Timer} with
 *     {@code publishPercentileHistogram()} and {@code serviceLevelObjectives(...)}, and deliberately NOT
 *     {@code publishPercentiles(...)}. Those two answer different questions and only one of them
 *     survives having more than one instance: client-side quantiles are computed per process and cannot
 *     be averaged, summed or otherwise aggregated afterwards, so the 99th percentile of a fleet is
 *     simply not recoverable from them. Buckets can - {@code histogram_quantile(0.99, sum by (le)
 *     (rate(order_allocation_seconds_bucket[5m])))} is a fleet-wide answer. Adding
 *     {@code publishPercentiles} on top would publish both (verified against this pinned registry: the
 *     {@code quantile=} series appear alongside the buckets, they are <em>not</em> dropped), which
 *     doubles the series for an answer nobody can aggregate.</li>
 *     <li><b>In-flight concurrency</b> - {@link #ordersInAllocation}, a {@link LongTaskTimer}. A plain
 *     {@code Timer} cannot answer "how many right now", because it only records on completion. Its
 *     histogram is deliberately omitted: the default long-task-timer buckets start at <em>120 seconds</em>,
 *     which is useless for sub-second order processing.</li>
 *     <li><b>Throughput</b> - the same Timer's own {@code _count}
 *     ({@code rate(order_allocation_seconds_count[1m])}). A separate counter would be redundant.</li>
 *     <li><b>Failure rate</b> - {@link #outcomeCounter(String)}, a {@link Counter} tagged by a bounded
 *     {@code outcome}. Never by order or customer id: see the comment on {@link #OUTCOMES}.</li>
 * </ul>
 * Beside these, Parallel Consumer publishes its own {@code pc_*} meters onto the same registry (wired in
 * the tagged region below), and {@link #close()} prints them so a reader sees real exposition text
 * without standing up Grafana.
 * <p>
 * <b>Micrometer package pin.</b> The registry imported here is {@code io.micrometer.prometheus}, not the
 * {@code io.micrometer.prometheusmetrics} package that Micrometer 1.13 moved it to. This module pins
 * {@code micrometer-registry-prometheus} to a pre-1.13 version on purpose; do not follow a tutorial that
 * shows the newer package.
 * <p>
 * <b>What is scaffolding and what is the library.</b> Only the regions tagged {@code orderFulfilment} and
 * {@code orderFulfilmentProcess} - the options builder, its meter-registry wiring and the poll call - are
 * Parallel Consumer. {@link #fulfilStage(ConsumerRecord)}, {@link ConcurrencyObserver} and
 * {@link RunSummary} are this
 * example's own instrumentation, and live outside the tags on purpose: they come from a module that is
 * never published to Maven Central, so a snippet that referenced them would not compile in the reader's
 * project.
 *
 * @see CoreApp the minimal example beside this one, which answers "how do I wire a registry up?"
 */
@Slf4j
public class OrderFulfilmentApp {

    public static final String METRICS_ENDPOINT = "/prometheus";

    /**
     * Deliberately NOT the port {@link CoreApp} binds. Both examples' tests run in the same forked JVM,
     * and {@link CoreApp} never stops its {@link HttpServer}, so a shared port would leave whichever
     * example ran second unable to bind at all. {@code src/test/resources/prometheus.yml} carries a
     * scrape target for each.
     */
    public static final int METRICS_PORT = 7002;

    /**
     * The stages an order moves through, in the order it moves through them. KEY ordering is what stops
     * them being applied out of order.
     */
    public static final List<String> FULFILMENT_STAGES = UniLists.of("PICKED", "PACKED", "DISPATCHED");

    /**
     * Base name of the allocation latency {@link Timer}. Exposed to Prometheus as
     * {@code order_allocation_seconds_bucket} / {@code _count} / {@code _sum}.
     */
    public static final String ALLOCATION_TIMER = "order.allocation";

    /**
     * Base name of the in-flight {@link LongTaskTimer}. Exposed to Prometheus as
     * {@code order_allocation_active_seconds_active_count} and {@code ..._duration_sum}.
     */
    public static final String ALLOCATION_ACTIVE_TIMER = "order.allocation.active";

    /**
     * Base name of the outcome {@link Counter}. Exposed to Prometheus as
     * {@code order_allocation_outcomes_total}.
     */
    public static final String ALLOCATION_OUTCOMES_COUNTER = "order.allocation.outcomes";

    public static final String OUTCOME_ALLOCATED = "allocated";

    public static final String OUTCOME_OUT_OF_STOCK = "out_of_stock";

    public static final String OUTCOME_ERROR = "error";

    /**
     * <b>The whole cardinality lesson of this example lives in this field.</b> The outcome tag draws from
     * this fixed set of three values, so the counter is three time series for ever, whatever the traffic.
     * <p>
     * Tagging by order id or customer id instead - {@code .tag("order", order.key())} - is the mistake
     * this example exists to visibly avoid. It looks harmless in a demo with four customers and produces
     * one time series per order in production: millions of series, each with its own chunk in Prometheus'
     * head block, and a query that is simultaneously unaffordable and useless, because nobody dashboards
     * a single order's failure rate. Identifiers belong in logs and traces, which are indexed for exactly
     * that; metric tags belong to dimensions with a small, <em>bounded</em> set of values -
     * {@code outcome} here, {@code warehouse} on the timer.
     */
    private static final List<String> OUTCOMES =
            UniLists.of(OUTCOME_ALLOCATED, OUTCOME_OUT_OF_STOCK, OUTCOME_ERROR);

    /**
     * The service level objectives the allocation timer publishes an explicit bucket for, on top of the
     * percentile histogram's own. These are the boundaries an on-call engineer actually alerts on.
     */
    private static final Duration[] ALLOCATION_SLOS = {
            Duration.ofMillis(50), Duration.ofMillis(100), Duration.ofMillis(250),
            Duration.ofMillis(500), Duration.ofSeconds(1)
    };

    /**
     * Bounds the percentile histogram. Without them the registry emits its full default range (1 ms to
     * 30 s) as buckets, which is a lot of series for a call known to take tens of milliseconds.
     */
    private static final Duration ALLOCATION_HISTOGRAM_MIN = Duration.ofMillis(10);

    private static final Duration ALLOCATION_HISTOGRAM_MAX = Duration.ofSeconds(2);

    /**
     * The ceiling handed to Parallel Consumer, and the figure the run summary reports beside the peak it
     * actually observed. Under KEY ordering the distinct customer count is usually the smaller cap.
     */
    static final int MAX_CONCURRENCY = 100;

    /**
     * Stable and bounded, not a random UUID per restart. It is a tag value on every {@code pc_*} series,
     * so a fresh value on every deploy would churn the series set for the same reason an order id would.
     */
    static final String PC_INSTANCE = "order-fulfilment";

    @Getter
    private final String inputTopic = "customer-orders";

    private final Map<String, String> envVars = System.getenv();

    /**
     * The registry both this example's own meters and Parallel Consumer's {@code pc_*} meters are
     * published on, and the one the {@link #METRICS_ENDPOINT} scrape endpoint serves.
     */
    @Getter
    private final PrometheusMeterRegistry meterRegistry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);

    /**
     * One timer per warehouse, registered up front. Registering the whole bounded set at startup is what
     * makes the cardinality of this family a property of the code rather than of the traffic.
     */
    private final Map<String, Timer> allocationTimers = new LinkedHashMap<>();

    private final Map<String, Counter> outcomeCounters = new LinkedHashMap<>();

    /**
     * How many orders are inside the warehouse allocation call <em>right now</em>. A
     * {@link LongTaskTimer}, because a plain timer only records on completion and so cannot answer that
     * question at all.
     */
    private final LongTaskTimer ordersInAllocation;

    /**
     * A consumer supplied via the constructor (e.g. a mock in tests), or {@code null} to build a real one
     * from the environment. Constructor injection rather than a package-private {@code getKafkaConsumer()}
     * the test overrides: the override seam relies on package-private access and breaks the moment a test
     * moves to another package - as this module's integration tests have.
     */
    private final Consumer<String, String> injectedConsumer;

    @Getter
    private final FulfilmentService fulfilmentService;

    /**
     * How much work was really in flight at once. Example scaffolding - see the class javadoc.
     */
    @Getter
    private final ConcurrencyObserver observer = new ConcurrencyObserver();

    private final Set<String> customersSeen = ConcurrentHashMap.newKeySet();

    private final Set<Integer> partitionsSeen = ConcurrentHashMap.newKeySet();

    private final AtomicLong stagesFulfilled = new AtomicLong();

    private final ExecutorService metricsEndpointExecutor = Executors.newSingleThreadExecutor();

    /**
     * Kept so {@link #close()} can stop it. {@link CoreApp} does not, which is why its port stays bound
     * for the life of the JVM.
     */
    private HttpServer metricsServer;

    /**
     * The summary {@link #close()} printed, kept so a test can assert on exactly what was emitted rather
     * than on a second one built for the occasion. Empty until the app has closed, and empty afterwards
     * if it never fulfilled a stage.
     */
    @Getter
    private Optional<RunSummary> emittedSummary = Optional.empty();

    /**
     * The {@code pc_*} exposition text {@link #close()} printed - the point being that the reader sees
     * what Prometheus actually scrapes, not a prose description of it. Sampled just before the processor
     * is closed, because closing it deregisters those meters.
     */
    @Getter
    private Optional<String> emittedMetricFamilies = Optional.empty();

    ParallelStreamProcessor<String, String> parallelConsumer;

    public OrderFulfilmentApp() {
        this(null, new FulfilmentService());
    }

    protected OrderFulfilmentApp(Consumer<String, String> injectedConsumer, FulfilmentService fulfilmentService) {
        this.injectedConsumer = injectedConsumer;
        this.fulfilmentService = fulfilmentService;
        for (String warehouse : FulfilmentService.knownWarehouses()) {
            allocationTimers.put(warehouse, buildAllocationTimer(warehouse));
        }
        for (String outcome : OUTCOMES) {
            outcomeCounters.put(outcome, buildOutcomeCounter(outcome));
        }
        this.ordersInAllocation = LongTaskTimer.builder(ALLOCATION_ACTIVE_TIMER)
                .description("Orders inside the warehouse allocation call right now")
                // NO histogram here on purpose: a LongTaskTimer's default buckets start at 120 SECONDS,
                // which would put every one of these sub-second allocations in the same bucket.
                .register(meterRegistry);
    }

    private Timer buildAllocationTimer(String warehouse) {
        return Timer.builder(ALLOCATION_TIMER)
                .description("Time taken to allocate stock for one order stage")
                // Bounded dimension: there are three distribution centres and there will still be three
                // when the order rate is a thousand times higher.
                .tag("warehouse", warehouse)
                // Server-side buckets, NOT publishPercentiles(): only buckets can be aggregated across
                // instances by histogram_quantile(). Adding percentiles as well publishes both families
                // (they are not dropped - measured), for an answer that cannot be aggregated.
                .publishPercentileHistogram()
                .serviceLevelObjectives(ALLOCATION_SLOS)
                .minimumExpectedValue(ALLOCATION_HISTOGRAM_MIN)
                .maximumExpectedValue(ALLOCATION_HISTOGRAM_MAX)
                .register(meterRegistry);
    }

    private Counter buildOutcomeCounter(String outcome) {
        return Counter.builder(ALLOCATION_OUTCOMES_COUNTER)
                .description("Order stages by how their warehouse allocation ended")
                // See the OUTCOMES javadoc: bounded tag values only, never an order or customer id.
                .tag("outcome", outcome)
                .register(meterRegistry);
    }

    /**
     * The latency timer for one distribution centre. Pre-registered, so this is a lookup and never a
     * registration - a meter registered from a request path is how an unbounded tag gets in.
     */
    Timer allocationTimer(String warehouse) {
        Timer timer = allocationTimers.get(warehouse);
        if (timer == null) {
            throw new IllegalArgumentException("no timer registered for warehouse '" + warehouse
                    + "' - the tag values are fixed at startup, see FulfilmentService.knownWarehouses()");
        }
        return timer;
    }

    Counter outcomeCounter(String outcome) {
        Counter counter = outcomeCounters.get(outcome);
        if (counter == null) {
            throw new IllegalArgumentException("no counter registered for outcome '" + outcome
                    + "' - the tag values are fixed at startup: " + OUTCOMES);
        }
        return counter;
    }

    /**
     * The setup half of the README snippet, tagged {@code orderFulfilment}. The poll call carries a tag of
     * its own, {@code orderFulfilmentProcess}, and the README includes the two as separate blocks in that
     * order: the generator (asciidoc-template-maven-plugin) reads only the FIRST region of any given tag
     * name, so a second region sharing this tag would be silently dropped rather than concatenated.
     */
    @SuppressWarnings({"FeatureEnvy", "MagicNumber"})
    ParallelStreamProcessor<String, String> setupParallelConsumer() {
        Consumer<String, String> kafkaConsumer = getKafkaConsumer();

        // tag::orderFulfilment[]
        var options = ParallelConsumerOptions.<String, String>builder()
                // Orders are keyed by customer, and a customer's order must never be dispatched before it
                // is packed. KEY ordering guarantees that - at most one stage per customer in flight, in
                // offset order - while DIFFERENT customers' orders still run in parallel. UNORDERED would
                // be faster and wrong; PARTITION would be correct and no faster than a plain consumer.
                .ordering(KEY) // <1>
                // Sized for the warehouse system's connection pool, not for the machine's core count:
                // these threads spend their lives blocked on it, which is the work PC overlaps
                .maxConcurrency(MAX_CONCURRENCY) // <2>
                .consumer(kafkaConsumer)
                // Hand PC the SAME registry the application's own meters use, so pc_* series sit beside
                // order_* series and a dashboard can put "orders in flight" next to "records in flight"
                .meterRegistry(meterRegistry) // <3>
                // Tags added to every pc_* meter. Bounded values only - these multiply the series count
                .metricsTags(Tags.of(Tag.of("service", "order-fulfilment"))) // <4>
                // Distinguishes this instance's pc_* series from another instance's. Stable, not a random
                // UUID per restart: a fresh value on each deploy churns the series set for no benefit
                .pcInstanceTag(PC_INSTANCE) // <5>
                .build();

        ParallelStreamProcessor<String, String> orderProcessor =
                ParallelStreamProcessor.createEosStreamProcessor(options);

        orderProcessor.subscribe(UniLists.of(inputTopic));

        return orderProcessor;
        // end::orderFulfilment[]
    }

    @SuppressWarnings("UnqualifiedFieldAccess")
    public void run() {
        this.parallelConsumer = setupParallelConsumer();

        postSetup();

        // tag::orderFulfilmentProcess[]
        parallelConsumer.poll(context -> { // <1>
            ConsumerRecord<String, String> order = context.getSingleRecord().getConsumerRecord();

            // fulfilStage() is THIS EXAMPLE's scaffolding - the meters and the per-order logging - and
            // deliberately lives outside this region. All it wraps is one blocking call to a warehouse
            // management system, which is where every millisecond of this example's latency is spent, and
            // which throws when there is no stock so that Parallel Consumer retries the order.
            fulfilStage(order); // <2>
        });
        // end::orderFulfilmentProcess[]
    }

    /**
     * Hook for tests to finish wiring their mock consumer once the processor has subscribed.
     */
    protected void postSetup() {
        setupPrometheusEndpoint();
    }

    /**
     * Allocates stock for one order stage, with this example's instrumentation around it.
     * <p>
     * Outside the tagged region on purpose (see the class javadoc): it is the part of the user function a
     * reader must NOT copy, because {@link ConcurrencyObserver} lives in a module that is never
     * published.
     * <p>
     * The meters are updated in a {@code finally} block, so a failed allocation is timed and counted
     * exactly like a successful one - a failure path that skipped the timer would quietly make the
     * latency distribution a success-only distribution, which is the most flattering way to be wrong.
     *
     * @param order the order stage being fulfilled
     * @throws FulfilmentService.StockUnavailableException when the warehouse has no stock for this order
     *                                                     - propagated, so Parallel Consumer retries it
     */
    protected void fulfilStage(ConsumerRecord<String, String> order) {
        String customerId = order.key();
        String stage = order.value();
        String warehouse = FulfilmentService.warehouseFor(customerId);

        try (ConcurrencyObserver.Scope ignored = observer.enter()) {
            log.info("[{}] order for {} {} at {} - {} stage(s) now in flight",
                    Thread.currentThread().getName(), customerId, stage, warehouse, observer.getInFlight());

            LongTaskTimer.Sample active = ordersInAllocation.start();
            Timer.Sample latency = Timer.start(meterRegistry);
            String outcome = OUTCOME_ERROR;
            try {
                String allocation = fulfilmentService.allocate(customerId, stage, warehouse);
                outcome = OUTCOME_ALLOCATED;

                customersSeen.add(customerId);
                partitionsSeen.add(order.partition());
                stagesFulfilled.incrementAndGet();

                log.info("[{}] order for {} {} - allocated {} from {}",
                        Thread.currentThread().getName(), customerId, stage, allocation, warehouse);
            } catch (FulfilmentService.StockUnavailableException e) {
                outcome = OUTCOME_OUT_OF_STOCK;
                throw e;
            } finally {
                latency.stop(allocationTimer(warehouse));
                active.stop();
                outcomeCounter(outcome).increment();
            }
        }
    }

    void setupPrometheusEndpoint() {
        try {
            HttpServer server = HttpServer.create(new InetSocketAddress(METRICS_PORT), 0);
            server.createContext(METRICS_ENDPOINT, httpExchange -> {
                byte[] response = meterRegistry.scrape().getBytes(StandardCharsets.UTF_8);
                httpExchange.sendResponseHeaders(200, response.length);
                try (OutputStream os = httpExchange.getResponseBody()) {
                    os.write(response);
                }
            });
            this.metricsServer = server;
            metricsEndpointExecutor.submit(server::start);
        } catch (IOException e) {
            throw new IllegalStateException("could not bind the metrics endpoint on port " + METRICS_PORT, e);
        }
    }

    /**
     * Drains and closes the processor, releases the scrape endpoint's port, then prints the run summary
     * and the {@code pc_*} exposition text.
     */
    public void close() {
        // Taken BEFORE the processor closes, and this ordering is load-bearing: closing Parallel Consumer
        // REMOVES its own meters from the registry again, so a scrape taken afterwards contains no pc_*
        // family at all - the closing report would silently print nothing.
        this.emittedMetricFamilies = Optional.of(parallelConsumerMetricFamilies());

        if (parallelConsumer != null) {
            parallelConsumer.close();
        }
        stopPrometheusEndpoint();

        this.emittedSummary = buildRunSummary();
        emittedSummary.ifPresent(RunSummary::log);

        log.info("Parallel Consumer's own meters, as Prometheus scraped them:{}{}",
                System.lineSeparator(), emittedMetricFamilies.get());
    }

    /**
     * Stops the scrape endpoint so the port is free again. {@link CoreApp} does not do this, which is
     * exactly why the two examples cannot share a port.
     */
    private void stopPrometheusEndpoint() {
        if (metricsServer != null) {
            metricsServer.stop(0);
            metricsServer = null;
        }
        metricsEndpointExecutor.shutdownNow();
    }

    /**
     * The {@code pc_*} families out of {@link PrometheusMeterRegistry#scrape()}, {@code HELP} and
     * {@code TYPE} comments included - real exposition text, so a reader can see what a dashboard is
     * built from without standing up Grafana.
     */
    String parallelConsumerMetricFamilies() {
        StringBuilder families = new StringBuilder();
        for (String line : meterRegistry.scrape().split("\\R")) {
            if (isParallelConsumerLine(line)) {
                families.append(line).append(System.lineSeparator());
            }
        }
        return families.toString();
    }

    private static boolean isParallelConsumerLine(String line) {
        return line.startsWith("pc_")
                || line.startsWith("# HELP pc_")
                || line.startsWith("# TYPE pc_");
    }

    /**
     * The summary's partition and key counts are what was actually OBSERVED, not what was configured: a
     * summary reporting a configured ceiling the data never contained would describe a run that did not
     * happen.
     */
    private Optional<RunSummary> buildRunSummary() {
        long stages = stagesFulfilled.get();
        if (stages < 1) {
            log.info("Order fulfilment closed without fulfilling a stage - nothing to summarise");
            return Optional.empty();
        }
        return Optional.of(RunSummary.builder()
                .exampleName("Order fulfilment (metrics)")
                .recordCount(stages)
                .partitionCount(partitionsSeen.size())
                .distinctKeys(customersSeen.size())
                .maxConcurrency(MAX_CONCURRENCY)
                .ordering(KEY)
                .simulatedLatency(fulfilmentService.getAllocationLatency())
                .observer(observer)
                .build());
    }

    private Consumer<String, String> getKafkaConsumer() {
        if (injectedConsumer != null) {
            return injectedConsumer;
        }
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, envVars.getOrDefault("BOOTSTRAP_SERVERS", "kafka:9092"));
        props.put(ConsumerConfig.GROUP_ID_CONFIG, envVars.getOrDefault("GROUP_ID", "order-fulfilment"));
        // Parallel Consumer commits the offsets itself, once the stock has actually been allocated
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        return new KafkaConsumer<>(props);
    }
}
