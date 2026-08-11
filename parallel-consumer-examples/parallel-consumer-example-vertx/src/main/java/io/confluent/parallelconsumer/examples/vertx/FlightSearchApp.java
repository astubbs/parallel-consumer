package io.confluent.parallelconsumer.examples.vertx;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.examples.support.ConcurrencyObserver;
import io.confluent.parallelconsumer.examples.support.RunSummary;
import io.confluent.parallelconsumer.vertx.VertxParallelEoSStreamProcessor;
import io.confluent.parallelconsumer.vertx.VertxParallelStreamProcessor;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpResponseExpectation;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import lombok.Getter;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import pl.tlinkowski.unij.api.UniLists;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import static io.confluent.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.KEY;

/**
 * Travel retail: flight searches, each priced by asking every fare provider at once.
 * <p>
 * A traveller asks for a route on a date. Nobody sells that answer from a single database - the price
 * comes from several fare providers, each a remote HTTP API of its own, and the cheapest of their
 * answers is the one shown. Searches are keyed by <em>route</em>, because a route's searches are the
 * ones whose cached results and price history must be applied in order; searches for different routes
 * are wholly independent.
 * <p>
 * That is the shape this example exists for, and it has <b>two</b> dimensions of concurrency, which is
 * what makes it worth reading beside the core one:
 * <ul>
 *     <li><b>Within one record</b>, the {@value #FAN_OUT_WIDTH} fare-provider calls are issued together
 *     and joined with {@link Future#all(List)}, so a search costs about the <em>slowest</em> provider,
 *     not the sum of all of them.</li>
 *     <li><b>Across records</b>, Parallel Consumer keeps {@link #MAX_CONCURRENCY} searches in flight,
 *     while {@link ParallelConsumerOptions.ProcessingOrder#KEY} ordering keeps one route's searches in
 *     offset order.</li>
 * </ul>
 * <p>
 * <b>Nothing here is a worker-per-record, and the logs must not imply one.</b> Parallel Consumer's
 * Vert.x engine forces its dispatch pool to a <em>single</em> thread
 * ({@code VertxParallelEoSStreamProcessor.setupWorkerPool(1)}) because the user function does not block:
 * it composes a {@link Future} and returns immediately. So the thread names in this example's log tell a
 * different story from the core example's - one dispatch thread, then Vert.x event loops. What is
 * genuinely concurrent is the <em>outstanding HTTP exchanges</em> and the <em>records they belong to</em>.
 * The run summary's "distinct worker threads" reads 1 here, and that is correct rather than a defect.
 * <p>
 * <b>Why {@code vertxFuture} and not the JStream API.</b> {@link VertxApp} beside this one uses
 * {@code JStreamVertxParallelStreamProcessor}, whose result deque is never cleared on close - an open
 * memory leak - and which the project intends to deprecate and remove. New code composes its own
 * {@link Future} and hands it to
 * {@link VertxParallelStreamProcessor#vertxFuture(java.util.function.Function)}.
 * <p>
 * <b>The connection pool is the trap, and it is why the {@link WebClient} is built here rather than left
 * to Parallel Consumer.</b> {@link VertxParallelEoSStreamProcessor}'s convenience constructor sizes
 * {@link WebClientOptions#setMaxPoolSize(int)} to {@link ParallelConsumerOptions#getMaxConcurrency()} -
 * correct for one request per record, and {@value #FAN_OUT_WIDTH} times too small for this fan-out. With
 * a pool that size two of every three legs would silently queue behind a connection, the fan-out would
 * degrade into a sequence, and nothing would report it. See {@link #setupParallelConsumer()}.
 * <p>
 * <b>Every leg recovers before the join.</b> {@link Future#all(List)} fails fast on the first failure, so
 * one unreachable provider would fail the whole search. Recovering each leg individually is both the
 * mechanical requirement and the domain-correct behaviour: a search that can still quote two providers
 * is a search, not an error.
 * <p>
 * This sits beside {@link VertxApp}, which answers "how do I call this?". This one answers "does it
 * actually do what it claims?" - so it logs the fan-out for every search and prints a {@link RunSummary}
 * when it closes.
 * <p>
 * <b>What is scaffolding and what is the library.</b> Only the regions tagged {@code flightSearch} and
 * {@code flightSearchProcess} - the options builder, the client construction and the
 * {@code vertxFuture} call - are Parallel Consumer. {@link #beginSearch(FlightSearch)},
 * {@link #finishSearch(FlightSearch, AsyncResult)},
 * {@link ConcurrencyObserver} and {@link RunSummary} are this example's own instrumentation, and live
 * outside the tags on purpose: they come from a module that is never published to Maven Central, so a
 * snippet that referenced them would not compile in the reader's project.
 */
@Slf4j
public class FlightSearchApp {

    /**
     * The fare providers every search asks, all at once. Fictional, but the shape is the real one: each
     * is an independent HTTP API on its own path, any of which can be slow or down without the search
     * being lost.
     */
    public static final List<FareProvider> FARE_PROVIDERS = UniLists.of(
            new FareProvider("Skylark", "/fares/skylark"),
            new FareProvider("Nimbus", "/fares/nimbus"),
            new FareProvider("Albatross", "/fares/albatross"));

    /**
     * How many provider calls one search fans out to. The number the connection pool has to be
     * multiplied by, and the reason a search costs one provider's latency rather than three.
     */
    public static final int FAN_OUT_WIDTH = 3;

    /**
     * What one fare-provider call is taken to cost. Simulated by the provider stubs, not by anything in
     * this class - a sleep here would block a Vert.x event loop and trip Vert.x's own
     * {@code BlockedThreadChecker}. Because the legs are concurrent, this is also what a whole
     * <em>search</em> costs, which is what the run summary reports as the per-record latency.
     */
    public static final Duration FARE_LOOKUP_LATENCY = Duration.ofMillis(100);

    /**
     * Searches in flight across routes. Under {@link ParallelConsumerOptions.ProcessingOrder#KEY}
     * ordering the distinct route count is usually the smaller cap, which is what the run summary
     * reports beside this.
     */
    public static final int MAX_CONCURRENCY = 16;

    /**
     * Query parameter naming the route being priced - and, with {@link #DATE_PARAM}, what identifies the
     * originating record in the provider's own request log.
     */
    public static final String ROUTE_PARAM = "route";

    public static final String DATE_PARAM = "date";

    /**
     * Separates the two fields of a provider's answer: {@code <currency> <totalMinorUnits>}, e.g.
     * {@code GBP 31950}.
     * <p>
     * A deliberately trivial wire format. A real fare API answers JSON, and Vert.x would parse it with
     * {@code response.bodyAsJsonObject()} - but this example is about the fan-out, and a body it can
     * read with {@link String#split(String, int)} keeps the reader's eye on that. Prices are in minor
     * units (pence, cents), so comparing two fares never needs floating point.
     */
    public static final String FARE_FIELD_SEPARATOR = " ";

    public static final String DEFAULT_CURRENCY = "GBP";

    private static final int MINOR_UNITS_PER_UNIT = 100;

    @Getter
    private final String inputTopic = "flight-searches";

    private final Map<String, String> envVars = System.getenv();

    /**
     * A consumer supplied via the constructor (e.g. a mock in tests), or {@code null} to build a real one
     * from the environment. Constructor injection rather than a package-private {@code getKafkaConsumer()}
     * the test overrides: the override seam relies on package-private access and breaks the moment a test
     * moves to another package.
     */
    private final Consumer<String, String> injectedConsumer;

    /**
     * How many searches were really in flight at once. Example scaffolding - see the class javadoc.
     */
    @Getter
    private final ConcurrencyObserver observer = new ConcurrencyObserver();

    /**
     * One open observation scope per search being priced, keyed by {@link FlightSearch#getId()}. Opened
     * in {@link #beginSearch(FlightSearch)} and closed in {@link #finishSearch(FlightSearch, AsyncResult)},
     * because a search's scope spans an asynchronous fan-out rather than one method call - the user
     * function has already returned by the time the providers answer.
     */
    private final Map<String, ConcurrencyObserver.Scope> openScopes = new ConcurrentHashMap<>();

    /**
     * The cheapest quote each completed search settled on, keyed by {@link FlightSearch#getId()}. Kept so
     * a test can assert on what the fan-out actually produced rather than only on offsets moving.
     */
    private final Map<String, FareQuote> cheapestBySearch = new ConcurrentHashMap<>();

    private final Set<String> routesSeen = ConcurrentHashMap.newKeySet();

    private final Set<Integer> partitionsSeen = ConcurrentHashMap.newKeySet();

    /**
     * Threads the fan-out's completion actually ran on - Vert.x event loops, never a pool of Parallel
     * Consumer workers. Named in the closing log because this example's story is in-flight requests, not
     * thread count.
     */
    @Getter
    private final Set<String> completionThreads = ConcurrentHashMap.newKeySet();

    private final AtomicLong searchesPriced = new AtomicLong();

    /**
     * Provider calls that came back unusable - a non-200, a connection failure, an unparseable body - and
     * were recovered so the rest of the search could continue.
     */
    private final AtomicLong unavailableLegs = new AtomicLong();

    /**
     * The summary {@link #close()} printed, kept so a test can assert on exactly what was emitted rather
     * than on a second one built for the occasion. Empty until the app has closed, and empty afterwards
     * if it never priced a search.
     */
    @Getter
    private Optional<RunSummary> emittedSummary = Optional.empty();

    /**
     * Created in {@link #setupParallelConsumer()} and handed to Parallel Consumer, which closes both it
     * and the {@link Vertx} instance when the processor closes - so this class must not close them again.
     */
    private WebClient fareApiClient;

    VertxParallelStreamProcessor<String, String> parallelConsumer;

    public FlightSearchApp() {
        this(null);
    }

    protected FlightSearchApp(Consumer<String, String> injectedConsumer) {
        this.injectedConsumer = injectedConsumer;
    }

    /**
     * Builds the value one flight search carries: the departure date being priced.
     */
    public static String searchValue(String departureDate) {
        if (departureDate == null || departureDate.trim().isEmpty()) {
            throw new IllegalArgumentException("departureDate must be set - a search is a route on a date");
        }
        return departureDate.trim();
    }

    /**
     * Renders a price in minor units as a human figure, e.g. {@code 18999} as {@code GBP 189.99}.
     */
    public static String renderPrice(String currency, int totalMinorUnits) {
        return String.format(Locale.ROOT, "%s %d.%02d",
                currency == null ? DEFAULT_CURRENCY : currency,
                totalMinorUnits / MINOR_UNITS_PER_UNIT,
                totalMinorUnits % MINOR_UNITS_PER_UNIT);
    }

    /**
     * The setup half of the README snippet, tagged {@code flightSearch}. The {@code vertxFuture} call
     * carries a tag of its own, {@code flightSearchProcess}, and the README includes the two as separate
     * blocks in that order: the generator (asciidoc-template-maven-plugin) reads only the FIRST region of
     * any given tag name, so a second region sharing this tag would be silently dropped rather than
     * concatenated.
     */
    @SuppressWarnings("FeatureEnvy")
    VertxParallelStreamProcessor<String, String> setupParallelConsumer() {
        Consumer<String, String> kafkaConsumer = getKafkaConsumer();

        // tag::flightSearch[]
        var options = ParallelConsumerOptions.<String, String>builder()
                // Searches are keyed by route, and a route's searches must be priced in offset order -
                // a later search must not overwrite a newer cached fare with an older one. KEY ordering
                // guarantees that (at most one search per route in flight, in offset order) while
                // searches for DIFFERENT routes are priced in parallel.
                .ordering(KEY) // <1>
                // Searches in flight ACROSS routes. Each of them is FAN_OUT_WIDTH outstanding HTTP
                // requests, not one - which is what the connection pool below has to be sized for.
                .maxConcurrency(MAX_CONCURRENCY) // <2>
                .consumer(kafkaConsumer)
                .build();

        // ONE WebClient for the whole application, reused by every record. Vert.x's own guidance: a
        // client per record throws away connection pooling and leaks the connections it opened.
        //
        // And it is built HERE rather than left to Parallel Consumer, because PC's convenience
        // constructor sizes the pool to maxConcurrency - one connection per record in flight. This
        // example issues FAN_OUT_WIDTH requests per record, so that pool would be three times too
        // small: two of every three legs would silently queue behind a connection, the fan-out would
        // degrade into a sequence, and no error would say so. Size it providers x maxConcurrency.
        Vertx searchVertx = Vertx.vertx();
        WebClient fareApi = WebClient.create(searchVertx, new WebClientOptions()
                .setMaxPoolSize(FAN_OUT_WIDTH * MAX_CONCURRENCY)
                .setHttp2MaxPoolSize(FAN_OUT_WIDTH * MAX_CONCURRENCY)); // <3>

        // The three-argument constructor is the one that accepts a pre-configured client; the
        // single-argument one would build its own, with the pool sizing described above.
        VertxParallelStreamProcessor<String, String> flightSearchProcessor =
                new VertxParallelEoSStreamProcessor<>(searchVertx, fareApi, options);

        flightSearchProcessor.subscribe(UniLists.of(inputTopic));
        // end::flightSearch[]

        this.fareApiClient = fareApi;
        return flightSearchProcessor;
    }

    @SuppressWarnings("UnqualifiedFieldAccess")
    public void run() {
        this.parallelConsumer = setupParallelConsumer();

        postSetup();

        String fareApiHost = getFareApiHost();
        int fareApiPort = getFareApiPort();
        WebClient fareApi = this.fareApiClient;

        // tag::flightSearchProcess[]
        // vertxFuture() is the non-blocking entry point: return the Future your composition produces and
        // Parallel Consumer completes the record when that Future does. NOTHING blocks here - PC's
        // Vert.x engine deliberately dispatches on a SINGLE thread, because the user function's job is
        // to start work, not to wait for it. What overlaps is the outstanding HTTP exchanges and the
        // records they belong to, not a pool of worker threads.
        parallelConsumer.vertxFuture(context -> { // <1>
            ConsumerRecord<String, String> record = context.getSingleRecord().getConsumerRecord();
            FlightSearch search = FlightSearch.parse(record.key(), record.partition(), record.value());

            // beginSearch() / finishSearch() are THIS EXAMPLE's scaffolding - the concurrency
            // observation and the per-search logging - and deliberately live outside this region.
            beginSearch(search);

            // FAN OUT. Every provider is asked at once: the loop only builds Futures, it does not wait
            // for any of them, so all FAN_OUT_WIDTH requests are on the wire together and the search
            // costs the SLOWEST provider rather than the sum of them.
            List<Future<FareQuote>> legs = new ArrayList<>(FARE_PROVIDERS.size());
            for (FareProvider provider : FARE_PROVIDERS) { // <2>
                legs.add(fareApi
                        .get(fareApiPort, fareApiHost, provider.getPath())
                        .addQueryParam(ROUTE_PARAM, search.getRoute())
                        .addQueryParam(DATE_PARAM, search.getDepartureDate())
                        .send()
                        // expecting() is declared on io.vertx.core.Future, NOT on HttpRequest - so the
                        // status check goes AFTER send(), not on the request builder. HttpRequest only
                        // has the deprecated expect(ResponsePredicate), and
                        // client.get(...).expecting(...).send() does not compile.
                        .expecting(HttpResponseExpectation.SC_OK)
                        .map(response -> FareQuote.parse(provider, response.bodyAsString()))
                        // RECOVER PER LEG, BEFORE THE JOIN. Future.all fails fast on the first failure,
                        // so without this one unreachable provider would fail the whole search. It is
                        // also the domain-correct behaviour: two quotes still make a search.
                        .recover(failure -> Future.succeededFuture(FareQuote.unavailable(provider, failure)))); // <3>
            }

            // JOIN. Future.all(List<? extends Future<?>>) - NOT CompositeFuture.all(List), which is
            // deprecated and takes a raw list. Future.all returns the CompositeFuture directly.
            return Future.all(legs) // <4>
                    .map(joined -> cheapestOf(search, joined.<FareQuote>list()))
                    .onComplete(outcome -> finishSearch(search, outcome));
        });
        // end::flightSearchProcess[]
    }

    /**
     * Hook for tests to finish wiring their mock consumer once the processor has subscribed.
     */
    protected void postSetup() {
        // nothing to do when running against a real broker
    }

    /**
     * Where the fare providers are reachable. Overridable so a test can point the fan-out at a stub
     * server on an ephemeral port.
     */
    protected String getFareApiHost() {
        return envVars.getOrDefault("FARE_API_HOST", "localhost");
    }

    /**
     * @see #getFareApiHost()
     */
    protected int getFareApiPort() {
        return Integer.parseInt(envVars.getOrDefault("FARE_API_PORT", "8080"));
    }

    /**
     * Marks a search as in flight, with this example's instrumentation around it.
     * <p>
     * Outside the tagged region on purpose (see the class javadoc). Overridable so a test can watch what
     * happens at the start of a search without the example having to grow a test hook of its own.
     * <p>
     * Runs on Parallel Consumer's single dispatch thread - the returned {@link Future} is what carries
     * the work, so this method must never block.
     */
    protected void beginSearch(FlightSearch search) {
        openScopes.put(search.getId(), observer.enter());
        routesSeen.add(search.getRoute());
        partitionsSeen.add(search.getPartition());
        log.info("[{}] {} on {} - asking {} fare provider(s) at once; {} search(es) now in flight",
                Thread.currentThread().getName(), search.getRoute(), search.getDepartureDate(),
                FARE_PROVIDERS.size(), observer.getInFlight());
    }

    /**
     * Closes a search's observation scope, whatever the fan-out's outcome was, and records the fare it
     * settled on.
     * <p>
     * Runs on a Vert.x event loop, not on the dispatch thread that opened the scope - which is precisely
     * the point of the example, and why the scope is held in a map rather than in a try-with-resources.
     * <p>
     * Idempotent: the scope's removal is the guard, so a second completion signal cannot double-count the
     * search or drive the in-flight count negative.
     */
    protected void finishSearch(FlightSearch search, AsyncResult<FareQuote> outcome) {
        ConcurrencyObserver.Scope scope = openScopes.remove(search.getId());
        if (scope == null) {
            return;
        }
        scope.close();
        completionThreads.add(Thread.currentThread().getName());

        if (outcome.failed()) {
            // Not swallowed: the Future Parallel Consumer is holding has already failed, so it will retry
            // this search. This line is what tells the operator why.
            log.warn("[{}] {} on {} - no fare could be priced; Parallel Consumer will retry the search",
                    Thread.currentThread().getName(), search.getRoute(), search.getDepartureDate(),
                    outcome.cause());
            return;
        }

        FareQuote cheapest = outcome.result();
        cheapestBySearch.put(search.getId(), cheapest);
        searchesPriced.incrementAndGet();
        log.info("[{}] {} on {} - cheapest is {} at {}; {} search(es) still in flight",
                Thread.currentThread().getName(), search.getRoute(), search.getDepartureDate(),
                cheapest.getProviderName(), cheapest.render(), observer.getInFlight());
    }

    /**
     * Picks the cheapest usable quote, and counts the ones that were not usable.
     * <p>
     * Example domain logic rather than library surface, but it is where the {@code recover}-before-join
     * decision becomes visible: the unusable legs arrive here as ordinary quotes marked unavailable, and
     * the search proceeds on whatever is left.
     *
     * @throws NoFareAvailableException when every provider failed - a genuine failure, propagated so
     *                                  Parallel Consumer retries the search rather than committing a
     *                                  search that was never answered
     */
    private FareQuote cheapestOf(FlightSearch search, List<FareQuote> quotes) {
        FareQuote cheapest = null;
        for (FareQuote quote : quotes) {
            if (!quote.isAvailable()) {
                unavailableLegs.incrementAndGet();
                log.warn("[{}] {} on {} - {} could not quote ({}); the search continues on the remaining provider(s)",
                        Thread.currentThread().getName(), search.getRoute(), search.getDepartureDate(),
                        quote.getProviderName(), quote.getUnavailableReason());
                continue;
            }
            if (cheapest == null || quote.getTotalMinorUnits() < cheapest.getTotalMinorUnits()) {
                cheapest = quote;
            }
        }
        if (cheapest == null) {
            throw new NoFareAvailableException("none of the " + FARE_PROVIDERS.size()
                    + " fare providers could quote " + search.getRoute()
                    + " on " + search.getDepartureDate());
        }
        return cheapest;
    }

    /**
     * Drains and closes the processor, then prints the run summary - which is the only place this example
     * reports numbers, and reports them as a demonstration rather than as a benchmark.
     * <p>
     * Closing the processor also closes the {@link WebClient} and the {@link Vertx} instance it was
     * given, so neither is closed again here.
     */
    public void close() {
        if (parallelConsumer != null) {
            parallelConsumer.closeDrainFirst();
        }
        logFanOutShape();
        this.emittedSummary = buildRunSummary();
        emittedSummary.ifPresent(RunSummary::log);
    }

    /**
     * States what was actually concurrent, because the run summary's thread figures would otherwise
     * invite the wrong reading: this engine has exactly one dispatch thread by design, and a reader who
     * takes "distinct worker threads: 1" for "nothing ran in parallel" has learnt the opposite of the
     * lesson.
     */
    private void logFanOutShape() {
        log.info("Fan-out shape - {} search(es) priced, each issuing {} concurrent fare lookups "
                        + "({} leg(s) in total, {} of which the provider could not serve and were recovered). "
                        + "Parallel Consumer's Vert.x engine dispatches on a SINGLE thread by design, so "
                        + "'distinct worker threads' below reads 1: what overlapped is the outstanding HTTP "
                        + "requests and the {} search(es) they belonged to, completing on Vert.x event loop(s) {}. "
                        + "Connection pool sized to {} = {} providers x maxConcurrency {}, because PC's own "
                        + "default assumes one request per record.",
                searchesPriced.get(), FARE_PROVIDERS.size(),
                searchesPriced.get() * FARE_PROVIDERS.size(), unavailableLegs.get(),
                observer.getPeakInFlight(), sortedCompletionThreads(),
                FAN_OUT_WIDTH * MAX_CONCURRENCY, FAN_OUT_WIDTH, MAX_CONCURRENCY);
    }

    /**
     * The summary's partition and route counts are what was actually OBSERVED, not what was configured: a
     * summary that reported a configured ceiling the data never contained would be describing a run that
     * did not happen.
     */
    private Optional<RunSummary> buildRunSummary() {
        long searches = searchesPriced.get();
        if (searches < 1) {
            log.info("Flight search closed without pricing a search - nothing to summarise");
            return Optional.empty();
        }
        return Optional.of(RunSummary.builder()
                .exampleName("Flight search (vertx)")
                .recordCount(searches)
                .partitionCount(partitionsSeen.size())
                .distinctKeys(routesSeen.size())
                .maxConcurrency(MAX_CONCURRENCY)
                .ordering(KEY)
                // The legs are concurrent, so ONE search costs one provider's latency - not three
                .simulatedLatency(FARE_LOOKUP_LATENCY)
                .observer(observer)
                .build());
    }

    /**
     * Searches that completed with a fare.
     */
    public long getSearchesPriced() {
        return searchesPriced.get();
    }

    /**
     * Provider calls that came back unusable and were recovered, across the whole run.
     */
    public long getUnavailableLegs() {
        return unavailableLegs.get();
    }

    /**
     * The cheapest quote each completed search settled on, keyed by {@link FlightSearch#getId()}.
     */
    public Map<String, FareQuote> getCheapestBySearch() {
        return Collections.unmodifiableMap(cheapestBySearch);
    }

    private List<String> sortedCompletionThreads() {
        List<String> names = new ArrayList<>(completionThreads);
        Collections.sort(names);
        return names;
    }

    private Consumer<String, String> getKafkaConsumer() {
        if (injectedConsumer != null) {
            return injectedConsumer;
        }
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, envVars.getOrDefault("BOOTSTRAP_SERVERS", "kafka:9092"));
        props.put(ConsumerConfig.GROUP_ID_CONFIG, envVars.getOrDefault("GROUP_ID", "flight-search"));
        // Parallel Consumer commits the offsets itself, once every provider has answered or been recovered
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        return new KafkaConsumer<>(props);
    }

    /**
     * One fare provider: what to call it in the logs, and the path its API answers on.
     */
    @Value
    public static class FareProvider {

        String name;

        String path;
    }

    /**
     * One search request: the route it was keyed by, the partition it arrived on, and the departure date
     * being priced.
     */
    @Value
    public static class FlightSearch {

        /**
         * The record's key. KEY ordering is what stops one route's searches being priced out of order.
         */
        String route;

        /**
         * The partition the search arrived on. Carried so the run summary can report the partition count
         * "peak in-flight" has to be read against.
         */
        int partition;

        String departureDate;

        public static FlightSearch parse(String route, int partition, String value) {
            if (route == null || route.trim().isEmpty()) {
                throw new IllegalArgumentException("a flight search must be keyed by its route");
            }
            if (value == null || value.trim().isEmpty()) {
                throw new IllegalArgumentException("search for route " + route
                        + " carried no departure date - a search is a route on a date");
            }
            return new FlightSearch(route, partition, value.trim());
        }

        /**
         * Identifies this search among the searches in flight - a route and the date it prices.
         */
        public String getId() {
            return route + "/" + departureDate;
        }
    }

    /**
     * One provider's answer: either a price, or the reason there is not one.
     */
    @Value
    public static class FareQuote {

        String providerName;

        String currency;

        /**
         * The fare in minor units, or {@link Integer#MAX_VALUE} when {@link #available} is false, so an
         * unavailable quote can never win a comparison by accident.
         */
        int totalMinorUnits;

        boolean available;

        /**
         * Why the provider could not quote, or {@code null} when it did.
         */
        String unavailableReason;

        /**
         * Reads {@code <currency> <totalMinorUnits>}, e.g. {@code GBP 31950}.
         *
         * @throws IllegalArgumentException if the provider answered 200 with a body this example cannot
         *                                  read - recovered by the leg's {@code recover}, and counted
         *                                  as an unavailable provider
         */
        public static FareQuote parse(FareProvider provider, String body) {
            String[] fields = body == null ? new String[0] : body.trim().split(FARE_FIELD_SEPARATOR, 2);
            if (fields.length != 2) {
                throw new IllegalArgumentException("fare provider " + provider.getName()
                        + " answered with a body this search cannot read - expected '<currency>"
                        + FARE_FIELD_SEPARATOR + "<totalMinorUnits>', was: '" + body + "'");
            }
            try {
                return new FareQuote(provider.getName(), fields[0], Integer.parseInt(fields[1].trim()), true, null);
            } catch (NumberFormatException e) {
                // Classified, never swallowed: an unreadable price is recovered as an unavailable
                // provider, and the reason travels with it into the log.
                throw new IllegalArgumentException("fare provider " + provider.getName()
                        + " quoted a non-numeric fare: '" + fields[1] + "'", e);
            }
        }

        public static FareQuote unavailable(FareProvider provider, Throwable failure) {
            String reason = failure == null || failure.getMessage() == null
                    ? String.valueOf(failure)
                    : failure.getMessage();
            return new FareQuote(provider.getName(), null, Integer.MAX_VALUE, false, reason);
        }

        public String render() {
            return available ? renderPrice(currency, totalMinorUnits) : "unavailable (" + unavailableReason + ")";
        }
    }

    /**
     * Every fare provider failed, so there is no answer to give the traveller. An ordinary unchecked
     * exception, so Parallel Consumer's retry behaviour sees exactly what a real failure would look like.
     */
    public static class NoFareAvailableException extends RuntimeException {

        private static final long serialVersionUID = 1L;

        public NoFareAvailableException(String message) {
            super(message);
        }
    }
}
