package io.confluent.parallelconsumer.examples.core;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.ParallelStreamProcessor;
import io.confluent.parallelconsumer.examples.support.ConcurrencyObserver;
import io.confluent.parallelconsumer.examples.support.RunSummary;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import pl.tlinkowski.unij.api.UniLists;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import static io.confluent.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.KEY;

/**
 * Parcel logistics: a carrier's scan events, geocoded as they arrive.
 * <p>
 * A courier network emits a scan every time a parcel is handled - picked up, arrived at a depot, loaded
 * for delivery, delivered. Each scan names the depot that handled it, and the tracking page needs the
 * depot's coordinates, which come from a geocoding API that blocks for
 * {@link GeocodeService#DEFAULT_LATENCY} and is sometimes unavailable. Scans are keyed by consignment id,
 * and one topic partition carries the scans of many consignments.
 * <p>
 * That is the shape this example exists for. Processing the partition one scan at a time makes the
 * network's throughput a function of the geocoder's latency, not of the depots' workload. Processing it
 * with no ordering at all is faster and <em>wrong</em>: a consignment's status must never go backwards.
 * Parallel Consumer's {@link ParallelConsumerOptions.ProcessingOrder#KEY} ordering is the third option -
 * many consignments in flight at once, but never two scans of the <em>same</em> consignment.
 * <p>
 * This sits beside {@link CoreApp}, which answers "how do I call this?". This one answers "does it
 * actually do what it claims?" - so it logs the thread, the consignment and the in-flight count for every
 * scan, and prints a {@link RunSummary} when it closes.
 * <p>
 * <b>What is scaffolding and what is the library.</b> Only the regions tagged {@code parcelTracking} and
 * {@code parcelTrackingProcess} - the options builder and the poll call - are Parallel Consumer.
 * {@link #trackScan(ConsumerRecord)}, {@link ConcurrencyObserver} and {@link RunSummary} are this
 * example's own instrumentation, and live outside the tags on purpose: they come from a module that is
 * never published to Maven Central, so a snippet that referenced them would not compile in the reader's
 * project.
 */
@Slf4j
public class ParcelTrackingApp {

    /**
     * The statuses a consignment moves through, in the order it moves through them. KEY ordering is what
     * stops them being applied out of order.
     */
    public static final List<String> SCAN_STATUSES =
            UniLists.of("PICKED_UP", "AT_DEPOT", "OUT_FOR_DELIVERY", "DELIVERED");

    /**
     * Separates the two fields of a scan's value: {@code <status>@<depot>}. Not a regex metacharacter, so
     * it can be handed to {@link String#split(String, int)} as-is.
     */
    static final String SCAN_FIELD_SEPARATOR = "@";

    /**
     * The ceiling handed to Parallel Consumer, and the figure the run summary reports beside the peak it
     * actually observed. Under KEY ordering the distinct consignment count is usually the smaller cap.
     */
    static final int MAX_CONCURRENCY = 100;

    @Getter
    private final String inputTopic = "parcel-scans";

    @Getter
    private final String positionTopic = "parcel-positions";

    private final Map<String, String> envVars = System.getenv();

    /**
     * A consumer supplied via the constructor (e.g. a mock in tests), or {@code null} to build a real one
     * from the environment. Constructor injection rather than a package-private
     * {@code getKafkaConsumer()} the test overrides: the override seam relies on package-private access
     * and breaks the moment a test moves to another package.
     */
    private final Consumer<String, String> injectedConsumer;

    /**
     * @see #injectedConsumer
     */
    private final Producer<String, String> injectedProducer;

    @Getter
    private final GeocodeService geocodeService;

    /**
     * How much work was really in flight at once. Example scaffolding - see the class javadoc.
     */
    @Getter
    private final ConcurrencyObserver observer = new ConcurrencyObserver();

    private final Set<String> consignmentsSeen = ConcurrentHashMap.newKeySet();

    private final Set<Integer> partitionsSeen = ConcurrentHashMap.newKeySet();

    private final AtomicLong scansTracked = new AtomicLong();

    /**
     * The summary {@link #close()} printed, kept so a test can assert on exactly what was emitted rather
     * than on a second one built for the occasion. Empty until the app has closed, and empty afterwards if
     * it never tracked a scan.
     */
    @Getter
    private Optional<RunSummary> emittedSummary = Optional.empty();

    ParallelStreamProcessor<String, String> parallelConsumer;

    public ParcelTrackingApp() {
        this(null, null);
    }

    protected ParcelTrackingApp(Consumer<String, String> injectedConsumer, Producer<String, String> injectedProducer) {
        this.injectedConsumer = injectedConsumer;
        this.injectedProducer = injectedProducer;
        this.geocodeService = new GeocodeService();
    }

    /**
     * Builds the value a parcel scan carries: {@code <status>@<depot>}.
     */
    public static String scanValue(String status, String depot) {
        return status + SCAN_FIELD_SEPARATOR + depot;
    }

    static String statusOf(String scanValue) {
        return fieldOf(scanValue, 0);
    }

    static String depotOf(String scanValue) {
        return fieldOf(scanValue, 1);
    }

    private static String fieldOf(String scanValue, int index) {
        if (scanValue == null) {
            throw new IllegalArgumentException("a parcel scan's value must not be null");
        }
        String[] fields = scanValue.split(SCAN_FIELD_SEPARATOR, 2);
        if (fields.length != 2 || fields[0].trim().isEmpty() || fields[1].trim().isEmpty()) {
            throw new IllegalArgumentException("a parcel scan's value must be '<status>"
                    + SCAN_FIELD_SEPARATOR + "<depot>', was: '" + scanValue + "'");
        }
        return fields[index];
    }

    /**
     * The setup half of the README snippet, tagged {@code parcelTracking}. The poll call carries a tag of
     * its own, {@code parcelTrackingProcess}, and the README includes the two as separate blocks in that
     * order: the generator (asciidoc-template-maven-plugin) reads only the FIRST region of any given tag
     * name, so a second region sharing this tag would be silently dropped rather than concatenated.
     */
    @SuppressWarnings({"FeatureEnvy", "MagicNumber"})
    ParallelStreamProcessor<String, String> setupParallelConsumer() {
        Consumer<String, String> kafkaConsumer = getKafkaConsumer();
        Producer<String, String> kafkaProducer = getKafkaProducer();

        // tag::parcelTracking[]
        var options = ParallelConsumerOptions.<String, String>builder()
                // Scans are keyed by consignment id, and a consignment's status must never go backwards:
                // DELIVERED must not be applied before OUT_FOR_DELIVERY. KEY ordering is what guarantees
                // that - at most one scan per consignment is in flight, in offset order - while scans for
                // DIFFERENT consignments still run in parallel. UNORDERED would be faster and wrong here;
                // PARTITION would be correct and no faster than a plain consumer.
                .ordering(KEY) // <1>
                // Sized for the geocoding API's connection pool, not for the machine's core count: these
                // threads spend their lives blocked on a network call, which is the work PC overlaps
                .maxConcurrency(MAX_CONCURRENCY) // <2>
                .consumer(kafkaConsumer)
                .producer(kafkaProducer)
                .build();

        ParallelStreamProcessor<String, String> parcelScanProcessor =
                ParallelStreamProcessor.createEosStreamProcessor(options);

        parcelScanProcessor.subscribe(UniLists.of(inputTopic));

        return parcelScanProcessor;
        // end::parcelTracking[]
    }

    @SuppressWarnings("UnqualifiedFieldAccess")
    public void run() {
        this.parallelConsumer = setupParallelConsumer();

        postSetup();

        // tag::parcelTrackingProcess[]
        parallelConsumer.pollAndProduce(context -> { // <1>
            ConsumerRecord<String, String> scan = context.getSingleRecord().getConsumerRecord();

            // trackScan() is THIS EXAMPLE's scaffolding - the concurrency observation and the per-scan
            // logging - and deliberately lives outside this region. All it wraps is one blocking call to
            // a geocoding API, which is where every millisecond of this example's latency is spent, and
            // which throws when the API is unavailable so that Parallel Consumer retries the scan.
            String position = trackScan(scan); // <2>

            return new ProducerRecord<>(positionTopic, scan.key(), position);
        }, result -> log.debug("Position for consignment {} written at offset {}",
                result.getOut().key(), result.getMeta().offset()));
        // end::parcelTrackingProcess[]
    }

    /**
     * Hook for tests to finish wiring their mock consumer once the processor has subscribed.
     */
    protected void postSetup() {
        // nothing to do when running against a real broker
    }

    /**
     * Geocodes one scan, with this example's instrumentation around it.
     * <p>
     * Outside the tagged region on purpose (see the class javadoc): it is the only part of the user
     * function that a reader must NOT copy, because {@link ConcurrencyObserver} lives in a module that is
     * never published. Overridable so a test can watch what happens inside the user function without the
     * example having to grow a test hook of its own.
     *
     * @param scan the parcel scan being processed
     * @return the scanning depot's coordinate, as "latitude,longitude"
     * @throws GeocodeService.GeocodeUnavailableException when the geocoding API is down for this scan -
     *                                                    propagated, so Parallel Consumer retries it
     */
    protected String trackScan(ConsumerRecord<String, String> scan) {
        String consignmentId = scan.key();
        String status = statusOf(scan.value());
        String depot = depotOf(scan.value());

        try (ConcurrencyObserver.Scope ignored = observer.enter()) {
            log.info("[{}] consignment {} {} at {} - {} scan(s) now in flight",
                    Thread.currentThread().getName(), consignmentId, status, depot, observer.getInFlight());

            String position = geocodeService.lookup(depot);

            consignmentsSeen.add(consignmentId);
            partitionsSeen.add(scan.partition());
            scansTracked.incrementAndGet();

            log.info("[{}] consignment {} {} located at {} ({})",
                    Thread.currentThread().getName(), consignmentId, status, position, depot);
            return position;
        }
    }

    /**
     * Drains and closes the processor, then prints the run summary - which is the only place this example
     * reports numbers, and reports them as a demonstration rather than as a benchmark.
     */
    public void close() {
        if (parallelConsumer != null) {
            // DRAIN first: close() is closeDontDrainFirst(), which abandons records that are already
            // queued locally but not yet started - so the summary below would be built from work the
            // example threw away, and would under-report its own run
            parallelConsumer.closeDrainFirst();
        }
        this.emittedSummary = buildRunSummary();
        emittedSummary.ifPresent(RunSummary::log);
    }

    /**
     * The summary's partition and key counts are what was actually OBSERVED, not what was configured: a
     * summary that reported a configured ceiling the data never contained would be describing a run that
     * did not happen.
     */
    private Optional<RunSummary> buildRunSummary() {
        long scans = scansTracked.get();
        if (scans < 1) {
            log.info("Parcel tracking closed without tracking a scan - nothing to summarise");
            return Optional.empty();
        }
        return Optional.of(RunSummary.builder()
                .exampleName("Parcel tracking (core)")
                .recordCount(scans)
                .partitionCount(partitionsSeen.size())
                .distinctKeys(consignmentsSeen.size())
                .maxConcurrency(MAX_CONCURRENCY)
                .ordering(KEY)
                .simulatedLatency(geocodeService.getLatency())
                .observer(observer)
                .build());
    }

    private Consumer<String, String> getKafkaConsumer() {
        if (injectedConsumer != null) {
            return injectedConsumer;
        }
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, envVars.getOrDefault("BOOTSTRAP_SERVERS", "kafka:9092"));
        props.put(ConsumerConfig.GROUP_ID_CONFIG, envVars.getOrDefault("GROUP_ID", "parcel-tracking"));
        // Parallel Consumer commits the offsets itself, once the scan has actually been geocoded
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        return new KafkaConsumer<>(props);
    }

    private Producer<String, String> getKafkaProducer() {
        if (injectedProducer != null) {
            return injectedProducer;
        }
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, envVars.getOrDefault("BOOTSTRAP_SERVERS", "kafka:9092"));
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        return new KafkaProducer<>(props);
    }
}
