package bz.stub.parallelconsumer.examples.core;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumer;
import bz.stub.parallelconsumer.fluent.ConsumerHandle;
import bz.stub.parallelconsumer.fluent.Outcome;
import bz.stub.parallelconsumer.fluent.ParallelConsumerDefinition;
import bz.stub.parallelconsumer.fluent.ParkedRecord;
import bz.stub.parallelconsumer.fluent.ParkedView;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.time.Duration;
import java.util.Properties;

import static bz.stub.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.KEY;

/**
 * The README quickstart, and the build signal behind it: the fluent API's whole first screen, compiled on every
 * build and run broker-free on every build by the sandbox module's {@code FluentQuickstartAppTest}.
 * <p>
 * Core cannot depend on the examples module, so core's {@code FluentQuickstartIT} re-states the same SHAPE
 * against a real broker in the types core can read on its own - it does not run what is here. What this file
 * is worth beyond the sandbox run is the compiler: if the fluent API drifts, the README's first example stops
 * building.
 *
 * <h2>Why this class exists rather than a snippet in the README</h2>
 * The README's first example is the surface's primary success signal (KD7): if the fluent API drifts, this file
 * stops compiling and the tests around it go red, rather than the README quietly starting to lie. The
 * {@code quickstart} region below is pulled into {@code src/docs/README_TEMPLATE.adoc} by the asciidoc template
 * plugin, so what a reader sees is this source, not a copy of it.
 *
 * <h2>What it demonstrates, in the order the README leads with</h2>
 * <ul>
 *   <li><b>Park in place.</b> The scans route's downstream is down, so its records exhaust their two retries and
 *       park: they stay incomplete in the offset map and hold no worker, and the records above them go on being
 *       processed and recorded as complete in the commit metadata. The COMMITTED OFFSET itself does not move past
 *       a parked record - it is the highest sequential succeeded offset plus one, and a parked record is never
 *       sequentially succeeded - so consumer-group lag reads as stuck at the oldest parked record for the life of
 *       the assignment. The README's park section owns that consequence, and
 *       {@code SandboxConsumer#awaitEveryPublishedRecordCommitted} depends on it. No dead-letter topic is
 *       involved, and none is declared - export at capacity is a later milestone.</li>
 *   <li><b>The parked set.</b> {@link #reportParked} asks the handle what is parked and why.</li>
 *   <li><b>Typed routes.</b> Two topics, two value types, one function each - no casts, no {@code instanceof} on a
 *       shared handler, no hand-rolled deserialisation.</li>
 *   <li><b>Outcomes.</b> A normal return is success; a returned {@link Outcome#filtered()} completes the record
 *       without processing it; a throw is a retry.</li>
 * </ul>
 *
 * <h2>The definition does not change between a broker and the sandbox</h2>
 * {@link #defineConsumer} builds the definition and returns it, unstarted. {@link #run} starts it against Kafka;
 * the test starts the same method's result in the sandbox with no broker anywhere. That the two share this method
 * rather than resembling each other is the whole claim being tested.
 */
@Slf4j
public class FluentQuickstartApp {

    public static final String ORDERS_TOPIC = "orders";

    public static final String SCANS_TOPIC = "parcel-scans";

    /**
     * The value the scans route's function refuses to accept, so that the quickstart always has something to park.
     */
    private static final String TRACKING_IS_DOWN = "the parcel-tracking service is not reachable";

    // tag::quickstart[]
    ParallelConsumerDefinition defineConsumer(Properties kafkaProperties) {
        ParallelConsumerDefinition pc = ParallelConsumer.connect(kafkaProperties) // <1>
                .defaultOrdering(KEY);

        pc.json(ORDERS_TOPIC, Order.class) // <2>
                .process(context -> {
                    Order order = context.value();
                    if ("RETURNED".equals(order.getStatus())) {
                        return Outcome.filtered(); // <3>
                    }
                    reserveStock(order);
                    return Outcome.succeeded(); // <4>
                });

        pc.string(SCANS_TOPIC) // <5>
                .retryLimit(2) // <6>
                .retryDelay(Duration.ofMillis(200))
                .onParked((record, failure, attempts) -> // <7>
                        log.warn("Parked scan at offset {} after {} attempts",
                                record.offset(), attempts, failure))
                .process(context -> {
                    recordScan(context.value()); // <8>
                    return Outcome.succeeded();
                });

        return pc;
    }
    // end::quickstart[]

    // tag::quickstartRun[]
    void run(Properties kafkaProperties) {
        try (ConsumerHandle handle = defineConsumer(kafkaProperties).start()) { // <1>
            handle.awaitShutdown(); // <2>
        }
    }
    // end::quickstartRun[]

    // tag::quickstartParked[]
    void reportParked(ConsumerHandle handle) {
        ParkedView parked = handle.topic(SCANS_TOPIC).parked(); // <1>
        log.info("{} scans parked, oldest {}", parked.count(), parked.oldestAge().orElse(Duration.ZERO));
        for (ParkedRecord record : parked.records()) { // <2>
            log.info("  partition {} offset {} key {} after {} attempts: {}",
                    record.partition(), record.offset(), record.key(), record.attempts(), record.reason());
        }
        log.info("{} records parked across every route", handle.parkedAllTopics().count()); // <3>
    }
    // end::quickstartParked[]

    /**
     * Stands in for your own inventory client. The quickstart never fails here, so the orders route shows the two
     * outcomes a healthy route reaches: succeeded, and filtered.
     */
    /**
     * One order for the sandbox to publish, addressed by the record's index.
     * <p>
     * <b>The sandbox knows how to pace and stop; it does not know what an order looks like.</b> That is this
     * method - and it is the shape any application writes, because nothing but the application knows its own
     * data. It is deliberately not random: one order in five is RETURNED, so the filtered outcome the README
     * calls out happens on a schedule a reader can follow rather than when a dice roll says so.
     */
    static Order anOrder(long index) {
        String status = index % 5 == 0 ? "RETURNED" : "IN_TRANSIT";
        return new Order("order-" + index, "customer-" + index, CITIES[(int) (index % CITIES.length)],
                1 + (int) (index % 3), status);
    }

    /**
     * One parcel scan for the sandbox to publish. The scans route always fails and parks, so what a scan says
     * matters only in the log line that reports the park.
     */
    static String aScan(long index) {
        return "scan-" + index + " at " + CITIES[(int) (index % CITIES.length)];
    }

    /**
     * A handful of destinations, so a run reads like a topic rather than like one city repeated.
     */
    private static final String[] CITIES = {"Leeds", "Bristol", "Glasgow", "Cardiff"};

    private static void reserveStock(Order order) {
        log.info("Reserving stock for order {} - {} parcel(s) to {}",
                order.getOrderId(), order.getParcelCount(), order.getDestinationCity());
    }

    /**
     * Stands in for your own parcel-tracking client, and in the quickstart it is always down.
     * <p>
     * Deliberately: a retry that always succeeds teaches nothing, and the behaviour worth seeing on the first
     * screen is the one only this library's offset map allows - a record that has run out of attempts staying
     * exactly where it is, costing map capacity rather than a worker, while everything behind it keeps moving.
     */
    private static void recordScan(String scan) {
        throw new IllegalStateException(TRACKING_IS_DOWN + " (scan: " + scan + ")");
    }

    /**
     * Run the quickstart against a broker. Point it at one with {@code --bootstrap-servers}, or leave it to the
     * local default. Nothing in the build calls this - see the note on this class.
     */
    public static void main(String[] args) {
        String bootstrapServers = args.length > 0 ? args[0] : "localhost:9092";
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "parallel-consumer-quickstart");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        new FluentQuickstartApp().run(properties);
    }
}
