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
 * build.
 * <p>
 * <b>Nothing executes this class.</b> Core cannot depend on the examples module, so core's
 * {@code FluentQuickstartIT} re-states the same SHAPE against a real broker in the types core can read on its own -
 * it does not run what is here. The broker-free run of this definition lives in the sandbox module, which is
 * stacked above this work as astubbs/parallel-consumer#507. What this file is worth is the compiler: if the fluent
 * API drifts, the README's first example stops building.
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
 *       the assignment. The README's park section owns that consequence. No dead-letter topic is
 *       involved, and none is declared - export at capacity is a later milestone.</li>
 *   <li><b>The parked set.</b> {@link #reportParked} asks the handle what is parked and why.</li>
 *   <li><b>Typed routes.</b> Two topics, two value types, one function each - no casts, no {@code instanceof} on a
 *       shared handler, no hand-rolled deserialisation.</li>
 *   <li><b>Outcomes.</b> A normal return is success; a returned {@link Outcome#filtered()} completes the record
 *       without processing it; a throw is a retry.</li>
 * </ul>
 *
 * <h2>It is straight-line code, deliberately</h2>
 * The quickstart used to be wrapped in a {@code defineConsumer(Properties)} method that built the definition and
 * handed it back. The wrapper was there to show that a definition is separate from how it is started - which is
 * true and matters, but not on the first screen: it put a method signature between the reader and the first line
 * of the API, and a reader who wanted to try this had to unpick it first. The separation is shown where it is
 * actually needed, and here the properties, the routes and the start read top to bottom as a program.
 */
@Slf4j
public class FluentQuickstartApp {

    /**
     * The value the scans route's function refuses to accept, so that the quickstart always has something to park.
     */
    private static final String TRACKING_IS_DOWN = "the parcel-tracking service is not reachable";

    /**
     * The quickstart itself, top to bottom - the properties, the two routes and the start, in the order the README
     * shows them. Nothing in the build calls this; point it at your own broker by changing the bootstrap servers on
     * the first line.
     */
    public static void main(String[] args) {
        // tag::quickstart[]
        Properties kafkaProperties = new Properties();
        kafkaProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"); // <1>
        kafkaProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "parallel-consumer-quickstart");
        kafkaProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        ParallelConsumerDefinition pc = ParallelConsumer.connect(kafkaProperties) // <2>
                .defaultOrdering(KEY);

        pc.json("orders", Order.class) // <3>
                .process(context -> {
                    Order order = context.value();
                    if ("RETURNED".equals(order.getStatus())) {
                        return Outcome.filtered(); // <4>
                    }
                    reserveStock(order);
                    System.out.println("Processed order " + order.getOrderId()); // <5>
                    return Outcome.succeeded(); // <6>
                });

        pc.string("parcel-scans") // <7>
                .retryLimit(2) // <8>
                .retryDelay(Duration.ofMillis(200))
                .onParked((record, failure, attempts) -> // <9>
                        System.out.println("Parked scan at offset " + record.offset()
                                + " after " + attempts + " attempts"))
                .process(context -> {
                    recordScan(context.value()); // <10>
                    return Outcome.succeeded();
                });
        // end::quickstart[]

        // tag::quickstartRun[]
        try (ConsumerHandle handle = pc.start()) { // <1>
            handle.awaitShutdown(); // <2>
        }
        // end::quickstartRun[]
    }

    // tag::quickstartParked[]
    void reportParked(ConsumerHandle handle) {
        ParkedView parked = handle.topic("parcel-scans").parked(); // <1>
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

}
