package bz.stub.parallelconsumer.examples.core;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumer;
import bz.stub.parallelconsumer.fluent.ParallelConsumerInstance;
import bz.stub.parallelconsumer.fluent.Outcome;
import bz.stub.parallelconsumer.fluent.ParallelConsumerDefinition;
import bz.stub.parallelconsumer.fluent.ParkedRecord;
import bz.stub.parallelconsumer.fluent.ParkedView;
import bz.stub.parallelconsumer.fluent.Produced;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;

import java.time.Duration;
import java.util.Properties;

import static bz.stub.parallelconsumer.fluent.Formats.json;

/**
 * The smallest fluent programs that do anything: one per idea, each short enough to read in one go.
 * <p>
 * Deliberately <b>not</b> a demonstration. {@link FluentQuickstartApp} is the README's first screen and shows
 * several ideas at once because that is what a first screen is for; each method here shows exactly one, with no
 * second topic, no second type and no narrative. They are what to reach for when the question is "what does the
 * smallest version of this look like", which is the question the quickstart cannot answer without stopping being
 * a quickstart.
 * <p>
 * <b>Nothing in the build runs these.</b> What they are worth is the compiler: they are compiled on every build
 * like the rest of this module, so an API change that would make them wrong makes the build red instead. Each
 * carries a {@code tag::} region so the README can include one without a copy being made; none is included yet.
 */
@SuppressWarnings({"unused", "MagicNumber"})
public class FluentMinimalExamples {

    /**
     * Private: these are examples to read, not a utility to hold. Nothing here has any state.
     */
    private FluentMinimalExamples() {
    }

    /**
     * Connect, one route, process, close. The whole of the API a program needs before it needs anything else.
     */
    static void processOneTopic(Properties kafkaProperties) {
        // tag::minimalProcess[]
        ParallelConsumerDefinition pc = ParallelConsumer.connect(kafkaProperties);

        pc.json("orders", Order.class)
                .process(context -> {
                    System.out.println("Order " + context.value().getOrderId());
                    return Outcome.succeeded();
                });

        try (ParallelConsumerInstance instance = pc.start()) {
            instance.awaitShutdown();
        }
        // end::minimalProcess[]
    }

    /**
     * A record that cannot be processed, and what becomes of it. Two retries, then the record parks: it stays
     * incomplete in the offset map, holds no worker, and everything behind it carries on. The parked set is what
     * an operator reads afterwards to find out what is stuck and why - nothing is lost and nothing is hidden,
     * which is the point of parking rather than swallowing.
     */
    static void parkAfterRetriesAndReadWhatParked(Properties kafkaProperties) {
        // tag::minimalPark[]
        ParallelConsumerDefinition pc = ParallelConsumer.connect(kafkaProperties);

        pc.string("parcel-scans")
                .retryLimit(2)
                .retryDelay(Duration.ofSeconds(1))
                .process(context -> {
                    throw new IllegalStateException("the parcel-tracking service is not reachable");
                });

        try (ParallelConsumerInstance instance = pc.start()) {
            // Named because it is deliberately ignored: the point is to run for a minute and then look, and this
            // instance is not expected to have shut down by then.
            boolean ignoredShutDown = instance.awaitShutdown(Duration.ofMinutes(1));

            ParkedView parked = instance.topic("parcel-scans").parked();
            System.out.println(parked.count() + " scans parked");
            for (ParkedRecord record : parked.records()) {
                System.out.println("  offset " + record.offset() + " after "
                        + record.attempts() + " attempts: " + record.reason());
            }
        }
        // end::minimalPark[]
    }

    /**
     * Producing to another topic. A route that declares produced types may return them and nothing else; a route
     * that declares none cannot return a produced record at all, and in Java that is a compile error rather than
     * a runtime surprise. The send and this record's offset commit together, so a produced record cannot be lost
     * by a commit that happened without it.
     */
    static void produceToAnotherTopic(Properties kafkaProperties) {
        // tag::minimalProduce[]
        ParallelConsumerDefinition pc = ParallelConsumer.connect(kafkaProperties);

        pc.json("orders", Order.class)
                .produced(Produced.with(Serdes.String(), json(Dispatch.class)))
                .process(context -> Outcome.produce(
                        new ProducerRecord<>("dispatches",
                                context.value().getOrderId(),
                                Dispatch.of(context.value()))));

        try (ParallelConsumerInstance instance = pc.start()) {
            instance.awaitShutdown();
        }
        // end::minimalProduce[]
    }

    /**
     * Runs one of the three against a broker, so that these are runnable rather than only readable. Nothing in
     * the build calls this - see the note on this class.
     */
    public static void main(String[] args) {
        Properties kafkaProperties = new Properties();
        kafkaProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        kafkaProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "parallel-consumer-minimal-example");
        kafkaProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        processOneTopic(kafkaProperties);
    }
}
