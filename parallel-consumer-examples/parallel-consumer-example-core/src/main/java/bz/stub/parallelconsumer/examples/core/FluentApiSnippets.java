package bz.stub.parallelconsumer.examples.core;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumer;
import bz.stub.parallelconsumer.ParallelConsumerOptions.CommitMode;
import bz.stub.parallelconsumer.fluent.Consumed;
import bz.stub.parallelconsumer.fluent.ParallelConsumerInstance;
import bz.stub.parallelconsumer.fluent.Outcome;
import bz.stub.parallelconsumer.fluent.ParallelConsumerDefinition;
import bz.stub.parallelconsumer.fluent.Produced;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;

import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

import static bz.stub.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.KEY;
import static bz.stub.parallelconsumer.fluent.AfterRetries.dlqImmediately;
import static bz.stub.parallelconsumer.fluent.AfterRetries.park;
import static bz.stub.parallelconsumer.fluent.AfterRetries.stop;
import static bz.stub.parallelconsumer.fluent.Formats.json;

/**
 * The fluent API's README examples, one tagged region each.
 * <p>
 * <b>Nothing here is ever called.</b> These methods exist so that the README's fluent-API section shows real code
 * rather than prose that resembles code: the {@code tag::} regions below are pulled into
 * {@code src/docs/README_TEMPLATE.adoc} by the asciidoc template plugin, so compiling this file is what stops those
 * examples silently going stale. If a name on the fluent API changes, this fails to compile instead of the README
 * quietly starting to lie. It is the same arrangement {@link CoreApp} has for the classic API's examples.
 * <p>
 * These regions are <em>shown</em>, not run - and neither is {@link FluentQuickstartApp}, whose own note says why.
 * What runs against a broker is core's {@code FluentQuickstartIT}, which re-states the quickstart's shape in the
 * types core can read without depending on this module. Anything here that has to be proved rather than merely
 * shown belongs there instead.
 */
@Slf4j
@SuppressWarnings({"unused", "MagicNumber"})
public class FluentApiSnippets {

    private Properties connectionProperties() {
        return new Properties();
    }

    private Consumer<byte[], byte[]> myOwnConsumer() {
        return null;
    }

    private Producer<byte[], byte[]> myOwnProducer() {
        return null;
    }

    /**
     * The general route form, for a key that is not a string or a deserialiser of your own, and the produced types
     * that let a route produce at all.
     */
    void generalRouteForm() {
        ParallelConsumerDefinition pc = ParallelConsumer.connect(connectionProperties());
        // tag::fluentRouteForm[]
        pc.topic("orders")
                .consumed(Consumed.with(Serdes.Long(), json(Order.class)))     // <1>
                .produced(Produced.with(Serdes.String(), json(Dispatch.class))) // <2>
                .process(context -> Outcome.produce(
                        new ProducerRecord<>("dispatches", context.value().getOrderId(),
                                Dispatch.of(context.value()))));                // <3>

        pc.topics(Arrays.asList("audit", "audit-replay"))                       // <4>
                .process(context -> Outcome.succeeded());
        // end::fluentRouteForm[]
    }

    /**
     * Instance-wide settings, per-route defaults, and a route overriding its copy.
     */
    void instanceAndRouteSettings() {
        // tag::fluentSettings[]
        ParallelConsumerDefinition pc = ParallelConsumer.connect(connectionProperties())
                .withCommitMode(CommitMode.PERIODIC_CONSUMER_ASYNCHRONOUS)  // <1>
                .withDefaultOrdering(KEY)                                   // <2>
                .withDefaultConcurrency(100)
                .withDefaultRetryLimit(10)
                .withDefaultRetryDelay(Duration.ofSeconds(1));

        pc.json("payments", Order.class)
                .retryLimit(3)                                          // <3>
                .concurrency(8)
                .process(context -> Outcome.succeeded());
        // end::fluentSettings[]
    }

    /**
     * What happens to a record that has run out of attempts: the three reactions, as data.
     */
    void afterRetriesPolicy() {
        ParallelConsumerDefinition pc = ParallelConsumer.connect(connectionProperties());
        // tag::fluentAfterRetries[]
        pc.json("orders", Order.class)
                .afterRetries(park())                                        // <1>
                .process(context -> Outcome.succeeded());

        pc.json("scans", Order.class)
                .afterRetries(park()
                        .thenRetryAfter(Duration.ofMinutes(30))               // <2>
                        .forCycles(4))
                .process(context -> Outcome.succeeded());

        pc.json("payments", Order.class)
                .afterRetries(dlqImmediately("payments.dlq"))                 // <3>
                .process(context -> Outcome.succeeded());

        pc.json("schema-sensitive", Order.class)
                .afterRetries(stop())                                         // <4>
                .process(context -> Outcome.succeeded());
        // end::fluentAfterRetries[]
    }

    /**
     * The outcomes a processing function may report, in one function.
     */
    void outcomes() {
        ParallelConsumerDefinition pc = ParallelConsumer.connect(connectionProperties());
        // tag::fluentOutcomes[]
        pc.json("orders", Order.class)
                .process(context -> {
                    Order order = context.value();
                    if ("RETURNED".equals(order.getStatus())) {
                        return Outcome.filtered();                      // <1>
                    }
                    if (order.getParcelCount() > 1_000) {
                        return Outcome.park("no lane handles a load this size"); // <2>
                    }
                    if (order.getOrderId() == null) {
                        return Outcome.stop("orders with no id mean the wrong topic is wired up"); // <3>
                    }
                    warehouse(order);                                   // <4>
                    return Outcome.succeeded();                         // <5>
                });
        // end::fluentOutcomes[]
    }

    /**
     * An existing user with hand-built clients reaches the fluent API without migrating how they build them.
     */
    void handBuiltClients() {
        // tag::fluentOwnClients[]
        ParallelConsumerDefinition pc = ParallelConsumer.connect(connectionProperties())
                .withConsumer(myOwnConsumer())      // <1>
                .withProducer(myOwnProducer());     // <2>
        // end::fluentOwnClients[]
    }

    /**
     * The first documented workaround: a dead-letter topic written by hand, with the failure swallowed so that the
     * offset commits.
     */
    void migrationDeadLetter() {
        ParallelConsumerDefinition pc = ParallelConsumer.connect(connectionProperties());
        // tag::fluentMigrationDeadLetter[]
        pc.json("orders", Order.class)
                .retryLimit(5)
                .afterRetries(park())          // or dlqTo("orders.dlq") once export lands
                .process(context -> {
                    warehouse(context.value());
                    return Outcome.succeeded();
                });
        // end::fluentMigrationDeadLetter[]
    }

    /**
     * The second: one handler switching on the topic name, because the classic API has one function per instance.
     */
    void migrationTopicSwitch() {
        ParallelConsumerDefinition pc = ParallelConsumer.connect(connectionProperties());
        // tag::fluentMigrationTopics[]
        pc.json("orders", Order.class).process(context -> {
            warehouse(context.value());
            return Outcome.succeeded();
        });
        pc.string("audit").process(context -> {
            log.info("{}", context.value());
            return Outcome.succeeded();
        });
        // end::fluentMigrationTopics[]
    }

    /**
     * The third: consuming raw bytes so that the payload can be deserialised by hand inside the function, because a
     * deserialisation failure on the poll thread ends the instance.
     */
    void migrationRawBytes() {
        ParallelConsumerDefinition pc = ParallelConsumer.connect(connectionProperties());
        // tag::fluentMigrationTypes[]
        pc.json("orders", Order.class)              // <1>
                .process(context -> {
                    warehouse(context.value());
                    return Outcome.succeeded();
                });

        pc.json("events")                            // <2>
                .process(context -> {
                    Map<String, Object> payload = context.value();
                    log.info("{}", payload.get("type"));
                    return Outcome.succeeded();
                });
        // end::fluentMigrationTypes[]
    }

    /**
     * The running instance: what it offers, and how it ends.
     */
    void theInstance() {
        ParallelConsumerDefinition pc = ParallelConsumer.connect(connectionProperties());
        pc.json("orders", Order.class).process(context -> Outcome.succeeded());
        // tag::fluentInstance[]
        try (ParallelConsumerInstance instance = pc.start()) {  // <1>
            instance.awaitShutdown();                           // <2>
            instance.stopRequest().ifPresent(stop ->            // <3>
                    log.warn("A route stopped the instance: {}", stop.reason()));
            instance.failureCause().ifPresent(cause ->          // <4>
                    log.error("The instance failed", cause));
        }
        // end::fluentInstance[]
    }

    private static void warehouse(Order order) {
        // your own client
    }
}
