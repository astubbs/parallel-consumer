package bz.stub.parallelconsumer.fluent;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import static bz.stub.parallelconsumer.AbstractParallelEoSStreamProcessorTestBase.defaultTimeout;
import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * A record reaches exactly one terminal outcome, and which one is what the function returned (R7, R8, F3, AE5).
 * <p>
 * Driven through a real engine over the shipped mock consumer, because the claims are about what the <em>engine</em>
 * does with what the wrapper returns - an empty list completes and commits, a produced list is sent first - and a
 * test that called the wrapper directly would be asserting the wrapper's arithmetic instead.
 */
@Timeout(60)
class RouteDispatchOutcomesTest extends AbstractFluentEngineTest {




    /**
     * AE5, F3. A filtered record completes and commits exactly as a success does, and is counted apart from one -
     * so the two numbers together must account for every record, and neither alone may.
     */
    @Test
    void ofAThousandRecordsAHundredAreFilteredAndTheRestSucceedWithEveryOffsetCommitted() {
        int records = 1000;
        var pc = ParallelConsumer.connect(props());
        pc.string(TOPIC).process(context ->
                context.value().startsWith("no-customer-") ? Outcome.filtered() : Outcome.succeeded());

        handle = runtime.startAndAssign(pc, 1);
        for (int offset = 0; offset < records; offset++) {
            // every tenth record lacks the field the function needs
            runtime.publish(TOPIC, 0, offset, "key-" + offset,
                    offset % 10 == 0 ? "no-customer-" + offset : "order-" + offset);
        }

        RouteDispatcher dispatcher = pc.dispatcher();
        Awaitility.await().atMost(defaultTimeout).untilAsserted(() ->
                assertThat(dispatcher.succeededCount() + dispatcher.filteredCount()).isEqualTo(records));

        assertThat(dispatcher.filteredCount()).isEqualTo(100);
        assertThat(dispatcher.succeededCount()).isEqualTo(900);
        assertThat(dispatcher.parkedCount()).isEqualTo(0);

        // All offsets commit: the highest committed offset is one past the last record.
        Awaitility.await().atMost(defaultTimeout).untilAsserted(() ->
                assertThat(runtime.committedOffset(TOPIC, 0)).isEqualTo(records));

        // Nothing produced, and nothing was even asked to produce it: this definition opens no producer at all.
        assertThat(runtime.producerCalls).isEqualTo(0);
        assertThat(dispatcher.producedRecordCount()).isEqualTo(0);
    }

    /**
     * R10 bounds every failure a record can have on a route, and mapping the outcome is one of them.
     * <p>
     * A serialiser that consistently rejects a produced value used to escape to the engine, which retries for ever:
     * the route's finite limit was applied only to the function's own throws, so the after-retries reaction was
     * never reached, nothing parked, and under key ordering the key stayed blocked behind a record that could never
     * finish. It runs out of attempts and parks now, like any other failure on the route.
     */
    @Test
    void aSerialiserThatAlwaysRejectsRunsOutOfAttemptsAndParks() {
        var ran = new AtomicInteger();
        var pc = ParallelConsumer.connect(props());
        pc.string(TOPIC)
                .retryLimit(2)
                .retryDelay(Duration.ofMillis(10))
                .produced(Produced.with(Serdes.String(), alwaysRejecting()))
                .process(context -> {
                    ran.incrementAndGet();
                    return Outcome.produce(new ProducerRecord<>("order-events", context.key(), context.value()));
                });

        handle = runtime.startAndAssign(pc, 1);
        runtime.publish(TOPIC, 0, 0, "key-0", "an order nothing can serialise");

        RouteDispatcher dispatcher = pc.dispatcher();
        Awaitility.await().atMost(defaultTimeout).untilAsserted(() ->
                assertThat(dispatcher.parkedCount()).isEqualTo(1));

        // A limit of two allows three runs, and the third is the one that parks - so the function ran three times
        // and no more, rather than for ever.
        assertThat(ran.get()).isEqualTo(3);
        assertThat(dispatcher.succeededCount()).isEqualTo(0);
        assertThat(dispatcher.producedRecordCount()).isEqualTo(0);
        assertThat(runtime.mockProducer().history()).isEmpty();
    }

    /**
     * The park a function declares is what the mapping DID, not a failure of it - so it is handed back untouched and
     * counts once, and the reason recorded is the function's own.
     * <p>
     * The route's limit is zero, which is what makes this discriminating: a declared park sent through the
     * exhaustion path on a route with nothing left would park the already-parked record a second time, count it
     * twice, tell the observer twice, and overwrite the function's reason with "it ran out of attempts".
     */
    @Test
    void aDeclaredParkStillCountsOnceAndKeepsItsOwnReason() {
        var pc = ParallelConsumer.connect(props());
        pc.string(TOPIC)
                .retryLimit(0)
                .retryDelay(Duration.ofMillis(10))
                .process(context -> Outcome.park("the function already knows this one is hopeless"));

        handle = runtime.startAndAssign(pc, 1);
        runtime.publish(TOPIC, 0, 0, "key-0", "a hopeless order");

        RouteDispatcher dispatcher = pc.dispatcher();
        Awaitility.await().atMost(defaultTimeout).untilAsserted(() ->
                assertThat(handle.topic(TOPIC).parked().count()).isEqualTo(1));

        ParkedRecord parked = handle.topic(TOPIC).parked().records().get(0);
        assertThat(parked.reason()).contains("hopeless");
        assertWithMessage("the function's reason survives, rather than being overwritten by exhaustion")
                .that(parked.reason()).doesNotContain("ran out of attempts");
        assertThat(parked.attempts()).isEqualTo(1);
        assertWithMessage("one park event, not two").that(dispatcher.parkedCount()).isEqualTo(1);
    }

    /**
     * R3 makes producing from a route that declared no produced types a compile error - except for the one shape
     * {@code null} lets through, which no choice of type parameter can exclude: {@code null} inhabits every
     * reference type, so this call infers {@code ProducerRecord<Void, Void>} and compiles, and Kafka permits null
     * keys and values so it is a real record rather than an impossible generic value.
     * <p>
     * It used to reach the produced formats that are not there and fail as a {@code NullPointerException}. It is a
     * definition fault now - named, raised once, and fatal, because retrying it would fail identically for every
     * record on the topic for ever.
     */
    @Test
    void producingFromARouteWithNoProducedTypesIsADefinitionFault() {
        var pc = ParallelConsumer.connect(props());
        pc.string(TOPIC)
                .retryDelay(Duration.ofMillis(10))
                // Compiles: null inhabits Void, so this infers ProducerRecord<Void, Void>.
                .process(context -> Outcome.produce(new ProducerRecord<>("order-events", null, null)));

        handle = runtime.startAndAssign(pc, 1);
        runtime.publish(TOPIC, 0, 0, "key-0", "an order");

        IllegalStateException thrown = assertThrows(IllegalStateException.class,
                () -> handle.awaitShutdown(Duration.ofSeconds(30)));

        assertThat(thrown).hasMessageThat().contains("declares no produced types");
        assertThat(thrown).hasMessageThat().contains("fault of the definition");
        assertThat(thrown).hasMessageThat().contains(TOPIC);
        assertWithMessage("a definition fault stops the instance rather than parking every record")
                .that(pc.dispatcher().parkedCount()).isEqualTo(0);
    }

    /**
     * A serialiser that refuses everything, for the exhaustion test above. Written as the produced half only: what
     * this route does with a value on the way out is the whole of what is under test.
     */
    private static Serializer<String> alwaysRejecting() {
        return (topic, data) -> {
            throw new SerializationException("nothing on " + topic + " can be serialised");
        };
    }

    /**
     * A produced record is serialised with the route's <em>produced</em> types - which are not its consumed types -
     * and reaches the producer addressed to the topic the function named (R3).
     */
    @Test
    void aProducedRecordIsSerialisedWithTheRoutesProducedTypesAndReachesTheProducer() {
        var pc = ParallelConsumer.connect(props());
        pc.string(TOPIC)
                .produced(Produced.with(Serdes.String(), Serdes.Long()))
                .process(context -> Outcome.produce(
                        new ProducerRecord<>("order-events", context.key(), (long) context.value().length())));

        handle = runtime.startAndAssign(pc, 1);
        runtime.publish(TOPIC, 0, 0, "key-0", "twelve chars");

        Awaitility.await().atMost(defaultTimeout).untilAsserted(() ->
                assertThat(runtime.mockProducer().history()).hasSize(1));

        var sent = runtime.mockProducer().history().get(0);
        assertThat(sent.topic()).isEqualTo("order-events");
        assertThat(new String(sent.key(), StandardCharsets.UTF_8)).isEqualTo("key-0");
        // Serdes.Long() writes eight big-endian bytes, so this is the produced value serialiser having run and not
        // the consumed one.
        assertThat(sent.value()).hasLength(8);
        assertThat(Serdes.Long().deserializer().deserialize("order-events", sent.value())).isEqualTo(12L);
        assertThat(pc.dispatcher().producedRecordCount()).isEqualTo(1);
    }

    /**
     * A route that produces nothing reaches succeeded on a normal return, on the plain poll flow, with no producer
     * anywhere in the instance (R7, R4).
     */
    @Test
    void aRouteThatProducesNothingSucceedsOnANormalReturn() {
        var seen = new AtomicInteger();
        var pc = ParallelConsumer.connect(props());
        pc.string(TOPIC).process(context -> {
            seen.incrementAndGet();
            return Outcome.succeeded();
        });

        handle = runtime.startAndAssign(pc, 1);
        runtime.publish(TOPIC, 0, 0, "key-0", "an order");

        Awaitility.await().atMost(defaultTimeout).untilAsserted(() -> {
            assertThat(seen.get()).isEqualTo(1);
            assertThat(pc.dispatcher().succeededCount()).isEqualTo(1);
        });
        assertThat(runtime.mockProducer().history()).isEmpty();
    }

    /**
     * Each route decodes with its own types, and a record goes to the route that claims its topic and to no other
     * (R2, KTD2).
     */
    @Test
    void eachTopicIsDecodedAndRunByItsOwnRoute() {
        var ordersSeen = new AtomicInteger();
        var auditSeen = new AtomicInteger();
        var pc = ParallelConsumer.connect(props());
        pc.string(TOPIC).process(context -> {
            assertThat(context.topic()).isEqualTo(TOPIC);
            ordersSeen.incrementAndGet();
            return Outcome.succeeded();
        });
        pc.bytes("audit").process(context -> {
            assertThat(context.topic()).isEqualTo("audit");
            // the bytes route hands the value through undecoded, which is the point of it
            assertThat(new String(context.value(), StandardCharsets.UTF_8)).isEqualTo("audited");
            auditSeen.incrementAndGet();
            return Outcome.succeeded();
        });

        handle = runtime.startAndAssign(pc, 1);
        runtime.publish(TOPIC, 0, 0, "key-0", "an order");
        runtime.publish("audit", 0, 0, "key-0", "audited");

        Awaitility.await().atMost(defaultTimeout).untilAsserted(() -> {
            assertThat(ordersSeen.get()).isEqualTo(1);
            assertThat(auditSeen.get()).isEqualTo(1);
        });
    }
}
