package bz.stub.parallelconsumer.fluent;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Properties;

import static com.google.common.truth.Truth.assertThat;

/**
 * Covers AE8 - a route's declared types reach the function and the records it returns, with no casts - and R6's
 * rule that every per-route setting is a copy of the instance default until the route declares its own.
 */
class RouteTypingAndDefaultsTest {

    /**
     * A consumed value type. Public fields keep it a plain data carrier: what is under test is the typing, not
     * Jackson.
     */
    public static class Order {

        public String customerId = "";

        public long amount;
    }

    /**
     * A produced value type, deliberately different from {@link Order} so that a route producing its own types
     * cannot compile by accident.
     */
    public static class OrderEvent {

        public String customerId = "";

        public static OrderEvent from(Order order) {
            OrderEvent event = new OrderEvent();
            event.customerId = order.customerId;
            return event;
        }
    }

    private static ParallelConsumerDefinition define() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "route-typing-test");
        return ParallelConsumer.define(properties);
    }

    /**
     * Covers AE8. The function's parameter is typed by {@link Consumed} and its produced records by
     * {@link Produced}; every reference below would not compile if either were wrong, and the assertions read the
     * key and value back without a cast.
     */
    @Test
    void aRouteConsumingOneTypePairAndProducingAnotherIsTypedThroughout() throws Exception {
        ProcessFunction<String, Order, Long, OrderEvent> function = context -> {
            String customer = context.value().customerId;            // consumed value type
            String key = context.key();                              // consumed key type
            assertThat(key).isEqualTo("k1");
            assertThat(customer).isEqualTo("c1");
            return Outcome.produce(new ProducerRecord<>("order-events", 7L, OrderEvent.from(context.value())));
        };

        var pc = define();
        pc.json("orders", Order.class)
                .produced(Produced.with(Serdes.Long(), Formats.json(OrderEvent.class)))
                .process(function);

        Order order = new Order();
        order.customerId = "c1";
        var record = new ConsumerRecord<>("orders", 0, 0L,
                "k".getBytes(StandardCharsets.UTF_8), "{}".getBytes(StandardCharsets.UTF_8));
        Outcome<Long, OrderEvent> outcome = function.process(new ProcessContext<>(record, "k1", order));

        assertThat(outcome.kind()).isEqualTo(Outcome.Kind.PRODUCE);
        ProducerRecord<Long, OrderEvent> produced = outcome.records().get(0);
        long producedKey = produced.key();                            // no cast: the route's produced key type
        String producedCustomer = produced.value().customerId;
        assertThat(producedKey).isEqualTo(7L);
        assertThat(producedCustomer).isEqualTo("c1");
        assertThat(pc.route("orders").producesRecords()).isTrue();
    }

    /**
     * The non-producing shape of the same rule: a route that declares no {@link Produced} types reports it, which is
     * what {@link ParallelConsumerDefinition#requiresProducer()} reads and what the compiler enforces through
     * {@code Outcome<Void, Void>}.
     */
    @Test
    void aRouteThatDeclaresNoProducedTypesSaysSo() {
        var pc = define();
        pc.json("orders", Order.class).process(context -> Outcome.succeeded());

        assertThat(pc.route("orders").producesRecords()).isFalse();
        assertThat(pc.requiresProducer()).isFalse();
    }

    @Test
    void everyPerRouteSettingIsACopyOfTheInstanceDefault() {
        var breaker = CircuitBreakerPolicy.failureRate(0.5).over(100).openFor(Duration.ofSeconds(30));
        var afterRetries = AfterRetries.park();
        var pc = define()
                .defaultRetryLimit(7)
                .defaultRetryDelay(Duration.ofSeconds(3))
                .defaultConcurrency(9)
                .defaultAfterRetries(afterRetries)
                .defaultCircuitBreaker(breaker);
        pc.string("orders").process(context -> Outcome.succeeded());
        pc.string("audit").process(context -> Outcome.succeeded());

        for (String topic : new String[]{"orders", "audit"}) {
            RouteView route = pc.route(topic);
            assertThat(route.retryLimit().getAsInt()).isEqualTo(7);
            assertThat(route.retryDelay()).isEqualTo(Duration.ofSeconds(3));
            assertThat(route.concurrency()).isEqualTo(9);
            assertThat(route.afterRetries().reaction()).isEqualTo(AfterRetries.Reaction.PARK);
            assertThat(route.circuitBreaker().failureRate()).isEqualTo(0.5);
        }
        // A copy, not the instance's own object: a route editing part of a policy must not edit every other route's.
        assertThat(pc.route("orders").afterRetries()).isNotSameInstanceAs(afterRetries);
        assertThat(pc.route("orders").afterRetries()).isNotSameInstanceAs(pc.route("audit").afterRetries());
        assertThat(pc.route("orders").circuitBreaker()).isNotSameInstanceAs(breaker);
    }

    /**
     * The stop reaction is data like the rest of the policy: it copies into every route the same way park does, and
     * the behaviour that reads it is a later unit (R24, R27).
     */
    @Test
    void theStopReactionCopiesIntoEveryRouteAndARouteMayOverrideIt() {
        var pc = define().defaultAfterRetries(AfterRetries.stop());
        pc.string("orders").process(context -> Outcome.succeeded());
        pc.string("audit").afterRetries(AfterRetries.dlqImmediately("audit.dlq"))
                .process(context -> Outcome.succeeded());

        assertThat(pc.route("orders").afterRetries().reaction()).isEqualTo(AfterRetries.Reaction.STOP);
        assertThat(pc.route("audit").afterRetries().reaction()).isEqualTo(AfterRetries.Reaction.PARK);
        assertThat(pc.route("audit").afterRetries().destination()).isEqualTo("audit.dlq");
    }

    @Test
    void aRoutesOwnSettingOverridesOnlyItsOwnCopy() {
        var pc = define().defaultRetryLimit(7).defaultConcurrency(9);
        pc.string("orders").retryLimit(2).concurrency(4).process(context -> Outcome.succeeded());
        pc.string("audit").process(context -> Outcome.succeeded());

        assertThat(pc.route("orders").retryLimit().getAsInt()).isEqualTo(2);
        assertThat(pc.route("orders").concurrency()).isEqualTo(4);
        assertThat(pc.route("audit").retryLimit().getAsInt()).isEqualTo(7);
        assertThat(pc.route("audit").concurrency()).isEqualTo(9);
    }

    /**
     * Unbounded is opt-in on both levels, and empty is how the view spells it - the classic API's retry-forever
     * behaviour is available only by asking (R10).
     */
    @Test
    void unboundedRetriesAreOptInOnTheInstanceAndOnARoute() {
        var pc = define().defaultRetryForever();
        pc.string("orders").process(context -> Outcome.succeeded());
        pc.string("audit").retryLimit(3).process(context -> Outcome.succeeded());

        assertThat(pc.route("orders").retryLimit().isPresent()).isFalse();
        assertThat(pc.route("audit").retryLimit().getAsInt()).isEqualTo(3);

        var withRouteOverride = define();
        withRouteOverride.string("orders").retryForever().process(context -> Outcome.succeeded());
        assertThat(withRouteOverride.route("orders").retryLimit().isPresent()).isFalse();
    }

    /**
     * The default with nothing declared: ten attempts then park (R10, AE1's second clause).
     */
    @Test
    void theDefaultIsTenAttemptsThenPark() {
        var pc = define();
        pc.string("orders").process(context -> Outcome.succeeded());

        assertThat(pc.route("orders").retryLimit().getAsInt()).isEqualTo(10);
        assertThat(pc.route("orders").afterRetries().reaction()).isEqualTo(AfterRetries.Reaction.PARK);
        assertThat(pc.route("orders").afterRetries().destination()).isNull();
    }

    /**
     * A set of topics is one route: one function, one type pair, one admission target (R5).
     */
    @Test
    void aSetOfTopicsIsOneRouteWithOneAdmissionTarget() {
        var pc = define();
        pc.bytes(java.util.Arrays.asList("audit", "audit-replay")).concurrency(4)
                .process(context -> Outcome.succeeded());

        assertThat(pc.topics()).containsExactly("audit", "audit-replay");
        assertThat(pc.routes()).hasSize(1);
        assertThat(pc.route("audit")).isSameInstanceAs(pc.route("audit-replay"));
        assertThat(pc.route("audit").concurrency()).isEqualTo(4);
    }
}
