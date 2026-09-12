package bz.stub.parallelconsumer.fluent;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumer;
import bz.stub.parallelconsumer.RecordContext;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Properties;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;

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
        return ParallelConsumer.connect(properties);
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
        Outcome<Long, OrderEvent> outcome = function.process(
                new TypedRecordContext<>(contextFor(record), "k1", order));

        assertThat(outcome.kind()).isEqualTo(Outcome.Kind.PRODUCE);
        ProducerRecord<Long, OrderEvent> produced = outcome.records().get(0);
        long producedKey = produced.key();                            // no cast: the route's produced key type
        String producedCustomer = produced.value().customerId;
        assertThat(producedKey).isEqualTo(7L);
        assertThat(producedCustomer).isEqualTo("c1");
        assertThat(pc.route("orders").producesRecords()).isTrue();
    }

    /**
     * A {@link TypedRecordContext} is a view over the engine's own {@link RecordContext}, so building one by hand needs
     * one of those. This test asks it only about the record - what is under test is the compiler's view of the
     * route's types - so it is built over the record alone, with no work container behind it.
     */
    private static RecordContext<byte[], byte[]> contextFor(ConsumerRecord<byte[], byte[]> record) {
        return new RecordContext<>(null, record);
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
        var afterRetries = AfterRetries.park();
        var pc = define()
                .withDefaultRetryLimit(7)
                .withDefaultRetryDelay(Duration.ofSeconds(3))
                .withDefaultConcurrency(9)
                .withDefaultAfterRetries(afterRetries);
        pc.string("orders").process(context -> Outcome.succeeded());
        pc.string("audit").process(context -> Outcome.succeeded());

        for (String topic : new String[]{"orders", "audit"}) {
            RouteView route = pc.route(topic);
            assertThat(route.retryLimit().getAsInt()).isEqualTo(7);
            assertThat(route.retryDelay()).isEqualTo(Duration.ofSeconds(3));
            assertThat(route.afterRetries().reaction()).isEqualTo(AfterRetries.Reaction.PARK);
        }
        // A copy, not the instance's own object: a route editing part of a policy must not edit every other route's.
        assertThat(pc.route("orders").afterRetries()).isNotSameInstanceAs(afterRetries);
        assertThat(pc.route("orders").afterRetries()).isNotSameInstanceAs(pc.route("audit").afterRetries());
    }

    /**
     * The stop reaction is data like the rest of the policy: it copies into every route the same way park does, and
     * the behaviour that reads it is a later unit (R24, R27).
     */
    @Test
    void theStopReactionCopiesIntoEveryRouteAndARouteMayOverrideIt() {
        var pc = define().withDefaultAfterRetries(AfterRetries.stop());
        pc.string("orders").process(context -> Outcome.succeeded());
        pc.string("audit").afterRetries(AfterRetries.park()).process(context -> Outcome.succeeded());

        assertThat(pc.route("orders").afterRetries().reaction()).isEqualTo(AfterRetries.Reaction.STOP);
        assertThat(pc.route("audit").afterRetries().reaction()).isEqualTo(AfterRetries.Reaction.PARK);
    }

    @Test
    void aRoutesOwnSettingOverridesOnlyItsOwnCopy() {
        var pc = define().withDefaultRetryLimit(7).withDefaultConcurrency(9);
        pc.string("orders").retryLimit(2).process(context -> Outcome.succeeded());
        pc.string("audit").process(context -> Outcome.succeeded());

        assertThat(pc.route("orders").retryLimit().getAsInt()).isEqualTo(2);
        assertThat(pc.route("audit").retryLimit().getAsInt()).isEqualTo(7);
    }

    /**
     * Unbounded is opt-in on both levels, and empty is how the view spells it - the classic API's retry-forever
     * behaviour is available only by asking (R10).
     */
    @Test
    void unboundedRetriesAreOptInOnTheInstanceAndOnARoute() {
        var pc = define().withDefaultRetryForever();
        pc.string("orders").process(context -> Outcome.succeeded());
        pc.string("audit").retryLimit(3).process(context -> Outcome.succeeded());

        assertThat(pc.route("orders").retryLimit().isPresent()).isFalse();
        assertThat(pc.route("audit").retryLimit().getAsInt()).isEqualTo(3);

        var withRouteOverride = define();
        withRouteOverride.string("orders").retryForever().process(context -> Outcome.succeeded());
        assertThat(withRouteOverride.route("orders").retryLimit().isPresent()).isFalse();
    }

    /**
     * A definition stays mutable until it starts, and {@code validate()} is documented as a way to fail early
     * without ending that - so a default moved after either of those still has to reach every route that inherits
     * it. A route resolves and caches on the first read of its policy, and both of these sequences take that read
     * before the definition is finished: validating early, and reading a route through the view.
     */
    @Test
    void aDefaultMovedAfterARouteHasResolvedStillReachesIt() {
        var pc = define();
        pc.string("orders").process(context -> Outcome.succeeded());
        // Both of the reads that resolve a route, before the default moves.
        pc.validate();
        assertThat(pc.route("orders").retryLimit().getAsInt()).isEqualTo(10);

        pc.withDefaultRetryLimit(0).withDefaultRetryDelay(Duration.ofSeconds(5))
                .withDefaultConcurrency(3).withDefaultAfterRetries(AfterRetries.stop());

        assertThat(pc.route("orders").retryLimit().getAsInt()).isEqualTo(0);
        assertThat(pc.route("orders").retryDelay()).isEqualTo(Duration.ofSeconds(5));
        assertThat(pc.route("orders").afterRetries().reaction()).isEqualTo(AfterRetries.Reaction.STOP);
        // And the engine is configured from the same resolved value, not from the one it had already cached.
        assertThat(pc.buildOptions(new RecordingClientRuntime()).getMaxConcurrency()).isEqualTo(3);
    }

    /**
     * A route that declared its own setting is not disturbed by the default moving under it: invalidating a
     * resolution re-runs the same fallback, it does not overwrite a declaration (R6).
     */
    @Test
    void aRoutesOwnSettingSurvivesTheDefaultMoving() {
        var pc = define().withDefaultRetryLimit(7);
        pc.string("orders").retryLimit(2).process(context -> Outcome.succeeded());
        assertThat(pc.route("orders").retryLimit().getAsInt()).isEqualTo(2);

        pc.withDefaultRetryLimit(0);

        assertThat(pc.route("orders").retryLimit().getAsInt()).isEqualTo(2);
    }

    /**
     * {@link RouteView} promises to change nothing, and the after-retries policy is the one setting on it that is a
     * mutable object - so it is handed out as a copy. Editing what the view returns used to reach past every
     * validation rule and onto the two plain fields a worker reads once per failed record.
     */
    @Test
    void thePolicyOnTheViewCannotBeUsedToChangeTheRoute() {
        var pc = define();
        pc.string("orders").afterRetries(AfterRetries.park().thenRetryAfter(Duration.ofSeconds(30)).forCycles(2))
                .process(context -> Outcome.succeeded());

        AfterRetries throughTheView = pc.route("orders").afterRetries();
        AfterRetries ignoredSameObject = throughTheView.thenRetryAfter(Duration.ofMinutes(10)).forCycles(99);

        assertThat(pc.route("orders").afterRetries().parkDelay()).isEqualTo(Duration.ofSeconds(30));
        assertThat(pc.route("orders").afterRetries().parkCycles()).isEqualTo(2);
        assertWithMessage("two reads of the view are two copies, so neither can be the route's own")
                .that(pc.route("orders").afterRetries())
                .isNotSameInstanceAs(pc.route("orders").afterRetries());
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
    }

    /**
     * A set of topics is one route: one function, one type pair, one policy (R5).
     * <p>
     * It used to say "one admission target" and prove it with a per-route concurrency declaration; that setting is
     * withdrawn from this milestone (owner-directed, 2026-09-12), so the retry limit carries the same claim - what
     * matters is that both topics resolve through the same route object rather than which setting demonstrates it.
     */
    @Test
    void aSetOfTopicsIsOneRouteWithOnePolicy() {
        var pc = define();
        pc.bytes(java.util.Arrays.asList("audit", "audit-replay")).retryLimit(4)
                .process(context -> Outcome.succeeded());

        assertThat(pc.topics()).containsExactly("audit", "audit-replay");
        assertThat(pc.routes()).hasSize(1);
        assertThat(pc.route("audit")).isSameInstanceAs(pc.route("audit-replay"));
        assertThat(pc.route("audit-replay").retryLimit().getAsInt()).isEqualTo(4);
    }
}
