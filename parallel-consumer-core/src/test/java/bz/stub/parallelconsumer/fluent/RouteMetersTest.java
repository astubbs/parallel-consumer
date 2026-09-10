package bz.stub.parallelconsumer.fluent;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.FakeRuntimeException;
import bz.stub.parallelconsumer.ParallelConsumer;
import bz.stub.parallelconsumer.ParallelConsumerOptions.ProcessingOrder;
import bz.stub.parallelconsumer.internal.utils.LogCapture;
import bz.stub.parallelconsumer.metrics.PCMetrics;
import ch.qos.logback.classic.Level;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.search.Search;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.truth.Truth.assertThat;

/**
 * The fluent API's meters (R19, KTD8): what each route did with its records, and what is parked right now.
 * <p>
 * They register through the {@code PCModule} the definition builds, which is what puts them in the user's own
 * registry beside every engine meter and has them swept by the same close - so what is asserted here is both that
 * they arrive with the right tags and that they leave.
 */
@Timeout(180)
class RouteMetersTest {

    private static final String TOPIC = "orders";

    /**
     * Unique to this test: {@link LogCapture} reads a logger shared with every other test in this module, so the
     * filter has to be something only this test can produce.
     */
    private static final String UNASSIGNED_TOPIC = "audit-nobody-assigns";

    private static final String OUTCOME_COUNTER = "pc.route.records";

    private static final String PARKED_GAUGE = "pc.route.parked.records";

    private static final String OLDEST_AGE_GAUGE = "pc.route.parked.oldest.age";

    private final RecordingClientRuntime runtime = new RecordingClientRuntime();

    private final SimpleMeterRegistry registry = new SimpleMeterRegistry();

    private ConsumerHandle handle;

    @AfterEach
    void closeTheInstance() {
        if (handle != null) {
            RecordingClientRuntime.closeWithoutDraining(handle);
        }
    }

    private static Properties props() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "route-meters-test");
        return properties;
    }

    private double counter(String topic, String outcome) {
        return Search.in(registry).name(OUTCOME_COUNTER).tag("topic", topic).tag("outcome", outcome)
                .counter().count();
    }

    /**
     * One counter per topic per outcome, from the first record - a meter that appears only once something has gone
     * wrong is a meter nobody has a dashboard for.
     */
    @Test
    void outcomeCountersCarryTheTopicAndTheOutcome() {
        var pc = ParallelConsumer.define(props())
                .meterRegistry(registry)
                .defaultOrdering(ProcessingOrder.UNORDERED);
        pc.string(TOPIC)
                .retryLimit(0)
                .retryDelay(Duration.ofMillis(10))
                .process(context -> {
                    switch (context.value()) {
                        case "succeed":
                            return Outcome.succeeded();
                        case "filter":
                            return Outcome.filtered();
                        default:
                            throw new FakeRuntimeException("this record parks");
                    }
                });

        handle = runtime.startAndAssign(pc, 1);
        // Registered at start, before any record: all four outcomes are there reading zero.
        assertThat(counter(TOPIC, "succeeded")).isEqualTo(0d);
        assertThat(counter(TOPIC, "filtered")).isEqualTo(0d);
        assertThat(counter(TOPIC, "parked")).isEqualTo(0d);
        assertThat(counter(TOPIC, "stopped")).isEqualTo(0d);

        runtime.publish(TOPIC, 0, 0, "key-0", "succeed");
        runtime.publish(TOPIC, 0, 1, "key-1", "filter");
        runtime.publish(TOPIC, 0, 2, "key-2", "park");

        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            assertThat(counter(TOPIC, "succeeded")).isEqualTo(1d);
            assertThat(counter(TOPIC, "filtered")).isEqualTo(1d);
            assertThat(counter(TOPIC, "parked")).isEqualTo(1d);
        });
        assertThat(counter(TOPIC, "stopped")).isEqualTo(0d);
    }

    /**
     * The gauges are the live parked set, per partition - which is a different figure from the parked counter, and
     * R19 asks for both. The counter counts park events and never goes down; this is the size of the set an
     * operator can act on.
     */
    @Test
    void parkedGaugesCarryTheTopicAndPartitionAndReadTheLiveSet() {
        var pc = ParallelConsumer.define(props())
                .meterRegistry(registry)
                .defaultOrdering(ProcessingOrder.UNORDERED);
        pc.string(TOPIC)
                .retryLimit(0)
                .retryDelay(Duration.ofMillis(10))
                .process(context -> {
                    throw new FakeRuntimeException("this record parks");
                });

        handle = runtime.startAndAssign(pc, 2);
        runtime.publish(TOPIC, 0, 0, "key-0", "an order");
        runtime.publish(TOPIC, 0, 1, "key-1", "another order");
        runtime.publish(TOPIC, 1, 0, "key-2", "an order on the other partition");

        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            assertThat(gauge(PARKED_GAUGE, 0)).isEqualTo(2d);
            assertThat(gauge(PARKED_GAUGE, 1)).isEqualTo(1d);
            assertThat(gauge(OLDEST_AGE_GAUGE, 0)).isGreaterThan(0d);
        });
        // A gauge exists for every assigned partition, whether or not anything is parked on it.
        assertThat(Search.in(registry).name(PARKED_GAUGE).gauges()).hasSize(2);
        assertThat(counter(TOPIC, "parked")).isEqualTo(3d);
    }

    private double gauge(String name, int partition) {
        Gauge gauge = Search.in(registry).name(name).tag("topic", TOPIC)
                .tag("partition", String.valueOf(partition)).gauge();
        return gauge == null ? Double.NaN : gauge.value();
    }

    /**
     * Closing takes every meter this instance registered back out of the user's registry - the registry outlives
     * the instance, so anything left behind is a leak reported for ever at its last value.
     */
    @Test
    void everyRouteMeterIsGoneAfterTheInstanceCloses() {
        var pc = ParallelConsumer.define(props())
                .meterRegistry(registry)
                .defaultOrdering(ProcessingOrder.UNORDERED);
        pc.string(TOPIC)
                .retryLimit(0)
                .retryDelay(Duration.ofMillis(10))
                .process(context -> {
                    throw new FakeRuntimeException("this record parks");
                });

        ConsumerHandle started = runtime.startAndAssign(pc, 1);
        handle = started;
        runtime.publish(TOPIC, 0, 0, "key-0", "an order");
        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            assertThat(Search.in(registry).name(PARKED_GAUGE).gauges()).isNotEmpty();
            assertThat(counter(TOPIC, "parked")).isEqualTo(1d);
        });

        // Not the handle's drain: a parked record never completes, so a drain would wait out the drain timeout.
        RecordingClientRuntime.closeWithoutDraining(started);
        started.close();
        handle = null;

        assertThat(Search.in(registry).name(OUTCOME_COUNTER).meters()).isEmpty();
        assertThat(Search.in(registry).name(PARKED_GAUGE).meters()).isEmpty();
        assertThat(Search.in(registry).name(OLDEST_AGE_GAUGE).meters()).isEmpty();
    }

    /**
     * Deregistration on its own, over a registry nothing else is touching.
     * <p>
     * <b>The instance-level test above cannot see this method work</b>, because the engine's own close sweeps every
     * meter registered through its {@code PCMetrics} - including these - so it passes whether or not this runs.
     * Measured: making {@code deregister()} a no-op leaves that test green. What this method buys is a removal at a
     * moment the handle chooses rather than one that depends on the engine's shutdown reaching its metrics step, and
     * this is the test that holds it to it.
     */
    @Test
    void deregisteringRemovesEveryMeterItRegistered() {
        PCMetrics metrics = new PCMetrics(registry, Collections.emptyList(), "route-meters-unit-test");
        ParkedSnapshots snapshots = new ParkedSnapshots(new ParkedRecords());
        FluentMeters meters = FluentMeters.registerFor(metrics, Collections.singletonList(TOPIC), snapshots);
        meters.syncPartitionGauges(Collections.singleton(new TopicPartition(TOPIC, 0)));

        assertThat(Search.in(registry).name(OUTCOME_COUNTER).meters()).hasSize(4);
        assertThat(Search.in(registry).name(PARKED_GAUGE).meters()).hasSize(1);
        assertThat(Search.in(registry).name(OLDEST_AGE_GAUGE).meters()).hasSize(1);

        meters.deregister();

        assertThat(Search.in(registry).name(OUTCOME_COUNTER).meters()).isEmpty();
        assertThat(Search.in(registry).name(PARKED_GAUGE).meters()).isEmpty();
        assertThat(Search.in(registry).name(OLDEST_AGE_GAUGE).meters()).isEmpty();
        // Idempotent: a handle that closes twice must not go back to the registry a second time.
        meters.deregister();
    }

    /**
     * A route whose topic was assigned no partition processes nothing and says nothing, which reads as a broken
     * function rather than as a subscription that matched nothing. It is said once, after the first assignment.
     */
    @Test
    void aRouteWithNoAssignmentIsLoggedOnce() {
        var processed = new AtomicInteger();
        var pc = ParallelConsumer.define(props()).defaultOrdering(ProcessingOrder.UNORDERED);
        pc.string(TOPIC).process(context -> {
            processed.incrementAndGet();
            return Outcome.succeeded();
        });
        pc.string(UNASSIGNED_TOPIC).process(context -> Outcome.succeeded());

        List<String> warnings;
        try (LogCapture logs = LogCapture.of(ConsumerHandle.class, Level.WARN)) {
            Map<TopicPartition, Long> beginning = new HashMap<>();
            beginning.put(new TopicPartition(TOPIC, 0), 0L);
            runtime.mockConsumer().updateBeginningOffsets(beginning);
            handle = pc.start(runtime);
            // Only one of the two routed topics is assigned anything - the other's route is dark.
            runtime.mockConsumer().subscribeWithRebalanceAndAssignment(Collections.singletonList(TOPIC), 1);

            Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                    assertThat(logs.messagesAt(Level.WARN, "assigned no partition", UNASSIGNED_TOPIC)).isNotEmpty());
            // Several more control-loop passes, each of which runs the hook again: processing a record takes a
            // poll, a dispatch and a completion, so waiting for one is a wait for the loop rather than for a clock.
            runtime.publish(TOPIC, 0, 0, "key-0", "an order");
            Awaitility.await().atMost(Duration.ofSeconds(30)).until(() -> processed.get() == 1);
            warnings = logs.messagesAt(Level.WARN, "assigned no partition", UNASSIGNED_TOPIC);
        }

        assertThat(warnings).hasSize(1);
    }
}
