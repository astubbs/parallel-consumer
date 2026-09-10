package bz.stub.parallelconsumer.fluent;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.FakeRuntimeException;
import bz.stub.parallelconsumer.ParallelConsumer;
import bz.stub.parallelconsumer.ParallelConsumerOptions.ProcessingOrder;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.common.TopicPartition;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * The parked set as an operator reaches it: by route from the handle, with an instance-wide roll-up named apart
 * (R28, AE20's query half).
 * <p>
 * The view is read from the engine's own retry queue when the handle is asked, so there is one parked set rather
 * than a facade copy kept in step with one. Assertions still wait rather than read immediately, because a record
 * reaches the queue on the control thread a moment after the worker hands it back.
 */
@Timeout(180)
class ParkedViewOnTheHandleTest extends AbstractFluentEngineTest {


    private static final String OTHER_TOPIC = "audit";



    /**
     * A route that parks everything on its first failure, so a test can put a known number of records in the
     * parked set without waiting out any retries.
     */
    /**
     * Start a definition that parks everything, publish one record, and hand back its route's parked view once the
     * record is in it. Three of the scenarios below differ only in what they then ask that view.
     */
    private ParkedView oneParkedRecordOn(String topic) {
        var pc = definitionThatParksEverything(topic);
        handle = runtime.startAndAssign(pc, 1);
        runtime.publish(topic, 0, 0, "key-0", "a hopeless order");
        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(handle.topic(topic).parked().count()).isEqualTo(1));
        return handle.topic(topic).parked();
    }

    private ParallelConsumerDefinition definitionThatParksEverything(String... topics) {
        var pc = ParallelConsumer.connect(props()).defaultOrdering(ProcessingOrder.UNORDERED);
        for (String topic : topics) {
            pc.string(topic)
                    .retryLimit(0)
                    .retryDelay(Duration.ofMillis(10))
                    .process(context -> {
                        throw new FakeRuntimeException("nothing on " + context.topic() + " can be processed");
                    });
        }
        return pc;
    }

    /**
     * AE20's query half. Three parked records on one partition: the count, the oldest one's age, and the list with
     * everything an operator needs to decide what to do about each of them.
     */
    @Test
    void aRoutesParkedViewReportsTheCountTheOldestAgeAndTheRecords() {
        var pc = definitionThatParksEverything(TOPIC);
        handle = runtime.startAndAssign(pc, 1);
        Instant beforeAnyParked = Instant.now();
        runtime.publish(TOPIC, 0, 0, "key-0", "the first hopeless order");
        runtime.publish(TOPIC, 0, 1, "key-1", "the second hopeless order");
        runtime.publish(TOPIC, 0, 2, "key-2", "the third hopeless order");

        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(handle.topic(TOPIC).parked().count()).isEqualTo(3));

        ParkedView parked = handle.topic(TOPIC).parked();
        assertThat(parked.name()).isEqualTo(TOPIC);
        assertThat(parked.topics()).containsExactly(TOPIC);
        // The default view spans every partition, so it is not narrowed to one.
        assertThat(parked.partition().isPresent()).isFalse();
        assertThat(parked.oldestAge().isPresent()).isTrue();
        assertThat(parked.oldestAge().get()).isLessThan(Duration.between(beforeAnyParked, Instant.now()).plusSeconds(1));

        List<ParkedRecord> records = parked.records();
        assertThat(records).hasSize(3);
        for (ParkedRecord record : records) {
            assertThat(record.topic()).isEqualTo(TOPIC);
            assertThat(record.partition()).isEqualTo(0);
            assertThat(record.offset()).isIn(Arrays.asList(0L, 1L, 2L));
            assertThat(record.key()).isEqualTo("key-" + record.offset());
            // A limit of zero allows one run, and that run is what the count reports.
            assertThat(record.attempts()).isEqualTo(1);
            assertThat(record.cycles()).isEqualTo(0);
            assertThat(record.failure()).isInstanceOf(FakeRuntimeException.class);
            assertThat(record.reason()).contains("ran out of attempts");
            assertThat(record.parkedSince()).isAtLeast(beforeAnyParked);
        }
    }

    /**
     * The two commands an operator will reach for, and the answer they get today: a refusal that says which
     * milestone brings them, rather than a silent no-op or an empty success.
     */
    @Test
    void resumeAndDlqRefuseAndSayWhy() {
        ParkedView parked = oneParkedRecordOn(TOPIC);
        ParkedRecord record = parked.records().get(0);

        assertThat(assertThrows(UnsupportedOperationException.class, () -> parked.resume(record)))
                .hasMessageThat().contains("resume is not supported in this version");
        assertThat(assertThrows(UnsupportedOperationException.class, parked::resume))
                .hasMessageThat().contains("engine accessor");
        assertThat(assertThrows(UnsupportedOperationException.class, () -> parked.dlq(record)))
                .hasMessageThat().contains("dlq is not supported in this version");
        assertThat(assertThrows(UnsupportedOperationException.class, parked::dlq))
                .hasMessageThat().contains("engine accessor");

        // The record is still parked afterwards: a refused command changes nothing.
        assertThat(handle.topic(TOPIC).parked().count()).isEqualTo(1);
    }

    /**
     * The three figures R28 asks for that this version cannot answer read empty rather than zero. Zero would be a
     * number an operator could act on, and it would be wrong.
     */
    @Test
    void thePayloadFiguresReadEmptyUntilTheEngineAccessorsLand() {
        ParkedView parked = oneParkedRecordOn(TOPIC);
        assertThat(parked.payloadFraction().isPresent()).isFalse();
        assertThat(parked.estimatedTimeToExport().isPresent()).isFalse();
        assertThat(parked.heldBehind(parked.records().get(0)).isPresent()).isFalse();
        assertThat(parked.toString()).contains("not available");
    }

    /**
     * The roll-up totals every route and is reached by a name of its own, so the per-route accessor never has to
     * mean two things (R28).
     */
    @Test
    void theInstanceRollUpTotalsEveryRoute() {
        var pc = definitionThatParksEverything(TOPIC, OTHER_TOPIC);
        handle = runtime.startAndAssign(pc, 2);
        runtime.publish(TOPIC, 0, 0, "key-0", "a hopeless order");
        runtime.publish(TOPIC, 1, 0, "key-1", "another hopeless order");
        runtime.publish(OTHER_TOPIC, 0, 0, "key-2", "a hopeless audit record");

        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(handle.parkedAllTopics().count()).isEqualTo(3));

        assertThat(handle.parkedAllTopics().topics()).containsExactly(TOPIC, OTHER_TOPIC);
        // Each route sees only its own, and spans every partition of it by default.
        assertThat(handle.topic(TOPIC).parked().count()).isEqualTo(2);
        assertThat(handle.topic(OTHER_TOPIC).parked().count()).isEqualTo(1);

        // One partition on request - the rare case.
        assertThat(handle.topic(TOPIC).parked().partition(0).count()).isEqualTo(1);
        assertThat(handle.topic(TOPIC).parked().partition(1).count()).isEqualTo(1);
        assertThat(handle.topic(TOPIC).parked().partition(0).partition().getAsInt()).isEqualTo(0);

        List<ParkedView> byPartition = handle.topic(TOPIC).parked().byPartition();
        assertThat(byPartition).hasSize(2);
        assertThat(byPartition.get(0).partition().getAsInt()).isEqualTo(0);
        assertThat(byPartition.get(1).partition().getAsInt()).isEqualTo(1);
        assertThat(byPartition.get(0).count()).isEqualTo(1);
    }

    /**
     * A topic nothing routes is refused rather than answered with an empty parked set, which would read as good
     * news about a misspelled topic name.
     */
    @Test
    void askingAboutATopicNothingRoutesIsRefused() {
        var pc = definitionThatParksEverything(TOPIC);
        handle = runtime.startAndAssign(pc, 1);

        IllegalArgumentException refused = assertThrows(IllegalArgumentException.class,
                () -> handle.topic("a-topic-nobody-declared"));

        assertThat(refused).hasMessageThat().contains("No route claims topic a-topic-nobody-declared");
        assertThat(refused).hasMessageThat().contains(TOPIC);
    }

    /**
     * There is no second store to go stale, and this is what that buys (KTD14).
     * <p>
     * The parked set is the engine's retry queue, read when it is asked for. A revocation takes the partition's
     * containers out of that queue, so the parked view empties on its own - nothing has to notice the revocation
     * and clear anything, and there is no window in which the view lists a record the engine is no longer holding.
     * The predecessor of this test planted a phantom entry by hand in a facade-side map and asserted that a
     * reconciliation pass dropped it; there is no map to plant one in any more.
     */
    @Test
    void revokingAPartitionEmptiesItsParkedViewWithNothingHavingToClearIt() {
        var pc = ParallelConsumer.connect(props()).defaultOrdering(ProcessingOrder.UNORDERED);
        pc.string(TOPIC)
                .retryLimit(0)
                .retryDelay(Duration.ofMillis(10))
                .process(context -> Outcome.park("this record is hopeless"));

        handle = runtime.startAndAssign(pc, 1);
        runtime.publish(TOPIC, 0, 0, "key-0", "an order that parks");
        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(handle.topic(TOPIC).parked().count()).isEqualTo(1));

        runtime.mockConsumer().revoke(Collections.singletonList(new TopicPartition(TOPIC, 0)));

        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            assertThat(handle.topic(TOPIC).parked().count()).isEqualTo(0);
            assertThat(handle.parkedAllTopics().count()).isEqualTo(0);
        });
    }

    /**
     * The loop-end hook must never throw: the control loop runs its hooks as user code and a throw takes the
     * instance down, so a reporting fault would stop consuming.
     * <p>
     * What the hook does now is bring the parked gauges into line with the assignment, and registering a gauge
     * calls into the <b>user's</b> meter registry - third-party code, running inside PC's control loop, which is
     * exactly the shape that must not be able to stop it. A registry that throws on every gauge is the honest way
     * to exercise the containment: the parked read itself is a query on the caller's thread now, so a fault there
     * can only fail the query.
     */
    @Test
    void aMeterRegistryThatThrowsIsContainedAndTheInstanceKeepsRunning() {
        var processed = new AtomicInteger();
        var pc = ParallelConsumer.connect(props())
                .defaultOrdering(ProcessingOrder.UNORDERED)
                .meterRegistry(new ThrowingOnGaugeRegistry());
        pc.string(TOPIC).process(context -> {
            processed.incrementAndGet();
            return Outcome.succeeded();
        });
        handle = runtime.startAndAssign(pc, 1);
        runtime.publish(TOPIC, 0, 0, "key-0", "an order");
        Awaitility.await().atMost(Duration.ofSeconds(30)).until(() -> processed.get() == 1);

        // The instance is still consuming, and nobody awaiting it is told anything went wrong.
        runtime.publish(TOPIC, 0, 1, "key-1", "another order");
        Awaitility.await().atMost(Duration.ofSeconds(30)).until(() -> processed.get() == 2);
        assertThat(handle.awaitShutdown(Duration.ofMillis(500))).isFalse();
        assertThat(handle.failureCause().isPresent()).isFalse();
        assertThat(handle.processor().isClosedOrFailed()).isFalse();
    }

    /**
     * A user's registry that refuses exactly the parked gauges - the ones the loop-end hook registers, and only
     * those, so the engine's own meters still register and the instance starts normally. Refusing every gauge would
     * fail the engine's construction instead, which is a different test.
     */
    private static class ThrowingOnGaugeRegistry extends SimpleMeterRegistry {

        @Override
        protected <T> io.micrometer.core.instrument.Gauge newGauge(Meter.Id id, T obj,
                                                                   java.util.function.ToDoubleFunction<T> f) {
            if (id.getName().contains("route.parked")) {
                throw new FakeRuntimeException("this registry refuses the parked gauges");
            }
            return super.newGauge(id, obj, f);
        }
    }
}
