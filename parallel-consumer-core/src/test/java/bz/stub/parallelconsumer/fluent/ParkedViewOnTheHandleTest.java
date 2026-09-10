package bz.stub.parallelconsumer.fluent;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.FakeRuntimeException;
import bz.stub.parallelconsumer.ParallelConsumer;
import bz.stub.parallelconsumer.ParallelConsumerOptions.ProcessingOrder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * The parked set as an operator reaches it: by route from the handle, with an instance-wide roll-up named apart
 * (R28, AE20's query half).
 * <p>
 * The view is served from a control-thread snapshot rather than read live, because the facade's parked map and the
 * engine's offset map are written by different threads and a caller reading both would see them mid-step - and
 * because reconciling the two is what removes phantom entries, which is only sound on the control thread (KTD4).
 * Every assertion here therefore waits for a snapshot rather than reading immediately.
 */
@Timeout(180)
class ParkedViewOnTheHandleTest {

    private static final String TOPIC = "orders";

    private static final String OTHER_TOPIC = "audit";

    private final RecordingClientRuntime runtime = new RecordingClientRuntime();

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
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "parked-view-on-the-handle-test");
        return properties;
    }

    /**
     * A route that parks everything on its first failure, so a test can put a known number of records in the
     * parked set without waiting out any retries.
     */
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
        var pc = definitionThatParksEverything(TOPIC);
        handle = runtime.startAndAssign(pc, 1);
        runtime.publish(TOPIC, 0, 0, "key-0", "a hopeless order");
        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(handle.topic(TOPIC).parked().count()).isEqualTo(1));

        ParkedView parked = handle.topic(TOPIC).parked();
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
        var pc = definitionThatParksEverything(TOPIC);
        handle = runtime.startAndAssign(pc, 1);
        runtime.publish(TOPIC, 0, 0, "key-0", "a hopeless order");
        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(handle.topic(TOPIC).parked().count()).isEqualTo(1));

        ParkedView parked = handle.topic(TOPIC).parked();
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
     * Reconciliation (KTD4). A worker that finishes after its partition was revoked can leave an entry for a record
     * the engine no longer holds incomplete - and the entry is not merely hidden from the view, it is dropped from
     * the map, because it will never become true again.
     * <p>
     * The phantom is planted directly rather than raced into existence: the race is a revocation landing between a
     * worker's throw and its park, which no test can schedule, and what is being tested is the reconciliation, not
     * the race.
     */
    @Test
    void aParkedEntryTheEngineNoLongerHoldsIncompleteIsDropped() {
        var processed = new AtomicInteger();
        var pc = ParallelConsumer.connect(props()).defaultOrdering(ProcessingOrder.UNORDERED);
        pc.string(TOPIC).process(context -> {
            processed.incrementAndGet();
            return Outcome.succeeded();
        });
        handle = runtime.startAndAssign(pc, 1);
        runtime.publish(TOPIC, 0, 0, "key-0", "an order that succeeds");
        Awaitility.await().atMost(Duration.ofSeconds(30)).until(() -> processed.get() == 1);
        Awaitility.await().atMost(Duration.ofSeconds(30)).until(() ->
                handle.processor().workRemaining() == 0);

        // A parked entry for the offset that just completed: exactly the shape a worker finishing after a
        // revocation leaves behind.
        ConsumerRecord<byte[], byte[]> phantomRecord = new ConsumerRecord<>(TOPIC, 0, 0, null, null);
        boolean planted = pc.dispatcher().parkedRecords().park(new ParkedRecord(phantomRecord, "key-0", 1, 0,
                new FakeRuntimeException("a failure that arrived after the record had completed"),
                "it ran out of attempts", Instant.now()));
        assertThat(planted).isTrue();
        assertThat(pc.dispatcher().parkedRecords().count()).isEqualTo(1);

        // The next snapshot drops it - from the map, not just from the answer.
        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            assertThat(handle.topic(TOPIC).parked().count()).isEqualTo(0);
            assertThat(pc.dispatcher().parkedRecords().count()).isEqualTo(0);
        });
    }

    /**
     * The loop-end hook must never throw: the control loop runs its hooks as user code and a throw takes the
     * instance down, so a reporting fault would stop consuming (KTD4).
     * <p>
     * The engine's own accessor does not throw, so the only honest way to exercise the containment is to replace
     * the half of the snapshot that reads it - which is what {@code ParkedSnapshots.engineOffsets} exists for.
     */
    @Test
    void aSnapshotThatThrowsIsContainedAndTheInstanceKeepsRunning() {
        var processed = new AtomicInteger();
        var pc = ParallelConsumer.connect(props()).defaultOrdering(ProcessingOrder.UNORDERED);
        pc.string(TOPIC).process(context -> {
            processed.incrementAndGet();
            return Outcome.succeeded();
        });
        handle = runtime.startAndAssign(pc, 1);
        runtime.publish(TOPIC, 0, 0, "key-0", "an order");
        Awaitility.await().atMost(Duration.ofSeconds(30)).until(() -> processed.get() == 1);

        // Something for the snapshot to walk, so the throwing lookup is actually reached.
        ConsumerRecord<byte[], byte[]> parkedRecord = new ConsumerRecord<>(TOPIC, 0, 5, null, null);
        pc.dispatcher().parkedRecords().park(new ParkedRecord(parkedRecord, "key-5", 1, 0,
                new FakeRuntimeException("a failure"), "it ran out of attempts", Instant.now()));
        handle.parkedSnapshots().engineOffsets(parked -> {
            throw new FakeRuntimeException("the engine accessor blew up");
        });

        Awaitility.await().atMost(Duration.ofSeconds(30)).until(() -> handle.parkedSnapshots().hasFailed());

        // The instance is still consuming, and nobody awaiting it is told anything went wrong.
        runtime.publish(TOPIC, 0, 1, "key-1", "another order");
        Awaitility.await().atMost(Duration.ofSeconds(30)).until(() -> processed.get() == 2);
        assertThat(handle.awaitShutdown(Duration.ofMillis(500))).isFalse();
        assertThat(handle.failureCause().isPresent()).isFalse();
        assertThat(handle.processor().isClosedOrFailed()).isFalse();
    }
}
