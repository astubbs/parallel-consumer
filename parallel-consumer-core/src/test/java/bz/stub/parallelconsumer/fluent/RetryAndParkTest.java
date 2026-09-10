package bz.stub.parallelconsumer.fluent;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.FakeRuntimeException;
import bz.stub.parallelconsumer.ParallelConsumer;
import bz.stub.parallelconsumer.ParallelConsumerOptions.ProcessingOrder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.truth.Truth.assertThat;

/**
 * Retry, the limit, and what happens when it runs out (R9, R10, R11, R27, AE1, AE2).
 * <p>
 * <b>The limit counts the attempts after the first</b>, so a limit of two allows three runs and the fourth never
 * happens - which is the reading the definition's own javadoc and refusal messages already carry, and the one AE2
 * states. The parked record then stays incomplete in the offset map, holds no worker, and offsets past it still
 * commit.
 */
@Timeout(120)
class RetryAndParkTest {

    private static final String TOPIC = "orders";

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
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "retry-and-park-test");
        return properties;
    }

    /**
     * AE1's first clause. Retry-forever is opt-in, and it is the classic API's behaviour: the record is retried
     * indefinitely, nothing parks, and under partition ordering no offset past it commits, so the records behind
     * it never run either.
     */
    @Test
    void underTheExplicitUnboundedLimitAnAlwaysFailingRecordRetriesForeverAndBlocksItsPartition() {
        var attempts = new AtomicInteger();
        var laterRecordsRun = new AtomicInteger();
        var pc = ParallelConsumer.define(props()).defaultOrdering(ProcessingOrder.PARTITION);
        pc.string(TOPIC)
                .retryForever()
                .retryDelay(Duration.ofMillis(10))
                .process(context -> {
                    if (context.offset() == 0) {
                        attempts.incrementAndGet();
                        throw new FakeRuntimeException("this record never succeeds");
                    }
                    laterRecordsRun.incrementAndGet();
                    return Outcome.succeeded();
                });

        handle = runtime.startAndAssign(pc, 1);
        runtime.publish(TOPIC, 0, 0, "key-0", "the record that never succeeds");
        runtime.publish(TOPIC, 0, 1, "key-1", "behind it");
        runtime.publish(TOPIC, 0, 2, "key-2", "behind it too");

        RouteDispatcher dispatcher = pc.dispatcher();
        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(attempts.get()).isAtLeast(5));

        assertThat(dispatcher.parkedCount()).isEqualTo(0);
        // Partition ordering: the failing record is its partition's head, so nothing behind it runs...
        assertThat(laterRecordsRun.get()).isEqualTo(0);
        // ...and no offset past it commits.
        assertThat(runtime.committedOffset(TOPIC, 0)).isAtMost(0L);
    }

    /**
     * AE1's second clause and AE2. With no limit declared the route takes the default of ten attempts after the
     * first, so the record runs eleven times and then parks.
     */
    @Test
    void withNoLimitDeclaredTheDefaultIsTenAttemptsAfterTheFirstAndThenPark() {
        var attempts = new AtomicInteger();
        var pc = ParallelConsumer.define(props());
        pc.string(TOPIC)
                .retryDelay(Duration.ofMillis(5))
                .process(context -> {
                    attempts.incrementAndGet();
                    throw new FakeRuntimeException("this record never succeeds");
                });

        handle = runtime.startAndAssign(pc, 1);
        runtime.publish(TOPIC, 0, 0, "key-0", "the record that never succeeds");

        RouteDispatcher dispatcher = pc.dispatcher();
        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(dispatcher.parkedCount()).isEqualTo(1));

        assertThat(attempts.get()).isEqualTo(11);
        assertThat(dispatcher.ledger().attempts(TOPIC, 0, 0)).isEqualTo(11);
    }

    /**
     * AE2. A limit of two, three throws, no fourth attempt - and the parked record leaves the partition working:
     * the records past it complete and the partition commits with its offset map carrying what is still
     * incomplete, which is what park in place means (R11, R27).
     */
    @Test
    void aRecordAtItsLimitParksAndThePartitionCommitsPastIt() {
        var attempts = new AtomicInteger();
        var pc = ParallelConsumer.define(props()).defaultOrdering(ProcessingOrder.KEY);
        pc.string(TOPIC)
                .retryLimit(2)
                .retryDelay(Duration.ofMillis(10))
                .process(context -> {
                    if (context.offset() == 0) {
                        attempts.incrementAndGet();
                        throw new FakeRuntimeException("this record never succeeds");
                    }
                    return Outcome.succeeded();
                });

        handle = runtime.startAndAssign(pc, 1);
        runtime.publish(TOPIC, 0, 0, "hopeless", "the record that never succeeds");
        runtime.publish(TOPIC, 0, 1, "fine-1", "an order");
        runtime.publish(TOPIC, 0, 2, "fine-2", "another order");

        RouteDispatcher dispatcher = pc.dispatcher();
        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            assertThat(dispatcher.parkedCount()).isEqualTo(1);
            assertThat(dispatcher.succeededCount()).isEqualTo(2);
        });

        assertThat(attempts.get()).isEqualTo(3);
        assertThat(dispatcher.ledger().attempts(TOPIC, 0, 0)).isEqualTo(3);

        // The parked record stays incomplete, so the committed base offset cannot move past it...
        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(runtime.committedMetadata(TOPIC, 0)).isNotEmpty());
        assertThat(runtime.committedOffset(TOPIC, 0)).isEqualTo(0L);
        // ...and the records past it committed anyway, encoded in the commit's offset map. That payload is the
        // whole mechanism park in place rests on: without it, one hopeless record would hold the partition.

        // No fourth attempt, however long the instance runs: the far-future delay is doing its job.
        Awaitility.await().pollDelay(Duration.ofMillis(500)).atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> assertThat(attempts.get()).isEqualTo(3));
    }

    /**
     * The function's own park: a record it already knows is hopeless skips its remaining attempts (R8).
     */
    @Test
    void aFunctionThatReturnsParkSkipsTheRemainingAttempts() {
        var attempts = new AtomicInteger();
        var pc = ParallelConsumer.define(props());
        pc.string(TOPIC)
                .retryLimit(10)
                .retryDelay(Duration.ofMillis(10))
                .process(context -> {
                    attempts.incrementAndGet();
                    return Outcome.park("the schema is one this build cannot read");
                });

        handle = runtime.startAndAssign(pc, 1);
        runtime.publish(TOPIC, 0, 0, "key-0", "an order");

        RouteDispatcher dispatcher = pc.dispatcher();
        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(dispatcher.parkedCount()).isEqualTo(1));

        assertThat(attempts.get()).isEqualTo(1);
        Awaitility.await().pollDelay(Duration.ofMillis(300)).atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> assertThat(attempts.get()).isEqualTo(1));
    }

    /**
     * R10's per-assignment rule, with the cross-check KTD4 asks for: after a revoke and a re-assignment the
     * facade's count restarts at one, and the engine's own count - rebuilt on reassignment - agrees with it at
     * every hand-back.
     * <p>
     * The two are different numbers by design ({@link AttemptLedger}), and in this milestone they agree because
     * every hand-back here is a real failure. This is the test that would go red if a later unit added a hand-back
     * that is not an attempt and forgot to keep the ledger out of it.
     */
    @Test
    void afterARevokeAndReassignmentTheCountRestartsAtOneAndTheEnginesCountAgrees() {
        var pc = ParallelConsumer.define(props());
        pc.string(TOPIC)
                .retryForever()
                .retryDelay(Duration.ofMillis(400))
                .process(context -> {
                    throw new FakeRuntimeException("this record never succeeds");
                });

        handle = runtime.startAndAssign(pc, 1);
        runtime.publish(TOPIC, 0, 0, "key-0", "the record that never succeeds");

        RouteDispatcher dispatcher = pc.dispatcher();
        // Two hand-backs before the rebalance, so the counts are unambiguously above one when it happens.
        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(dispatcher.attemptCountsAtLastHandBack(TOPIC, 0, 0)).isEqualTo(new int[]{2, 2}));

        var partition = new TopicPartition(TOPIC, 0);
        runtime.mockConsumer().revoke(Collections.singletonList(partition));
        assertThat(dispatcher.ledger().attempts(TOPIC, 0, 0)).isEqualTo(0);

        runtime.mockConsumer().assign(Collections.singletonList(partition));
        // The new assignee starts from the committed position and the record is delivered again.
        runtime.mockConsumer().seek(partition, 0);
        runtime.publish(TOPIC, 0, 0, "key-0", "the record that never succeeds");

        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(dispatcher.attemptCountsAtLastHandBack(TOPIC, 0, 0)).isEqualTo(new int[]{1, 1}));
    }
}
