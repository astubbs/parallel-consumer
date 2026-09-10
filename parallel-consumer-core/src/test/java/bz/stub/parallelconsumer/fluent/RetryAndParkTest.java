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
class RetryAndParkTest extends AbstractFluentEngineTest {




    /**
     * AE1's first clause. Retry-forever is opt-in, and it is the classic API's behaviour: the record is retried
     * indefinitely, nothing parks, and under partition ordering no offset past it commits, so the records behind
     * it never run either.
     */
    @Test
    void underTheExplicitUnboundedLimitAnAlwaysFailingRecordRetriesForeverAndBlocksItsPartition() {
        var attempts = new AtomicInteger();
        var laterRecordsRun = new AtomicInteger();
        var pc = ParallelConsumer.connect(props()).defaultOrdering(ProcessingOrder.PARTITION);
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
        var pc = ParallelConsumer.connect(props());
        pc.string(TOPIC)
                .retryDelay(Duration.ofMillis(5))
                .process(context -> {
                    attempts.incrementAndGet();
                    throw new FakeRuntimeException("this record never succeeds");
                });

        handle = runtime.startAndAssign(pc, 1);
        runtime.publish(TOPIC, 0, 0, "key-0", "the record that never succeeds");

        RouteDispatcher dispatcher = pc.dispatcher();
        // On the parked VIEW rather than the park counter: the counter moves on the worker thread at the moment of
        // the hand-back, and the record reaches the engine's retry queue - which is the view - a moment later on the
        // control thread. Waiting on one and reading the other is a race that only shows up under load.
        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(dispatcher.parkedForRoute(TOPIC)).hasSize(1));

        assertThat(attempts.get()).isEqualTo(11);
        assertThat(dispatcher.parkedForRoute(TOPIC).get(0).attempts()).isEqualTo(11);
    }

    /**
     * AE2. A limit of two, three throws, no fourth attempt - and the parked record leaves the partition working:
     * the records past it complete and the partition commits with its offset map carrying what is still
     * incomplete, which is what park in place means (R11, R27).
     */
    @Test
    void aRecordAtItsLimitParksAndThePartitionCommitsPastIt() {
        var attempts = new AtomicInteger();
        var pc = ParallelConsumer.connect(props()).defaultOrdering(ProcessingOrder.KEY);
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
            assertThat(dispatcher.parkedForRoute(TOPIC)).hasSize(1);
            assertThat(dispatcher.succeededCount()).isEqualTo(2);
        });

        assertThat(attempts.get()).isEqualTo(3);

        // It is in the parked view, which is what an operator reads to decide between resume and export (R28).
        assertThat(dispatcher.parkedForRoute(TOPIC)).hasSize(1);
        ParkedRecord parked = dispatcher.parkedForRoute(TOPIC).get(0);
        assertThat(parked.offset()).isEqualTo(0);
        assertThat(parked.key()).isEqualTo("hopeless");
        assertThat(parked.attempts()).isEqualTo(3);
        assertThat(parked.failure()).isInstanceOf(FakeRuntimeException.class);

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
        var pc = ParallelConsumer.connect(props());
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
     * R10's per-assignment rule: after a revoke and a re-assignment the record's attempt count restarts.
     * <p>
     * There is one count and it is the engine's - the facade kept a second one until the throw could say "this
     * hand-back was not an attempt", and the pair needed a cross-check to stay believable. What the reset rests on
     * now is the engine's own bookkeeping: a revoked partition's containers go, and the record that comes back is a
     * fresh container counting from zero. This test is what would go red if that stopped being true.
     * <p>
     * The count is read where a user would read it - the number of attempts the record's context reports - so the
     * assertion is over the same figure the retry limit is measured against.
     */
    @Test
    void afterARevokeAndReassignmentTheAttemptCountRestarts() {
        var attemptsSeen = new java.util.concurrent.CopyOnWriteArrayList<Integer>();
        var pc = ParallelConsumer.connect(props());
        pc.string(TOPIC)
                .retryForever()
                .retryDelay(Duration.ofMillis(200))
                .process(context -> {
                    attemptsSeen.add(context.failedAttempts());
                    throw new FakeRuntimeException("this record never succeeds");
                });

        handle = runtime.startAndAssign(pc, 1);
        runtime.publish(TOPIC, 0, 0, "key-0", "the record that never succeeds");

        // Three runs before the rebalance, so the count is unambiguously above zero when it happens: the third run
        // sees two prior failures.
        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(attemptsSeen).contains(2));

        var partition = new TopicPartition(TOPIC, 0);
        runtime.mockConsumer().revoke(Collections.singletonList(partition));
        attemptsSeen.clear();

        runtime.mockConsumer().assign(Collections.singletonList(partition));
        // The new assignee starts from the committed position and the record is delivered again.
        runtime.mockConsumer().seek(partition, 0);
        runtime.publish(TOPIC, 0, 0, "key-0", "the record that never succeeds");

        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(attemptsSeen).isNotEmpty());
        assertThat(attemptsSeen.get(0)).isEqualTo(0);
    }
}
