package bz.stub.parallelconsumer.connect;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions.ProcessingOrder;
import bz.stub.parallelconsumer.state.ShardKey;
import bz.stub.parallelconsumer.streams.PcTaskDispatcher;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Proves the property the direction rests on: several sink tasks share a partition, keys run concurrently
 * across lanes, and no lane is ever entered twice at once.
 */
class PcSinkTaskLaneRouterTest {

    private static final String TOPIC = "input";
    private static final long AWAIT_SECONDS = 10;

    /** Records every entry, and fails loudly the moment two threads are inside it together. */
    private static final class RecordingSinkTask extends SinkTask {

        private final AtomicInteger concurrentEntries = new AtomicInteger();
        private final AtomicInteger maxConcurrentEntries = new AtomicInteger();
        private final List<SinkRecord> received = new ArrayList<>();
        private final CountDownLatch entered;
        private final CountDownLatch release;

        private RecordingSinkTask(final CountDownLatch entered, final CountDownLatch release) {
            this.entered = entered;
            this.release = release;
        }

        @Override
        public void put(final Collection<SinkRecord> records) {
            final int now = concurrentEntries.incrementAndGet();
            maxConcurrentEntries.accumulateAndGet(now, Math::max);
            try {
                synchronized (received) {
                    received.addAll(records);
                }
                if (entered != null) {
                    entered.countDown();
                }
                if (release != null) {
                    release.await(AWAIT_SECONDS, TimeUnit.SECONDS);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
            } finally {
                concurrentEntries.decrementAndGet();
            }
        }

        List<SinkRecord> received() {
            synchronized (received) {
                return Collections.unmodifiableList(new ArrayList<>(received));
            }
        }

        @Override
        public String version() {
            return "test";
        }

        @Override
        public void start(final Map<String, String> props) {
        }

        @Override
        public void stop() {
        }
    }

    private static ConsumerRecord<byte[], byte[]> record(final int partition, final String key, final long offset) {
        return new ConsumerRecord<>(TOPIC, partition, offset, bytes(key), bytes("v" + offset));
    }

    private static byte[] bytes(final String s) {
        return s == null ? null : s.getBytes(StandardCharsets.UTF_8);
    }

    /**
     * These arms exercise the lane lock, not the durability barrier, so the completion is discarded. Offset
     * composition has its own probe - {@link OffsetCompositionProbeTest}.
     */
    private static Runnable prepared(final PcSinkTaskLaneRouter router,
                                     final ConsumerRecord<byte[], byte[]> record) {
        return router.prepare(record, new PcTaskDispatcher.CompletionHandle() {
            @Override
            public void succeeded() {
                // discarded: this test asserts on exclusion, not on durability
            }

            @Override
            public void failed(final Throwable cause) {
                throw new AssertionError("no arm here fails a record", cause);
            }
        });
    }

    private static SinkRecord project(final ConsumerRecord<byte[], byte[]> r) {
        return new SinkRecord(r.topic(), r.partition(), Schema.OPTIONAL_BYTES_SCHEMA, r.key(),
                Schema.OPTIONAL_BYTES_SCHEMA, r.value(), r.offset());
    }

    private static List<PcSinkTaskLane> lanes(final int count, final CountDownLatch entered, final CountDownLatch release) {
        return IntStream.range(0, count)
                .mapToObj(i -> new PcSinkTaskLane(new RecordingSinkTask(entered, release)))
                .collect(Collectors.toList());
    }

    @Test
    void oneKeyAlwaysReachesOneLane() {
        final PcSinkTaskLaneRouter router = new PcSinkTaskLaneRouter(lanes(4, null, null),
                PcSinkTaskLaneRouterTest::project);

        // Distinct byte[] instances holding the same bytes: ShardKey compares deeply, so these are one shard.
        final PcSinkTaskLane first = router.laneFor(record(0, "alpha", 0));
        final PcSinkTaskLane second = router.laneFor(record(0, "alpha", 99));

        assertThat(first).isSameAs(second);
    }

    @Test
    void oneKeyInDifferentPartitionsIsTwoShardsAndMayTakeDifferentLanes() {
        final PcSinkTaskLaneRouter router = new PcSinkTaskLaneRouter(lanes(4, null, null),
                PcSinkTaskLaneRouterTest::project);

        // Documents the ShardKey contract the router inherits: KEY shards are (partition, key), not key
        // alone. Assert on the shard itself rather than on which lane it landed in - the lanes are chosen by
        // hash, so two distinct shards may legitimately share one, and a lane comparison would be a
        // hash-fragile way of asking a question about identity.
        final ShardKey inPartitionZero = ShardKey.of(record(0, "alpha", 0), ProcessingOrder.KEY);
        final ShardKey inPartitionOne = ShardKey.of(record(1, "alpha", 0), ProcessingOrder.KEY);

        assertThat(inPartitionZero)
                .as("the same key bytes in a different partition is a DIFFERENT shard, so nothing forces "
                        + "the two records onto one lane or orders them against each other")
                .isNotEqualTo(inPartitionOne);
        assertThat(router.laneFor(record(0, "alpha", 0)))
                .as("and within one partition the key still pins the lane")
                .isSameAs(router.laneFor(record(0, "alpha", 7)));
    }

    @Test
    void nullKeysStayOnOneLanePerPartition() {
        final PcSinkTaskLaneRouter router = new PcSinkTaskLaneRouter(lanes(4, null, null),
                PcSinkTaskLaneRouterTest::project);

        assertThat(router.laneFor(record(0, null, 0)))
                .isSameAs(router.laneFor(record(0, null, 1)));
    }

    @Test
    void distinctLanesRunConcurrently() throws Exception {
        // The antecedent is established before either callback is released: both must be INSIDE put()
        // at the same moment, which a timing-only assertion could never prove.
        final CountDownLatch bothEntered = new CountDownLatch(2);
        final CountDownLatch release = new CountDownLatch(1);
        final List<PcSinkTaskLane> lanes = lanes(8, bothEntered, release);
        final PcSinkTaskLaneRouter router = new PcSinkTaskLaneRouter(lanes, PcSinkTaskLaneRouterTest::project);

        final List<ConsumerRecord<byte[], byte[]>> distinctLaneRecords = twoRecordsOnDistinctLanes(router);
        final ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            distinctLaneRecords.forEach(r -> pool.submit(prepared(router, r)));

            assertThat(bothEntered.await(AWAIT_SECONDS, TimeUnit.SECONDS))
                    .as("both lanes must be inside put() together - if this times out they serialised")
                    .isTrue();
        } finally {
            release.countDown();
            pool.shutdown();
            assertThat(pool.awaitTermination(AWAIT_SECONDS, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void oneLaneIsNeverEnteredTwiceAtOnce() throws Exception {
        final PcSinkTaskLane lane = new PcSinkTaskLane(new RecordingSinkTask(null, null));
        final PcSinkTaskLaneRouter router = new PcSinkTaskLaneRouter(Collections.singletonList(lane),
                PcSinkTaskLaneRouterTest::project);

        final int threads = 8;
        final int perThread = 50;
        final CountDownLatch start = new CountDownLatch(1);
        final ExecutorService pool = Executors.newFixedThreadPool(threads);
        try {
            for (int t = 0; t < threads; t++) {
                final int id = t;
                pool.submit(() -> {
                    start.await();
                    for (int i = 0; i < perThread; i++) {
                        prepared(router, record(0, "k" + id + "-" + i, i)).run();
                    }
                    return null;
                });
            }
            start.countDown();
            pool.shutdown();
            assertThat(pool.awaitTermination(AWAIT_SECONDS * 3, TimeUnit.SECONDS)).isTrue();
        } finally {
            pool.shutdownNow();
        }

        final RecordingSinkTask task = (RecordingSinkTask) lane.getTask();
        assertThat(task.maxConcurrentEntries.get())
                .as("the lane's own re-entrancy detector - more than one means the lock did not hold")
                .isEqualTo(1);
        assertThat(task.received()).hasSize(threads * perThread);
    }

    @Test
    void negativeControlWithoutTheLockDetectsConcurrentEntry() throws Exception {
        // Proves the detector above can actually fail. Same load, same task, but the lock is bypassed by
        // calling the task directly - if this does NOT trip, the seriality assertion proves nothing.
        final RecordingSinkTask unguarded = new RecordingSinkTask(null, null);
        final int threads = 8;
        final CountDownLatch start = new CountDownLatch(1);
        final ExecutorService pool = Executors.newFixedThreadPool(threads);
        try {
            for (int t = 0; t < threads; t++) {
                final int id = t;
                pool.submit(() -> {
                    start.await();
                    for (int i = 0; i < 200; i++) {
                        unguarded.put(Collections.singletonList(project(record(0, "k" + id + "-" + i, i))));
                    }
                    return null;
                });
            }
            start.countDown();
            pool.shutdown();
            assertThat(pool.awaitTermination(AWAIT_SECONDS * 3, TimeUnit.SECONDS)).isTrue();
        } finally {
            pool.shutdownNow();
        }

        assertThat(unguarded.maxConcurrentEntries.get())
                .as("without the lane lock, concurrent entry must be observable - otherwise the "
                        + "seriality test above is vacuous")
                .isGreaterThan(1);
    }

    @Test
    void lifecycleCallbacksCannotInterleaveWithAnInFlightPut() throws Exception {
        final CountDownLatch inPut = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        final PcSinkTaskLane lane = new PcSinkTaskLane(new RecordingSinkTask(inPut, release));
        final PcSinkTaskLaneRouter router = new PcSinkTaskLaneRouter(Collections.singletonList(lane),
                PcSinkTaskLaneRouterTest::project);

        final ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            pool.submit(prepared(router, record(0, "k", 0)));
            assertThat(inPut.await(AWAIT_SECONDS, TimeUnit.SECONDS)).isTrue();

            final AtomicInteger callbackRan = new AtomicInteger();
            final var callback = pool.submit(() -> lane.runExclusively(callbackRan::incrementAndGet));

            // The put is parked inside the lock, so the callback must still be waiting.
            assertThat(callbackRan.get())
                    .as("a lifecycle callback must not enter while put() holds the lane")
                    .isZero();

            release.countDown();
            callback.get(AWAIT_SECONDS, TimeUnit.SECONDS);
            assertThat(callbackRan.get()).isEqualTo(1);
        } finally {
            release.countDown();
            pool.shutdownNow();
        }
    }

    /**
     * A throw out of {@code put} must reach the barrier, not just the dispatcher. Before this the router
     * called {@code lane.put} bare: the dispatcher caught the throw and failed the {@code WorkContainer}
     * directly, so the barrier kept the record staged with a completion handle nothing would ever report.
     */
    @Test
    void aThrowingPutFailsTheRecordThroughTheBarrier() {
        final PcSinkTaskLane lane = new PcSinkTaskLane(new ThrowingSinkTask());
        final PcSinkTaskLaneRouter router = new PcSinkTaskLaneRouter(Collections.singletonList(lane),
                PcSinkTaskLaneRouterTest::project);

        final AtomicInteger failures = new AtomicInteger();
        final AtomicInteger successes = new AtomicInteger();
        final Runnable work = router.prepare(record(0, "k", 0), new PcTaskDispatcher.CompletionHandle() {
            @Override
            public void succeeded() {
                successes.incrementAndGet();
            }

            @Override
            public void failed(final Throwable cause) {
                failures.incrementAndGet();
            }
        });

        assertThatThrownBy(work::run)
                .as("the throw must still propagate - the dispatcher's own accounting depends on seeing it")
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("sink refuses");
        assertThat(failures.get())
                .as("and the barrier must have reported the record failed, exactly once")
                .isEqualTo(1);
        assertThat(successes.get()).isZero();
    }

    /** A sink that cannot accept anything, so {@code put} always throws. */
    private static final class ThrowingSinkTask extends SinkTask {

        @Override
        public void put(final Collection<SinkRecord> records) {
            throw new IllegalStateException("sink refuses everything");
        }

        @Override
        public String version() {
            return "throwing";
        }

        @Override
        public void start(final Map<String, String> props) {
        }

        @Override
        public void stop() {
        }
    }

    @Test
    void routerRejectsAnEmptyLaneSet() {
        assertThatThrownBy(() -> new PcSinkTaskLaneRouter(Collections.emptyList(), PcSinkTaskLaneRouterTest::project))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("at least one lane");
    }

    /** Two records whose keys land on different lanes - searched for, not assumed, so the test is not hash-fragile. */
    private static List<ConsumerRecord<byte[], byte[]>> twoRecordsOnDistinctLanes(final PcSinkTaskLaneRouter router) {
        final Map<PcSinkTaskLane, ConsumerRecord<byte[], byte[]>> seen = new HashMap<>();
        for (int i = 0; i < 500; i++) {
            final ConsumerRecord<byte[], byte[]> candidate = record(0, "key-" + i, i);
            final PcSinkTaskLane lane = router.laneFor(candidate);
            if (!seen.containsKey(lane)) {
                seen.put(lane, candidate);
                if (seen.size() == 2) {
                    return new ArrayList<>(seen.values());
                }
            }
        }
        throw new IllegalStateException("could not find two keys mapping to distinct lanes");
    }
}
