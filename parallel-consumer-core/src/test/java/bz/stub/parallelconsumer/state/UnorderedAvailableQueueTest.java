package bz.stub.parallelconsumer.state;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.ParallelConsumerOptions.ProcessingOrder;
import bz.stub.parallelconsumer.internal.EpochAndRecordsMap;
import bz.stub.parallelconsumer.internal.PCModuleTestEnv;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import pl.tlinkowski.unij.api.UniLists;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;

/**
 * Under {@code UNORDERED}, a shard offers only the records that are not out at a worker: a record leaves when it
 * is TAKEN and comes back when its delivery lands.
 * <p>
 * WHY THIS EXISTS. Departure-on-take is what makes unordered selection cost one examination per record instead of
 * one per in-flight record ahead of it - the difference between a direct-pull engine that works at 5,000 workers
 * and one that does not. It replaced an index of unheld offsets ({@code ShardOccupancy}) that bought the same
 * number by walking beside the entry map instead of shrinking it. <b>The direction in which either can be wrong is
 * the same dangerous one</b>: a record that leaves the offerable set and is never put back is never offered again,
 * which is a silent stall, the shape of the confluentinc#857 family. Nothing in the delivery tests would notice,
 * because the other records still flow.
 * <p>
 * So every test here states what the shard should be holding in each half - offerable and in flight - from what
 * the test itself did, rather than asking the shard to agree with itself, and additionally asserts the invariant
 * that gives the whole design its cost: <b>no record in the offerable set is out at a worker</b>. That one is
 * scan-derived, so it cannot be satisfied by a counter that has drifted.
 *
 * @author Antony Stubbs
 * @see ProcessingShard#getWorkIfAvailable
 * @see UnorderedRetryOffsetOrderTest
 */
class UnorderedAvailableQueueTest {

    static final String TOPIC = "selectable-index-topic";
    static final TopicPartition TP = new TopicPartition(TOPIC, 0);

    PCModuleTestEnv module;
    WorkManager<String, String> wm;

    void setup(ProcessingOrder ordering) {
        module = new PCModuleTestEnv(ParallelConsumerOptions.<String, String>builder()
                .ordering(ordering)
                .build());
        wm = module.workManager();
        wm.onPartitionsAssigned(UniLists.of(TP));
    }

    void register(int fromOffset, int count) {
        List<ConsumerRecord<String, String>> recs = new ArrayList<>(count);
        for (int i = fromOffset; i < fromOffset + count; i++) {
            recs.add(new ConsumerRecord<>(TOPIC, 0, i, "any-key", "value-" + i));
        }
        Map<TopicPartition, List<ConsumerRecord<String, String>>> m = new HashMap<>();
        m.put(TP, recs);
        wm.registerWork(new EpochAndRecordsMap<>(new ConsumerRecords<>(m), wm.getPm()));
    }

    ProcessingShard<String, String> theShard() {
        var sm = wm.getSm();
        var shard = sm.getShard(sm.computeShardKey(new ConsumerRecord<>(TOPIC, 0, 0, "any-key", "v")));
        assertWithMessage("the shard must exist").that(shard.isPresent()).isTrue();
        return shard.get();
    }

    /**
     * The invariant, stated once, with both halves supplied by the caller from what it actually did - a shard that
     * merely agrees with itself proves nothing.
     * <p>
     * The third assertion is the one that cannot be faked by a drifted counter: it scans the offerable entries for
     * a record that is out at a worker, which under this design must never be there.
     */
    void assertShardHolds(ProcessingShard<String, String> shard, long offerable, long inFlight) {
        assertWithMessage("the shard must offer exactly the records that are neither out at a worker nor "
                        + "finished with. Too few is a stranded record - nothing is holding it, and nothing will "
                        + "ever offer it again.")
                .that(shard.countOfferable()).isEqualTo(offerable);
        assertWithMessage("the shard must still be responsible for both halves").that(shard.getCountOfWorkInFlight())
                .isEqualTo(inFlight);
        assertWithMessage("and it is responsible for their sum").that(shard.getCountOfWorkTracked())
                .isEqualTo(offerable + inFlight);
        assertWithMessage("no record out at a worker may still be sitting in the offerable set - that is the "
                        + "in-flight prefix this design exists to remove, and it is scanned rather than counted")
                .that(shard.countWorkInFlightByScan()).isEqualTo(0L);
    }

    @Test
    void everyRecordIsOfferableOnArrivalAndLeavesTheShardWhenClaimed() {
        setup(ProcessingOrder.UNORDERED);
        register(0, 50);
        var shard = theShard();
        assertShardHolds(shard, 50, 0);

        var taken = wm.getWorkIfAvailable(20);
        assertThat(taken).hasSize(20);
        assertWithMessage("twenty claimed records must have left the offerable set")
                .that(shard.countOfferable()).isEqualTo(30);
        assertShardHolds(shard, 30, 20);
    }

    /**
     * A failure returns the record to selection, so it has to return to the shard's offerable set - and this is
     * the transition a departure-on-take design most easily loses, because the record's own landing is the only
     * thing that can put it back.
     */
    @Test
    void aFailedRecordComesBackIntoTheOfferableSet() {
        setup(ProcessingOrder.UNORDERED);
        register(0, 10);
        var shard = theShard();

        var taken = wm.getWorkIfAvailable(1);
        assertThat(taken).hasSize(1);
        assertShardHolds(shard, 9, 1);

        var wc = taken.get(0);
        wc.onUserFunctionFailure(new RuntimeException("deliberate"));
        wm.handleFutureResult(wc);

        assertWithMessage("a failed record is selectable again, so it must be back in the offerable set")
                .that(shard.countOfferable()).isEqualTo(10);
        assertShardHolds(shard, 10, 0);
    }

    /**
     * Abandonment - a return with no verdict at all - is the other way back, and unlike a failure it earns no
     * retry delay, so the record must be immediately offerable again.
     */
    @Test
    void anAbandonedRecordComesBackIntoTheOfferableSet() {
        setup(ProcessingOrder.UNORDERED);
        register(0, 10);
        var shard = theShard();

        var wc = wm.getWorkIfAvailable(1).get(0);
        assertShardHolds(shard, 9, 1);

        wc.markAbandoned(wc.getDeliveryCount());
        wm.handleFutureResult(wc);

        assertShardHolds(shard, 10, 0);
        assertWithMessage("and it is handed out again straight away, with no retry delay to wait out")
                .that(wm.getWorkIfAvailable(10)).hasSize(10);
    }

    @Test
    void aSucceededRecordLeavesTheShardAltogether() {
        setup(ProcessingOrder.UNORDERED);
        register(0, 10);
        var shard = theShard();

        var taken = wm.getWorkIfAvailable(1);
        var wc = taken.get(0);
        wc.onUserFunctionSuccess();
        wm.handleFutureResult(wc);

        assertWithMessage("a succeeded record has left the shard, so it must not be left behind as offerable")
                .that(shard.countOfferable()).isEqualTo(9);
        assertShardHolds(shard, 9, 0);
        assertWithMessage("and the conservation figure agrees it is gone")
                .that(wm.getNumberOfRecordsInShards()).isEqualTo(9L);
    }

    /**
     * THE ONE THAT MATTERS. Every record must still be delivered exactly once when many workers select
     * concurrently - which is the way departure-on-take breaks, by omission rather than by throwing.
     * <p>
     * Records are completed as they are taken, so the shard drains and the run ends only if every record was
     * offered. A stranded record does not fail an assertion here, it hangs the test - which is why the drain is
     * bounded and the count is asserted afterwards rather than being the loop's only exit.
     */
    @Test
    void everyRecordIsStillDeliveredExactlyOnceUnderConcurrentSelection() throws Exception {
        setup(ProcessingOrder.UNORDERED);
        int total = 2_000;
        register(0, total);

        int pullers = 8;
        var handedOut = Collections.synchronizedList(new ArrayList<WorkContainer<String, String>>());
        ExecutorService pool = Executors.newFixedThreadPool(pullers);
        var start = new CountDownLatch(1);
        long deadline = System.currentTimeMillis() + 60_000;

        for (int i = 0; i < pullers; i++) {
            pool.submit(() -> {
                start.await();
                while (handedOut.size() < total && System.currentTimeMillis() < deadline) {
                    var work = wm.getWorkIfAvailable(1);
                    for (var wc : work) {
                        handedOut.add(wc);
                        wc.onUserFunctionSuccess();
                    }
                }
                return null;
            });
        }
        start.countDown();

        // Returns are the controller's job in the engine, so they are one thread's job here too.
        int completed = 0;
        while (completed < total && System.currentTimeMillis() < deadline) {
            if (completed < handedOut.size()) {
                wm.handleFutureResult(handedOut.get(completed));
                completed++;
            } else {
                Thread.yield();
            }
        }
        pool.shutdownNow();
        assertThat(pool.awaitTermination(30, TimeUnit.SECONDS)).isTrue();

        Set<WorkContainer<String, String>> distinct = Collections.newSetFromMap(new IdentityHashMap<>());
        List<WorkContainer<String, String>> duplicates = new ArrayList<>();
        for (var wc : handedOut) {
            if (!distinct.add(wc)) {
                duplicates.add(wc);
            }
        }
        assertWithMessage("no record may be handed to two pullers").that(duplicates).isEmpty();
        assertWithMessage("every record must have been offered - a shortfall means a landing failed to put one "
                        + "back, which is a silent stall rather than an error")
                .that(handedOut).hasSize(total);
        assertWithMessage("and the shard is empty afterwards").that(wm.getNumberOfRecordsInShards()).isEqualTo(0L);
    }

    /**
     * The ordered modes must NOT take a record out of their shard when it is claimed, because their scan does
     * double duty - meeting an in-flight record at the head is how a shard refuses a second taker. This asserts
     * the behaviour rather than the wiring: a KEY shard with a record out hands out nothing, and would hand out
     * its next offset if departure-on-take had been applied to the ordered path too.
     */
    @Test
    void anOrderedShardStillRefusesASecondRecordWhileOneIsHeld() {
        setup(ProcessingOrder.KEY);
        register(0, 10);

        var first = wm.getWorkIfAvailable(1);
        assertThat(first).hasSize(1);

        var second = wm.getWorkIfAvailable(5);
        assertWithMessage("a KEY shard already holding a record out at a worker must hand out nothing else")
                .that(second).isEmpty();
    }
}
