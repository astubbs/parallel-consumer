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
 * The index of unheld offsets that the {@code UNORDERED} dispatch scan walks instead of the shard's entry map.
 * <p>
 * WHY THIS EXISTS. {@link ShardOccupancy}'s index is what makes unordered selection cost one examination per
 * record instead of one per in-flight record ahead of it - the difference between a direct-pull engine that works
 * at 5,000 workers and one that does not. But it is a second thing that has to agree with the entry map, and
 * <b>the direction in which it can be wrong is the dangerous one</b>: an index that is missing a selectable record
 * never offers that record again, which is a silent stall, the shape of the confluentinc#857 family. Nothing in
 * the delivery tests would notice, because the other records still flow.
 * <p>
 * So every test here holds the index against an independent count of what the shard actually holds, in the same
 * arrangement {@link ShardInFlightCountTest} uses for the in-flight counter - a structure that agrees with itself
 * proves nothing.
 *
 * @author Antony Stubbs
 * @see ShardOccupancy
 * @see ProcessingShard#getWorkIfAvailable
 */
class ShardSelectableIndexTest {

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
     * The invariant, stated once: the index holds exactly the records the shard is tracking that are not out at a
     * worker. Fewer means a record has been stranded; more means the index has kept an offset whose entry has
     * gone, which only costs a wasted examination but is still drift worth failing on.
     */
    void assertIndexMatchesTheShard(ProcessingShard<String, String> shard) {
        long tracked = shard.getCountOfWorkTracked();
        long inFlight = shard.getCountOfWorkInFlight();
        assertWithMessage("the index must hold exactly the tracked records that are NOT out at a worker "
                        + "(tracked=%s, in flight=%s). Too few is a stranded record - the record is in the shard, "
                        + "nothing is holding it, and nothing will ever offer it again.",
                tracked, inFlight)
                .that((long) shard.countSelectableByIndex()).isEqualTo(tracked - inFlight);
    }

    @Test
    void everyRecordIsIndexedOnArrivalAndLeavesTheIndexWhenClaimed() {
        setup(ProcessingOrder.UNORDERED);
        register(0, 50);
        var shard = theShard();
        assertIndexMatchesTheShard(shard);

        var taken = wm.getWorkIfAvailable(20);
        assertThat(taken).hasSize(20);
        assertWithMessage("twenty claimed records must have left the index")
                .that(shard.countSelectableByIndex()).isEqualTo(30);
        assertIndexMatchesTheShard(shard);
    }

    /**
     * A failure returns the record to selection, so it has to return to the index too - and this is the transition
     * where an index maintained at the entry-map's edges rather than at the flight's edges would silently lose it:
     * nothing is added to or removed from {@code entries} when a record fails.
     */
    @Test
    void aFailedRecordComesBackIntoTheIndex() {
        setup(ProcessingOrder.UNORDERED);
        register(0, 10);
        var shard = theShard();

        var taken = wm.getWorkIfAvailable(1);
        assertThat(taken).hasSize(1);
        assertThat(shard.countSelectableByIndex()).isEqualTo(9);

        var wc = taken.get(0);
        wc.onUserFunctionFailure(new RuntimeException("deliberate"));
        wm.handleFutureResult(wc);

        assertWithMessage("a failed record is selectable again, so it must be back in the index")
                .that(shard.countSelectableByIndex()).isEqualTo(10);
        assertIndexMatchesTheShard(shard);
    }

    @Test
    void aSucceededRecordLeavesBothTheShardAndTheIndex() {
        setup(ProcessingOrder.UNORDERED);
        register(0, 10);
        var shard = theShard();

        var taken = wm.getWorkIfAvailable(1);
        var wc = taken.get(0);
        wc.onUserFunctionSuccess();
        wm.handleFutureResult(wc);

        assertWithMessage("a succeeded record has left the shard, so it must not be left behind in the index")
                .that(shard.countSelectableByIndex()).isEqualTo(9);
        assertIndexMatchesTheShard(shard);
    }

    /**
     * THE ONE THAT MATTERS. Every record must still be delivered exactly once when many workers select
     * concurrently - which is the only thing the index can break, and it breaks it by omission rather than by
     * throwing.
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
        assertWithMessage("every record must have been offered - a shortfall means the index stranded one, "
                        + "which is a silent stall rather than an error")
                .that(handedOut).hasSize(total);
        assertWithMessage("and the shard is empty afterwards").that(wm.getNumberOfRecordsInShards()).isEqualTo(0L);
    }

    /**
     * The ordered modes must not read the index, because their scan does double duty - meeting an in-flight record
     * at the head is how a shard refuses a second taker. This asserts the behaviour rather than the wiring: a
     * KEY shard with a record out hands out nothing, and would hand out its next offset if the ordered path had
     * been switched over to an index that (correctly) no longer contains the held record.
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
