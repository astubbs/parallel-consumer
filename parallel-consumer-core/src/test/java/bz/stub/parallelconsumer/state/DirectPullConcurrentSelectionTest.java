package bz.stub.parallelconsumer.state;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.ParallelConsumerOptions.ProcessingOrder;
import bz.stub.parallelconsumer.internal.EpochAndRecordsMap;
import bz.stub.parallelconsumer.internal.PCModuleTestEnv;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import pl.tlinkowski.unij.api.UniLists;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;

/**
 * What happens to work selection when it stops being single-threaded.
 * <p>
 * WHY THIS EXISTS. The engine PC ships has exactly one thread that ever calls
 * {@link WorkManager#getWorkIfAvailable(int)} - the control loop - and every safety property of selection was
 * written under that assumption. The direct-pull engine
 * ({@code bz.stub.parallelconsumer.internal.DirectPullWorkerPool}, selected by {@code -Dpc.directPull=true})
 * has every worker call it instead. These tests drive {@link WorkManager} from many threads at once, which is
 * the only thing direct pull changes about selection, so they hold whether or not that engine is switched on
 * and they do not need it to be.
 * <p>
 * THE INVARIANT THAT MATTERS MOST is the third one below: under {@link ProcessingOrder#KEY} and
 * {@link ProcessingOrder#PARTITION}, a shard must have at most one record out at a time. Per-record atomicity
 * does <em>not</em> imply it - two workers claiming two <em>different</em> records of the same shard would
 * satisfy every CAS and still break ordering - so it is asserted directly rather than inferred.
 *
 * @author Antony Stubbs
 * @see ProcessingShard#getWorkIfAvailable
 * @see WorkContainer#onQueueingForExecution()
 */
@Slf4j
class DirectPullConcurrentSelectionTest {

    static final String TOPIC = "direct-pull-topic";

    /**
     * Enough threads to interleave on a machine with a handful of cores, without the test itself becoming the
     * scheduling bottleneck.
     */
    static final int PULLERS = 8;

    PCModuleTestEnv module;
    WorkManager<String, String> wm;

    void setup(ProcessingOrder ordering) {
        module = new PCModuleTestEnv(ParallelConsumerOptions.<String, String>builder()
                .ordering(ordering)
                .build());
        wm = module.workManager();
    }

    TopicPartition tp(int partition) {
        return new TopicPartition(TOPIC, partition);
    }

    /**
     * Assigned once and only once per test. Re-assigning a partition bumps its epoch, which makes everything
     * already registered against it stale - so a helper that assigned on every registration would silently
     * throw away all but the last batch, and the test would pass or fail for reasons that have nothing to do
     * with concurrency.
     */
    void assign(int... partitions) {
        List<TopicPartition> tps = new ArrayList<>();
        for (int p : partitions) {
            tps.add(tp(p));
        }
        wm.onPartitionsAssigned(tps);
    }

    /**
     * @param key the record key, which is what {@link ProcessingOrder#KEY} shards on
     */
    void register(int partition, String key, int fromOffset, int count) {
        List<ConsumerRecord<String, String>> recs = new ArrayList<>(count);
        for (int i = fromOffset; i < fromOffset + count; i++) {
            recs.add(new ConsumerRecord<>(TOPIC, partition, i, key, "value-" + i));
        }
        Map<TopicPartition, List<ConsumerRecord<String, String>>> m = new HashMap<>();
        m.put(tp(partition), recs);
        wm.registerWork(new EpochAndRecordsMap<>(new ConsumerRecords<>(m), wm.getPm()));
    }

    /**
     * THE ORDERED-MODE INVARIANT, asserted with no timing window at all.
     * <p>
     * One record of the shard is taken and deliberately never returned, so the shard is - and stays -
     * "occupied" for the whole test. Every other thread then hammers selection. If anything can get a second
     * record out of an occupied shard, this finds it; and because the occupying record never comes back, a run
     * that hands out zero extra records cannot have done so by being too fast to observe.
     * <p>
     * WHY THIS IS NOT IMPLIED BY THE CAS ON {@link WorkContainer#isInFlight()}. That CAS makes "is it free? then
     * take it" indivisible for <em>one</em> record, so two workers cannot claim the same record. The ordering
     * guarantee is a different statement - at most one record <em>of the shard</em> - and two workers claiming
     * two different records of the same shard breaks it while satisfying every CAS.
     * <p>
     * WHAT ACTUALLY BRIDGES THE TWO, and the line this test pins: in
     * {@link ProcessingShard#getWorkIfAvailable} the {@code if (isOrderRestricted()) break;} sits
     * <em>outside</em> the take/skip branch, so a scanner that finds the head record unavailable stops there
     * rather than walking on to the next one. In-flight records are removed from the shard on success, not on
     * being taken, so the occupied head is still the first entry every concurrent scanner meets. Move that
     * {@code break} inside the successful-take branch and this test goes red.
     */
    @ParameterizedTest
    @EnumSource(value = ProcessingOrder.class, names = {"KEY", "PARTITION"})
    void anOccupiedOrderedShardHandsOutNothingElseHoweverManyThreadsAsk(ProcessingOrder ordering) throws Exception {
        setup(ordering);
        assign(0);
        register(0, "the-only-key", 0, 20);

        var occupying = wm.getWorkIfAvailable(1);
        assertThat(occupying).hasSize(1);

        var handedOutWhileOccupied = Collections.synchronizedList(new ArrayList<WorkContainer<String, String>>());
        hammerSelection(handedOutWhileOccupied, 500);

        assertWithMessage("an ordered shard already holding a record out at a worker must hand out nothing "
                + "else, however many threads ask concurrently - but %s more were handed out",
                handedOutWhileOccupied.size())
                .that(handedOutWhileOccupied).isEmpty();
    }

    /**
     * The same invariant with several shards in play, which is where a scanner that walked on past a blocked
     * shard head could hide: with one shard the only two outcomes are "nothing" and "obviously broken", whereas
     * with several, work IS legitimately available elsewhere on every pass, so the threads are genuinely
     * selecting rather than short-circuiting.
     * <p>
     * Every shard is occupied first, so the correct answer is still exactly zero.
     */
    @Test
    void everyOccupiedKeyShardStaysClosedWhileOtherShardsAreLiveTargets() throws Exception {
        setup(ProcessingOrder.KEY);
        int shards = 6;
        assign(0);
        for (int k = 0; k < shards; k++) {
            register(0, "key-" + k, k * 100, 20);
        }

        var occupying = wm.getWorkIfAvailable(shards);
        assertWithMessage("one record out of each of the %s shards", shards)
                .that(occupying).hasSize(shards);

        var handedOutWhileOccupied = Collections.synchronizedList(new ArrayList<WorkContainer<String, String>>());
        hammerSelection(handedOutWhileOccupied, 500);

        assertWithMessage("every shard is occupied, so nothing at all may be handed out - got %s",
                describe(handedOutWhileOccupied))
                .that(handedOutWhileOccupied).isEmpty();
    }

    /**
     * The per-record half of the guarantee: no record may be handed to two pullers.
     * <p>
     * {@link ProcessingOrder#UNORDERED} deliberately, because that is the mode with no shard-level exclusion to
     * fall back on - every puller is free inside the same shard at the same time, so the only thing standing
     * between them is the compare-and-set in {@link WorkContainer#onQueueingForExecution()}. Records are never
     * completed, so the whole population stays in the shard and the pullers keep colliding on it.
     * <p>
     * ONE RECORD PER PULL, deliberately. An unordered shard hands out a batch from a single walk, so a puller
     * asking for four spends most of its time walking rather than claiming, and the threads spread across four
     * different records instead of contending for one. Asking for one makes every thread walk to the same first
     * free entry and attempt the same claim, which is the only interleaving that can expose a non-atomic one -
     * and the difference is not theoretical: with a batch of four, a check-then-set mutation of
     * {@link WorkContainer#onQueueingForExecution()} survived this test.
     * <p>
     * WHY THE POPULATION IS SMALL. Taken records stay in the shard until they SUCCEED, and an unordered scan
     * walks past every one of them, so one record per pull makes the run quadratic in the population and every
     * puller pays it. At 800 records this test took thirty seconds; at 250 it takes a couple, with the same
     * number of contended claims per record. That cost is not an artefact of the test - it is the mechanism
     * behind the engine's measured collapse at high concurrency, reproduced in miniature.
     */
    @Test
    void noTwoConcurrentPullersEverGetTheSameRecord() throws Exception {
        setup(ProcessingOrder.UNORDERED);
        int total = 250;
        assign(0);
        register(0, "any-key", 0, total);

        var handedOut = Collections.synchronizedList(new ArrayList<WorkContainer<String, String>>());
        hammerSelection(handedOut, 100, 1);

        Set<WorkContainer<String, String>> distinct =
                Collections.newSetFromMap(new IdentityHashMap<>());
        List<WorkContainer<String, String>> duplicates = new ArrayList<>();
        for (var wc : handedOut) {
            if (!distinct.add(wc)) {
                duplicates.add(wc);
            }
        }

        assertWithMessage("the same record was handed to more than one puller: %s", describe(duplicates))
                .that(duplicates).isEmpty();
        assertWithMessage("sanity - the pullers really did take work, otherwise the run proves nothing")
                .that(handedOut.size()).isGreaterThan(0);
        assertWithMessage("and every record carries exactly one delivery")
                .that(handedOut.stream().filter(wc -> wc.getDeliveryCount() != 1).count())
                .isEqualTo(0L);
    }

    /**
     * {@code numberRecordsOutForProcessing} gates the broker poller, so drift in it stalls the consumer while it
     * still looks alive - which is the shape of the confluentinc#857 family. Under direct pull it is incremented
     * by whichever worker took the work rather than by the one control thread, so this holds the counter against
     * the records the pullers can actually account for.
     * <p>
     * Asserted as an exact equality against an independently collected list, not as "roughly right". One record
     * per pull for the same reason as the test above: it puts every increment of the counter in contention.
     */
    @Test
    void theInFlightCounterMatchesWhatTheConcurrentPullersActuallyReceived() throws Exception {
        setup(ProcessingOrder.UNORDERED);
        int total = 250;
        assign(0);
        register(0, "any-key", 0, total);

        var handedOut = Collections.synchronizedList(new ArrayList<WorkContainer<String, String>>());
        hammerSelection(handedOut, 100, 1);

        assertWithMessage("the in-flight counter must equal the number of records the pullers were given")
                .that(wm.getNumberRecordsOutForProcessing())
                .isEqualTo(handedOut.size());
        assertWithMessage("and the conservation figure must still match a scan of the shards - selection is not "
                + "a departure, so nothing may have left")
                .that(wm.getNumberOfRecordsInShards())
                .isEqualTo(wm.getSm().countRecordsInShardsByScan());
    }

    /**
     * The counter has to survive concurrent pulls happening <em>while</em> results are being returned, which is
     * the real shape of the engine: many pullers taking, one control thread returning.
     * <p>
     * Returns go through a single thread on purpose. {@link WorkManager#handleFutureResult} is documented as
     * control-thread-only and direct pull does not change that - it changes who takes work, never who returns
     * it. A test that returned from the worker threads would be testing a design nobody built.
     */
    @Test
    void theInFlightCounterNetsBackToZeroWithPullsAndReturnsOverlapping() throws Exception {
        setup(ProcessingOrder.UNORDERED);
        int total = 1_500;
        assign(0);
        register(0, "any-key", 0, total);

        var toReturn = new java.util.concurrent.LinkedBlockingQueue<WorkContainer<String, String>>();
        var takenCount = new AtomicInteger();
        var failure = new AtomicReference<Throwable>();

        // the "control loop": the only thread allowed to hand results back
        var returner = new Thread(() -> {
            try {
                int returned = 0;
                while (returned < total) {
                    var wc = toReturn.poll(30, TimeUnit.SECONDS);
                    if (wc == null) {
                        break;
                    }
                    wc.onUserFunctionSuccess();
                    wm.handleFutureResult(wc);
                    returned++;
                }
            } catch (Throwable t) {
                failure.compareAndSet(null, t);
            }
        }, "returner");
        returner.start();

        var pool = Executors.newFixedThreadPool(PULLERS);
        var start = new CountDownLatch(1);
        for (int i = 0; i < PULLERS; i++) {
            pool.execute(() -> {
                try {
                    start.await();
                    while (takenCount.get() < total) {
                        var batch = wm.getWorkIfAvailable(4);
                        if (batch.isEmpty()) {
                            Thread.yield();
                            continue;
                        }
                        takenCount.addAndGet(batch.size());
                        toReturn.addAll(batch);
                    }
                } catch (Throwable t) {
                    failure.compareAndSet(null, t);
                }
            });
        }
        start.countDown();
        pool.shutdown();
        assertThat(pool.awaitTermination(60, TimeUnit.SECONDS)).isTrue();
        returner.join(TimeUnit.SECONDS.toMillis(60));

        if (failure.get() != null) {
            throw new AssertionError("a puller or the returner threw", failure.get());
        }

        assertWithMessage("every record was taken exactly once, so exactly the population was returned")
                .that(takenCount.get()).isEqualTo(total);
        assertWithMessage("the in-flight counter nets back to zero - drift here stalls the poller silently")
                .that(wm.getNumberRecordsOutForProcessing()).isEqualTo(0);
        assertWithMessage("and the shards are empty, by an O(n) scan independent of the conservation figure")
                .that(wm.getSm().countRecordsInShardsByScan()).isEqualTo(0L);
        assertWithMessage("conservation agrees")
                .that(wm.getNumberOfRecordsInShards()).isEqualTo(0L);
    }

    /**
     * Fairness across shards has to survive concurrent readers of the iteration resume point.
     * <p>
     * {@link ShardManager} remembers where the last scan stopped so the next one starts elsewhere, which is what
     * stops the first shard being drained while the rest starve. Under direct pull every worker reads and writes
     * that field concurrently; a reader that never saw another thread's update would keep restarting at the same
     * place forever, and the shards behind it would never be served.
     * <p>
     * The assertion is the observable consequence - every shard gets served - and not the field, so it stays
     * valid however the resume point is implemented.
     */
    @Test
    void everyShardIsServedUnderConcurrentPullRatherThanTheFirstOneHogging() throws Exception {
        setup(ProcessingOrder.UNORDERED);
        int partitions = 8;
        int perPartition = 500;
        assign(0, 1, 2, 3, 4, 5, 6, 7);
        for (int p = 0; p < partitions; p++) {
            register(p, "any-key", 0, perPartition);
        }

        var handedOut = Collections.synchronizedList(new ArrayList<WorkContainer<String, String>>());
        // small batches, and far fewer pulls than there are records, so a scan that always restarted at the
        // head of the shard map could only ever reach the first shard or two
        hammerSelection(handedOut, 40);

        Map<Integer, Integer> perShard = new HashMap<>();
        for (var wc : handedOut) {
            perShard.merge(wc.getTopicPartition().partition(), 1, Integer::sum);
        }

        assertWithMessage("sanity - the pullers took work")
                .that(handedOut.size()).isGreaterThan(partitions);
        assertWithMessage("every one of the %s shards was served; served counts were %s", partitions, perShard)
                .that(perShard.keySet()).hasSize(partitions);
    }

    /**
     * The default batch: more than one, so that under an ordered mode the shard's own "stop after one" bound is
     * exercised rather than satisfied trivially by the request size.
     */
    private void hammerSelection(List<WorkContainer<String, String>> sink, int pullsEach) throws Exception {
        hammerSelection(sink, pullsEach, 4);
    }

    /**
     * Runs {@link #PULLERS} threads through {@code pullsEach} rounds of selection, collecting everything handed
     * out.
     */
    private void hammerSelection(List<WorkContainer<String, String>> sink, int pullsEach, int batch) throws Exception {
        var pool = Executors.newFixedThreadPool(PULLERS);
        var start = new CountDownLatch(1);
        var failure = new AtomicReference<Throwable>();
        for (int i = 0; i < PULLERS; i++) {
            pool.execute(() -> {
                try {
                    start.await();
                    for (int p = 0; p < pullsEach; p++) {
                        sink.addAll(wm.getWorkIfAvailable(batch));
                    }
                } catch (Throwable t) {
                    failure.compareAndSet(null, t);
                }
            });
        }
        start.countDown();
        pool.shutdown();
        assertWithMessage("the pullers finished within the time budget")
                .that(pool.awaitTermination(120, TimeUnit.SECONDS)).isTrue();
        if (failure.get() != null) {
            throw new AssertionError("a puller threw", failure.get());
        }
    }

    private String describe(List<WorkContainer<String, String>> work) {
        var counts = new ConcurrentHashMap<String, Integer>();
        for (var wc : work) {
            counts.merge(wc.getTopicPartition() + ":" + wc.offset() + ":" + wc.getCr().key(), 1, Integer::sum);
        }
        return counts.toString();
    }
}
