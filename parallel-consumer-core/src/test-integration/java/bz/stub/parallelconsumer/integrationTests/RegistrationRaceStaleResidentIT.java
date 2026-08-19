package bz.stub.parallelconsumer.integrationTests;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.ParallelConsumerOptions.CommitMode;
import bz.stub.parallelconsumer.ParallelConsumerOptions.ProcessingOrder;
import bz.stub.parallelconsumer.ParallelEoSStreamProcessor;
import bz.stub.parallelconsumer.integrationTests.utils.KafkaClientUtils;
import bz.stub.parallelconsumer.internal.PCModule;
import bz.stub.parallelconsumer.state.PausableInsertShardManager;
import bz.stub.parallelconsumer.state.ShardManager;
import bz.stub.parallelconsumer.state.WorkManager;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomUtils;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import pl.tlinkowski.unij.api.UniLists;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static bz.stub.parallelconsumer.state.PausableInsertShardManager.PARK_DEADLINE_SECONDS;
import static com.google.common.truth.Truth.assertWithMessage;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;

/**
 * Deterministic broker-level reproduction of the confluentinc#909 registration race (fork fix:
 * astubbs#31) - the interleaving the Chaos Pain Suite was measured NOT to produce by chance (see
 * {@code docs/solutions/logic-errors/909-needs-a-saturated-pipeline-the-third-precondition-2026-08-19.md},
 * which retired the inflight note that first recorded the measurement): scenario w4 at seed
 * 424242 stayed green with the fix reverted. Building this test established WHY chance never finds it -
 * the loss needs THREE coincidences, not two:
 * <ol>
 *   <li>a rebalance landing INSIDE {@code PartitionState.maybeRegisterNewPollBatchAsWork}'s per-record
 *       insert loop, after the batch passed the {@code epochIsStale} guard - a microseconds-wide window;</li>
 *   <li>the partition coming BACK to the same consumer, so the uncommitted tail re-delivers and collides
 *       in {@code ProcessingShard.addWorkContainer} with the stale residents the loop left behind;</li>
 *   <li><b>no take-scan of that shard between the stale inserts and the fresh registration</b> -
 *       {@code ProcessingShard.getWorkIfAvailable} lazily evicts stale containers as it scans
 *       ({@code "there are still stale work container"}), so an idle pipeline heals the collision before
 *       it happens. Only a consumer whose worker pipeline is already at target (control loop's
 *       {@code delta <= 0}, so {@code ShardManager.getWorkIfAvailable} never enters its scan loop) keeps
 *       the residents alive long enough to eat the fresh arrivals.</li>
 * </ol>
 * This test drives all three on purpose, using the same inject-a-latch idiom as
 * {@link RebalanceEoSDeadlockTest}:
 * <ol>
 *   <li><b>Saturate the pipeline</b>: {@code maxConcurrency=4} with {@code messageBufferSize=8} (static
 *       load factor - the dynamic one would grow the target mid-experiment and re-open the scan). Stage-1
 *       records (offsets 0..11) are fed to a user function GATED on a latch: 8 records go out for
 *       processing and stay there, pinning {@code delta} at 0.</li>
 *   <li><b>Park mid-loop</b>: stage-2 records (offsets 12..49) arrive; {@link PausableInsertShardManager}
 *       (injected through the {@link PCModule#shardManager(WorkManager)} DI seam) parks the control
 *       thread at the FIRST insert of offset {@value #PAUSE_AT_OFFSET}.</li>
 *   <li><b>Rebalance under the park</b>: a second consumer joins the same GROUP subscribed to a DIFFERENT
 *       topic - under the eager (range) assignor the PC's poll thread runs a full revoke/re-assign cycle
 *       (epoch bumps + stale-container sweeps) and gets the data partition straight back. The latch is
 *       released only once the test listener has SEEN the re-assignment and the group has been QUIET for
 *       5s (a late join round would sweep-and-redeliver, healing the loss).</li>
 *   <li><b>Collide</b>: the released loop inserts offsets {@value #PAUSE_AT_OFFSET}..49 with the OLD
 *       epoch - the stale residents. Nothing was committed, so the re-fetch starts at offset 0; the test
 *       waits (via the instrument's fresh-epoch insert counter) until all 50 re-delivered records have
 *       registered - the collisions at {@value #PAUSE_AT_OFFSET}..49 included - and only THEN opens the
 *       processing gate.</li>
 * </ol>
 * <b>The invariant asserted</b> is the chaos ledger's own: every produced record is eventually processed.
 * Pre-fix (stale resident preferred, fresh arrival dropped), offsets {@value #PAUSE_AT_OFFSET}..49 can
 * never complete - the resident is epoch-fenced at take, the dropped arrival is never re-fetched while
 * the assignment holds - so the final await times out naming the missing keys. With the fix (stale
 * resident REPLACED by the fresh arrival) everything completes in seconds.
 */
@Timeout(300)
@Slf4j
class RegistrationRaceStaleResidentIT extends BrokerIntegrationTest<String, String> {

    /** In stage 2, past the saturation records, so the pre-pause inserts prove the guard passed. */
    static final long PAUSE_AT_OFFSET = 25;

    static final int STAGE_1_RECORDS = 12;
    static final int RECORD_COUNT = 50;
    static final int MAX_CONCURRENCY = 4;
    /** With {@code maxConcurrency=4} gives a STATIC load factor of 2: out-for-processing target is 8,
     * permanently - the scan-suppression arithmetic in the class javadoc depends on it not stepping. */
    static final int MESSAGE_BUFFER_SIZE = 8;

    private ParallelEoSStreamProcessor<String, String> pc;
    private final AtomicReference<PausableInsertShardManager> pausableSm = new AtomicReference<>();
    private KafkaConsumer<String, String> secondGroupMember;
    private Thread secondMemberPollLoop;
    private final AtomicBoolean secondMemberRunning = new AtomicBoolean(true);
    private final AtomicBoolean secondMemberHoldsAssignment = new AtomicBoolean(false);
    /** Gate in front of the user function: closed = workers park, pipeline stays saturated, no take-scan. */
    private final CountDownLatch processingGate = new CountDownLatch(1);

    @Test
    void freshArrivalCollidingWithStaleShardResidentMustStillGetProcessed() throws Exception {
        setupTopic();

        // stage 1 - just enough records to fill the out-for-processing target (8) with a margin
        Set<String> expectedKeys = ConcurrentHashMap.newKeySet();
        produceKeyedRange(0, STAGE_1_RECORDS, expectedKeys);

        KafkaConsumer<String, String> consumer = getKcu().createNewConsumer(KafkaClientUtils.GroupOption.NEW_GROUP);
        var options = ParallelConsumerOptions.<String, String>builder()
                .consumer(consumer)
                .ordering(ProcessingOrder.UNORDERED)
                .maxConcurrency(MAX_CONCURRENCY)
                .messageBufferSize(MESSAGE_BUFFER_SIZE)
                .commitMode(CommitMode.PERIODIC_CONSUMER_SYNC)
                .build();

        // inject the pausable ShardManager through the module's DI seam - production code carries no hook
        PCModule<String, String> module = new PCModule<>(options) {
            @Override
            public ShardManager<String, String> shardManager(WorkManager<String, String> workManagerInstance) {
                if (pausableSm.get() == null) {
                    pausableSm.set(new PausableInsertShardManager(this, workManagerInstance, PAUSE_AT_OFFSET));
                }
                return pausableSm.get();
            }
        };
        pc = new ParallelEoSStreamProcessor<>(options, module);

        // counts assignments of the data topic; fires AFTER WorkManager's own callback, so when the second
        // assignment is observed the epoch bump and stale-container sweep have already happened. The event
        // timestamp feeds the quiescence gate below.
        AtomicInteger dataTopicAssignments = new AtomicInteger();
        AtomicLong lastRebalanceEventNanos = new AtomicLong(System.nanoTime());
        pc.subscribe(UniLists.of(getTopic()), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                log.info("(test listener) revoked: {}", partitions);
                lastRebalanceEventNanos.set(System.nanoTime());
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                log.info("(test listener) assigned: {}", partitions);
                lastRebalanceEventNanos.set(System.nanoTime());
                if (partitions.stream().anyMatch(tp -> tp.topic().equals(getTopic()))) {
                    dataTopicAssignments.incrementAndGet();
                }
            }
        });

        Set<String> consumedKeys = ConcurrentHashMap.newKeySet();
        AtomicInteger workersInsideGate = new AtomicInteger();
        AtomicBoolean workerGateTimedOut = new AtomicBoolean(false);
        pc.poll(ctx -> {
            workersInsideGate.incrementAndGet();
            try {
                // derived from the park deadline so the gate always outlasts the park (plus the
                // registration await that follows release) - a gate timeout de-saturates the pipeline,
                // which is precondition 3 dying silently, so it is also flagged and asserted on below
                if (!processingGate.await(PARK_DEADLINE_SECONDS + 60, SECONDS)) {
                    workerGateTimedOut.set(true);
                    throw new RuntimeException("test wiring failure: processing gate never opened");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            } finally {
                workersInsideGate.decrementAndGet();
            }
            consumedKeys.add(ctx.key());
        });
        assertWithMessage("instrumented ShardManager must have been constructed through the module DI seam")
                .that(pausableSm.get()).isNotNull();

        // 1 - saturation: all 4 workers parked inside the gate, 8 records out for processing => the
        //     control loop's work-request delta is 0 and stays 0, so no take-scan can evict stale
        //     residents in the window that matters (precondition 3 in the class javadoc)
        await().alias("worker pipeline saturated (4 workers parked in the gate)")
                .atMost(Duration.ofSeconds(30))
                .until(() -> workersInsideGate.get() == MAX_CONCURRENCY);

        // stage 2 - the records whose registration will be caught mid-loop
        produceKeyedRange(STAGE_1_RECORDS, RECORD_COUNT, expectedKeys);

        // 2 - control thread parks mid-registration-loop at the designated offset
        assertWithMessage("control thread must reach the mid-loop pause point (offset %s)", PAUSE_AT_OFFSET)
                .that(pausableSm.get().awaitPausePoint(30, SECONDS)).isTrue();
        int assignmentsBeforeRebalance = dataTopicAssignments.get();
        log.info("Pause point reached; data-topic assignments so far: {}", assignmentsBeforeRebalance);

        // 3 - force a full eager rebalance that hands the partition straight back: a second group member
        //     subscribing to a DIFFERENT topic (it must stay alive until after the assertion - its exit
        //     would rebalance again and heal the loss via sweep + re-delivery)
        String parkingTopic = ensureParkingTopic();
        secondGroupMember = getKcu().createNewConsumer(KafkaClientUtils.GroupOption.REUSE_GROUP);
        secondMemberPollLoop = new Thread(() -> {
            secondGroupMember.subscribe(UniLists.of(parkingTopic));
            while (secondMemberRunning.get()) {
                secondGroupMember.poll(Duration.ofMillis(200));
                // the release gate below needs the second member's join fully SETTLED: its first join
                // round can precede its metadata discovering the just-created parking topic, in which
                // case a metadata refresh triggers a re-join round several seconds later - which, if it
                // landed after the latch release, would sweep the stale residents and re-deliver
                secondMemberHoldsAssignment.set(!secondGroupMember.assignment().isEmpty());
            }
            secondGroupMember.close();
        }, "test-second-group-member");
        secondMemberPollLoop.start();

        // 4 - wait until the PC's poll thread has completed the revoke/re-assign cycle (sweeps included)
        //     AND the group has gone QUIET, with the control thread parked inside the loop the whole time
        await().alias("data topic re-assigned, second member holding its assignment, rebalance rounds settled")
                .atMost(Duration.ofSeconds(60))
                .until(() -> dataTopicAssignments.get() > assignmentsBeforeRebalance
                        && secondMemberHoldsAssignment.get()
                        && System.nanoTime() - lastRebalanceEventNanos.get() > Duration.ofSeconds(5).toNanos());

        // 5 - release: the rest of the paused batch (offsets 25..49) is now inserted with the OLD epoch -
        //     the stale residents. The re-fetch from offset 0 then collides with them at the defect site.
        //     The rebalance state is captured first: the validity assertions at the end prove it did not
        //     move again, because a rebalance AFTER this point sweeps the residents and heals the loss.
        int assignmentsAtRelease = dataTopicAssignments.get();
        long rebalanceClockAtRelease = lastRebalanceEventNanos.get();
        pausableSm.get().releaseRegistrationLoop();

        // 6 - hold the gate closed until every re-delivered record has REGISTERED (collisions included) -
        //     opening it earlier would let the pipeline drain, drop delta below target, and let the next
        //     take-scan lazily evict the stale residents before the fresh arrivals reach them
        await().alias("all re-delivered (fresh-epoch) records registered while the pipeline stayed saturated")
                .atMost(Duration.ofSeconds(30))
                .until(() -> pausableSm.get().getFreshEpochInsertCount().get() >= RECORD_COUNT);
        processingGate.countDown();

        // the invariant: every produced record is eventually processed. Pre-fix, the collided offsets'
        // fresh arrivals were dropped in favour of unexecutable stale residents => timeout, missing keys.
        await().alias("all records processed despite the mid-registration rebalance")
                .atMost(Duration.ofSeconds(90))
                .untilAsserted(() -> assertWithMessage(
                        "confluentinc#909 signature: a fresh arrival colliding with a stale shard resident "
                                + "was dropped, and is never re-delivered while the consumer stays up")
                        .that(consumedKeys).containsAtLeastElementsIn(expectedKeys));

        // ANTI-ROT: green above is only evidence if the staged collision actually happened and stayed
        // unhealed. The quiescence gate at step 4 runs BEFORE release, so a rebalance landing after it
        // (coordinator move, session-timeout blip) would sweep the stale residents and re-deliver -
        // and the invariant would then pass on defective code too. A broken run must fail as
        // "run invalid", never pass.
        assertWithMessage("run invalid: the data topic was re-assigned after release - the sweep healed "
                + "the staged collision, so the green result above proves nothing about the defect")
                .that(dataTopicAssignments.get()).isEqualTo(assignmentsAtRelease);
        assertWithMessage("run invalid: a rebalance event fired after release - the stale residents may "
                + "have been swept, so the green result above proves nothing about the defect")
                .that(lastRebalanceEventNanos.get()).isEqualTo(rebalanceClockAtRelease);
        assertWithMessage("run invalid: a worker's processing-gate await timed out, de-saturating the "
                + "pipeline - a take-scan could then have evicted the stale residents (precondition 3) "
                + "before the fresh arrivals collided with them")
                .that(workerGateTimedOut.get()).isFalse();
    }

    /** Produces offsets [from..to) with per-offset unique keys "k-<i>" - offset<->key correspondence is
     * what lets a missing key name the lost offset (the shared produceMessages helper regenerates its key
     * sequence per call, so two calls collide on keys). */
    private void produceKeyedRange(int fromInclusive, int toExclusive, Set<String> expectedKeys) {
        try (Producer<String, String> producer = getKcu().createNewProducer(false)) {
            List<Future<RecordMetadata>> sends = new ArrayList<>();
            for (int i = fromInclusive; i < toExclusive; i++) {
                String key = "k-" + i;
                expectedKeys.add(key);
                sends.add(producer.send(new ProducerRecord<>(getTopic(), key, "v-" + i)));
            }
            for (Future<RecordMetadata> send : sends) {
                send.get();
            }
            log.info("Produced offsets [{}..{})", fromInclusive, toExclusive);
        } catch (Exception e) {
            throw new RuntimeException("Producer failed at range [" + fromInclusive + ".." + toExclusive + ")", e);
        }
    }

    private String ensureParkingTopic() {
        String parkingTopic = getClass().getSimpleName() + "-parking-" + RandomUtils.nextInt();
        ensureTopic(parkingTopic, 1);
        return parkingTopic;
    }

    @AfterEach
    void cleanUp() throws InterruptedException {
        // unpark the control thread and the workers first, in case an assertion failed mid-choreography
        if (pausableSm.get() != null) {
            pausableSm.get().releaseRegistrationLoop();
        }
        processingGate.countDown();
        secondMemberRunning.set(false);
        if (secondMemberPollLoop != null) {
            secondMemberPollLoop.join(10_000);
        }
        if (pc != null) {
            pc.close();
        }
    }
}
