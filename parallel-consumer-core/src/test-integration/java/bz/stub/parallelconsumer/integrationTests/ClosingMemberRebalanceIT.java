package bz.stub.parallelconsumer.integrationTests;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.integrationTests.utils.ManagedPCInstance;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.ConsumerGroupState;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import static bz.stub.parallelconsumer.ParallelConsumerOptions.CommitMode.PERIODIC_CONSUMER_ASYNCHRONOUS;
import static bz.stub.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.UNORDERED;
import static com.google.common.truth.Truth.assertWithMessage;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;
import static pl.tlinkowski.unij.api.UniLists.of;

/**
 * Deterministic reproducer for the rebalance stall {@code MultiInstanceRebalanceTest.largeNumberOfInstances}
 * draws about one run in twenty: <b>members closed while the group is mid-rebalance must not hold the
 * group.</b> Built on {@link ManagedPCInstance}, the same harness as that profile, so what differs is
 * only scale and randomness - and the two variables below, which this test moves ONE AT A TIME.
 * <p>
 * <b>What is already known, so this matrix does not re-derive it:</b>
 * <ul>
 *   <li>A single member closing mid-rebalance under the eager assignor is handled cleanly: LeaveGroup
 *   answered in ~10ms, close in ~0.1s, survivors reassigned in ~2.6s. Measured on master AND on the
 *   branch carrying the {@code doClose()} discharge poll - both pass, so that fix does not touch this
 *   case. (The first cut of this test reported a "freeze" here; it was an exhausted topic, see
 *   {@link #TO_PRODUCE}.)</li>
 *   <li>A single SYNCHRONOUS stop per round under the cooperative assignor is
 *   {@code scriptedChurnRoundsCompleteWithoutStall}, 17/17 green.</li>
 * </ul>
 * What neither of those has, and the capacity profile has in abundance, is <b>several members closing
 * at once</b> - up to six of eleven per chaos round. Hence the {@code closers} axis.
 * <p>
 * <b>Reading a failure.</b> {@code -Dkafka.coordinator.log.level=debug} raises exactly the two
 * coordinator loggers, which say per member whether LeaveGroup was sent, what generation it held at
 * {@code onLeavePrepare}, and when the survivors were reassigned. The stall dump in the capacity
 * profile shows closers parked in {@code AbstractCoordinator.close -> awaitPendingRequests}; the
 * question this test exists to settle is whether that is the CAUSE of the freeze or a closing member
 * merely waiting on a group that froze for another reason.
 * See {@code docs/inflight/test-largenumberofinstances-residual-failures-measured-not-explained.md}.
 */
@Timeout(300)
@Testcontainers
@Slf4j
class ClosingMemberRebalanceIT extends BrokerIntegrationTest<String, String> {

    /**
     * Sized so the backlog OUTLIVES the scenario, and guarded rather than assumed: the first cut produced
     * 4,000 records at 2ms each, the group finished them before the new member had even joined, and
     * "the survivors consumed nothing after the close" read as a freeze when it was completion - the last
     * committed offsets summed to exactly 4,000. A liveness assertion is vacuous unless work provably
     * remains, so {@link #REMAINING_FLOOR} is checked at the close and again after the window.
     * <p>
     * And sized so the at-least-once drain at the end is AFFORDABLE: the second cut produced 150,000 at
     * 25ms and the four matrix cases each passed every property, then timed out draining the backlog -
     * 363s each on a two-core hosted runner, 1,469s for the class in the gating lane. The drain is the
     * ledger, not the property; the backlog only has to outlast a 15s liveness window. Both guards
     * still fire if a faster box exhausts it.
     */
    private static final int TO_PRODUCE = 30_000;
    private static final int REMAINING_FLOOR = 3_000;
    private static final int PER_RECORD_MS = 10;
    private static final int SETTLED_MEMBERS = 5;

    private ExecutorService pcExecutor;

    @BeforeEach
    void setup() {
        numPartitions = 12; // two per settled member, so every close moves real partitions
        setupTopic();
        pcExecutor = Executors.newCachedThreadPool();
    }

    /**
     * {@code cooperative} is the capacity profile's assignor; {@code closers} is how many of the settled
     * members are closed simultaneously the moment the coordinator reports a rebalance in flight.
     * {@code false,1} is the known-good baseline and must stay green; it is here as the control arm.
     */
    @ParameterizedTest(name = "cooperative={0} closers={1}")
    @CsvSource({"false,1", "true,3"}) // the extremes; the middle cells added runner-minutes and no discrimination
    void closingMembersMidRebalanceMustNotHoldTheGroup(boolean cooperative, int closers) throws Exception {
        List<String> producedKeys = produceMessages(TO_PRODUCE);
        Set<String> processed = ConcurrentHashMap.newKeySet();

        ManagedPCInstance.Config config = ManagedPCInstance.Config.builder()
                .commitMode(PERIODIC_CONSUMER_ASYNCHRONOUS) // the capacity profile's mode
                .order(UNORDERED)
                .inputTopic(getTopic())
                .pollDelayMs(PER_RECORD_MS)
                .useCooperativeAssignor(cooperative)
                .build();

        // ---- a settled group of SETTLED_MEMBERS, every one of them consuming
        List<ManagedPCInstance> settled = new ArrayList<>();
        for (int i = 0; i < SETTLED_MEMBERS; i++) {
            ManagedPCInstance instance = new ManagedPCInstance(config, getKcu(), processed::add);
            instance.start(pcExecutor);
            settled.add(instance);
        }
        await().alias("every settled member is consuming")
                .atMost(90, SECONDS)
                .untilAsserted(() -> settled.forEach(pc ->
                        assertWithMessage("instance %s consuming", pc.getInstanceId())
                                .that(pc.getConsumedKeys().size()).isAtLeast(20)));

        // ---- a newcomer joins: this is what opens a rebalance
        ManagedPCInstance newcomer = new ManagedPCInstance(config, getKcu(), processed::add);
        newcomer.start(pcExecutor);

        // ---- force the window: observe the coordinator's own state rather than guessing the timing
        String groupId = getKcu().getGroupId();
        await().alias("coordinator reports the group is rebalancing after the newcomer joined")
                .atMost(30, SECONDS)
                .pollInterval(Duration.ofMillis(25))
                .untilAsserted(() -> assertWithMessage("group state")
                        .that(groupState(groupId))
                        .isAnyOf(ConsumerGroupState.PREPARING_REBALANCE, ConsumerGroupState.COMPLETING_REBALANCE));

        assertWithMessage("NON-DISCRIMINATING RUN: too little backlog left at the close - raise TO_PRODUCE " +
                "or PER_RECORD_MS; a liveness assertion on an exhausted topic proves nothing")
                .that(producedKeys.size() - processed.size())
                .isAtLeast(REMAINING_FLOOR);

        // ---- close N settled members SIMULTANEOUSLY, each on its own thread, each timed on that thread
        List<ManagedPCInstance> victims = settled.subList(0, closers);
        List<ManagedPCInstance> survivors = new ArrayList<>(settled.subList(closers, SETTLED_MEMBERS));
        survivors.add(newcomer);
        int survivorsBefore = survivors.stream().mapToInt(pc -> pc.getConsumedKeys().size()).sum();

        long closeStartedNanos = System.nanoTime();
        List<AtomicLong> closeFinishedNanos = new ArrayList<>();
        List<Thread> closerThreads = new ArrayList<>();
        for (ManagedPCInstance victim : victims) {
            AtomicLong finished = new AtomicLong();
            closeFinishedNanos.add(finished);
            Thread t = new Thread(() -> {
                try {
                    victim.stop();
                } finally {
                    finished.set(System.nanoTime());
                }
            }, "test-closer-" + victim.getInstanceId());
            closerThreads.add(t);
        }
        closerThreads.forEach(Thread::start);

        try {
            // 1) GROUP LIVENESS: the survivors must keep consuming while the victims close. The defect
            //    freezes them behind a coordinator that is waiting on members that will never answer.
            await().alias("survivors make progress while " + closers + " member(s) close mid-rebalance")
                    .atMost(15, SECONDS)
                    .pollInterval(Duration.ofMillis(200))
                    .untilAsserted(() -> assertWithMessage("survivors' consumption since the closes began")
                            .that(survivors.stream().mapToInt(pc -> pc.getConsumedKeys().size()).sum())
                            .isAtLeast(survivorsBefore + 100));
        } finally {
            for (Thread t : closerThreads) {
                t.join(Duration.ofSeconds(90).toMillis());
            }
            for (int i = 0; i < victims.size(); i++) {
                long fin = closeFinishedNanos.get(i).get();
                log.warn("victim {} close took {}s", victims.get(i).getInstanceId(),
                        fin == 0 ? "NOT FINISHED" : String.format("%.1f", (fin - closeStartedNanos) / 1e9));
            }
        }

        assertWithMessage("NON-DISCRIMINATING RUN: the backlog ran out during the liveness window")
                .that(producedKeys.size() - processed.size())
                .isAtLeast(500);

        // 2) CLOSE DURATION: the defect costs each victim ~request.timeout.ms (30s) waiting on a JoinGroup
        //    nobody answers. 10s is not "fast"; it is "was answered rather than timed out".
        for (int i = 0; i < victims.size(); i++) {
            long fin = closeFinishedNanos.get(i).get();
            assertWithMessage("victim %s close should have completed", victims.get(i).getInstanceId())
                    .that(fin).isNotEqualTo(0L);
            assertWithMessage("victim %s mid-rebalance close duration (seconds)", victims.get(i).getInstanceId())
                    .that((fin - closeStartedNanos) / 1e9).isLessThan(10.0);
        }

        // 3) LEDGER: at-least-once across the group
        await().alias("every record consumed by some member")
                .atMost(180, SECONDS)
                .untilAsserted(() -> assertWithMessage("at-least-once")
                        .that(processed).containsAtLeastElementsIn(producedKeys));

        survivors.forEach(ManagedPCInstance::close);
        pcExecutor.shutdownNow();
    }

    /**
     * The arm the settled-member matrix cannot reach: <b>a member closed while its OWN JoinGroup is
     * still unanswered.</b> Under the capacity profile's churn that is a just-restarted instance toggled
     * off again before the join phase completes - and it is the one state in which the coordinator is
     * waiting on a member that has already decided to leave.
     * <p>
     * The newcomer IS that member. The settled members learn of its join only on their next heartbeat
     * (3s), so its JoinGroup stays pending for roughly that long, and the admin-state trigger fires
     * within tens of milliseconds of {@code PREPARING_REBALANCE} - well inside the window.
     * <p>
     * If LeaveGroup releases the pending JoinGroup on the coordinator side, this passes like the
     * settled-member cases. If it does not, the newcomer's close waits on that JoinGroup until the
     * client's own {@code request.timeout.ms} fails it (~30s) - which is the duration the capacity
     * profile's stuck instances show - and the group waits with it.
     */
    @ParameterizedTest(name = "cooperative={0} joiners={1}")
    @CsvSource({"false,1", "true,1", "true,3"})
    void closingAMemberWhoseOwnJoinIsUnansweredMustNotHoldTheGroup(boolean cooperative, int joiners) throws Exception {
        List<String> producedKeys = produceMessages(TO_PRODUCE);
        Set<String> processed = ConcurrentHashMap.newKeySet();

        ManagedPCInstance.Config config = ManagedPCInstance.Config.builder()
                .commitMode(PERIODIC_CONSUMER_ASYNCHRONOUS)
                .order(UNORDERED)
                .inputTopic(getTopic())
                .pollDelayMs(PER_RECORD_MS)
                .useCooperativeAssignor(cooperative)
                .build();

        List<ManagedPCInstance> settled = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            ManagedPCInstance instance = new ManagedPCInstance(config, getKcu(), processed::add);
            instance.start(pcExecutor);
            settled.add(instance);
        }
        await().alias("every settled member is consuming")
                .atMost(90, SECONDS)
                .untilAsserted(() -> settled.forEach(pc ->
                        assertWithMessage("instance %s consuming", pc.getInstanceId())
                                .that(pc.getConsumedKeys().size()).isAtLeast(20)));

        // ---- the joiners: started together, then closed the instant the coordinator opens the rebalance,
        // i.e. while their JoinGroups are pending. They never consume; that is the point.
        List<ManagedPCInstance> joiningMembers = new ArrayList<>();
        for (int i = 0; i < joiners; i++) {
            ManagedPCInstance joiner = new ManagedPCInstance(config, getKcu(), processed::add);
            joiner.start(pcExecutor);
            joiningMembers.add(joiner);
        }
        String groupId = getKcu().getGroupId();
        await().alias("coordinator reports the group is rebalancing after the joiners arrived")
                .atMost(30, SECONDS)
                .pollInterval(Duration.ofMillis(25))
                .untilAsserted(() -> assertWithMessage("group state")
                        .that(groupState(groupId))
                        .isAnyOf(ConsumerGroupState.PREPARING_REBALANCE, ConsumerGroupState.COMPLETING_REBALANCE));

        assertWithMessage("NON-DISCRIMINATING RUN: too little backlog left at the close")
                .that(producedKeys.size() - processed.size()).isAtLeast(REMAINING_FLOOR);

        int survivorsBefore = settled.stream().mapToInt(pc -> pc.getConsumedKeys().size()).sum();
        long closeStartedNanos = System.nanoTime();
        List<AtomicLong> closeFinishedNanos = new ArrayList<>();
        List<Thread> closerThreads = new ArrayList<>();
        for (ManagedPCInstance joiner : joiningMembers) {
            AtomicLong finished = new AtomicLong();
            closeFinishedNanos.add(finished);
            Thread t = new Thread(() -> {
                try {
                    joiner.stop();
                } finally {
                    finished.set(System.nanoTime());
                }
            }, "test-closer-joiner-" + joiner.getInstanceId());
            closerThreads.add(t);
        }
        closerThreads.forEach(Thread::start);

        try {
            await().alias("settled members make progress while " + joiners + " joining member(s) close mid-join")
                    .atMost(20, SECONDS)
                    .pollInterval(Duration.ofMillis(200))
                    .untilAsserted(() -> assertWithMessage("survivors' consumption since the closes began")
                            .that(settled.stream().mapToInt(pc -> pc.getConsumedKeys().size()).sum())
                            .isAtLeast(survivorsBefore + 100));
        } finally {
            for (Thread t : closerThreads) {
                t.join(Duration.ofSeconds(90).toMillis());
            }
            for (int i = 0; i < joiningMembers.size(); i++) {
                long fin = closeFinishedNanos.get(i).get();
                log.warn("joiner {} close took {}s", joiningMembers.get(i).getInstanceId(),
                        fin == 0 ? "NOT FINISHED" : String.format("%.1f", (fin - closeStartedNanos) / 1e9));
            }
        }

        assertWithMessage("NON-DISCRIMINATING RUN: the backlog ran out during the liveness window")
                .that(producedKeys.size() - processed.size()).isAtLeast(1_000);

        for (int i = 0; i < joiningMembers.size(); i++) {
            long fin = closeFinishedNanos.get(i).get();
            assertWithMessage("joiner %s close should have completed", joiningMembers.get(i).getInstanceId())
                    .that(fin).isNotEqualTo(0L);
            assertWithMessage("joiner %s close duration while its own JoinGroup was pending (seconds)", joiningMembers.get(i).getInstanceId())
                    .that((fin - closeStartedNanos) / 1e9).isLessThan(10.0);
        }

        settled.forEach(ManagedPCInstance::close);
        pcExecutor.shutdownNow();
    }

    private ConsumerGroupState groupState(String groupId) throws Exception {
        return getKcu().getAdmin()
                .describeConsumerGroups(of(groupId))
                .describedGroups().get(groupId).get(5, SECONDS)
                .state();
    }
}
