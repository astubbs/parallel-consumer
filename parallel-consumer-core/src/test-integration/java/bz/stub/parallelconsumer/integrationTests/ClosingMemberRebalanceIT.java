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
 * <b>Proven red, 2026-09-07</b> ({@code docs/testing-at-write-time.md}): sabotaged by holding the
 * member not-polling and not-left - a 20s sleep at the top of {@code BrokerPollSystem#doClose()},
 * before the consumer leaves the group, which is the defect shape this test exists to detect. Every
 * joiner then closed in 20.0s against the 10s bound, and the assertion named the member. That check
 * earns its place here more than usual: every property this test asserts already holds on master, so
 * a green run says nothing about whether the test can see the failure.
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
     * <p>
     * The throughput those numbers are sized against is NOT {@code 1 / PER_RECORD_MS} per instance:
     * {@code ManagedPCInstance.Config} defaults {@code maxConcurrency} to 10 and this test does not
     * override it, so under {@code UNORDERED} each instance's ceiling is about
     * {@code maxConcurrency / PER_RECORD_MS} - a thousand records a second, low thousands for the
     * fleet. The floors were tuned empirically (4,000 -> 150,000 -> 30,000) and hold with that
     * multiplier; if this ever flakes on a busier runner, that multiplier is the first thing to
     * re-check, which is why it is named here rather than left to be rediscovered.
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

        ManagedPCInstance.Config config = profileConfig(cooperative);

        // ---- a settled group of SETTLED_MEMBERS, every one of them consuming
        List<ManagedPCInstance> settled = startSettledMembers(config, SETTLED_MEMBERS, processed);

        // ---- a newcomer joins: this is what opens a rebalance
        ManagedPCInstance newcomer = new ManagedPCInstance(config, getKcu(), processed::add);
        newcomer.start(pcExecutor);

        // ---- force the window: observe the coordinator's own state rather than guessing the timing
        awaitRebalanceOpened(ConsumerGroupState.PREPARING_REBALANCE, ConsumerGroupState.COMPLETING_REBALANCE);

        requireBacklog(producedKeys, processed, REMAINING_FLOOR, "at the close");

        // ---- close N settled members SIMULTANEOUSLY, each on its own thread, each timed on that thread
        List<ManagedPCInstance> victims = settled.subList(0, closers);
        List<ManagedPCInstance> survivors = new ArrayList<>(settled.subList(closers, SETTLED_MEMBERS));
        survivors.add(newcomer);
        int survivorsBefore = survivors.stream().mapToInt(pc -> pc.getConsumedKeys().size()).sum();

        SimultaneousClose closing = SimultaneousClose.start(victims, "test-closer-");

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
            closing.joinAndLog("victim");
        }

        requireBacklog(producedKeys, processed, 500, "after the liveness window");

        // 2) CLOSE DURATION: the defect costs each victim ~request.timeout.ms (30s) waiting on a JoinGroup
        //    nobody answers. 10s is not "fast"; it is "was answered rather than timed out".
        closing.assertAllClosedWithin(10.0, "mid-rebalance close");

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

        ManagedPCInstance.Config config = profileConfig(cooperative);

        List<ManagedPCInstance> settled = startSettledMembers(config, 3, processed);

        // ---- the joiners: started together, then closed the instant the coordinator opens the rebalance,
        // i.e. while their JoinGroups are pending. They never consume; that is the point.
        List<ManagedPCInstance> joiningMembers = new ArrayList<>();
        for (int i = 0; i < joiners; i++) {
            ManagedPCInstance joiner = new ManagedPCInstance(config, getKcu(), processed::add);
            joiner.start(pcExecutor);
            joiningMembers.add(joiner);
        }
        // PREPARING_REBALANCE only, deliberately narrower than the sibling test: the coordinator answers
        // JoinGroups at the transition to COMPLETING_REBALANCE, so a joiner observed in that state may
        // already hold its JoinGroup response and be waiting on SyncGroup instead - and this test's
        // whole premise is that the JoinGroup is still pending when the joiner is closed. The sibling
        // accepts either state because its claim is only "a rebalance is in flight". Found in review.
        awaitRebalanceOpened(ConsumerGroupState.PREPARING_REBALANCE);

        requireBacklog(producedKeys, processed, REMAINING_FLOOR, "at the close");

        int survivorsBefore = settled.stream().mapToInt(pc -> pc.getConsumedKeys().size()).sum();
        SimultaneousClose closing = SimultaneousClose.start(joiningMembers, "test-closer-joiner-");

        try {
            await().alias("settled members make progress while " + joiners + " joining member(s) close mid-join")
                    .atMost(20, SECONDS)
                    .pollInterval(Duration.ofMillis(200))
                    .untilAsserted(() -> assertWithMessage("survivors' consumption since the closes began")
                            .that(settled.stream().mapToInt(pc -> pc.getConsumedKeys().size()).sum())
                            .isAtLeast(survivorsBefore + 100));
        } finally {
            closing.joinAndLog("joiner");
        }

        requireBacklog(producedKeys, processed, 500, "after the liveness window");

        closing.assertAllClosedWithin(10.0, "close while its own JoinGroup was pending");

        settled.forEach(ManagedPCInstance::close);
        pcExecutor.shutdownNow();
    }

    /** The capacity profile's configuration - async commits, unordered, its assignor, the per-record cost above. */
    private ManagedPCInstance.Config profileConfig(boolean cooperative) {
        return ManagedPCInstance.Config.builder()
                .commitMode(PERIODIC_CONSUMER_ASYNCHRONOUS)
                .order(UNORDERED)
                .inputTopic(getTopic())
                .pollDelayMs(PER_RECORD_MS)
                .useCooperativeAssignor(cooperative)
                .build();
    }

    /**
     * The forcing step: do not guess that a rebalance is in flight - observe it. The capacity profile
     * draws this window by chance; here the admin client reports the coordinator's own state, so the
     * close that follows happens while the join phase is genuinely open.
     */
    private void awaitRebalanceOpened(ConsumerGroupState... acceptable) {
        String groupId = getKcu().getGroupId();
        await().alias("coordinator reports the group in " + java.util.Arrays.toString(acceptable))
                .atMost(30, SECONDS)
                .pollInterval(Duration.ofMillis(25))
                .untilAsserted(() -> assertWithMessage("group state")
                        .that(groupState(groupId))
                        .isIn(java.util.Arrays.asList(acceptable)));
    }

    /**
     * A non-discriminating run must fail as one, not as a freeze: "no progress" on an exhausted topic
     * means "nothing left", which is the misreading the first cut of this test made.
     */
    private static void requireBacklog(List<String> produced, Set<String> processed, int floor, String when) {
        assertWithMessage("NON-DISCRIMINATING RUN: too little backlog left %s - raise TO_PRODUCE or " +
                "PER_RECORD_MS; a liveness assertion on an exhausted topic proves nothing", when)
                .that(produced.size() - processed.size())
                .isAtLeast(floor);
    }

    /** Start {@code count} members on the executor and wait until every one of them has consumed. */
    private List<ManagedPCInstance> startSettledMembers(ManagedPCInstance.Config config, int count, Set<String> processed) {
        List<ManagedPCInstance> settled = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            ManagedPCInstance instance = new ManagedPCInstance(config, getKcu(), processed::add);
            instance.start(pcExecutor);
            settled.add(instance);
        }
        await().alias("every settled member is consuming")
                .atMost(90, SECONDS)
                .untilAsserted(() -> settled.forEach(pc ->
                        assertWithMessage("instance %s consuming", pc.getInstanceId())
                                .that(pc.getConsumedKeys().size()).isAtLeast(20)));
        return settled;
    }

    /**
     * Closes a set of members at the same instant, one thread each, and records each close's duration
     * ON ITS OWN THREAD the moment {@code stop()} returns - the first cut read the clock from the main
     * thread after an await and reported the await's 15s as a 0.3s close. A member whose close never
     * returned reads as {@code NaN}, never as a number.
     */
    private static final class SimultaneousClose {
        private final List<ManagedPCInstance> members;
        private final long startedNanos = System.nanoTime();
        private final List<AtomicLong> finishedNanos = new ArrayList<>();
        private final List<Thread> threads = new ArrayList<>();

        private SimultaneousClose(List<ManagedPCInstance> members) {
            this.members = members;
        }

        static SimultaneousClose start(List<ManagedPCInstance> members, String threadPrefix) {
            SimultaneousClose sc = new SimultaneousClose(members);
            for (ManagedPCInstance member : members) {
                AtomicLong finished = new AtomicLong();
                sc.finishedNanos.add(finished);
                sc.threads.add(new Thread(() -> {
                    try {
                        member.stop();
                    } finally {
                        finished.set(System.nanoTime());
                    }
                }, threadPrefix + member.getInstanceId()));
            }
            sc.threads.forEach(Thread::start);
            return sc;
        }

        private double secondsOrNaN(int i) {
            long fin = finishedNanos.get(i).get();
            return fin == 0 ? Double.NaN : (fin - startedNanos) / 1e9;
        }

        /** Waits for every closer (bounded), then logs each duration - in a finally, so it survives a failed assertion. */
        void joinAndLog(String role) throws InterruptedException {
            for (Thread t : threads) {
                t.join(Duration.ofSeconds(90).toMillis());
            }
            for (int i = 0; i < members.size(); i++) {
                double secs = secondsOrNaN(i);
                log.warn("{} {} close took {}s", role, members.get(i).getInstanceId(),
                        Double.isNaN(secs) ? "NOT FINISHED" : String.format("%.1f", secs));
            }
        }

        void assertAllClosedWithin(double seconds, String what) {
            for (int i = 0; i < members.size(); i++) {
                assertWithMessage("%s: member %s close should have completed", what, members.get(i).getInstanceId())
                        .that(finishedNanos.get(i).get()).isNotEqualTo(0L);
                assertWithMessage("%s: member %s duration (seconds)", what, members.get(i).getInstanceId())
                        .that(secondsOrNaN(i)).isLessThan(seconds);
            }
        }
    }

    private ConsumerGroupState groupState(String groupId) throws Exception {
        return getKcu().getAdmin()
                .describeConsumerGroups(of(groupId))
                .describedGroups().get(groupId).get(5, SECONDS)
                .state();
    }
}
