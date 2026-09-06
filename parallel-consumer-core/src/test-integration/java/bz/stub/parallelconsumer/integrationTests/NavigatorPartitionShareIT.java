package bz.stub.parallelconsumer.integrationTests;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.integrationTests.utils.ChildLedgerRecord;
import bz.stub.parallelconsumer.integrationTests.utils.ChildPcMain;
import bz.stub.parallelconsumer.integrationTests.utils.ChildPcOptions;
import bz.stub.parallelconsumer.integrationTests.utils.ChildPcProcess;
import bz.stub.parallelconsumer.integrationTests.utils.FiringLedger;
import bz.stub.parallelconsumer.integrationTests.utils.NavigatorProofEnvelope.FleetIdentity;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

import static bz.stub.parallelconsumer.integrationTests.utils.NavigatorProofEnvelope.CONTRACT;
import static bz.stub.parallelconsumer.integrationTests.utils.NavigatorProofEnvelope.CONVERGENCE_DEADLINE;
import static bz.stub.parallelconsumer.integrationTests.utils.NavigatorProofEnvelope.RATE_PER_SECOND;
import static bz.stub.parallelconsumer.integrationTests.utils.NavigatorProofEnvelope.RATE_TOLERANCE_PERCENT;
import static bz.stub.parallelconsumer.integrationTests.utils.NavigatorProofEnvelope.RESOURCE;
import static bz.stub.parallelconsumer.integrationTests.utils.NavigatorProofEnvelope.randomSuffix;
import static bz.stub.parallelconsumer.integrationTests.utils.NavigatorProofEnvelope.WINDOW;
import static bz.stub.parallelconsumer.integrationTests.utils.NavigatorProofEnvelope.assertCountWithinTolerance;
import static bz.stub.parallelconsumer.integrationTests.utils.NavigatorProofEnvelope.assertFleetIdentity;
import static bz.stub.parallelconsumer.integrationTests.utils.NavigatorProofEnvelope.expectedFirings;
import static bz.stub.parallelconsumer.integrationTests.utils.NavigatorProofEnvelope.joinUndershootFloor;
import static bz.stub.parallelconsumer.integrationTests.utils.NavigatorProofEnvelope.overshootBound;
import static java.time.Duration.ofSeconds;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * The partition-share plan's asserted twin (U5, R11): the in-process {@code NavigatorRateShareTest} storyline
 * lifted across real process boundaries. Every child is its own JVM running one PC instance under the default
 * {@code PARTITION_SHARE} strategy; the parent counts firings on the BROKER's clock through the
 * {@link FiringLedger} and reads group state through the admin client, so nothing here depends on a child's
 * clock or its stdout except the dashboard share line R9 asks it to print.
 * <p>
 * <b>Measurement discipline (KTD8, KTD13).</b> Every gate is a COUNT over a window anchored to an observed
 * event after observed group stability; the envelope those counts are judged against is predeclared in
 * {@link bz.stub.parallelconsumer.integrationTests.utils.NavigatorProofEnvelope} and shared with the ladder
 * and the demo. Kill-to-rebalance and join-to-stable latencies are OBSERVED and logged, never gated: the
 * post-transition window opens a fixed deadline after the transition's broker-time anchor, so a slow rebalance
 * shows up as a short count, not as a timeout. No sleeps stand in for synchronisation - the ledger's broker-time
 * fence and the group waits do.
 * <p>
 * <b>Every method ends in the fleet identity (R10, AE7).</b> Children the storyline did not kill are stopped
 * gracefully, their end-of-run records collected from the broker, each identity checked, and the fleet's
 * minted plus overdraft held inside the summed shares plus the envelope's conservation slack.
 * <p>
 * Scenarios run one at a time ({@link ExecutionMode#SAME_THREAD}) on a fresh group and fresh topics each: real
 * JVMs compete for the machine, and the latencies reported here would be contaminated by siblings.
 */
@Slf4j
@Execution(ExecutionMode.SAME_THREAD)
class NavigatorPartitionShareIT extends BrokerIntegrationTest<String, String> {

    private static final Duration START_BUDGET = ofSeconds(60);
    private static final Duration GROUP_BUDGET = ofSeconds(60);
    private static final Duration FIRING_BUDGET = ofSeconds(60);
    private static final Duration FENCE_BUDGET = WINDOW.plus(CONVERGENCE_DEADLINE).plus(ofSeconds(60));
    private static final Duration STOP_BUDGET = ofSeconds(60);
    private static final Duration LEDGER_BUDGET = ofSeconds(60);

    /** Per partition: far more than a share can spend over a storyline (at most ~2/s for ~a minute). */
    private static final int BACKLOG_PER_PARTITION = 300;

    private String groupId;
    private String outputTopic;
    private String ledgerTopic;
    private FiringLedger ledger;
    private final List<ChildPcProcess> children = new ArrayList<>();

    @BeforeEach
    void fleetSetup() {
        groupId = "nav-share-" + randomSuffix();
        getKcu().setGroupId(groupId); // describeGroup() and the stability waits read the children's group
        outputTopic = "nav-share-out-" + randomSuffix();
        ledgerTopic = "nav-share-ledger-" + randomSuffix();
        getKcu().createLogAppendTimeTopic(outputTopic);
        getKcu().createLogAppendTimeTopic(ledgerTopic);
        ledger = new FiringLedger(kafkaContainer.getBootstrapServers(), outputTopic, ledgerTopic);
    }

    @AfterEach
    void fleetTeardown() {
        for (ChildPcProcess child : children) {
            child.close();
        }
        ledger.close();
    }

    // ------------------------------------------------------------------
    // AE1: two JVMs share the rate; an untagged bystander in the same group is untouched
    // ------------------------------------------------------------------

    @Test
    void twoTaggedChildrenSplitTheRateAndAnUntaggedBystanderDrainsUnthrottled() {
        String taggedTopic = setupTopicWith("nav-share-tagged", 2);
        String bystanderTopic = setupTopicWith("nav-share-bystander", 1);
        ChildPcProcess left = launch(tagged("left", taggedTopic));
        ChildPcProcess right = launch(tagged("right", taggedTopic));
        ChildPcProcess bystander = launch(untagged("bystander", bystanderTopic));
        awaitStarted(left, right, bystander);
        awaitGroupStable(3, GROUP_BUDGET);

        Instant anchor = ledger.anchorNow();
        produce(taggedTopic, 2);
        produce(bystanderTopic, 1);
        Instant leftFirst = ledger.awaitFiringAtOrAfter("left", anchor, FIRING_BUDGET);
        Instant rightFirst = ledger.awaitFiringAtOrAfter("right", anchor, FIRING_BUDGET);
        Instant bystanderFirst = ledger.awaitFiringAtOrAfter("bystander", anchor, FIRING_BUDGET);
        Instant commonStart = earlier(leftFirst, rightFirst);
        ledger.awaitBrokerTimePast(latest(leftFirst, rightFirst, bystanderFirst).plus(WINDOW), FENCE_BUDGET);

        long leftCount = ledger.countIn("left", leftFirst, leftFirst.plus(WINDOW));
        long rightCount = ledger.countIn("right", rightFirst, rightFirst.plus(WINDOW));
        long aggregate = ledger.countAmong(commonStart, commonStart.plus(WINDOW), "left", "right");
        long bystanderCount = ledger.countIn("bystander", bystanderFirst, bystanderFirst.plus(WINDOW));
        log.info("AE1 observation: window {}s - left {} right {} (expected {} each), aggregate {} over the "
                        + "common window (bound {}), bystander {} (unthrottled)",
                WINDOW.getSeconds(), leftCount, rightCount, expectedFirings(0.5), aggregate,
                overshootBound(WINDOW), bystanderCount);

        assertCountWithinTolerance(leftCount, expectedFirings(0.5), "AE1/R2: left fires at its half share");
        assertCountWithinTolerance(rightCount, expectedFirings(0.5), "AE1/R2: right fires at its half share");
        assertThat((double) aggregate).as("AE1/R8: the tagged pair's aggregate is inside the bound")
                .isLessThanOrEqualTo(overshootBound(WINDOW));
        assertThat((double) bystanderCount)
                .as("AE1/R3: the bystander drains past anything the navigator would allow a tagged pair")
                .isGreaterThan(overshootBound(WINDOW));
        assertThat(dashboardShares(bystander)).as("AE1: the bystander's dashboard never reports a share")
                .isNotEmpty().containsOnly("-");

        FiringLedger.FleetLedger fleet = stopAndAssertFleetIdentity(left, right, bystander);
        ChildLedgerRecord bystanderRecord = fleet.getRecords().stream()
                .filter(record -> record.getInstanceId().equals("bystander")).findFirst()
                .orElseThrow(IllegalStateException::new);
        assertThat(bystanderRecord.getResource()).as("AE1: the bystander's record names no resource")
                .isEqualTo(ChildLedgerRecord.UNTAGGED_RESOURCE);
        assertThat(bystanderRecord.getMinted()).as("AE1: the bystander minted nothing").isZero();
        assertThat(bystanderRecord.getFired()).as("AE1: the bystander did the draining")
                .isGreaterThanOrEqualTo(bystanderCount);
    }

    // ------------------------------------------------------------------
    // AE2: kill one, the survivor inherits the whole rate through the group's rebalance
    // ------------------------------------------------------------------

    @Test
    void killingOneTaggedChildLetsTheSurvivorInheritTheFullRateInsideTheBound() {
        String topic = setupTopicWith("nav-share-tagged", 2);
        ChildPcProcess survivor = launch(tagged("survivor", topic));
        ChildPcProcess victim = launch(tagged("victim", topic));
        awaitStarted(survivor, victim);
        awaitGroupStable(2, GROUP_BUDGET);

        Instant anchor = ledger.anchorNow();
        produce(topic, 2);
        Instant survivorFirst = ledger.awaitFiringAtOrAfter("survivor", anchor, FIRING_BUDGET);
        Instant victimFirst = ledger.awaitFiringAtOrAfter("victim", anchor, FIRING_BUDGET);
        Instant steadyStart = earlier(survivorFirst, victimFirst);
        ledger.awaitBrokerTimePast(latest(survivorFirst, victimFirst).plus(WINDOW), FENCE_BUDGET);
        long survivorSteady = ledger.countIn("survivor", survivorFirst, survivorFirst.plus(WINDOW));
        long victimSteady = ledger.countIn("victim", victimFirst, victimFirst.plus(WINDOW));
        log.info("AE2 steady state: survivor {} victim {} over {}s (expected {} each)", survivorSteady,
                victimSteady, WINDOW.getSeconds(), expectedFirings(0.5));
        assertCountWithinTolerance(survivorSteady, expectedFirings(0.5), "AE2 steady: survivor at half share");
        assertCountWithinTolerance(victimSteady, expectedFirings(0.5), "AE2 steady: victim at half share");

        Instant killAt = ledger.brokerNow();
        Instant killWall = Instant.now();
        victim.kill();
        Duration memberGone = awaitGroupMemberCount(1, GROUP_BUDGET);
        ConsumerGroupDescription rebalanced = awaitGroupStable(1, GROUP_BUDGET);
        Duration killToStable = Duration.between(killWall, Instant.now());
        assertThat(partitionsHeldBy(rebalanced, "survivor")).as("the survivor holds both partitions").hasSize(2);

        Instant convergedStart = killAt.plus(CONVERGENCE_DEADLINE);
        Instant convergedEnd = convergedStart.plus(WINDOW);
        ledger.awaitBrokerTimePast(convergedEnd, FENCE_BUDGET);
        long survivorConverged = ledger.countIn("survivor", convergedStart, convergedEnd);
        Duration transitionSpan = Duration.between(steadyStart, convergedEnd);
        long fleetOverTransition = ledger.countAmong(steadyStart, convergedEnd, "survivor", "victim");
        Instant victimTail = ledger.latestFiringOf("victim").orElseThrow(IllegalStateException::new);
        log.info("AE2 observation: KILL LATENCY member gone {} / group stable {} after the kill (session "
                        + "timeout {} ms) - reported, not gated; victim's last firing {} after the kill anchor; "
                        + "survivor {} in the post-deadline window [{}, {}) (expected {}); fleet {} over the {}s "
                        + "transition span (bound {})",
                memberGone, killToStable, victim.getOptions().getSessionTimeoutMs(),
                Duration.between(killAt, victimTail), survivorConverged, convergedStart, convergedEnd,
                expectedFirings(1.0), fleetOverTransition, transitionSpan.getSeconds(),
                overshootBound(transitionSpan));

        assertCountWithinTolerance(survivorConverged, expectedFirings(1.0),
                "AE2/R4/R11: the survivor fires at the full rate once the convergence deadline has passed");
        assertThat((double) fleetOverTransition)
                .as("AE2/R8: the fleet's aggregate over the whole transition is inside the bound")
                .isLessThanOrEqualTo(overshootBound(transitionSpan));

        // the victim died without a record, by definition - the identity is over the survivor's
        stopAndAssertFleetIdentity(survivor);
    }

    // ------------------------------------------------------------------
    // AE3 + R9: four partitions three-to-one give three quarters and one quarter, and each child says so
    // ------------------------------------------------------------------

    @Test
    void threeToOnePartitionsGiveThreeQuartersAndOneQuarterOfTheRateAndEachChildReportsItsFraction() {
        // the range assignor divides PER TOPIC, so one four-partition topic splits two-two; three partitions
        // plus one split 2:1 and 1:0 - four partitions of the subscription, three-to-one (the harness proof's shape)
        String topicA = setupTopicWith("nav-share-a", 3);
        String topicB = setupTopicWith("nav-share-b", 1);
        ChildPcProcess major = launch(tagged("major", topicA, topicB));
        ChildPcProcess minor = launch(tagged("minor", topicA, topicB));
        awaitStarted(major, minor);
        ConsumerGroupDescription stable = awaitGroupStable(2, GROUP_BUDGET);
        log.info("AE3 split: {}", describeMembers(stable));
        assertThat(partitionsHeldBy(stable, "major")).as("major holds three of the four").hasSize(3);
        assertThat(partitionsHeldBy(stable, "minor")).as("minor holds one of the four").hasSize(1);

        Instant anchor = ledger.anchorNow();
        produce(topicA, 3);
        produce(topicB, 1);
        Instant majorFirst = ledger.awaitFiringAtOrAfter("major", anchor, FIRING_BUDGET);
        Instant minorFirst = ledger.awaitFiringAtOrAfter("minor", anchor, FIRING_BUDGET);
        Instant commonStart = earlier(majorFirst, minorFirst);
        ledger.awaitBrokerTimePast(latest(majorFirst, minorFirst).plus(WINDOW), FENCE_BUDGET);

        long majorCount = ledger.countIn("major", majorFirst, majorFirst.plus(WINDOW));
        long minorCount = ledger.countIn("minor", minorFirst, minorFirst.plus(WINDOW));
        long aggregate = ledger.countAmong(commonStart, commonStart.plus(WINDOW), "major", "minor");
        List<String> majorShares = recentDashboardShares(major, 3);
        List<String> minorShares = recentDashboardShares(minor, 3);
        log.info("AE3 observation: major {} (expected {}) minor {} (expected {}) over {}s, aggregate {} "
                        + "(bound {}); dashboard shares major {} minor {}",
                majorCount, expectedFirings(0.75), minorCount, expectedFirings(0.25), WINDOW.getSeconds(),
                aggregate, overshootBound(WINDOW), majorShares, minorShares);

        assertCountWithinTolerance(majorCount, expectedFirings(0.75), "AE3/R2: three partitions of four fire at 1.5Hz");
        assertCountWithinTolerance(minorCount, expectedFirings(0.25), "AE3/R2/R3: one partition of four fires at 0.5Hz");
        assertThat((double) aggregate).as("AE3/R8: aggregate inside the bound")
                .isLessThanOrEqualTo(overshootBound(WINDOW));
        assertThat(majorShares).as("AE3/R9: the three-partition holder reports three quarters")
                .isNotEmpty().containsOnly(fraction(0.75));
        assertThat(minorShares).as("AE3/R9: the one-partition holder reports one quarter")
                .isNotEmpty().containsOnly(fraction(0.25));

        stopAndAssertFleetIdentity(major, minor);
    }

    // ------------------------------------------------------------------
    // AE5: more members than partitions - the one holding nothing has no share and fires nothing
    // ------------------------------------------------------------------

    @Test
    void aTaggedChildHoldingNoPartitionHasNoShareMintsNothingAndFiresNothing() {
        String topic = setupTopicWith("nav-share-tagged", 2);
        ChildPcProcess holderA = launch(tagged("holder-a", topic));
        ChildPcProcess holderB = launch(tagged("holder-b", topic));
        ChildPcProcess spare = launch(tagged("spare", topic));
        awaitStarted(holderA, holderB, spare);
        ConsumerGroupDescription stable = awaitGroupStable(3, GROUP_BUDGET);
        log.info("AE5 split: {}", describeMembers(stable));
        assertThat(partitionsHeldBy(stable, "spare")).as("the spare member holds nothing").isEmpty();

        Instant anchor = ledger.anchorNow();
        produce(topic, 2);
        Instant aFirst = ledger.awaitFiringAtOrAfter("holder-a", anchor, FIRING_BUDGET);
        Instant bFirst = ledger.awaitFiringAtOrAfter("holder-b", anchor, FIRING_BUDGET);
        Instant windowEnd = latest(aFirst, bFirst).plus(WINDOW);
        ledger.awaitBrokerTimePast(windowEnd, FENCE_BUDGET);

        long aCount = ledger.countIn("holder-a", aFirst, aFirst.plus(WINDOW));
        long bCount = ledger.countIn("holder-b", bFirst, bFirst.plus(WINDOW));
        List<String> spareShares = recentDashboardShares(spare, 3);
        log.info("AE5 observation: holder-a {} holder-b {} (expected {} each); spare fired {} ever, dashboard "
                + "shares {}", aCount, bCount, expectedFirings(0.5), ledger.firingsOf("spare").size(), spareShares);

        assertCountWithinTolerance(aCount, expectedFirings(0.5), "AE5: holder-a keeps its half share");
        assertCountWithinTolerance(bCount, expectedFirings(0.5), "AE5: holder-b keeps its half share");
        assertThat(ledger.instancesSeen()).as("AE5/R5: the spare never fired").doesNotContain("spare");
        assertThat(spareShares).as("AE5/R5/R9: the spare reports a zero share").isNotEmpty()
                .containsOnly(fraction(0.0));

        FiringLedger.FleetLedger fleet = stopAndAssertFleetIdentity(holderA, holderB, spare);
        ChildLedgerRecord spareRecord = recordOf(fleet, "spare");
        assertThat(spareRecord.getMinted()).as("AE5/R5: the spare minted nothing").isZero();
        assertThat(spareRecord.getOverdraft()).as("AE5/R5: the spare overdrew nothing").isZero();
        assertThat(spareRecord.getFired()).as("AE5/R5: the spare dispatched nothing").isZero();
    }

    // ------------------------------------------------------------------
    // AE8: two-two split, backlog on one side - the fleet runs at half rate and the idle share expires
    // ------------------------------------------------------------------

    @Test
    void backlogOnOneSideOfATwoTwoSplitFiresAtHalfRateAndTheIdleShareExpiresUnspent() {
        String topic = setupTopicWith("nav-share-tagged", 4);
        ChildPcProcess busy = launch(tagged("busy", topic));
        ChildPcProcess idle = launch(tagged("idle", topic));
        awaitStarted(busy, idle);
        ConsumerGroupDescription stable = awaitGroupStable(2, GROUP_BUDGET);
        Set<TopicPartition> busyPartitions = partitionsHeldBy(stable, "busy");
        log.info("AE8 split: {} - producing only to busy's {}", describeMembers(stable), busyPartitions);
        assertThat(busyPartitions).as("a two-two split").hasSize(2);

        Instant anchor = ledger.anchorNow();
        for (TopicPartition partition : busyPartitions) {
            produceToPartition(partition);
        }
        Instant busyFirst = ledger.awaitFiringAtOrAfter("busy", anchor, FIRING_BUDGET);
        Instant windowEnd = busyFirst.plus(WINDOW);
        ledger.awaitBrokerTimePast(windowEnd, FENCE_BUDGET);

        long busyCount = ledger.countIn("busy", busyFirst, windowEnd);
        long idleCount = ledger.countIn("idle", anchor, windowEnd);
        log.info("AE8 observation: busy {} (expected {} - the fleet at half rate), idle {} over the window",
                busyCount, expectedFirings(0.5), idleCount);
        assertCountWithinTolerance(busyCount, expectedFirings(0.5),
                "AE8/R8: with backlog on half the partitions the fleet fires at half the rate");
        assertThat(idleCount).as("AE8: the idle child, holding no backlog, fires nothing").isZero();

        FiringLedger.FleetLedger fleet = stopAndAssertFleetIdentity(busy, idle);
        ChildLedgerRecord idleRecord = recordOf(fleet, "idle");
        // The idle child held half the partitions for at least the window, so its ENTITLEMENT - the share its
        // view reported, sampled per quantum - covers at least the window's half-rate credits; none of it was
        // spent, and whatever it minted expired. Minting is lazy (a share nobody read is never minted and never
        // expires - ConservationLedger's definition), and an idle control loop blocks up to the commit interval
        // between passes, so it reads only one quantum in several: the first run of this lane saw minted 3,
        // expired 3 against a sampled share of 14. So the expired counter grows, and the gap between the share
        // and the mint is credit that never existed - not credit that leaked, which the fleet count above rules out.
        double idleShareFloor = Math.floor(expectedFirings(0.5) * (100 - RATE_TOLERANCE_PERCENT) / 100.0);
        assertThat(idleRecord.getFired()).as("AE8: the idle child dispatched nothing").isZero();
        assertThat(idleRecord.getSpent()).as("AE8: the idle child spent nothing").isZero();
        assertThat(idleRecord.getSharesSummed())
                .as("AE8/R8: the idle child's share over the window was real - at least half the rate's credits")
                .isGreaterThanOrEqualTo(idleShareFloor);
        assertThat(idleRecord.getExpired())
                .as("AE8/R8: what the idle child minted expired unspent (minted %s, expired %s, outstanding %s)",
                        idleRecord.getMinted(), idleRecord.getExpired(), idleRecord.getOutstanding())
                .isPositive();
    }

    // ------------------------------------------------------------------
    // AE9: two groups tagging the same name each get the full rate - the documented per-group scope
    // ------------------------------------------------------------------

    @Test
    void twoGroupsTaggingTheSameResourceNameEachGetTheFullRate() {
        String groupB = groupId + "-b";
        String topicA = setupTopicWith("nav-share-group-a", 2);
        String topicB = setupTopicWith("nav-share-group-b", 2);
        ChildPcProcess a1 = launch(tagged("a1", topicA));
        ChildPcProcess a2 = launch(tagged("a2", topicA));
        ChildPcProcess b1 = launch(tagged("b1", topicB).toBuilder().groupId(groupB).build());
        ChildPcProcess b2 = launch(tagged("b2", topicB).toBuilder().groupId(groupB).build());
        awaitStarted(a1, a2, b1, b2);
        awaitGroupStable(2, GROUP_BUDGET);
        getKcu().setGroupId(groupB);
        awaitGroupStable(2, GROUP_BUDGET);

        Instant anchor = ledger.anchorNow();
        produce(topicA, 2);
        produce(topicB, 2);
        Instant a1First = ledger.awaitFiringAtOrAfter("a1", anchor, FIRING_BUDGET);
        Instant a2First = ledger.awaitFiringAtOrAfter("a2", anchor, FIRING_BUDGET);
        Instant b1First = ledger.awaitFiringAtOrAfter("b1", anchor, FIRING_BUDGET);
        Instant b2First = ledger.awaitFiringAtOrAfter("b2", anchor, FIRING_BUDGET);
        Instant commonStart = earlier(earlier(a1First, a2First), earlier(b1First, b2First));
        Instant commonEnd = commonStart.plus(WINDOW);
        ledger.awaitBrokerTimePast(latest(a1First, a2First, b1First, b2First).plus(WINDOW), FENCE_BUDGET);

        long a1Count = ledger.countIn("a1", a1First, a1First.plus(WINDOW));
        long a2Count = ledger.countIn("a2", a2First, a2First.plus(WINDOW));
        long b1Count = ledger.countIn("b1", b1First, b1First.plus(WINDOW));
        long b2Count = ledger.countIn("b2", b2First, b2First.plus(WINDOW));
        long groupATotal = ledger.countAmong(commonStart, commonEnd, "a1", "a2");
        long groupBTotal = ledger.countAmong(commonStart, commonEnd, "b1", "b2");
        log.info("AE9 observation: group A {}+{}={} group B {}+{}={} over the common {}s window (expected {} "
                        + "per child, {} per group, one group's bound {})",
                a1Count, a2Count, groupATotal, b1Count, b2Count, groupBTotal, WINDOW.getSeconds(),
                expectedFirings(0.5), expectedFirings(1.0), overshootBound(WINDOW));

        for (long count : Arrays.asList(a1Count, a2Count, b1Count, b2Count)) {
            assertCountWithinTolerance(count, expectedFirings(0.5), "AE9/R1: each child at half of ITS group's rate");
        }
        assertCountWithinTolerance(groupATotal, expectedFirings(1.0), "AE9/R1: group A collectively at the rate");
        assertCountWithinTolerance(groupBTotal, expectedFirings(1.0), "AE9/R1: group B collectively at the rate");
        assertThat((double) groupATotal).as("AE9/R8: group A inside one group's bound")
                .isLessThanOrEqualTo(overshootBound(WINDOW));
        assertThat((double) groupBTotal).as("AE9/R8: group B inside one group's bound")
                .isLessThanOrEqualTo(overshootBound(WINDOW));
        assertThat((double) (groupATotal + groupBTotal))
                .as("AE9/R8: against the NAME the two groups together exceed one group's bound - the rate is "
                        + "per group, not per name")
                .isGreaterThan(overshootBound(WINDOW));

        stopAndAssertFleetIdentity(a1, a2, b1, b2);
    }

    // ------------------------------------------------------------------
    // AE10: a joiner takes a partition - the fleet undershoots for a quantum plus the rebalance, never overshoots
    // ------------------------------------------------------------------

    @Test
    void aJoiningChildTakingAPartitionUndershootsForAtMostAQuantumPlusTheRebalanceAndNeverOvershoots() {
        String topic = setupTopicWith("nav-share-tagged", 3);
        ChildPcProcess first = launch(tagged("first", topic));
        ChildPcProcess second = launch(tagged("second", topic));
        awaitStarted(first, second);
        ConsumerGroupDescription before = awaitGroupStable(2, GROUP_BUDGET);
        log.info("AE10 split before the join: {}", describeMembers(before));
        double firstShare = partitionsHeldBy(before, "first").size() / 3.0;
        double secondShare = partitionsHeldBy(before, "second").size() / 3.0;

        Instant anchor = ledger.anchorNow();
        produce(topic, 3);
        Instant firstFirst = ledger.awaitFiringAtOrAfter("first", anchor, FIRING_BUDGET);
        Instant secondFirst = ledger.awaitFiringAtOrAfter("second", anchor, FIRING_BUDGET);
        Instant steadyStart = earlier(firstFirst, secondFirst);
        Instant steadyEnd = steadyStart.plus(WINDOW);
        ledger.awaitBrokerTimePast(latest(firstFirst, secondFirst).plus(WINDOW), FENCE_BUDGET);
        long firstSteady = ledger.countIn("first", firstFirst, firstFirst.plus(WINDOW));
        long secondSteady = ledger.countIn("second", secondFirst, secondFirst.plus(WINDOW));
        long fleetSteady = ledger.countAmong(steadyStart, steadyEnd, "first", "second");
        log.info("AE10 steady state: first {} (share {}) second {} (share {}), fleet {} (expected {})",
                firstSteady, firstShare, secondSteady, secondShare, fleetSteady, expectedFirings(1.0));
        assertCountWithinTolerance(firstSteady, expectedFirings(firstShare), "AE10 steady: first at its fraction");
        assertCountWithinTolerance(secondSteady, expectedFirings(secondShare), "AE10 steady: second at its fraction");
        assertCountWithinTolerance(fleetSteady, expectedFirings(1.0), "AE10 steady: the fleet at the rate");

        Instant joinAt = ledger.brokerNow();
        Instant joinWall = Instant.now();
        ChildPcProcess third = launch(tagged("third", topic));
        third.awaitStarted(START_BUDGET);
        ConsumerGroupDescription after = awaitGroupStable(3, GROUP_BUDGET);
        Duration joinToStable = Duration.between(joinWall, Instant.now());
        log.info("AE10 split after the join: {} - stable {} after the launch (reported, not gated)",
                describeMembers(after), joinToStable);
        assertThat(partitionsHeldBy(after, "third")).as("the joiner took one partition").hasSize(1);

        Instant convergedStart = joinAt.plus(CONVERGENCE_DEADLINE);
        Instant convergedEnd = convergedStart.plus(WINDOW);
        ledger.awaitBrokerTimePast(convergedEnd, FENCE_BUDGET);
        long fleetOverTransition = ledger.countAll(joinAt, convergedStart);
        double transitionExpectation = RATE_PER_SECOND * CONVERGENCE_DEADLINE.getSeconds();
        long firstConverged = ledger.countIn("first", convergedStart, convergedEnd);
        long secondConverged = ledger.countIn("second", convergedStart, convergedEnd);
        long thirdConverged = ledger.countIn("third", convergedStart, convergedEnd);
        Duration wholeSpan = Duration.between(steadyStart, convergedEnd);
        long fleetOverWholeSpan = ledger.countAll(steadyStart, convergedEnd);
        log.info("AE10 observation: fleet {} over the {}s transition window (steady expectation {}, observed "
                        + "undershoot {}, priced floor {}, bound {}); converged window first {} second {} third {} "
                        + "(expected {} each); fleet {} over the whole {}s span (bound {})",
                fleetOverTransition, CONVERGENCE_DEADLINE.getSeconds(), transitionExpectation,
                transitionExpectation - fleetOverTransition, joinUndershootFloor(CONVERGENCE_DEADLINE),
                overshootBound(CONVERGENCE_DEADLINE), firstConverged, secondConverged, thirdConverged,
                expectedFirings(1 / 3.0), fleetOverWholeSpan, wholeSpan.getSeconds(), overshootBound(wholeSpan));

        assertThat((double) fleetOverTransition)
                .as("AE10/R4: across the join the fleet undershoots by at most one quantum plus the rebalance")
                .isGreaterThanOrEqualTo(joinUndershootFloor(CONVERGENCE_DEADLINE));
        assertThat((double) fleetOverTransition).as("AE10/R8: the transition window never overshoots")
                .isLessThanOrEqualTo(overshootBound(CONVERGENCE_DEADLINE));
        assertCountWithinTolerance(firstConverged, expectedFirings(1 / 3.0), "AE10/R4: first at a third after the join");
        assertCountWithinTolerance(secondConverged, expectedFirings(1 / 3.0), "AE10/R4: second at a third after the join");
        assertCountWithinTolerance(thirdConverged, expectedFirings(1 / 3.0), "AE10/R4: the joiner at its third");
        assertThat((double) fleetOverWholeSpan).as("AE10/R8: the whole storyline never overshoots")
                .isLessThanOrEqualTo(overshootBound(wholeSpan));

        stopAndAssertFleetIdentity(first, second, third);
    }

    // ------------------------------------------------------------------
    // R10: the fleet identity, at the end of every storyline
    // ------------------------------------------------------------------

    /**
     * Stops the named children gracefully (exit 0 each), collects their end-of-run records from the broker,
     * asserts each identity balances, and holds the fleet's minted plus overdraft inside its summed shares plus
     * the envelope's conservation slack (R10, AE7). Children the storyline killed are not passed - a killed
     * child has no record, by definition.
     */
    private FiringLedger.FleetLedger stopAndAssertFleetIdentity(ChildPcProcess... toStop) {
        FiringLedger.FleetLedger fleet = ledger.stopAndCollect(Arrays.asList(toStop), STOP_BUDGET, LEDGER_BUDGET);
        FleetIdentity ignoredIdentity = assertFleetIdentity(fleet); // the ladder records it; here the assertion is the point
        return fleet;
    }

    private static ChildLedgerRecord recordOf(FiringLedger.FleetLedger fleet, String instanceId) {
        return fleet.forResource(RESOURCE).stream().filter(r -> r.getInstanceId().equals(instanceId)).findFirst()
                .orElseThrow(() -> new IllegalStateException("no " + RESOURCE + " record from " + instanceId));
    }

    // ------------------------------------------------------------------
    // The children's own reports (R9): the dashboard share field
    // ------------------------------------------------------------------

    /** Every {@code share=} value the child's dashboard has printed, in order. */
    private static List<String> dashboardShares(ChildPcProcess child) {
        return child.stdoutLines().stream()
                .filter(line -> line.startsWith(ChildPcMain.DASHBOARD_PREFIX))
                .map(line -> dashboardField(line, "share="))
                .collect(Collectors.toList());
    }

    /** The last {@code count} dashboard share values - the steady state the window just measured. */
    private static List<String> recentDashboardShares(ChildPcProcess child, int count) {
        List<String> all = dashboardShares(child);
        return all.subList(Math.max(0, all.size() - count), all.size());
    }

    private static String dashboardField(String line, String key) {
        int at = line.indexOf(" " + key);
        if (at < 0) {
            throw new IllegalStateException("dashboard line without '" + key + "': " + line);
        }
        int start = at + 1 + key.length();
        int end = line.indexOf(' ', start);
        return end < 0 ? line.substring(start) : line.substring(start, end);
    }

    /** A share fraction as the dashboard formats it. */
    private static String fraction(double value) {
        return String.format(Locale.ROOT, "%.3f", value);
    }

    // ------------------------------------------------------------------
    // Construction
    // ------------------------------------------------------------------

    private ChildPcOptions tagged(String instanceId, String... inputTopics) {
        return untagged(instanceId, inputTopics).toBuilder().resourceTag(RESOURCE).contract(CONTRACT).build();
    }

    private ChildPcOptions untagged(String instanceId, String... inputTopics) {
        return ChildPcOptions.builder()
                .bootstrapServers(kafkaContainer.getBootstrapServers())
                .groupId(groupId)
                .instanceId(instanceId)
                .inputTopics(Arrays.asList(inputTopics))
                .outputTopic(outputTopic)
                .ledgerTopic(ledgerTopic)
                .build();
    }

    private ChildPcProcess launch(ChildPcOptions options) {
        ChildPcProcess child = ChildPcProcess.launch(options);
        children.add(child);
        return child;
    }

    private static void awaitStarted(ChildPcProcess... launched) {
        for (ChildPcProcess child : launched) {
            Duration took = child.awaitStarted(START_BUDGET);
            log.info("child {} ready after {}", child.getOptions().getInstanceId(), took);
        }
    }

    private String setupTopicWith(String name, int partitions) {
        super.numPartitions = partitions;
        return setupTopic(name);
    }

    /** {@link #BACKLOG_PER_PARTITION} per partition, keyed, so every partition of the topic has work. */
    @SneakyThrows
    private void produce(String topic, int partitions) {
        getKcu().produceMessages(topic, (long) BACKLOG_PER_PARTITION * partitions);
    }

    @SneakyThrows
    private void produceToPartition(TopicPartition partition) {
        getKcu().produceMessagesToPartition(partition.topic(), partition.partition(), BACKLOG_PER_PARTITION);
    }

    private static Set<TopicPartition> partitionsHeldBy(ConsumerGroupDescription description, String clientId) {
        for (MemberDescription member : description.members()) {
            if (member.clientId().equals(clientId)) {
                return member.assignment().topicPartitions();
            }
        }
        throw new IllegalStateException("no member with client id '" + clientId + "' in "
                + describeMembers(description));
    }

    private static Instant earlier(Instant a, Instant b) {
        return a.isBefore(b) ? a : b;
    }

    private static Instant latest(Instant... instants) {
        Instant result = instants[0];
        for (Instant instant : instants) {
            if (instant.isAfter(result)) {
                result = instant;
            }
        }
        return result;
    }

}
