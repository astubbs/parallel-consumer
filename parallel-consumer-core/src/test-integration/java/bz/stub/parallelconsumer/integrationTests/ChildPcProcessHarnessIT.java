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
import bz.stub.parallelconsumer.navigator.ResourceContract;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.MemberDescription;
import org.awaitility.core.ConditionTimeoutException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import pl.tlinkowski.unij.api.UniLists;
import pl.tlinkowski.unij.api.UniSets;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.OptionalInt;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static bz.stub.parallelconsumer.integrationTests.utils.NavigatorProofEnvelope.fleetIdentity;
import static bz.stub.parallelconsumer.integrationTests.utils.NavigatorProofEnvelope.randomSuffix;
import static java.time.Duration.ofSeconds;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The multi-process harness's own proof (the partition-share plan's U4): every failure of the harness must be
 * distinguishable from a failure of the mechanism before any lane (U5's two-JVM proof, U6's churn ladder)
 * depends on it. Each scenario here is a claim about {@link ChildPcProcess}, {@link ChildPcMain},
 * {@link FiringLedger} or the group-observation waits on {@link BrokerIntegrationTest} - never about the
 * navigator's rate sharing, which those lanes assert.
 * <p>
 * Scenarios run one at a time ({@link ExecutionMode#SAME_THREAD}): each launches real JVMs, and the kill-to-
 * rebalance latency this class REPORTS would be contaminated by siblings competing for the machine. Nothing here
 * gates on a duration except the harness's own reap slack (KTD13).
 */
@Slf4j
@Execution(ExecutionMode.SAME_THREAD)
class ChildPcProcessHarnessIT extends BrokerIntegrationTest<String, String> {

    private static final String RESOURCE = "api-x";
    private static final ResourceContract TWO_PER_SECOND = new ResourceContract(RESOURCE, 2.0, 2, ofSeconds(1));
    private static final Duration START_BUDGET = ofSeconds(60);
    private static final Duration GROUP_BUDGET = ofSeconds(60);
    private static final int BACKLOG = 600;

    private String groupId;
    private String outputTopic;
    private String ledgerTopic;
    private FiringLedger ledger;
    private final List<ChildPcProcess> children = new ArrayList<>();

    @BeforeEach
    void harnessSetup() {
        groupId = "child-pc-" + randomSuffix();
        getKcu().setGroupId(groupId); // describeGroup() and the stability waits read the children's group
        outputTopic = "child-pc-out-" + randomSuffix();
        ledgerTopic = "child-pc-ledger-" + randomSuffix();
        getKcu().createLogAppendTimeTopic(outputTopic);
        getKcu().createLogAppendTimeTopic(ledgerTopic);
        ledger = new FiringLedger(kafkaContainer.getBootstrapServers(), outputTopic, ledgerTopic);
    }

    @AfterEach
    void harnessTeardown() {
        for (ChildPcProcess child : children) {
            child.close();
        }
        ledger.close();
    }

    // ------------------------------------------------------------------
    // Scenario 1: the classpath proof - a child loads PC off the failsafe JVM's classpath and is counted
    // ------------------------------------------------------------------

    @Test
    void childLaunchedFromTheFailsafeJvmLoadsPcJoinsTheGroupAndItsFiringsAreCountedOnBrokerTime() {
        log.info("CLASSPATH PROOF: the parent's java.class.path is {}", ChildPcProcess.describeClasspath());
        String topic = setupTopic("child-pc-in");
        ChildPcProcess child = launch(tagged("solo", topic));

        Duration startTook = child.awaitStarted(START_BUDGET);
        ConsumerGroupDescription stable = awaitGroupStable(1, GROUP_BUDGET);
        assertThat(clientIds(stable)).as("the child joined the group under its own instance id")
                .containsExactly("solo");
        Instant anchor = ledger.anchorNow();
        produce(topic, BACKLOG);

        Instant firstFiring = ledger.awaitFiringAtOrAfter("solo", anchor, ofSeconds(60));
        Instant windowEnd = firstFiring.plusSeconds(4);
        ledger.awaitBrokerTimePast(windowEnd, ofSeconds(60));
        long counted = ledger.countIn("solo", firstFiring, windowEnd);
        Instant brokerNow = ledger.brokerNow();
        log.info("CLASSPATH PROOF: child started in {}, first firing at broker time {}, {} firings in [{}, {}), "
                + "broker now {}", startTook, firstFiring, counted, firstFiring, windowEnd, brokerNow);

        assertThat(counted).as("firings counted on broker timestamps inside the window").isGreaterThan(0);
        // the child keeps firing while this reads, so a firing filed just after brokerNow's marker is legitimate
        assertThat(ledger.brokerTimesOf("solo"))
                .as("every firing's broker timestamp lies between the anchor and broker-now (plus live slack)")
                .allSatisfy(firing -> assertThat(firing).isBetween(anchor, brokerNow.plusSeconds(5)));
        assertThat(child.isAlive()).as("the child is still running").isTrue();
        assertThat(child.stdoutLines()).as("the child printed its READY line").contains(ChildPcMain.READY_LINE);
    }

    // ------------------------------------------------------------------
    // Scenario 2: an early exit is the child's failure, reported with its stderr - never a group timeout
    // ------------------------------------------------------------------

    @Test
    void childThatThrowsBeforeSubscribingIsReportedAsAnEarlyExitWithItsStderrInsideTheStartBudget() {
        String topic = setupTopic("child-pc-in");
        ChildPcProcess child = launch(tagged("doomed", topic).toBuilder().failBeforeSubscribe(true).build());

        Instant before = Instant.now();
        assertThatThrownBy(() -> child.awaitStarted(START_BUDGET))
                .isInstanceOf(ChildPcProcess.EarlyExitException.class)
                .hasMessageContaining("exited with code 1")
                .hasMessageContaining(ChildPcMain.DELIBERATE_FAILURE_MESSAGE)
                .hasMessageContaining("This is the CHILD failing");
        Duration reported = Duration.between(before, Instant.now());
        log.info("EARLY EXIT: reported after {} (start budget {})", reported, START_BUDGET);
        assertThat(reported).as("reported well inside the start budget, not at its end").isLessThan(START_BUDGET);
        assertThat(child.stderrText()).contains(ChildPcMain.DELIBERATE_FAILURE_MESSAGE);
        assertThat(describeGroup().members()).as("the doomed child never joined").isEmpty();
    }

    // ------------------------------------------------------------------
    // Scenario 2b: a parent that dies (stdin EOF, no stop line) does not leave an orphan running forever
    // ------------------------------------------------------------------

    @Test
    void childStopsGracefullyAndEmitsItsLedgerWhenStdinClosesWithoutAStopLine() {
        String topic = setupTopic("child-pc-in");
        ChildPcProcess child = launch(tagged("orphan", topic));
        child.awaitStarted(START_BUDGET);
        awaitGroupStable(1, GROUP_BUDGET);

        child.closeStdin();
        OptionalInt exit = child.awaitExit(ofSeconds(60));
        assertThat(exit.isPresent()).as("the child noticed stdin EOF and exited on its own" + child.diagnostics())
                .isTrue();
        assertThat(exit.getAsInt()).as("EOF is the graceful route: exit 0").isZero();
        FiringLedger.FleetLedger fleet = ledger.awaitLedgerRecords(UniSets.of("orphan"), ofSeconds(60));
        assertThat(fleet.forResource(RESOURCE)).as("the ledger record was emitted on the EOF path").hasSize(1);
    }

    // ------------------------------------------------------------------
    // Scenario 3: a child that floods stdout before its first poll neither stalls nor loses a line
    // ------------------------------------------------------------------

    @Test
    void childThatPrintsPastThePipeBufferBeforeItsFirstPollNeitherStallsNorLosesLines() {
        int spam = 30_000; // ~2.4MB, far past the 64KB pipe buffer
        String topic = setupTopic("child-pc-in");
        ChildPcProcess child = launch(tagged("chatty", topic).toBuilder().spamStdoutLines(spam).build());

        Duration startTook = child.awaitStarted(START_BUDGET);
        awaitGroupStable(1, GROUP_BUDGET);
        long spamLines = child.stdoutLines().stream().filter(line -> line.startsWith(ChildPcMain.SPAM_PREFIX)).count();
        log.info("PIPE PUMP: {} spam lines captured of {}, child ready after {}", spamLines, spam, startTook);
        assertThat(spamLines).as("every spam line captured, none lost").isEqualTo(spam);
        assertThat(child.stdoutLines()).contains(ChildPcMain.READY_LINE);
    }

    // ------------------------------------------------------------------
    // Scenario 4: destroyForcibly is reaped inside the slack; the group notices after the session timeout
    // ------------------------------------------------------------------

    @Test
    void killIsReapedInsideTheSlackAndTheGroupReportsTheMemberGoneAfterTheSessionTimeout() {
        String topic = setupTopic("child-pc-in");
        ChildPcProcess child = launch(tagged("victim", topic));
        child.awaitStarted(START_BUDGET);
        awaitGroupStable(1, GROUP_BUDGET);

        Instant killedAt = Instant.now();
        Duration reap = child.kill();
        assertThat(child.isAlive()).isFalse();
        assertThat(reap).as("destroyForcibly reaped inside the slack").isLessThan(ChildPcProcess.REAP_SLACK);

        Duration gone = awaitGroupMemberCount(0, ofSeconds(60));
        Duration killToRebalance = Duration.between(killedAt, Instant.now());
        log.info("KILL LATENCY: reap took {}; the group reported the member gone {} after the kill "
                        + "(session timeout {} ms, heartbeat {} ms) - an OBSERVATION, not a gate",
                reap, killToRebalance, child.getOptions().getSessionTimeoutMs(),
                child.getOptions().getHeartbeatIntervalMs());
        assertThat(gone).as("the wait itself returned a duration").isPositive();
        assertThat(child.exitCode()).as("a killed child has an exit code").isPresent();
    }

    // ------------------------------------------------------------------
    // Scenario 5: clock offsets are invisible on the broker's clock, and demonstrably present in the child
    // ------------------------------------------------------------------

    @Test
    void childrenWithOpposingClockOffsetsEmitFiringsOnOneBrokerClock() {
        long offsetMillis = 30_000;
        setupTopicWith("child-pc-in", 2);
        String topic = getTopic();
        ChildPcProcess ahead = launch(tagged("ahead", topic).toBuilder().clockOffsetMillis(offsetMillis).build());
        ChildPcProcess behind = launch(tagged("behind", topic).toBuilder().clockOffsetMillis(-offsetMillis).build());
        ahead.awaitStarted(START_BUDGET);
        behind.awaitStarted(START_BUDGET);
        awaitGroupStable(2, GROUP_BUDGET);
        Instant anchor = ledger.anchorNow();
        produce(topic, BACKLOG);

        Instant aheadFirst = ledger.awaitFiringAtOrAfter("ahead", anchor, ofSeconds(60));
        Instant behindFirst = ledger.awaitFiringAtOrAfter("behind", anchor, ofSeconds(60));
        Instant windowEnd = (aheadFirst.isAfter(behindFirst) ? aheadFirst : behindFirst).plusSeconds(4);
        Instant brokerNow = ledger.awaitBrokerTimePast(windowEnd, ofSeconds(60));

        // the children keep firing while this reads, so a firing filed a millisecond after the fence is
        // legitimately past brokerNow; the claim is that NO firing sits anywhere near +-30s of it, which a child
        // clock on the record would put there - a few seconds of live slack keeps the check discriminating
        Duration liveSlack = ofSeconds(5);
        for (String instance : UniLists.of("ahead", "behind")) {
            assertThat(ledger.brokerTimesOf(instance))
                    .as("%s: every broker timestamp lies within the parent's observed broker-time span (offset "
                            + "%sms would put it far outside)", instance, offsetMillis)
                    .isNotEmpty()
                    .allSatisfy(firing -> assertThat(firing).isBetween(anchor, brokerNow.plus(liveSlack)));
        }
        double aheadSkew = medianChildMinusBrokerMillis("ahead");
        double behindSkew = medianChildMinusBrokerMillis("behind");
        log.info("SKEW: median(child clock - broker time) ahead={}ms behind={}ms (injected +{}/-{} ms)",
                aheadSkew, behindSkew, offsetMillis, offsetMillis);
        assertThat(aheadSkew).as("the +offset really reached the ahead child's module clock")
                .isBetween(offsetMillis - 5_000.0, offsetMillis + 5_000.0);
        assertThat(behindSkew).as("the -offset really reached the behind child's module clock")
                .isBetween(-offsetMillis - 5_000.0, -offsetMillis + 5_000.0);
    }

    private double medianChildMinusBrokerMillis(String instance) {
        List<Long> deltas = ledger.firingsOf(instance).stream()
                .map(firing -> firing.getChildClock().toEpochMilli() - firing.getBrokerTime().toEpochMilli())
                .sorted().collect(Collectors.toList());
        return deltas.get(deltas.size() / 2);
    }

    // ------------------------------------------------------------------
    // Scenario 6: the stability wait accepts a three-to-one split and rejects a group at the wrong size
    // ------------------------------------------------------------------

    @Test
    void stabilityWaitAcceptsAThreeToOneSplitAndRejectsAGroupThatIsNotAtTheExpectedSize() {
        // the range assignor divides PER TOPIC: topic A (3 partitions) splits 2:1, topic B (1 partition) 1:0 - a
        // 3:1 total, which the one-partition-each wait can never accept and this one must
        String topicA = setupTopicWith("child-pc-in-a", 3);
        String topicB = setupTopicWith("child-pc-in-b", 1);
        ChildPcProcess first = launch(tagged("first", topicA, topicB));
        ChildPcProcess second = launch(tagged("second", topicA, topicB));
        first.awaitStarted(START_BUDGET);
        second.awaitStarted(START_BUDGET);

        ConsumerGroupDescription stable = awaitGroupStable(2, GROUP_BUDGET);
        List<Integer> split = stable.members().stream().map(m -> m.assignment().topicPartitions().size())
                .sorted().collect(Collectors.toList());
        log.info("STABILITY WAIT: accepted {}", describeMembers(stable));
        assertThat(split).as("a three-to-one split").containsExactly(1, 3);

        Duration shortBudget = ofSeconds(5);
        assertThatThrownBy(() -> awaitGroupStable(3, shortBudget))
                .as("a group at the wrong size is rejected, naming what was observed")
                .isInstanceOf(ConditionTimeoutException.class)
                .hasMessageContaining("was not stable with 3 members")
                .hasMessageContaining("2 members");
    }

    // ------------------------------------------------------------------
    // Scenario 7: the inter-rung barrier opens the next window only after the previous rung's tail
    // ------------------------------------------------------------------

    @Test
    void rungBarrierWaitsForTheDepartedMembersTailBeforeTheNextWindowOpens() {
        setupTopicWith("child-pc-in", 2);
        String topic = getTopic();
        ChildPcProcess stayer = launch(tagged("stayer", topic));
        ChildPcProcess leaver = launch(tagged("leaver", topic));
        stayer.awaitStarted(START_BUDGET);
        leaver.awaitStarted(START_BUDGET);
        awaitGroupStable(2, GROUP_BUDGET);
        Instant rung1 = ledger.anchorNow();
        produce(topic, BACKLOG);
        ledger.awaitFiringAtOrAfter("stayer", rung1, ofSeconds(60));
        ledger.awaitFiringAtOrAfter("leaver", rung1, ofSeconds(60));

        leaver.kill();
        Duration settle = ofSeconds(2);
        Instant rung2 = rungBarrier(ledger, 1, UniSets.of("leaver"), settle, ofSeconds(90));

        Instant leaverTail = ledger.latestFiringOf("leaver").orElseThrow(IllegalStateException::new);
        log.info("RUNG BARRIER: leaver's last firing at broker time {}, next window opens at {} ({} later)",
                leaverTail, rung2, Duration.between(leaverTail, rung2));
        assertThat(rung2).as("the next window opens at least the settle period after the leaver's last firing")
                .isAfterOrEqualTo(leaverTail.plus(settle));
        assertThat(ledger.countIn("leaver", rung2, ledger.brokerNow()))
                .as("nothing of the leaver's lands in the next window").isZero();
        Instant stayerAfter = ledger.awaitFiringAtOrAfter("stayer", rung2, ofSeconds(60));
        assertThat(stayerAfter).as("the survivor keeps firing into the new window").isAfterOrEqualTo(rung2);
    }

    // ------------------------------------------------------------------
    // Scenario 8: every child's end-of-run record arrives, and the fleet identity aggregates from the broker
    // ------------------------------------------------------------------

    @Test
    void eachChildsLedgerRecordArrivesAndTheFleetIdentityAggregatesOnATwoChildRun() {
        setupTopicWith("child-pc-in", 2);
        String topic = getTopic();
        ChildPcProcess left = launch(tagged("left", topic));
        ChildPcProcess right = launch(tagged("right", topic));
        left.awaitStarted(START_BUDGET);
        right.awaitStarted(START_BUDGET);
        awaitGroupStable(2, GROUP_BUDGET);
        Instant anchor = ledger.anchorNow();
        produce(topic, BACKLOG);
        Instant leftFirst = ledger.awaitFiringAtOrAfter("left", anchor, ofSeconds(60));
        Instant rightFirst = ledger.awaitFiringAtOrAfter("right", anchor, ofSeconds(60));
        Instant windowEnd = (leftFirst.isAfter(rightFirst) ? leftFirst : rightFirst).plusSeconds(6);
        ledger.awaitBrokerTimePast(windowEnd, ofSeconds(60));
        log.info("FLEET: firings in the 6s window - left {} right {} (two members of a 2/s resource: an "
                        + "observation for the lanes, not a gate here)",
                ledger.countIn("left", leftFirst, windowEnd), ledger.countIn("right", rightFirst, windowEnd));

        int leftExit = left.stopGracefully(ofSeconds(60));
        int rightExit = right.stopGracefully(ofSeconds(60));
        assertThat(UniLists.of(leftExit, rightExit)).as("graceful stops exit 0").containsOnly(0);

        FiringLedger.FleetLedger fleet = ledger.awaitLedgerRecords(UniSets.of("left", "right"), ofSeconds(60));
        fleet.assertEachIdentityBalances();
        List<ChildLedgerRecord> records = fleet.forResource(RESOURCE);
        assertThat(records).as("one record per child for the tagged resource").hasSize(2);
        for (ChildLedgerRecord record : records) {
            log.info("FLEET: {}", record.format());
            assertThat(record.getQuantaObserved()).as("%s sampled its share", record.getInstanceId()).isPositive();
            assertThat(record.getFired()).as("%s fired", record.getInstanceId()).isPositive();
        }
        FleetIdentity identity = fleetIdentity(fleet);
        log.info("FLEET IDENTITY: minted {} + overdraft {} against summed shares {} across {} children",
                identity.getMinted(), identity.getOverdraft(), identity.getSharesSummed(),
                identity.getTaggedChildren());
        assertThat(identity.getMinted()).as("the fleet's minted credits never exceed its summed shares plus the "
                        + "envelope's conservation slack")
                .isLessThanOrEqualTo(identity.getCeiling());
    }

    // ------------------------------------------------------------------
    // Construction
    // ------------------------------------------------------------------

    private ChildPcOptions tagged(String instanceId, String... inputTopics) {
        return ChildPcOptions.builder()
                .bootstrapServers(kafkaContainer.getBootstrapServers())
                .groupId(groupId)
                .instanceId(instanceId)
                .inputTopics(Arrays.asList(inputTopics))
                .outputTopic(outputTopic)
                .ledgerTopic(ledgerTopic)
                .resourceTag(RESOURCE)
                .contract(TWO_PER_SECOND)
                .build();
    }

    private ChildPcProcess launch(ChildPcOptions options) {
        ChildPcProcess child = ChildPcProcess.launch(options);
        children.add(child);
        return child;
    }

    private String setupTopicWith(String name, int partitions) {
        super.numPartitions = partitions;
        return setupTopic(name);
    }

    @SneakyThrows
    private void produce(String topic, int records) {
        getKcu().produceMessages(topic, records);
    }


    private static Set<String> clientIds(ConsumerGroupDescription description) {
        return description.members().stream().map(MemberDescription::clientId)
                .collect(Collectors.toCollection(TreeSet::new));
    }
}
