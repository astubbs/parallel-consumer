package bz.stub.parallelconsumer.integrationTests;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.integrationTests.utils.ChildPcOptions;
import bz.stub.parallelconsumer.integrationTests.utils.ChildPcOptions.Assignor;
import bz.stub.parallelconsumer.integrationTests.utils.ChildPcProcess;
import bz.stub.parallelconsumer.integrationTests.utils.FiringLedger;
import bz.stub.parallelconsumer.integrationTests.utils.NavigatorLadderRecord;
import bz.stub.parallelconsumer.integrationTests.utils.NavigatorLadderRecord.MoveObservation;
import bz.stub.parallelconsumer.integrationTests.utils.NavigatorLadderRecord.RungObservation;
import bz.stub.parallelconsumer.integrationTests.utils.NavigatorProofEnvelope.FleetIdentity;
import lombok.SneakyThrows;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

import static bz.stub.parallelconsumer.integrationTests.utils.NavigatorProofEnvelope.CONTRACT;
import static bz.stub.parallelconsumer.integrationTests.utils.NavigatorProofEnvelope.CONVERGENCE_DEADLINE;
import static bz.stub.parallelconsumer.integrationTests.utils.NavigatorProofEnvelope.QUANTUM;
import static bz.stub.parallelconsumer.integrationTests.utils.NavigatorProofEnvelope.RATE_PER_SECOND;
import static bz.stub.parallelconsumer.integrationTests.utils.NavigatorProofEnvelope.RESOURCE;
import static bz.stub.parallelconsumer.integrationTests.utils.NavigatorProofEnvelope.randomSuffix;
import static bz.stub.parallelconsumer.integrationTests.utils.NavigatorProofEnvelope.WINDOW;
import static bz.stub.parallelconsumer.integrationTests.utils.NavigatorProofEnvelope.assertFleetIdentity;
import static bz.stub.parallelconsumer.integrationTests.utils.NavigatorProofEnvelope.overshootBound;
import static java.time.Duration.ofSeconds;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * The churn ladder (the partition-share plan's U6, R12, KD4, KD5): N child JVMs join and leave under both
 * assignment protocols, with and without injected clock skew, while the parent measures the fleet's maximum
 * aggregate firing count over ANY quantum-aligned window on the BROKER's clock - and every rung is asserted
 * inside its bound. The ladder IS the test: each rung is one parameterized invocation, in ladder order, and the
 * curve it climbs is written by the run as a dated record ({@link NavigatorLadderRecord}, under
 * {@code target/navigator-ladder/}) whether or not a rung fails, for the workflow to upload beside the failsafe
 * report and a maintainer to commit under {@code docs/test-hardening/} with the run's provenance.
 * <p>
 * <b>A rung.</b> N-1 children start on a fresh group and the inter-rung barrier opens the window once the
 * previous rung's departed tail is quiet on the broker's clock (KTD8). After a dwell the Nth child joins (the
 * first move); the convergence deadline later the FIRST child is killed with {@code destroyForcibly} (the
 * second move, KTD10); the window closes one quantum past the kill's own deadline. Every window of
 * {@code WINDOW_QUANTA} quanta aligned to the broker's quantum boundaries inside that span is counted, and the
 * largest is the rung's observation. The survivors stop gracefully and the fleet identity is asserted over
 * their records (R10, AE7).
 * <p>
 * <b>The bound, pre-registered (KTD11, KTD13).</b> Zero-offset rungs: {@code rate x window + burst + one
 * quantum's credits} - {@code NavigatorProofEnvelope.overshootBound(WINDOW)} - with no re-derivation; a crossing
 * there, or a fleet-conservation failure, is a defect and the rung fails. Offset rungs add the skew term: for
 * each move, the summed per-quantum share of the partitions that changed hands between two holders whose
 * injected offsets differ, rounded up to a whole credit and to at least one, summed over the rung's two moves
 * (AE4). Offsets are opposite fractions of a quantum on the first child (the one later killed) and the joiner,
 * so both moves cross skewed clocks; the other children run on the real clock.
 * <p>
 * <b>Gates are counts; latencies are reported (KTD13).</b> Join-to-stable and kill-to-stable are observed and
 * written into the record, never gated; the undershoot each transition costs over the convergence deadline
 * is reported per rung, which is where the eager protocol's revoke-everything gap and the cooperative
 * protocol's smaller one become visible side by side.
 * <p>
 * <b>Calibration status.</b> Shape and timings calibrated on the implementing machine (an Apple Silicon
 * laptop, one TestContainers broker, up to four child JVMs); the record names the machine each run was taken
 * on, and the hosted runner's first run is where the values are confirmed (KTD14). If a rung is too slow, the
 * lever is the dwell before the join - never the tolerance or the bound.
 */
@Slf4j
@Execution(ExecutionMode.SAME_THREAD)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class NavigatorChurnLadderIT extends BrokerIntegrationTest<String, String> {

    /** A common multiple of every N the ladder climbs, so every rung's shares are exact fractions. */
    private static final int PARTITIONS = 12;

    /** The N of each rung: N-1 children at the open, the Nth joins, the first is killed. */
    private static final List<Integer> MEMBER_LADDER = Arrays.asList(2, 3, 4);

    /** The injected skew, a fraction of a quantum: the first child runs ahead by this, the joiner behind. */
    private static final Duration SKEW_OFFSET = Duration.ofMillis(400);

    /** Quanta of N-1 steady state between the window opening and the join. */
    private static final int PRE_JOIN_DWELL_QUANTA = 6;

    /** How long a departed member's tail must be silent on the broker's clock before the next rung opens. */
    private static final Duration TAIL_SETTLE = QUANTUM.multipliedBy(2);

    /** Per partition, once for the whole ladder: every rung re-reads the topic from the beginning. */
    private static final int BACKLOG_PER_PARTITION = 100;

    private static final Duration START_BUDGET = ofSeconds(60);
    private static final Duration GROUP_BUDGET = ofSeconds(60);
    private static final Duration BARRIER_BUDGET = ofSeconds(90);
    private static final Duration FENCE_BUDGET = WINDOW.plus(CONVERGENCE_DEADLINE).plus(ofSeconds(60));
    private static final Duration STOP_BUDGET = ofSeconds(60);
    private static final Duration LEDGER_BUDGET = ofSeconds(60);

    /** One rung of the ladder. */
    @Value
    static class Rung {
        int members;
        Assignor assignor;
        boolean skewed;

        @Override
        public String toString() {
            return "N=" + members + " " + assignor + (skewed ? " skewed" : " zero-offset");
        }
    }

    // --- ladder state, shared across rungs (PER_CLASS) ---

    /** What the whole ladder shares: opened by the first rung, closed after the last. */
    private static final class Ladder {
        final String inputTopic;
        final String outputTopic;
        final String ledgerTopic;
        final FiringLedger ledger;
        final NavigatorLadderRecord record;

        Ladder(String inputTopic, String outputTopic, String ledgerTopic, FiringLedger ledger,
               NavigatorLadderRecord record) {
            this.inputTopic = inputTopic;
            this.outputTopic = outputTopic;
            this.ledgerTopic = ledgerTopic;
            this.ledger = ledger;
            this.record = record;
        }
    }

    private Optional<Ladder> ladder = Optional.empty();
    private int rungsRun;
    private Set<String> previousDeparted = Collections.emptySet();
    private final List<ChildPcProcess> liveChildren = new ArrayList<>();

    List<Rung> rungs() {
        List<Rung> rungs = new ArrayList<>();
        for (int members : MEMBER_LADDER) {
            for (Assignor assignor : Assignor.values()) {
                rungs.add(new Rung(members, assignor, false));
                rungs.add(new Rung(members, assignor, true));
            }
        }
        return rungs;
    }

    /** Lazily, from the first rung: the broker clients the base class opens are per-method, not per-class. */
    private Ladder ladder() {
        if (ladder.isPresent()) {
            return ladder.get();
        }
        String suffix = Integer.toString(randomSuffix());
        String inputTopic = "nav-ladder-in-" + suffix;
        String outputTopic = "nav-ladder-out-" + suffix;
        String ledgerTopic = "nav-ladder-ledger-" + suffix;
        getKcu().createTopic(inputTopic, PARTITIONS);
        getKcu().createLogAppendTimeTopic(outputTopic);
        getKcu().createLogAppendTimeTopic(ledgerTopic);
        produceBacklog(inputTopic);
        Path basedir = Paths.get(System.getProperty("basedir", System.getProperty("user.dir")));
        Ladder opened = new Ladder(inputTopic, outputTopic, ledgerTopic,
                new FiringLedger(kafkaContainer.getBootstrapServers(), outputTopic, ledgerTopic),
                new NavigatorLadderRecord(basedir, rungs().size()));
        ladder = Optional.of(opened);
        log.info("LADDER open: input topic {} ({} partitions), output {}, ledger {}, record under {}", inputTopic,
                PARTITIONS, outputTopic, ledgerTopic, basedir);
        return opened;
    }

    @AfterAll
    void ladderTeardown() {
        for (ChildPcProcess child : liveChildren) {
            child.close();
        }
        if (ladder.isPresent()) {
            Ladder opened = ladder.get();
            opened.ledger.close();
            Path written = opened.record.write();
            log.info("LADDER record:\n{}", opened.record.render());
            assertThat(written).as("the ladder record is on disk for the workflow to upload").exists();
        }
    }

    // ------------------------------------------------------------------
    // The ladder: one rung per invocation, in ladder order
    // ------------------------------------------------------------------

    @ParameterizedTest(name = "rung {index}: {0}")
    @MethodSource("rungs")
    void everyRungStaysInsideItsBoundAndItsFleetIdentityHolds(Rung rung) {
        Ladder opened = ladder();
        rungsRun++;
        String offsets = rung.isSkewed()
                ? "first +" + SKEW_OFFSET.toMillis() + ", joiner -" + SKEW_OFFSET.toMillis() + ", others 0"
                : "all 0";
        RungObservation observation = new RungObservation(rungsRun, rung.toString(), rung.getMembers(),
                rung.getAssignor().name(), offsets);
        Instant wallStart = Instant.now();
        List<ChildPcProcess> fleet = new ArrayList<>();
        Set<String> rungInstances = new TreeSet<>();
        try {
            runRung(opened, rung, observation, fleet, rungInstances);
        } catch (Throwable t) {
            String verdict = observation.getVerdict();
            observation.verdict((verdict.startsWith("did not") ? "error" : verdict) + " - "
                    + t.getClass().getSimpleName() + ": " + firstLine(t));
            throw t;
        } finally {
            observation.recordWallTime(Duration.between(wallStart, Instant.now()));
            opened.record.add(observation);
            for (ChildPcProcess child : fleet) {
                child.close();
                liveChildren.remove(child);
            }
            previousDeparted = rungInstances;
            log.info("RUNG {} done in {}: {}", rungsRun, observation.getWallTime(), observation.getVerdict());
        }
    }

    private void runRung(Ladder opened, Rung rung, RungObservation observation, List<ChildPcProcess> fleet,
                         Set<String> rungInstances) {
        FiringLedger ledger = opened.ledger;
        int members = rung.getMembers();
        String tag = String.format(Locale.ROOT, "r%02d", rungsRun);
        String groupId = "nav-ladder-" + tag + "-" + randomSuffix();
        getKcu().setGroupId(groupId); // the stability waits and describeGroup() read this rung's group
        Map<String, Long> offsets = new HashMap<>();
        Set<String> departedBefore = previousDeparted;

        // --- N-1 children, then the barrier: the window opens on a stable group after the previous tail ---
        for (int i = 1; i < members; i++) {
            long offset = rung.isSkewed() && i == 1 ? SKEW_OFFSET.toMillis() : 0;
            fleet.add(launch(child(opened, tag + "-c" + i, groupId, rung.getAssignor(), offset), offsets,
                    rungInstances));
        }
        for (ChildPcProcess child : fleet) {
            log.info("rung {}: child {} ready after {}", tag, child.getOptions().getInstanceId(),
                    child.awaitStarted(START_BUDGET));
        }
        Instant open = rungBarrier(ledger, members - 1, departedBefore, TAIL_SETTLE, BARRIER_BUDGET);
        Optional<Instant> previousTail = latestFiringAmong(ledger, departedBefore);
        previousTail.ifPresent(tail -> assertThat(open)
                .as("ladder isolation: the window opens only once the previous rung's tail (%s) has been quiet "
                        + "for %s", tail, TAIL_SETTLE)
                .isAfterOrEqualTo(tail.plus(TAIL_SETTLE)));
        ConsumerGroupDescription atOpen = awaitGroupStable(members - 1, GROUP_BUDGET);
        log.info("rung {} OPEN at broker {} (previous tail {}): {}", tag, open,
                previousTail.map(Instant::toString).orElse("none"), describeMembers(atOpen));

        // --- the join: the Nth child, opposite offset when skewed ---
        Instant joinAt = ledger.awaitBrokerTimePast(open.plus(QUANTUM.multipliedBy(PRE_JOIN_DWELL_QUANTA)),
                FENCE_BUDGET);
        Instant joinWall = Instant.now();
        long joinerOffset = rung.isSkewed() ? -SKEW_OFFSET.toMillis() : 0;
        ChildPcProcess joiner = launch(child(opened, tag + "-c" + members, groupId, rung.getAssignor(),
                joinerOffset), offsets, rungInstances);
        fleet.add(joiner);
        joiner.awaitStarted(START_BUDGET);
        ConsumerGroupDescription afterJoin = awaitGroupStable(members, GROUP_BUDGET);
        Duration joinToStable = Duration.between(joinWall, Instant.now());
        log.info("rung {} JOIN at broker {}: stable {} later - {}", tag, joinAt, joinToStable,
                describeMembers(afterJoin));

        // --- the kill: the first child, destroyForcibly, the convergence deadline after the join ---
        Instant killAt = ledger.awaitBrokerTimePast(joinAt.plus(CONVERGENCE_DEADLINE), FENCE_BUDGET);
        Instant killWall = Instant.now();
        ChildPcProcess victim = fleet.get(0);
        victim.kill();
        Duration memberGone = awaitGroupMemberCount(members - 1, GROUP_BUDGET);
        ConsumerGroupDescription afterKill = awaitGroupStable(members - 1, GROUP_BUDGET);
        Duration killToStable = Duration.between(killWall, Instant.now());
        observation.recordKillMemberGone(memberGone);
        log.info("rung {} KILL of {} at broker {}: member gone {} / stable {} later - {}", tag,
                victim.getOptions().getInstanceId(), killAt, memberGone, killToStable, describeMembers(afterKill));

        // --- close, one quantum past the kill's deadline, and measure ---
        Instant close = ledger.awaitBrokerTimePast(killAt.plus(CONVERGENCE_DEADLINE).plus(QUANTUM), FENCE_BUDGET);
        MoveObservation join = move("join", atOpen, afterJoin, offsets, joinToStable,
                undershootOver(ledger, rungInstances, joinAt));
        MoveObservation kill = move("kill", afterJoin, afterKill, offsets, killToStable,
                undershootOver(ledger, rungInstances, killAt));
        observation.addMove(join);
        observation.addMove(kill);
        WindowScan scan = maxAlignedWindow(ledger, rungInstances, open, close);
        double strict = overshootBound(WINDOW);
        long skewTerm = rung.isSkewed() ? join.getTerm() + kill.getTerm() : 0;
        double bound = strict + skewTerm;
        observation.recordWindow(scan.max, scan.maxStart, scan.windows);
        observation.recordBound(strict, skewTerm);
        boolean inside = scan.max <= bound;
        observation.verdict(inside ? "inside (margin " + (bound - scan.max) + ")"
                : "CROSSED by " + (scan.max - bound) + (rung.isSkewed() ? "" : " - a DEFECT on a zero-offset rung"));
        long departedTail = ledger.countAmong(departedBefore, open, close);
        log.info("rung {} OBSERVATION: max aligned window {} from {} over {} windows in [{}, {}) - strict bound {}, "
                        + "skew term {} (join {} moved / {} skewed / share {} / term {}; kill {} moved / {} skewed / "
                        + "share {} / term {}), bound {} -> {}; join undershoot {} over the deadline, kill undershoot "
                        + "{}; previous rung's departed fired {} inside this window",
                tag, scan.max, scan.maxStart, scan.windows, open, close, strict, skewTerm, join.getPartitionsMoved(),
                join.getSkewedPartitionsMoved(), join.getSkewedShare(), join.getTerm(), kill.getPartitionsMoved(),
                kill.getSkewedPartitionsMoved(), kill.getSkewedShare(), kill.getTerm(), bound,
                observation.getVerdict(), join.getUndershoot(), kill.getUndershoot(), departedTail);

        // --- the fleet identity over the survivors (AE7), then the gates ---
        List<ChildPcProcess> survivors = new ArrayList<>(fleet);
        survivors.remove(victim);
        FiringLedger.FleetLedger fleetLedger = ledger.stopAndCollect(survivors, STOP_BUDGET, LEDGER_BUDGET);
        FleetIdentity identity = assertFleetIdentity(fleetLedger);
        observation.recordFleetIdentity(identity);

        assertThat((double) scan.max)
                .as("R12/R8 rung %s (%s): the largest aggregate count over any quantum-aligned %s-quantum window "
                                + "is inside the bound - strict %s plus skew term %s (offsets %s)", rungsRun, rung,
                        WINDOW.getSeconds(), strict, skewTerm, observation.getOffsetsMillis())
                .isLessThanOrEqualTo(bound);
        assertThat(departedTail)
                .as("ladder isolation: nothing of the previous rung's departed %s lands inside this rung's window",
                        departedBefore)
                .isZero();
    }

    // ------------------------------------------------------------------
    // Measurement on the broker's clock
    // ------------------------------------------------------------------

    private static final class WindowScan {
        long max = -1;
        /** Meaningful only once {@link #windows} is positive, which the scan asserts. */
        Instant maxStart = Instant.EPOCH;
        int windows;
    }

    /**
     * The largest aggregate count over every window of {@code WINDOW} whose start is a quantum boundary of the
     * broker's clock and which lies wholly inside {@code [open, close)}.
     */
    private static WindowScan maxAlignedWindow(FiringLedger ledger, Set<String> instances, Instant open,
                                               Instant close) {
        long quantumMillis = QUANTUM.toMillis();
        long firstStart = Math.floorDiv(open.toEpochMilli() + quantumMillis - 1, quantumMillis) * quantumMillis;
        WindowScan scan = new WindowScan();
        for (long start = firstStart; start + WINDOW.toMillis() <= close.toEpochMilli(); start += quantumMillis) {
            Instant windowStart = Instant.ofEpochMilli(start);
            long count = ledger.countAmong(instances, windowStart, windowStart.plus(WINDOW));
            scan.windows++;
            if (count > scan.max) {
                scan.max = count;
                scan.maxStart = windowStart;
            }
        }
        assertThat(scan.windows).as("the rung's span [%s, %s) holds at least one whole aligned window", open, close)
                .isPositive();
        return scan;
    }

    private static Optional<Instant> latestFiringAmong(FiringLedger ledger, Set<String> instances) {
        Optional<Instant> latest = Optional.empty();
        for (String instance : instances) {
            Optional<Instant> firing = ledger.latestFiringOf(instance);
            if (firing.isPresent() && (!latest.isPresent() || firing.get().isAfter(latest.get()))) {
                latest = firing;
            }
        }
        return latest;
    }

    /** Expected firings over the convergence deadline from {@code anchor}, minus what the fleet fired. */
    private static double undershootOver(FiringLedger ledger, Set<String> instances, Instant anchor) {
        double expected = RATE_PER_SECOND * CONVERGENCE_DEADLINE.toMillis() / 1000.0;
        return expected - ledger.countAmong(instances, anchor, anchor.plus(CONVERGENCE_DEADLINE));
    }

    // ------------------------------------------------------------------
    // Moves and the skew term (KTD11)
    // ------------------------------------------------------------------

    /**
     * What changed hands between two stable descriptions, and the skew term it contributes: the summed
     * per-quantum share of the partitions that moved between holders with DIFFERENT injected offsets, rounded up
     * to a whole credit and to at least one credit; zero when no skewed partition moved.
     */
    private static MoveObservation move(String kind, ConsumerGroupDescription before, ConsumerGroupDescription after,
                                        Map<String, Long> offsets, Duration anchorToStable, double undershoot) {
        Map<TopicPartition, String> ownersBefore = owners(before);
        Map<TopicPartition, String> ownersAfter = owners(after);
        int moved = 0;
        int skewedMoved = 0;
        for (Map.Entry<TopicPartition, String> entry : ownersAfter.entrySet()) {
            String previousOwner = ownersBefore.get(entry.getKey());
            if (previousOwner == null || previousOwner.equals(entry.getValue())) {
                continue;
            }
            moved++;
            if (offsetOf(offsets, previousOwner) != offsetOf(offsets, entry.getValue())) {
                skewedMoved++;
            }
        }
        double grantPerQuantum = RATE_PER_SECOND * QUANTUM.toMillis() / 1000.0;
        double skewedShare = skewedMoved * grantPerQuantum / PARTITIONS;
        long term = skewedMoved == 0 ? 0 : Math.max(1, (long) Math.ceil(skewedShare));
        return new MoveObservation(kind, moved, skewedMoved, skewedShare, term, anchorToStable, undershoot);
    }

    /** The injected offset of a child this rung launched; any other client id in the group is a harness bug. */
    private static long offsetOf(Map<String, Long> offsets, String instanceId) {
        Long offset = offsets.get(instanceId);
        if (offset == null) {
            throw new IllegalStateException("member '" + instanceId + "' is not a child this rung launched: "
                    + offsets.keySet());
        }
        return offset;
    }

    private static Map<TopicPartition, String> owners(ConsumerGroupDescription description) {
        Map<TopicPartition, String> owners = new HashMap<>();
        for (MemberDescription member : description.members()) {
            for (TopicPartition partition : member.assignment().topicPartitions()) {
                owners.put(partition, member.clientId());
            }
        }
        return owners;
    }

    // ------------------------------------------------------------------
    // Construction
    // ------------------------------------------------------------------

    private static ChildPcOptions child(Ladder opened, String instanceId, String groupId, Assignor assignor,
                                        long clockOffsetMillis) {
        return ChildPcOptions.builder()
                .bootstrapServers(kafkaContainer.getBootstrapServers())
                .groupId(groupId)
                .instanceId(instanceId)
                .inputTopic(opened.inputTopic)
                .outputTopic(opened.outputTopic)
                .ledgerTopic(opened.ledgerTopic)
                .resourceTag(RESOURCE)
                .contract(CONTRACT)
                .assignor(assignor)
                .clockOffsetMillis(clockOffsetMillis)
                .build();
    }

    private ChildPcProcess launch(ChildPcOptions options, Map<String, Long> offsets, Set<String> rungInstances) {
        ChildPcProcess child = ChildPcProcess.launch(options);
        liveChildren.add(child);
        offsets.put(options.getInstanceId(), options.getClockOffsetMillis());
        rungInstances.add(options.getInstanceId());
        return child;
    }

    @SneakyThrows
    private void produceBacklog(String inputTopic) {
        getKcu().produceMessages(inputTopic, (long) BACKLOG_PER_PARTITION * PARTITIONS);
    }

    private static String firstLine(Throwable t) {
        String message = String.valueOf(t.getMessage());
        int newline = message.indexOf('\n');
        return newline < 0 ? message : message.substring(0, newline);
    }

}
