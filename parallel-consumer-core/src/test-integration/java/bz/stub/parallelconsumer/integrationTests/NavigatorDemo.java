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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicReference;

import static bz.stub.parallelconsumer.integrationTests.utils.NavigatorProofEnvelope.BURST;
import static bz.stub.parallelconsumer.integrationTests.utils.NavigatorProofEnvelope.CONTRACT;
import static bz.stub.parallelconsumer.integrationTests.utils.NavigatorProofEnvelope.CONVERGENCE_DEADLINE;
import static bz.stub.parallelconsumer.integrationTests.utils.NavigatorProofEnvelope.QUANTUM;
import static bz.stub.parallelconsumer.integrationTests.utils.NavigatorProofEnvelope.RATE_PER_SECOND;
import static bz.stub.parallelconsumer.integrationTests.utils.NavigatorProofEnvelope.RESOURCE;
import static bz.stub.parallelconsumer.integrationTests.utils.NavigatorProofEnvelope.randomSuffix;
import static bz.stub.parallelconsumer.integrationTests.utils.NavigatorProofEnvelope.SESSION_TIMEOUT;
import static bz.stub.parallelconsumer.integrationTests.utils.NavigatorProofEnvelope.fleetIdentity;
import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;

/**
 * The navigator's human-watchable demonstration: global rate limiting, inside the consumer, <b>across separate
 * JVMs</b>, that explains itself. Three child processes share one consumer group. Two of them tag the shared
 * resource {@value bz.stub.parallelconsumer.integrationTests.utils.NavigatorProofEnvelope#RESOURCE} and hold one
 * partition each of a two-partition topic; the third declares nothing and drains its own topic. The parent
 * launches nothing but processes, counts every firing on the BROKER's clock through the {@link FiringLedger},
 * and prints a clean per-second dashboard - one row per second, no log prefixes - so a person can watch:
 * <ul>
 *   <li>the two tagged JVMs each fire at ~1Hz with a reported share of 0.500 (the 2/s policy split two ways),</li>
 *   <li>the untagged bystander drain its backlog flat-out, untouched,</li>
 *   <li>one tagged JVM KILLED outright ({@code destroyForcibly}) - and the fleet keep running at half rate for
 *       the session timeout, because that is how long the group takes to notice a process that died,</li>
 *   <li>the survivor pick up the whole 2Hz at a reported share of 1.000 once the group has rebalanced,</li>
 *   <li>and the books balance - every child's conservation ledger, collected from the broker, printed at the end.</li>
 * </ul>
 * Off by default, same discipline as the classic {@code Demo} and {@link AdaptiveConcurrencyDemo}: this is a
 * measurement with no assertions and must not run on every build. Run it with:
 * <pre>bin/demo-navigator.sh</pre>
 * or directly:
 * <pre>./mvnw -q verify -pl parallel-consumer-core -am -Dpc.demo=true -Dit.test=NavigatorDemo \
 *     -Dtest=skipall -DfailIfNoTests=false -Dsurefire.failIfNoSpecifiedTests=false \
 *     -Dfailsafe.failIfNoSpecifiedTests=false</pre>
 * <b>Its asserted twin is now {@link NavigatorPartitionShareIT}</b> (its AE1 and AE2 scenarios), which CI runs
 * on every PR; this class is the same storyline optimised for eyes instead of gates. Until the partition-share
 * rung the demo ran three PC instances inside one JVM against a shared in-process allocator, and its twin was
 * {@link NavigatorRateShareTest} - that lane is unchanged and still asserts the in-process storyline; what moved
 * here is the demo, onto the multi-process harness, so the rate is shown being shared the way an operator will
 * actually deploy it. The demo conventions this class follows (the asserted-twin rule, the wrapper, the output
 * discipline) are owned by {@code docs/demos.md}.
 * <p>
 * <b>Two things the storyline inherits from the harness.</b> A killed member is only rebalanced away after
 * {@code session.timeout.ms}, which the children run at the broker's floor (KTD10), so the second phase is
 * longer than the in-process demo's: about six seconds of the group not yet knowing, then the survivor at full
 * rate. And every count on a row is over the BROKER's timestamps, so the dashboard renders
 * {@link #RENDER_LAG} behind live - long enough for a firing to be appended and tailed before its row is
 * printed, short enough that the run still reads as it happens.
 *
 * @author Antony Stubbs
 * @see NavigatorPartitionShareIT
 * @see ChildPcProcess
 */
public class NavigatorDemo extends BrokerIntegrationTest<String, String> {

    /** The tagged topic's partitions: one per tagged child, so the split is an even half each. */
    private static final int TAGGED_PARTITIONS = 2;

    /** Per tagged partition: far more than a half share can spend over the storyline. */
    private static final int TAGGED_BACKLOG_PER_PARTITION = 400;

    /** The bystander's backlog: big enough that it is still visibly eating while the tagged pair crawls. */
    private static final int BYSTANDER_BACKLOG = 4_000;

    /** Both tagged children alive, sharing the rate. */
    private static final int PHASE_1_SECONDS = 12;

    /** One tagged child killed: the group's blind spot, the rebalance, then the survivor at the full rate. */
    private static final int PHASE_2_SECONDS = 18;

    private static final int TOTAL_SECONDS = PHASE_1_SECONDS + PHASE_2_SECONDS;

    /**
     * How far behind the broker's clock each row is rendered. A firing is a record on a log-append-time topic:
     * it has to be appended and read by the ledger's tailer before the second containing it can be counted.
     */
    private static final Duration RENDER_LAG = ofMillis(1_200);

    /** The first seconds of a phase are the group settling, not the steady state the summary reports. */
    private static final int SETTLING_SECONDS = 2;

    private static final int BAR_CAP = 40;

    /** What a child reports for a field it has nothing to say about - an untagged child's share, for instance. */
    private static final String ABSENT = "-";

    /** The share an instance reports while it holds no partition at all, as {@code ChildPcMain} formats it. */
    private static final String ZERO_SHARE = "0.000";

    private static final Duration START_BUDGET = ofSeconds(60);
    private static final Duration GROUP_BUDGET = ofSeconds(60);
    private static final Duration FIRING_BUDGET = ofSeconds(60);
    private static final Duration FENCE_BUDGET = ofSeconds(120);
    private static final Duration STOP_BUDGET = ofSeconds(60);
    private static final Duration LEDGER_BUDGET = ofSeconds(60);

    static {
        quietLogsForTheAudience();
    }

    private String groupId;
    private String outputTopic;
    private String ledgerTopic;
    private FiringLedger ledger;
    private final List<ChildPcProcess> children = new ArrayList<>();
    /** Narrative events waiting for the row whose second contains them; written by the group watcher too. */
    private final ConcurrentLinkedQueue<DashboardEvent> timeline = new ConcurrentLinkedQueue<>();
    /** Per child, every distinct share value that actually reached a dashboard row - what the audience saw. */
    private final Map<String, Set<String>> sharesShown = new HashMap<>();
    /** Per child, the broker instant it was killed at, so its column blanks from that second and stays blank. */
    private final Map<String, Instant> killedAtBroker = new HashMap<>();

    /** The broker instant row 0 ends at, and the offset between the broker's clock and this JVM's. */
    private Instant brokerAnchor;
    private long brokerAheadOfWallMillis;

    @AfterEach
    void teardown() {
        for (ChildPcProcess child : children) {
            child.close();
        }
        if (ledger != null) {
            ledger.close();
        }
    }

    @SneakyThrows
    @Test
    @EnabledIfSystemProperty(named = "pc.demo", matches = "true")
    void watchTwoJvmsShareOneRateLimitAndOneOfThemDie() {
        groupId = "nav-demo-" + randomSuffix();
        getKcu().setGroupId(groupId); // the group waits below describe the CHILDREN's group
        outputTopic = "nav-demo-out-" + randomSuffix();
        ledgerTopic = "nav-demo-ledger-" + randomSuffix();
        getKcu().createLogAppendTimeTopic(outputTopic);
        getKcu().createLogAppendTimeTopic(ledgerTopic);
        String taggedTopic = setupTopicWith("nav-demo-tagged", TAGGED_PARTITIONS);
        String bystanderTopic = setupTopicWith("nav-demo-bystander", 1);
        ledger = new FiringLedger(kafkaContainer.getBootstrapServers(), outputTopic, ledgerTopic);

        banner();

        System.out.println("  starting three JVMs and waiting for the consumer group to settle...");
        ChildPcProcess tagged1 = launch(tagged("tagged-1", taggedTopic));
        ChildPcProcess tagged2 = launch(tagged("tagged-2", taggedTopic));
        ChildPcProcess bystander = launch(untagged("bystander", bystanderTopic));
        for (ChildPcProcess child : children) {
            child.awaitStarted(START_BUDGET);
        }
        awaitGroupStable(3, GROUP_BUDGET);

        System.out.printf("  producing the backlogs (%,d records for the tagged pair, %,d for the bystander)...%n%n",
                TAGGED_BACKLOG_PER_PARTITION * TAGGED_PARTITIONS, BYSTANDER_BACKLOG);
        getKcu().produceMessages(taggedTopic, (long) TAGGED_BACKLOG_PER_PARTITION * TAGGED_PARTITIONS);
        getKcu().produceMessages(bystanderTopic, BYSTANDER_BACKLOG);

        Instant firstFiring = ledger.awaitFiringAtOrAfter("tagged-1", ledger.anchorNow(), FIRING_BUDGET);
        anchorOn(firstFiring);

        System.out.printf("%9s  %-19s %-19s %-19s%n", "", "tagged-1", "tagged-2", "bystander (no tags)");
        tick(1, PHASE_1_SECONDS, tagged1, tagged2, bystander);

        System.out.println();
        System.out.println("  >>> KILLING the tagged-2 JVM outright - SIGKILL, no close, no goodbye to the group.");
        System.out.println("  >>> Nobody can be told. The group only learns of it when the heartbeats stop arriving:");
        System.out.printf("  >>> session.timeout.ms is %ds at the broker's floor, counted from tagged-2's LAST%n",
                SESSION_TIMEOUT.getSeconds());
        System.out.println("  >>> heartbeat - so the fleet runs at HALF the rate for a few seconds first.");
        System.out.println();
        String tagged1ShareAtKill = latestDashboardField(tagged1, "share=");
        String tagged2ShareAtKill = latestDashboardField(tagged2, "share=");
        Instant killAt = brokerOf(Instant.now());
        Duration killTook = tagged2.kill();
        killedAtBroker.put(tagged2.getOptions().getInstanceId(), killAt);
        announce(killAt, "tagged-2 was KILLED in this second");

        AtomicReference<Duration> memberGone = new AtomicReference<>();
        AtomicReference<Duration> groupStable = new AtomicReference<>();
        AtomicReference<Instant> stableAtBroker = new AtomicReference<>();
        Thread watcher = watchForTheRebalance(memberGone, groupStable, stableAtBroker);

        tick(PHASE_1_SECONDS + 1, TOTAL_SECONDS, tagged1, tagged2, bystander);
        watcher.join(GROUP_BUDGET.toMillis());

        summary(tagged1, tagged2, bystander, killAt, killTook, memberGone.get(), groupStable.get(),
                stableAtBroker.get(), tagged1ShareAtKill, tagged2ShareAtKill);
    }

    /**
     * The dashboard is the show, so the INFO sources that survive the harness's warn default are lowered for
     * this forked demo JVM. The two named loggers carry EXPLICIT info pins in {@code logback-test.xml}
     * (the harness's own diagnostics), so the exact pinned loggers must be re-levelled - a parent-level set
     * does not override an explicit child pin. Runs from the static initialiser because the base class's
     * {@code @BeforeEach} already opens Kafka clients before any test method. The second of the two also covers
     * the multi-process harness ({@code ChildPcProcess}'s launch lines, the {@code FiringLedger}'s barrier
     * lines), which live in a sub-package of it. Demo-only - the asserted lane
     * ({@link NavigatorPartitionShareIT}) leaves levels alone.
     * <p>
     * The children's own logging needs nothing done to it: {@link ChildPcProcess} pipes both child streams and
     * its pumps only CAPTURE them, so a child's log lines reach the parent's memory and never its stdout.
     */
    private static void quietLogsForTheAudience() {
        ((ch.qos.logback.classic.Logger) org.slf4j.LoggerFactory
                .getLogger("org.apache.kafka.clients.consumer.internals.SubscriptionState"))
                .setLevel(ch.qos.logback.classic.Level.WARN);
        ((ch.qos.logback.classic.Logger) org.slf4j.LoggerFactory
                .getLogger("bz.stub.parallelconsumer.integrationTests"))
                .setLevel(ch.qos.logback.classic.Level.WARN);
    }

    // ------------------------------------------------------------------
    // The dashboard
    // ------------------------------------------------------------------

    private static void banner() {
        System.out.println();
        System.out.println("================================================================================");
        System.out.println("  PARALLEL CONSUMER NAVIGATOR - one rate limit, shared across JVMs");
        System.out.println("================================================================================");
        System.out.printf("  One shared resource: '%s' at %.0f credits/second (burst %d), declared identically%n",
                RESOURCE, RATE_PER_SECOND, BURST);
        System.out.println("  by each process. There is NO coordinator: each instance's share is simply the");
        System.out.println("  fraction of its subscription's partitions that the consumer group gave it.");
        System.out.println();
        System.out.println("  Three SEPARATE JVMs, one consumer group, real Kafka backlogs:");
        System.out.printf("    tagged-1, tagged-2  declare they use '%s'  -> one partition of two each,%n",
                RESOURCE);
        System.out.println("                                                   so share 0.500 -> ~1 rec/s each");
        System.out.println("    bystander           declares nothing        -> completely unthrottled");
        System.out.println();
        System.out.println("  Each # is one record processed in that second, counted on the BROKER's clock.");
        System.out.println("  's=' is the share each child reports for itself, from its own navigator view.");
        System.out.println("================================================================================");
        System.out.println();
    }

    /**
     * Prints one dashboard row per second of broker time from {@code fromSecond} to {@code toSecond} inclusive,
     * each rendered {@link #RENDER_LAG} after the second it covers has closed on the broker's clock.
     */
    @SneakyThrows
    private void tick(int fromSecond, int toSecond, ChildPcProcess tagged1, ChildPcProcess tagged2,
                      ChildPcProcess bystander) {
        for (int second = fromSecond; second <= toSecond; second++) {
            Instant rowEnd = brokerAnchor.plusSeconds(second);
            sleepUntilWall(wallOf(rowEnd).plus(RENDER_LAG));
            Instant rowStart = rowEnd.minusSeconds(1);

            System.out.printf("  t=%4ds  %-19s %-19s %-19s%s%n",
                    second,
                    taggedCell(tagged1, rowStart, rowEnd),
                    taggedCell(tagged2, rowStart, rowEnd),
                    bystanderCell(bystander, rowStart, rowEnd),
                    noteFor(rowEnd));
        }
    }

    /**
     * The narrative events - the kill, the group noticing, the rebalance - filed against the BROKER instant they
     * happened at and rendered on the row whose second contains that instant, so a stranger reading the
     * dashboard sees each one against the counts it explains. An event the watcher thread discovers late (the
     * group's own state is only observable by polling it) is still shown, on the first row printed after the
     * discovery.
     */
    private String noteFor(Instant rowEnd) {
        DashboardEvent next = timeline.peek();
        if (next == null || !next.at.isBefore(rowEnd)) {
            return "";
        }
        timeline.poll();
        return "   <<< " + next.text;
    }

    /** Files a narrative event at a broker instant, from either the story thread or the group watcher. */
    private void announce(Instant brokerAt, String text) {
        timeline.add(new DashboardEvent(brokerAt, text));
    }

    private static final class DashboardEvent {
        private final Instant at;
        private final String text;

        DashboardEvent(Instant at, String text) {
            this.at = at;
            this.text = text;
        }
    }

    /** A tagged child's cell: its firings that second, and the share it reported for itself. */
    private String taggedCell(ChildPcProcess child, Instant rowStart, Instant rowEnd) {
        String instanceId = child.getOptions().getInstanceId();
        // Keyed off WHEN the child died, not whether it is alive as this row renders: a row is rendered
        // RENDER_LAG after the second it covers, so `isAlive` would blank a second the child was still
        // working through, and the column would read killed / firing / killed.
        Instant killedAt = killedAtBroker.get(instanceId);
        if (killedAt != null && !rowStart.isBefore(killedAt)) {
            return "(killed)";
        }
        long count = ledger.countIn(instanceId, rowStart, rowEnd);
        String share = latestDashboardField(child, "share=");
        // what the AUDIENCE saw, not what the child ever printed: the summary may only explain a value that
        // reached a row. A child reports a share several times a second and a row samples one of them.
        sharesShown.computeIfAbsent(instanceId, ignored -> new LinkedHashSet<>()).add(share);
        return String.format(Locale.ROOT, "%-4s %3d  s=%-5s", bar(count), count, share);
    }

    /** The untouched arm's cell: no share to report, and it stops once its backlog is gone. */
    private String bystanderCell(ChildPcProcess child, Instant rowStart, Instant rowEnd) {
        String instanceId = child.getOptions().getInstanceId();
        long count = ledger.countIn(instanceId, rowStart, rowEnd);
        if (count == 0 && ledger.firingsOf(instanceId).size() >= BYSTANDER_BACKLOG) {
            return "(backlog drained)";
        }
        return String.format(Locale.ROOT, "%-4s %3d", bar(count), count);
    }

    private static String bar(long count) {
        int width = (int) Math.min(count, BAR_CAP);
        StringBuilder rendered = new StringBuilder();
        for (int i = 0; i < width; i++) {
            rendered.append('#');
        }
        if (count > BAR_CAP) {
            rendered.append('+');
        }
        return rendered.toString();
    }

    // ------------------------------------------------------------------
    // The summary
    // ------------------------------------------------------------------

    private void summary(ChildPcProcess tagged1, ChildPcProcess tagged2, ChildPcProcess bystander,
                         Instant killAt, Duration killTook, Duration memberGone, Duration groupStable,
                         Instant stableAtBroker, String tagged1ShareAtKill, String tagged2ShareAtKill) {
        Instant phase1Start = brokerAnchor.plusSeconds(SETTLING_SECONDS);
        Instant phase1End = brokerAnchor.plusSeconds(PHASE_1_SECONDS);
        double phase1Seconds = PHASE_1_SECONDS - SETTLING_SECONDS;
        // The survivor's converged stretch starts one quantum after the group was OBSERVED stable - a moved
        // share is first minted at the quantum boundary after its assignment. With no observation (a rebalance
        // still in flight), fall back to the envelope's pre-registered convergence deadline.
        Instant convergedStart = stableAtBroker == null
                ? killAt.plus(CONVERGENCE_DEADLINE)
                : stableAtBroker.plus(QUANTUM);
        Instant convergedEnd = brokerAnchor.plusSeconds(TOTAL_SECONDS);
        ledger.awaitBrokerTimePast(convergedEnd, FENCE_BUDGET);
        double convergedSeconds = Duration.between(convergedStart, convergedEnd).toMillis() / 1000.0;

        System.out.println();
        System.out.println("================================================================================");
        System.out.println("  WHAT JUST HAPPENED");
        System.out.println("================================================================================");
        System.out.printf("  Phase 1 (two tagged JVMs, one partition each): tagged-1 %.1f rec/s, tagged-2 %.1f%n",
                ledger.countIn("tagged-1", phase1Start, phase1End) / phase1Seconds,
                ledger.countIn("tagged-2", phase1Start, phase1End) / phase1Seconds);
        System.out.printf("    rec/s - the %.0f/s policy split two ways, by two processes that never spoke to%n",
                RATE_PER_SECOND);
        System.out.printf("    each other. Each read its own share off its own navigator view: tagged-1 %s,%n",
                tagged1ShareAtKill);
        System.out.printf("    tagged-2 %s - the fraction of the subscription's partitions each was given.%n",
                tagged2ShareAtKill);
        System.out.println();
        System.out.printf("  The kill: SIGKILL reaped in %s. The group reported the member gone %s after the%n",
                millis(killTook), millis(memberGone));
        System.out.printf("    kill and was stable again %s after it. session.timeout.ms is %ds, counted from%n",
                millis(groupStable), SESSION_TIMEOUT.getSeconds());
        System.out.println("    tagged-2's LAST heartbeat rather than from its death, which is why the group");
        System.out.println("    noticed a little sooner than the timeout itself.");
        System.out.println();
        if (convergedSeconds >= 1) {
            System.out.printf("  Phase 2 (survivor holds both partitions): tagged-1 %.1f rec/s over the %.0fs%n",
                    ledger.countIn("tagged-1", convergedStart, convergedEnd) / convergedSeconds, convergedSeconds);
            System.out.println("    after the rebalance - the survivor inherited the WHOLE rate, and now reports");
            System.out.printf("    share %s, worth %s credits per %ds quantum.%n",
                    latestDashboardField(tagged1, "share="), latestDashboardField(tagged1, "credits="),
                    QUANTUM.getSeconds());
        } else {
            System.out.println("  Phase 2: the rebalance landed too late in the storyline to measure a converged");
            System.out.println("    rate; the dashboard rows above are what happened.");
        }
        System.out.printf("  Bystander: %,d records processed, never throttled - no tags, zero cost.%n",
                ledger.firingsOf("bystander").size());
        if (sharesShown.getOrDefault("tagged-1", Collections.emptySet()).contains(ZERO_SHARE)) {
            System.out.println();
            System.out.printf("  Mid-rebalance you saw tagged-1 report s=%s for a second or two. That is the%n",
                    ZERO_SHARE);
            System.out.println("    eager assignor doing its job: it revokes every partition before handing the");
            System.out.println("    new assignment out, and an instance holding no partition is entitled to nothing.");
            System.out.println("    Nothing is lost - it simply is not that instance's turn to spend.");
        }
        System.out.println();
        printTheBooks(tagged1, bystander, tagged2.getOptions().getInstanceId());
        System.out.println("  No instance ever blocked a thread waiting - ineligible records simply were not");
        System.out.println("  selected until their credit arrived (soft, cooperative, bounded overshoot).");
        System.out.println("================================================================================");
        System.out.println();
    }

    /**
     * Stops the surviving children and prints the fleet's conservation books, read back from the broker - the
     * same {@code FiringLedger#stopAndCollect} step every navigator lane closes with, and the same fleet
     * identity, except that here the numbers are PRINTED. The lane
     * ({@code NavigatorProofEnvelope#assertFleetIdentity}) is what asserts them; a demo is never the evidence.
     */
    private void printTheBooks(ChildPcProcess tagged1, ChildPcProcess bystander, String killedInstanceId) {
        FiringLedger.FleetLedger fleet = ledger.stopAndCollect(Arrays.asList(tagged1, bystander),
                STOP_BUDGET, LEDGER_BUDGET);
        FleetIdentity identity = fleetIdentity(fleet);

        System.out.println("  THE BOOKS, collected from the broker after each survivor stopped cleanly:");
        for (ChildLedgerRecord record : fleet.getRecords()) {
            System.out.printf("    %-10s minted %3d + overdraft %d = spent %3d + expired %2d + outstanding %d  %s%n",
                    record.getInstanceId(), record.getMinted(), record.getOverdraft(), record.getSpent(),
                    record.getExpired(), record.getOutstanding(),
                    record.identityBalances() ? "(balances)" : "(DOES NOT BALANCE)");
        }
        System.out.printf("    fleet      minted %d + overdraft %d = %d, against a summed entitlement of %.1f%n",
                identity.getMinted(), identity.getOverdraft(), identity.getMinted() + identity.getOverdraft(),
                identity.getSharesSummed());
        System.out.printf("               credits across %d tagged child(ren) - the envelope's ceiling is %d.%n",
                identity.getTaggedChildren(), identity.getCeiling());
        System.out.printf("    %s left no record at all: it was killed, so nothing was flushed. That is the%n",
                killedInstanceId);
        System.out.println("               point - its share died with the process and nobody had to be told.");
        System.out.println();
    }

    // ------------------------------------------------------------------
    // The group's own account of the rebalance
    // ------------------------------------------------------------------

    /**
     * Watches the consumer group through the admin client while the dashboard keeps rendering, so the moment
     * the survivors take over is a row on the dashboard rather than a pause in it. Observed and printed only -
     * the lane is where a kill latency is measured against a deadline.
     */
    private Thread watchForTheRebalance(AtomicReference<Duration> memberGone, AtomicReference<Duration> groupStable,
                                        AtomicReference<Instant> stableAtBroker) {
        Instant killedAtWall = Instant.now();
        Thread watcher = new Thread(() -> {
            memberGone.set(awaitGroupMemberCount(2, GROUP_BUDGET));
            announce(brokerOf(Instant.now()), "the missed heartbeats added up - the group dropped tagged-2");
            awaitGroupStable(2, GROUP_BUDGET);
            Instant stableAtWall = Instant.now();
            stableAtBroker.set(brokerOf(stableAtWall));
            groupStable.set(Duration.between(killedAtWall, stableAtWall));
            announce(stableAtBroker.get(), "rebalanced - tagged-1 now holds BOTH partitions");
        }, "navigator-demo-group-watcher");
        watcher.setDaemon(true);
        watcher.start();
        return watcher;
    }

    // ------------------------------------------------------------------
    // The broker's clock, rendered on the wall
    // ------------------------------------------------------------------

    /**
     * Fixes row 0 at {@code anchor} on the broker's clock and measures how far ahead of this JVM's clock the
     * broker is, so a row covering a broker second can be printed at the right wall moment. Round-trip is
     * halved out the way a clock probe should be: the marker's timestamp is compared against the midpoint of
     * the wall readings that bracket the call.
     */
    private void anchorOn(Instant anchor) {
        long before = System.currentTimeMillis();
        Instant probe = ledger.brokerNow();
        long after = System.currentTimeMillis();
        brokerAnchor = anchor;
        brokerAheadOfWallMillis = probe.toEpochMilli() - (before + after) / 2;
    }

    private Instant wallOf(Instant brokerInstant) {
        return brokerInstant.minusMillis(brokerAheadOfWallMillis);
    }

    private Instant brokerOf(Instant wallInstant) {
        return wallInstant.plusMillis(brokerAheadOfWallMillis);
    }

    @SneakyThrows
    private static void sleepUntilWall(Instant wallInstant) {
        long millis = Duration.between(Instant.now(), wallInstant).toMillis();
        if (millis > 0) {
            Thread.sleep(millis);
        }
    }

    // ------------------------------------------------------------------
    // The children's own reports (R9): the dashboard share field
    // ------------------------------------------------------------------

    /** The most recent value of a {@code CHILD-PC DASHBOARD} field the child printed, or {@code "-"}. */
    private static String latestDashboardField(ChildPcProcess child, String key) {
        String latest = ABSENT;
        for (String line : child.stdoutLines()) {
            if (line.startsWith(ChildPcMain.DASHBOARD_PREFIX)) {
                latest = dashboardField(line, key);
            }
        }
        return latest;
    }

    /** One {@code key=value} field of a child's dashboard line, or {@value #ABSENT} if the line has no such key. */
    private static String dashboardField(String line, String key) {
        int at = line.indexOf(' ' + key);
        if (at < 0) {
            return ABSENT;
        }
        int start = at + 1 + key.length();
        int end = line.indexOf(' ', start);
        return end < 0 ? line.substring(start) : line.substring(start, end);
    }

    // ------------------------------------------------------------------
    // Construction (the NavigatorPartitionShareIT pattern, without the measurement scaffolding)
    // ------------------------------------------------------------------

    private ChildPcOptions tagged(String instanceId, String inputTopic) {
        return untagged(instanceId, inputTopic).toBuilder().resourceTag(RESOURCE).contract(CONTRACT).build();
    }

    private ChildPcOptions untagged(String instanceId, String inputTopic) {
        return ChildPcOptions.builder()
                .bootstrapServers(kafkaContainer.getBootstrapServers())
                .groupId(groupId)
                .instanceId(instanceId)
                .inputTopic(inputTopic)
                .outputTopic(outputTopic)
                .ledgerTopic(ledgerTopic)
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

    /** Sub-second durations in milliseconds - a forced kill reaped "in 0.0s" reads as nothing having happened. */
    private static String millis(Duration duration) {
        if (duration == null) {
            return "(not observed inside the storyline)";
        }
        long value = duration.toMillis();
        return value < 1_000 ? value + " ms" : String.format(Locale.ROOT, "%.1fs", value / 1000.0);
    }


}
