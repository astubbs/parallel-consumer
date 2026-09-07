package bz.stub.parallelconsumer.integrationTests.chaostests;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntFunction;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static org.awaitility.Awaitility.await;

/**
 * Non-vacuity regression for {@link ProgressProbe}'s instance-progress detector
 * ({@code INSTANCE_STALL/NO_WORK_COMPLETED}, {@link ProgressProbe#INSTANCE_STALL_BOUND}) - in BOTH
 * directions: a detector that cannot fire is decoration, and one that fires on healthy instances
 * would be disabled within a week. Drives {@link ProgressProbe#sampleInstanceProgress} directly with
 * constructed views and explicit instants - pure replay, no broker, no sampler thread - and is
 * deliberately NOT tagged {@code chaos}, so it gates every default integration build (the
 * {@link ProgressProbeLedgerIT} / {@link KeyOrderLedgerIT} pattern).
 */
class InstanceStallProbeIT {

    private static final Duration BOUND = ProgressProbe.INSTANCE_STALL_BOUND;
    private static final Instant T0 = Instant.parse("2026-01-01T00:00:00Z");

    /** Mutable scripted view - the test flips its fields between samples. */
    private static final class FakeInstance implements InstanceProgressView {
        final int id;
        boolean live = true;
        long queued;
        long outForProcessing;
        long workResultsReturned;
        Object incarnation = new Object();
        /** Unknown by default, so every test written before the busy-worker rule still exercises the old one. */
        int busyWorkers = InstanceStallDetector.BUSY_WORKERS_UNKNOWN;

        FakeInstance(int id) {
            this.id = id;
        }

        @Override
        public int busyWorkers() {
            return busyWorkers;
        }

        @Override
        public int instanceId() {
            return id;
        }

        @Override
        public boolean isLive() {
            return live;
        }

        @Override
        public long queuedInShards() {
            return queued;
        }

        @Override
        public long outForProcessing() {
            return outForProcessing;
        }

        @Override
        public long workResultsReturned() {
            return workResultsReturned;
        }

        @Override
        public Object incarnationMarker() {
            return incarnation;
        }
    }

    /** A probe with only the instance-progress detector armed - the ctor's null kcu is legal because
     * the sampler thread is never started. */
    private static ProgressProbe probeWatching(FakeInstance... instances) {
        List<InstanceProgressView> views = new ArrayList<>(Arrays.asList(instances));
        return ProgressProbe.forSeamTest("test-group", "test-topic")
                .withInstanceProgress(() -> views);
    }

    private static Instant pastBound(Instant from) {
        return from.plus(BOUND).plusSeconds(1);
    }

    @Test
    void firesWhenWorkIsHeldAndNothingCompletesPastTheBound() {
        FakeInstance instance = new FakeInstance(7);
        instance.queued = 132;
        instance.outForProcessing = 10;
        instance.workResultsReturned = 5_000;
        ProgressProbe probe = probeWatching(instance);

        probe.sampleInstanceProgress(T0);
        probe.sampleInstanceProgress(pastBound(T0));

        List<String> violations = probe.getViolations();
        assertWithMessage("held work + frozen completion count past the bound is the detector's whole prey")
                .that(violations).hasSize(1);
        assertThat(violations.get(0)).contains("INSTANCE_STALL/NO_WORK_COMPLETED");
        assertThat(violations.get(0)).contains("instance 7");
        assertThat(violations.get(0)).contains("queued=132");
    }

    @Test
    void silentWhileCompletionsAdvance() {
        FakeInstance instance = new FakeInstance(1);
        instance.queued = 500;
        instance.outForProcessing = 10;
        ProgressProbe probe = probeWatching(instance);

        // far more than one bound's worth of wall clock, but a result returns between samples every
        // time - the slow-but-progressing case CLASS2_STALL false-positives on, and the exact case
        // this detector must stay silent for
        Instant now = T0;
        for (int i = 0; i < 5; i++) {
            probe.sampleInstanceProgress(now);
            instance.workResultsReturned++;
            now = now.plus(Duration.ofSeconds(100));
        }
        probe.sampleInstanceProgress(now);

        assertWithMessage("an instance returning results is progressing, however slowly")
                .that(probe.getViolations()).isEmpty();
    }

    @Test
    void silentWhenNoWorkIsHeld() {
        FakeInstance instance = new FakeInstance(2);
        // nothing queued, nothing out: an idle instance's completion count legitimately never moves
        ProgressProbe probe = probeWatching(instance);

        probe.sampleInstanceProgress(T0);
        probe.sampleInstanceProgress(pastBound(T0));
        probe.sampleInstanceProgress(pastBound(pastBound(T0)));

        assertWithMessage("an idle instance is not a stalled instance")
                .that(probe.getViolations()).isEmpty();
    }

    @Test
    void silentWhenTheInstanceIsStopped() {
        FakeInstance instance = new FakeInstance(3);
        instance.queued = 40; // a stopping PC can still hold state - it must not be reported
        instance.live = false;
        ProgressProbe probe = probeWatching(instance);

        probe.sampleInstanceProgress(T0);
        probe.sampleInstanceProgress(pastBound(T0));

        assertWithMessage("the harness stops and restarts members constantly; a stopped instance is not a stall")
                .that(probe.getViolations()).isEmpty();
    }

    @Test
    void restartGrantsAFreshFullWindow() {
        FakeInstance instance = new FakeInstance(4);
        instance.queued = 40;
        ProgressProbe probe = probeWatching(instance);

        probe.sampleInstanceProgress(T0);
        Instant restartAt = T0.plus(Duration.ofSeconds(100));
        instance.incarnation = new Object(); // conductor restarted it: new PC, same instance id
        probe.sampleInstanceProgress(restartAt);

        // past the bound from T0, but only 51s into the new incarnation's window
        probe.sampleInstanceProgress(pastBound(T0));
        assertWithMessage("a fresh incarnation must not inherit the old PC's silence")
                .that(probe.getViolations()).isEmpty();

        // ...and the new incarnation is still covered: its own full window elapsing fires
        probe.sampleInstanceProgress(pastBound(restartAt));
        assertThat(probe.getViolations()).hasSize(1);
    }

    @Test
    void reArmsAfterFiringInsteadOfFiringEverySample() {
        FakeInstance instance = new FakeInstance(5);
        instance.outForProcessing = 3;
        ProgressProbe probe = probeWatching(instance);

        probe.sampleInstanceProgress(T0);
        Instant firstFire = pastBound(T0);
        probe.sampleInstanceProgress(firstFire);
        probe.sampleInstanceProgress(firstFire.plusSeconds(1)); // 1s later - must NOT double-report
        assertWithMessage("one violation per stalled window, not one per 1s sample")
                .that(probe.getViolations()).hasSize(1);

        // a further full window with still nothing returned is a further violation
        probe.sampleInstanceProgress(pastBound(firstFire));
        assertThat(probe.getViolations()).hasSize(2);
    }

    /** Every instance whose threads the sampler asked for, in call order - the dump COUNT is the
     * property under test, so a ledger rather than a flag. */
    private static final class DumpLedger implements IntFunction<String> {
        final List<Integer> dumped = Collections.synchronizedList(new ArrayList<>());

        @Override
        public String apply(int instanceId) {
            dumped.add(instanceId);
            return "  \"pc-control-PC-" + instanceId + "\" WAITING\n";
        }
    }

    /**
     * A firing takes ONE thread dump, and the default configuration is the case that needs saying so:
     * {@link InstanceStallDetector#INSTANCE_STALL_DUMP_AFTER} defaults to
     * {@link ProgressProbe#INSTANCE_STALL_BOUND} itself, so the first sample past the bound satisfies
     * the early-dump condition and the violation condition on the same {@code stalledMs}. Taking the
     * dump in both branches paid a second {@code ThreadMXBean#getThreadInfo(ids, true, true)} - the
     * expensive lock-info form - to print the same stacks twice, in the sample where the run is
     * already failing, and the unconfigured case is precisely the gating one.
     * <p>
     * Counting through the seam rather than reading the log is the point: the previous test on this
     * detector asserted only {@code violations.hasSize(1)}, which the double dump satisfied happily.
     */
    @Test
    void takesOneThreadDumpPerFiringInTheDefaultConfiguration() {
        FakeInstance instance = new FakeInstance(8);
        instance.outForProcessing = 4;
        DumpLedger dumps = new DumpLedger();
        ProgressProbe probe = probeWatching(instance).withThreadDumpSource(dumps);

        probe.sampleInstanceProgress(T0);
        Instant firstFire = pastBound(T0);
        probe.sampleInstanceProgress(firstFire);

        assertWithMessage("the firing itself, so the dump count below is a count per FIRING")
                .that(probe.getViolations()).hasSize(1);
        assertWithMessage("one dump, of the accused member - at the default both branches trip on this "
                + "one sample, and each dump is a full getThreadInfo with lock info")
                .that(dumps.dumped).containsExactly(8);

        // the re-armed stretch earns its own dump: the duplicate is what goes, not the coverage
        probe.sampleInstanceProgress(pastBound(firstFire));
        assertThat(probe.getViolations()).hasSize(2);
        assertWithMessage("each firing carries its own single dump")
                .that(dumps.dumped).containsExactly(8, 8).inOrder();
    }

    @Test
    void oneStalledInstanceIsNotHiddenByHealthySiblings() {
        // the granularity claim itself: the fleet-wide NO_PROGRESS watermark cannot see one wedged
        // member behind advancing siblings - this detector exists to
        FakeInstance healthy = new FakeInstance(10);
        healthy.queued = 100;
        FakeInstance wedged = new FakeInstance(11);
        wedged.queued = 100;
        ProgressProbe probe = probeWatching(healthy, wedged);

        Instant now = T0;
        for (int i = 0; i < 4; i++) {
            probe.sampleInstanceProgress(now);
            healthy.workResultsReturned++; // only the healthy sibling advances
            now = now.plus(Duration.ofSeconds(60));
        }

        List<String> violations = probe.getViolations();
        assertThat(violations).hasSize(1);
        assertThat(violations.get(0)).contains("instance 11");
    }

    /**
     * The detector's firing says an instance held work and completed nothing; only what happens
     * AFTER says whether it was wedged or merely quiet. The recovery diagnostic could not make that
     * call because it logs FLEET consumed/started only - so every {@code INSTANCE_STALL} sighting in
     * {@code docs/inflight/test-857-churn-storm-async-stalls.md} is an unclassified one. This
     * snapshot is what closes that, and it is asserted here because a diagnostic nobody checks is
     * how the previous round of this investigation produced four unusable cycles.
     */
    @Test
    void theInstanceSnapshotCarriesWhatClassifiesAFiring() {
        FakeInstance stalled = new FakeInstance(0);
        stalled.outForProcessing = 35; // the shape seed 6077035105695 reproduces: queued=0, work out
        stalled.workResultsReturned = 24_834;
        FakeInstance sibling = new FakeInstance(1);
        sibling.queued = 12;
        sibling.outForProcessing = 8;
        sibling.workResultsReturned = 30_112;
        ProgressProbe probe = probeWatching(stalled, sibling);

        String atTheFiring = probe.instanceProgressSnapshot();
        assertWithMessage("the firing's own numbers must be readable per instance, not just fleet-wide")
                .that(atTheFiring).isEqualTo(
                        "0(live q=0 out=35 res=24834) 1(live q=12 out=8 res=30112)");

        // the discriminating observation: this instance's completions moved and its held work drained,
        // so it recovered rather than wedging - the question the fleet-level line cannot answer
        stalled.workResultsReturned = 24_900;
        stalled.outForProcessing = 0;
        String afterRecovery = probe.instanceProgressSnapshot();

        // asserting the two DIFFER is the actual property: a snapshot that did not track the change
        // would be a diagnostic that cannot classify anything, however well-formed each reading looked
        assertWithMessage("the snapshot has to track the instance, not just render it once")
                .that(afterRecovery).isNotEqualTo(atTheFiring);
        assertThat(afterRecovery).contains("0(live q=0 out=0 res=24900)");
    }

    @Test
    void aStoppedInstanceIsMarkedRatherThanOmitted() {
        FakeInstance stopping = new FakeInstance(5);
        stopping.queued = 40;
        stopping.live = false;
        ProgressProbe probe = probeWatching(stopping);

        // the detector skips it, so the snapshot must SAY it was skipped - an omitted member reads as
        // a fleet that never had it, which is the silence-is-not-evidence trap this suite keeps hitting
        assertThat(probe.instanceProgressSnapshot()).isEqualTo("5(down q=40 out=0 res=0)");
    }

    /**
     * The not-wired case - ambient mode, or a scenario predating the per-instance supplier - and it
     * is the one contract {@code ChaosScenarioBase#logDiagnosticProgress} leans on: its
     * {@code !instances.isEmpty()} guard is what keeps the second {@code [diagnose]} line off for
     * those runs. Nothing else pins it, so a snapshot that started rendering a placeholder here would
     * add a per-poll line to every unwired scenario and no test would say so.
     */
    @Test
    void anUnwiredProbeRendersNothingAtAll() {
        ProgressProbe probe = ProgressProbe.forSeamTest("test-group", "test-topic");

        assertWithMessage("no supplier means no per-instance line, not a line saying nothing")
                .that(probe.instanceProgressSnapshot()).isEmpty();
    }

    @Test
    void anUnreadableSnapshotSaysSoRatherThanReadingAsAnEmptyFleet() {
        ProgressProbe probe = ProgressProbe.forSeamTest("test-group", "test-topic")
                .withInstanceProgress(() -> {
                    throw new IllegalStateException("PC mid-construction");
                });

        // it is called from inside the awaitility condition, so it must neither throw the wait off
        // course nor return "" - an empty string is indistinguishable from a fleet with no members
        assertThat(probe.instanceProgressSnapshot()).contains("unreadable");
    }

    /**
     * The dump taken when {@code INSTANCE_STALL/NO_WORK_COMPLETED} fires selects threads by the
     * {@code -PC-<id>} suffix PC puts on every thread it owns, and the match must be exact: instance
     * 1's suffix is a substring of instance 14's, so a {@code contains} match would fold a healthy
     * member's stacks into the accused one's dump and the reader would diagnose the wrong instance.
     * Parked threads stand in for PC's own, since what is under test is the selection, not the naming
     * - {@code CloseInterruptLivelockTest} pins the naming against a real PC.
     */
    @Test
    void threadDumpSelectsTheExactInstanceSuffixOnly() throws InterruptedException {
        CountDownLatch release = new CountDownLatch(1);
        Thread mine = parked("pc-pool-3-thread-2-PC-1", release);
        Thread lookalike = parked("pc-control-PC-14", release);
        try {
            String dump = InstanceStallDetector.instanceThreadDump(1);

            assertWithMessage("the accused instance's own thread, with its state and a frame to read")
                    .that(dump).contains("\"pc-pool-3-thread-2-PC-1\" WAITING");
            assertThat(dump).contains("CountDownLatch");
            assertWithMessage("-PC-1 is a substring of -PC-14; only a suffix match keeps instance 14 out")
                    .that(dump).doesNotContain("PC-14");
        } finally {
            release.countDown();
            mine.join(5_000);
            lookalike.join(5_000);
        }
    }

    /**
     * The 2026-09-07 diagnosis in one test: a member holding work with a frozen count and a worker
     * running user code is working, not stalled - ten busy workers or one, since a single long
     * function freezes the count and PC's backpressure counts records, not workers. It must not fail
     * the run, and it must not be invisible either - past the bound it is reported once.
     */
    @Test
    void aWorkingMemberIsReportedOnceAndNeverAccused() {
        for (int busy : new int[]{10, 1}) {
            FakeInstance instance = new FakeInstance(7);
            instance.queued = 0;
            instance.outForProcessing = 30;
            instance.workResultsReturned = 24_967;
            instance.busyWorkers = busy;
            ProgressProbe probe = probeWatching(instance);

            probe.sampleInstanceProgress(T0);
            probe.sampleInstanceProgress(pastBound(T0));
            probe.sampleInstanceProgress(pastBound(T0).plusSeconds(1));
            probe.sampleInstanceProgress(pastBound(pastBound(T0)));

            assertWithMessage("%s worker(s) in user code is a claim about the user function, not PC", busy)
                    .that(probe.getViolations()).isEmpty();
            List<String> observations = probe.getObservations();
            assertWithMessage("a busy stretch past the bound is reported exactly once (busy=%s)", busy)
                    .that(observations).hasSize(1);
            assertThat(observations.get(0)).contains("INSTANCE_BUSY_IN_USER_CODE: instance 7");
        }
    }

    /**
     * The half that keeps the detector a detector: the clock starts the moment the last worker leaves
     * user code while work is still held and the count still frozen - not before, and not from the
     * start of the busy stretch. A member that worked for a minute and then sits on held work with
     * nobody in user code for the whole bound has results with nobody: PC's stall, and it fires.
     */
    @Test
    void theStallClockStartsWhenTheLastWorkerLeavesUserCodeWithWorkStillHeld() {
        FakeInstance instance = new FakeInstance(7);
        instance.queued = 12;
        instance.outForProcessing = 10;
        instance.workResultsReturned = 500;
        instance.busyWorkers = 3;
        ProgressProbe probe = probeWatching(instance);

        Instant lastBusySample = pastBound(T0);
        probe.sampleInstanceProgress(T0);
        probe.sampleInstanceProgress(lastBusySample);
        assertWithMessage("busy past the bound: reported, not accused")
                .that(probe.getViolations()).isEmpty();

        instance.busyWorkers = 0;
        probe.sampleInstanceProgress(lastBusySample.plusSeconds(1));
        assertWithMessage("the workers just left user code - the busy minute must not count toward the stall")
                .that(probe.getViolations()).isEmpty();

        probe.sampleInstanceProgress(pastBound(lastBusySample));
        assertWithMessage("held work, frozen count, nobody in user code, for the whole bound: that is the stall")
                .that(probe.getViolations()).hasSize(1);
        assertThat(probe.getViolations().get(0)).contains("INSTANCE_STALL/NO_WORK_COMPLETED: instance 7");
    }

    /**
     * The count behind the rule, against a real pool: a worker parked between tasks sits in
     * {@code ThreadPoolExecutor.getTask}, a worker running one does not, and a worker the pool has not
     * created yet needs no accounting. Names follow PC's default-factory shape and the exact
     * {@code -PC-<id>} suffix, so the lookalike instance's worker is not counted.
     */
    @Test
    void busyWorkersAreCountedFromTheWorkersOwnStacks() throws InterruptedException {
        CountDownLatch release = new CountDownLatch(1);
        ThreadPoolExecutor pool = new ThreadPoolExecutor(3, 3, 0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(), pcNamed("pc-pool-77-thread-", "-PC-4242"));
        ThreadPoolExecutor lookalike = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(), pcNamed("pc-pool-78-thread-", "-PC-42421"));
        try {
            Runnable park = () -> {
                try {
                    release.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            };
            pool.submit(park);
            pool.submit(park);
            lookalike.submit(park);
            await().atMost(Duration.ofSeconds(5)).until(() -> pool.getActiveCount() == 2);
            await().atMost(Duration.ofSeconds(5)).until(() -> lookalike.getActiveCount() == 1);

            assertWithMessage("two tasks running, the third worker never created: two busy")
                    .that(InstanceStallDetector.busyWorkersOf(4242)).isEqualTo(2);
            assertWithMessage("-PC-4242 is a substring of -PC-42421; the lookalike's busy worker is its own")
                    .that(InstanceStallDetector.busyWorkersOf(42421)).isEqualTo(1);
            assertWithMessage("an instance with no pool threads at all counts nothing busy")
                    .that(InstanceStallDetector.busyWorkersOf(424_242)).isEqualTo(0);

            release.countDown();
            // awaited, not asserted: a worker's active flag clears a few instructions before it is
            // back inside getTask, and the count reads the frame, not the flag
            await().alias("released: both workers parked between tasks, none busy")
                    .atMost(Duration.ofSeconds(5))
                    .until(() -> InstanceStallDetector.busyWorkersOf(4242) == 0);
        } finally {
            release.countDown();
            pool.shutdownNow();
            lookalike.shutdownNow();
            boolean ignoredPool = pool.awaitTermination(5, TimeUnit.SECONDS); // best-effort cleanup; nothing to assert
            boolean ignoredLookalike = lookalike.awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    private static ThreadFactory pcNamed(String prefix, String suffix) {
        AtomicInteger n = new AtomicInteger();
        return r -> {
            Thread t = new Thread(r, prefix + n.incrementAndGet() + suffix);
            t.setDaemon(true);
            return t;
        };
    }

    /**
     * No matching threads is a finding, not an empty dump: it means the instance's threads are gone
     * or the naming contract moved, and an empty string would read as "nothing was running".
     */
    @Test
    void threadDumpSaysSoWhenTheInstanceHasNoThreads() {
        assertThat(InstanceStallDetector.instanceThreadDump(999_999)).contains("no threads named *-PC-999999");
    }

    private static Thread parked(String name, CountDownLatch until) {
        Thread thread = new Thread(() -> {
            try {
                until.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, name);
        thread.setDaemon(true);
        thread.start();
        // WAITING, not RUNNABLE: the dump's state column is asserted, so the thread must be parked first
        await().atMost(Duration.ofSeconds(5)).until(() -> thread.getState() == Thread.State.WAITING);
        return thread;
    }
}
