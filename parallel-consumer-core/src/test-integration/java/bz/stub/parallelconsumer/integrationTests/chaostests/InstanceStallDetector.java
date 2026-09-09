package bz.stub.parallelconsumer.integrationTests.chaostests;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import lombok.Getter;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.IntFunction;
import java.util.function.Supplier;

/**
 * The instance-progress detector ({@code INSTANCE_STALL/NO_WORK_COMPLETED}) and the two instruments
 * that read the accused member - its per-member progress tokens and its thread dump - extracted from
 * {@link ProgressProbe}, which owns the sampler thread, the finding sinks and the other detectors.
 * The probe delegates here and keeps its public anchors ({@code ProgressProbe#sampleInstanceProgress},
 * {@code ProgressProbe#instanceProgressSnapshot}, {@code ProgressProbe#INSTANCE_STALL_BOUND}), so
 * every record that cites them still resolves.
 * <p>
 * Findings go out through the {@link FindingSink} the probe hands in at construction: {@code violate}
 * for the gating claim, {@code observe} for the non-gating {@code INSTANCE_BUSY_IN_USER_CODE} report -
 * so the probe's record rule (mode-dependent log level, and the observation marker that
 * {@code bin/chaos-test.sh} counts) stays in one place.
 */
@Slf4j
class InstanceStallDetector {

    /**
     * INSTANCE-progress probe: no live instance may hold work while returning no work result for
     * longer than this. The liveness claim it makes is the one the Class 2 lag bound only
     * approximates: {@code CLASS2_STALL} watches a partition's COMMITTED offset, which one incomplete
     * record legitimately pins while the shard behind it completes work continuously - so a busy
     * fleet and a wedged fleet look identical to it (measured 2026-08-19, seed 4734674029169027864:
     * four arms all drained fully, three of four still tripped the 150s bound). This probe instead
     * watches COMPLETIONS, so it does not fire on an instance that is finishing records, however
     * slowly.
     * <p>
     * <b>Precisely: only a SUCCESSFUL result re-arms it, and that is narrower than "any returned
     * work result".</b> The harness re-arms through {@code WorkManager#addSuccessfulWorkListener},
     * which only {@code onSuccessResult} fires. {@code onFailureResult} and the revoked-partition
     * drop branch of {@code handleFutureResult} both return a work result and decrement the
     * in-flight count while notifying nothing. The bound arithmetic below budgets for the second of
     * those and not the first, so an instance whose work is all FAILING is, to this detector,
     * indistinguishable from a wedged one. Nothing has yet shown that case reachable in the chaos
     * scenarios (their user functions swallow interrupts and do not throw), which is why this is
     * recorded here rather than treated as a defect - but it is an assumption, not a proof, and a
     * scenario that adds a throwing user function invalidates it.
     * <p>
     * <b>Granularity is per INSTANCE, not per shard, and that is a reachability constraint, not the
     * ideal.</b> The owner's formulation is per shard ("no shard should go {@code INSTANCE_STALL_BOUND}
     * without returning a work result"), but "which shards hold queued work" lives in
     * {@code ShardManager}'s private {@code processingShards} map with no public accessor, and this
     * suite does not add main-code accessors for a probe. Per instance is still the confluentinc#857
     * wedge signature exactly: work results are counted where {@code WorkManager#onSuccessResult}
     * runs - PC's CONTROL thread - so a deadlocked control loop freezes the count even while worker
     * threads finish records and heartbeats keep flowing. What per-instance cannot see is one wedged
     * shard on an instance whose other shards keep completing; that case remains
     * {@code CLASS2_STALL}'s - which since 2026-08-25 reports it as an observation rather than
     * failing on it, precisely because it cannot tell that case from a slow one.
     * <b>Since 2026-09-09 half of that case gates again, and half still does not.</b>
     * {@link UncommittedCompletionDetector} covers a partition whose completed work never reaches
     * the broker - a commit-path defect - by reading the difference between a member's own
     * next-offset-to-commit and the group's committed offset, which is a position rather than a
     * duration and so cannot be crossed by load. What is still uncovered is a key-order SHARD that
     * will never be dispatched again inside a partition: any incomplete offset pins that partition's
     * local watermark too, so the difference reads zero. That remainder is tracked in
     * {@code docs/inflight/test-per-shard-liveness-has-no-gate.md}; do not read the demotion, or its
     * partial repayment, as evidence the whole case is covered.
     * <p>
     * Bound arithmetic (why 150s cannot fire legitimately): a completion arrives at the end of every
     * user-function execution, so the longest legitimate GAP is one heaviest record - W1's 45s dwell,
     * 3.3x under the bound. The other legitimate quiet stretch is an eager storm, where completions
     * of revoked in-flight work are dropped as stale (no listener fire): storm (60s) plus one
     * eviction horizon (30s) is 90s, still 60s under.
     * <b>That 60s assumes a storm that ENDS.</b> W1 ({@code ChaosChurnStormIT}) leaves
     * {@code useCooperativeAssignor} false and drives a membership change every 500-1500ms for the
     * whole run, so its eager revocations are continuous rather than a bounded episode, and the
     * headroom this paragraph computes is not established for that scenario. Whether the bound
     * transfers to W1 is open, and it is the same transfer question
     * {@code docs/inflight/test-no-progress-window-may-not-transfer-to-w1.md} raises for the
     * fleet-level window. Settle it the way {@link ProgressProbe#REBALANCE_DWELL_BOUND} was settled - against the
     * healthy peak, which {@link #getPeakInstanceStallMs()} already reports on every run. Sharing {@link ProgressProbe#LAG_STAGNATION_BOUND}'s 150s
     * figure is deliberate - it keeps the two detectors' verdicts comparable on the same run: a run
     * where Class 2 fires and this stays silent is measured slow-but-progressing, not wedged.
     * <p>
     * <b>The W1 transfer question above is answered, 2026-09-07: the bound does NOT transfer to
     * continuous eager churn, and the fix is a second input rather than a bigger number.</b> Under
     * that churn every heavy dwell is revoked before it ends and redelivered while the old copy sleeps
     * on, so a member's workers fill with dwells whose results will be dropped, and its count freezes
     * for as long as the churn keeps them stale - 53s on the replayed seed, longer than the bound on
     * the CI sightings. That is a working member, not a stalled one, and the detector now asks
     * {@link InstanceProgressView#busyWorkers()} before it counts: a member with any worker running
     * user code re-arms the clock on every sample and is reported past this bound as a non-gating
     * {@code INSTANCE_BUSY_IN_USER_CODE} observation; the violation is reserved for a member holding
     * work with NO worker in user code, which is the only shape that accuses PC's control loop - its
     * results are then with nobody. The record, the dumps and the control arm are in
     * {@code docs/inflight/test-857-churn-storm-async-stalls.md}, "DIAGNOSED, 2026-09-07".
     */
    public static final Duration INSTANCE_STALL_BOUND = Duration.ofSeconds(150);

    /** Instance-progress bookkeeping: the completion count last seen, which PC it was seen on, and
     * since when it has not advanced. */
    @Value
    private static class InstanceProgressMark {
        long workResultsReturned;
        Object incarnation;
        Instant since;
    }

    /** Fleet supplier for the instance-progress probe; null (ambient mode, or not wired) = inactive. */
    private volatile Supplier<List<InstanceProgressView>> instanceProgressSupplier;
    private final Map<Integer, InstanceProgressMark> instanceProgressMarks = new ConcurrentHashMap<>();
    /** Instances already given an early stall dump in their CURRENT frozen stretch - one per stretch, not per sample. */
    private final java.util.Set<Integer> stallDumpedThisStretch = ConcurrentHashMap.newKeySet();
    /** When each instance's CURRENT workers-in-user-code stretch began; absent = no worker busy. */
    private final Map<Integer, Instant> busySince = new ConcurrentHashMap<>();
    /** Instances whose current busy stretch has already been reported - once per stretch. */
    private final java.util.Set<Integer> busyObservedThisStretch = ConcurrentHashMap.newKeySet();
    /**
     * How the accused member's threads are read: {@link #instanceThreadDump} in every real run.
     * <p>
     * The seam exists because the cost this file guards is the NUMBER of
     * {@link #instanceThreadDump} calls one firing makes, and a log line cannot show that - the
     * default configuration once took two, because {@link #INSTANCE_STALL_DUMP_AFTER} defaults to
     * {@link #INSTANCE_STALL_BOUND} and both branches then fire on the same sample.
     * {@code InstanceStallProbeIT#takesOneThreadDumpPerFiringInTheDefaultConfiguration} counts
     * through here, so a return to two dumps fails a test rather than merely doubling a log.
     */
    private volatile IntFunction<String> threadDumpSource = InstanceStallDetector::instanceThreadDump;
    @Getter
    private volatile long peakInstanceStallMs = 0;

    /**
     * How long an instance may hold work and return nothing before its threads are dumped -
     * {@code -Dchaos.instanceStallDumpAfterSeconds=<n>}, defaulting to the bound itself, so an
     * unconfigured run dumps exactly once per firing and nowhere else.
     * <p>
     * <b>That "exactly once" is held by {@link #sample}, not by the default.</b> At
     * the default the two thresholds are EQUAL, so the first sample past the bound satisfies the
     * early-dump condition and the violation condition together - taking the dump in both branches
     * gives the gating, unconfigured run two near-identical dumps from one
     * {@code ThreadMXBean#getThreadInfo(ids, true, true)} each, at the moment the run is already
     * failing. The sampler therefore takes at most one dump per sample and the violation branch
     * points at the early dump when that branch already took it.
     * <p>
     * Lower it under
     * {@code -Dchaos.diagnoseStallRecovery=true} to see inside a stretch the run outlives: the
     * tokens from {@link #snapshot} can show a member frozen for the whole tail of a
     * run that still finishes under the bound, and then there is no firing to hang a dump on.
     */
    static final Duration INSTANCE_STALL_DUMP_AFTER = Duration.ofSeconds(
            Long.getLong("chaos.instanceStallDumpAfterSeconds", INSTANCE_STALL_BOUND.getSeconds()));

    /**
     * Where findings land. Two named methods rather than two {@code Consumer<String>} parameters,
     * because those are the same type and only argument order tells them apart - a swapped pair
     * compiles and files every gating claim as a non-gating observation.
     */
    interface FindingSink {
        /** A gating claim about PC - fails the run. */
        void violate(String message);

        /** Reported, never fails the run. */
        void observe(String message);
    }

    private final FindingSink sink;

    InstanceStallDetector(FindingSink sink) {
        this.sink = sink;
    }

    /** Arms the detector with a live view of the fleet - see {@link ProgressProbe#withInstanceProgress}. */
    void watch(Supplier<List<InstanceProgressView>> fleetSupplier) {
        this.instanceProgressSupplier = fleetSupplier;
    }

    /** Replaces the thread-dump reader for a seam test - see {@link #threadDumpSource}. */
    void threadDumpSource(IntFunction<String> source) {
        this.threadDumpSource = source;
    }

    /**
     * One compact token per fleet member for a {@code -Dchaos.diagnoseStallRecovery=true} run's log:
     * {@code <id>(live|down q=<queued> out=<outForProcessing> res=<workResultsReturned>)}.
     * <p>
     * <b>Why this exists.</b> An {@code INSTANCE_STALL/NO_WORK_COMPLETED} violation is a claim about
     * ONE member, and the recovery diagnostic that settled the fleet-level
     * {@code NO_PROGRESS} line logs fleet consumed/started only. Watching the fleet drain therefore
     * says nothing about whether the accused instance recovered, so every instance-level sighting in
     * {@code docs/inflight/test-857-churn-storm-async-stalls.md} is unclassified. The discriminating
     * observation is this instance's own {@code res=} advancing and its {@code out=} draining after
     * the firing, which is exactly what these tokens carry.
     * <p>
     * <b>Every member is rendered, including a stopped one</b> ({@code down}), which the detector
     * itself skips. Omitting it would read as a fleet that never had that member - the
     * silence-is-not-evidence trap recorded in
     * {@code docs/solutions/best-practices/silence-from-an-instrument-that-could-not-have-spoken-is-not-evidence.md}.
     * For the same reason a supplier that throws yields an {@code unreadable} marker rather than an
     * empty string: this is called from inside an awaitility condition, so it must neither abort the
     * wait nor let a failed read look like an empty fleet.
     * <p>
     * <b>What it cannot tell you, and it is the same trap one instrument along: a RESTART forges
     * recovery.</b> {@code res=} is {@code ManagedPCInstance#workResultsReturned}, which deliberately
     * spans incarnations and is never reset, and {@code out=} reads whatever {@code WorkManager} is
     * current. So when the conductor stops a wedged member and starts it again - {@code RESTART}
     * carries weight 3 in the churn storm, so it is a routine draw rather than an edge case - the
     * following tokens show {@code res=} climbing on from the frozen value and {@code out=} filling
     * from a brand-new {@code WorkManager}, which is exactly the shape read here as recovery. The
     * DETECTOR is immune, because {@link #sample} re-arms on
     * {@link InstanceProgressView#incarnationMarker()}; the rendered token carries no incarnation, so
     * a human reading the line is not. Until it does, pair an apparent recovery with the conductor's
     * own action log for that instance id before classifying anything from it.
     */
    String snapshot() {
        var supplier = instanceProgressSupplier;
        if (supplier == null) {
            return "";
        }
        try {
            return supplier.get().stream()
                    .map(view -> view.instanceId()
                            + (view.isLive() ? "(live q=" : "(down q=") + view.queuedInShards()
                            + " out=" + view.outForProcessing()
                            + " res=" + view.workResultsReturned() + ")")
                    .collect(java.util.stream.Collectors.joining(" "));
        } catch (Exception e) {
            return "unreadable (" + e + ")";
        }
    }

    /**
     * INSTANCE-progress detector - see {@link #INSTANCE_STALL_BOUND} for the property it asserts and
     * the granularity reasoning. Per live instance: if it holds work (queued in shards, or records out
     * for processing) and its returned-work-result count has not advanced within the bound, that is a
     * violation. The clock re-arms on ANY of: a result returned, the instance going idle (nothing
     * held), a restart (new PC incarnation), or the instance leaving the live set - so only a
     * continuous hold-work-return-nothing stretch can accumulate.
     * <p>
     * Package-private and taking {@code now} explicitly so {@code InstanceStallProbeIT} can drive it
     * deterministically, broker-free, in both directions - the sampler thread calls it with
     * {@code Instant.now()}.
     */
    void sample(Instant now) {
        var supplier = instanceProgressSupplier;
        if (supplier == null) return; // not wired (ambient mode, or a scenario predating the probe)
        for (InstanceProgressView view : supplier.get()) {
            int id = view.instanceId();
            if (!view.isLive()) {
                // stopped or mid-restart: torn-down state must never read as a stall
                instanceProgressMarks.remove(id);
                continue;
            }
            long returned = view.workResultsReturned();
            Object incarnation = view.incarnationMarker();
            InstanceProgressMark mark = instanceProgressMarks.get(id);
            boolean advanced = mark == null
                    || mark.getWorkResultsReturned() != returned
                    || mark.getIncarnation() != incarnation;
            long queued = view.queuedInShards();
            long outForProcessing = view.outForProcessing();
            boolean holdsWork = queued > 0 || outForProcessing > 0;
            if (advanced || !holdsWork) {
                instanceProgressMarks.put(id, new InstanceProgressMark(returned, incarnation, now));
                stallDumpedThisStretch.remove(id);
                busySince.remove(id);
                busyObservedThisStretch.remove(id);
                continue;
            }
            // Held work and a frozen count: the signal. Now ask what the workers are doing, because a
            // member with any worker running user code produces this signal for as long as that
            // function runs, and it is not a claim about PC (see InstanceProgressView#busyWorkers).
            int busy = view.busyWorkers();
            if (busy > 0) {
                // Working: the stall clock does not run. It is re-armed to NOW on every sample, so it
                // starts the moment the last worker leaves user code while the count is still frozen -
                // which is the case that IS PC's, because then the results are with nobody. The busy
                // stretch itself is reported once, non-gating, past the same bound, so a fleet that
                // spends its whole tail in user code is visible without failing the run.
                instanceProgressMarks.put(id, new InstanceProgressMark(returned, incarnation, now));
                Instant since = busySince.computeIfAbsent(id, ignored -> now);
                long busyMs = Duration.between(since, now).toMillis();
                if (busyMs > INSTANCE_STALL_DUMP_AFTER.toMillis() && stallDumpedThisStretch.add(id)) {
                    // The diagnostic dump keys on the busy stretch here, not the stall clock (which this
                    // branch keeps re-armed), or a working member could never be dumped - and a dump of
                    // a working member is exactly what told the stall apart from a wedge in the first place.
                    log.warn("INSTANCE_BUSY early dump ({}s in user code, bound {}s) for instance {}: {} worker(s) busy, {}\n{}",
                            busyMs / 1000, INSTANCE_STALL_BOUND.getSeconds(), id, busy, view.engineSnapshot(),
                            threadDumpSource.apply(id)); // through the seam, so the dump-count test sees this branch too
                }
                if (busyMs > INSTANCE_STALL_BOUND.toMillis() && busyObservedThisStretch.add(id)) {
                    sink.observe("INSTANCE_BUSY_IN_USER_CODE: instance " + id + " has held work (queued=" + queued
                            + ", outForProcessing=" + outForProcessing + ") for " + (busyMs / 1000)
                            + "s with " + busy + " worker(s) running user code and no work result returned - "
                            + "a working member, not a stalled control loop; the stall clock starts when the "
                            + "last worker leaves user code");
                }
                continue;
            }
            if (busySince.remove(id) != null) {
                // the busy stretch just ended with work still held: a fresh stretch, which may earn its own dump
                stallDumpedThisStretch.remove(id);
            }
            busyObservedThisStretch.remove(id);
            long stalledMs = Duration.between(mark.getSince(), now).toMillis();
            if (stalledMs > peakInstanceStallMs) peakInstanceStallMs = stalledMs;
            boolean earlyDumpedThisSample =
                    stalledMs > INSTANCE_STALL_DUMP_AFTER.toMillis() && stallDumpedThisStretch.add(id);
            // read once for the sample: the early dump and the firing below may both print it
            String engineSnapshot = view.engineSnapshot();
            if (earlyDumpedThisSample) {
                // Diagnostic only, and only when the property lowers it below the bound: a stretch that
                // ends before the bound leaves no violation and no dump, so a wedge that clears when
                // the run happens to finish first was invisible - which is how seed 6077035105695 read
                // as clean on a tree where its instance 0 sat frozen for the whole tail of the run.
                log.warn("INSTANCE_STALL early dump ({}s frozen, bound {}s) for instance {}: {}\n{}",
                        stalledMs / 1000, INSTANCE_STALL_BOUND.getSeconds(), id, engineSnapshot,
                        threadDumpSource.apply(id));
            }
            if (stalledMs > INSTANCE_STALL_BOUND.toMillis()) {
                sink.violate("INSTANCE_STALL/NO_WORK_COMPLETED: instance " + id + " holds work (queued="
                        + queued + ", outForProcessing=" + outForProcessing
                        + ") but has returned no work result for " + (stalledMs / 1000) + "s (bound "
                        + INSTANCE_STALL_BOUND.getSeconds() + "s) at " + returned
                        + " results returned - completions are counted on PC's control thread, so this "
                        + "instance's control loop is holding work and finishing nothing");
                // The dump is taken HERE, inside the sample that fired, because the accused instance's
                // threads are what the violation is a claim about, and nothing else captures them - a
                // gating run aborts on this violation and a CI log outlives the JVM by nothing.
                //
                // Unless this same sample already took it: at the DEFAULT the two thresholds are equal,
                // so the first sample past the bound satisfies both branches, and dumping again would
                // pay a second getThreadInfo(ids, true, true) to print the same stacks. See
                // INSTANCE_STALL_DUMP_AFTER for the contract this keeps.
                if (earlyDumpedThisSample) {
                    log.warn("INSTANCE_STALL thread dump for instance {} at the moment the detector fired: {}"
                                    + "\n  (its threads are in the early dump logged immediately above - same sample)",
                            id, engineSnapshot);
                } else {
                    log.warn("INSTANCE_STALL thread dump for instance {} at the moment the detector fired: {}\n{}",
                            id, engineSnapshot, threadDumpSource.apply(id));
                }
                instanceProgressMarks.put(id, new InstanceProgressMark(returned, incarnation, now)); // re-arm
                stallDumpedThisStretch.remove(id); // the re-armed stretch may earn its own early dump
            }
        }
    }

    /** Enough frames to see past the executor plumbing to whatever a worker is actually parked in. */
    private static final int STALL_DUMP_FRAMES = 40;

    /** {@link InstanceProgressView#busyWorkers()} when the view cannot count them. */
    static final int BUSY_WORKERS_UNKNOWN = -1;

    /**
     * How many of an instance's workers are running user code, read from the worker threads' own
     * stacks - the same reading {@link #instanceThreadDump} hands a human, made mechanical. A worker
     * between tasks is parked inside {@code ThreadPoolExecutor.getTask}; one running a task has no
     * such frame. A thread the pool has not created yet is not counted and needs no capacity to be
     * accounted for, which is the point of counting busy rather than idle.
     * <p>
     * Read from the stacks rather than from the pool because the pool is the engine's, held behind a
     * protected getter, and this suite does not add main-code accessors for a probe (the same rule
     * {@link #INSTANCE_STALL_BOUND}'s granularity note records). {@code Thread.getAllStackTraces} is
     * a JVM-wide walk, so the detector asks only for a member already holding work with a frozen
     * count - never on the healthy path.
     * <p>
     * Membership is the {@code pc-pool-} prefix and the exact {@code -PC-<id>} suffix, the names
     * {@code AbstractParallelEoSStreamProcessor#setupWorkerPool} gives the default factory's threads;
     * a scenario that supplies its own {@code managedThreadFactory} counts nothing busy and gets the
     * old rule, which accuses - a stricter reading than the truth, never a silent exemption.
     */
    static int busyWorkersOf(int instanceId) {
        String suffix = "-PC-" + instanceId;
        int busy = 0;
        Map<Thread, StackTraceElement[]> stacks;
        try {
            stacks = Thread.getAllStackTraces();
        } catch (RuntimeException e) {
            // A failed walk must not read as "nobody busy", which would accuse; unknown gets the old
            // rule - the same guard instanceThreadDump keeps around its own walk.
            return BUSY_WORKERS_UNKNOWN;
        }
        for (var entry : stacks.entrySet()) {
            String name = entry.getKey().getName();
            if (!name.startsWith("pc-pool-") || !name.endsWith(suffix)) continue;
            boolean betweenTasks = false;
            for (StackTraceElement frame : entry.getValue()) {
                if (frame.getClassName().equals("java.util.concurrent.ThreadPoolExecutor")
                        && frame.getMethodName().equals("getTask")) {
                    betweenTasks = true;
                    break;
                }
            }
            if (!betweenTasks) busy++;
        }
        return busy;
    }

    /**
     * Every thread belonging to one fleet member, with its state, the lock it is waiting for and who
     * holds it, and its top {@value #STALL_DUMP_FRAMES} frames - the discriminator an
     * {@code INSTANCE_STALL/NO_WORK_COMPLETED} firing has never had.
     * <p>
     * <b>Why this exists.</b> The per-instance tokens in {@link #snapshot} classified
     * the churn storm's instance-stall line as a live member that keeps accepting work and returns
     * nothing ({@code docs/inflight/test-857-churn-storm-async-stalls.md}, the {@code CLASSIFIED}
     * section) - and then stopped, because a counter can say the workers return nothing but not
     * what they are doing instead. Only their stacks say that, and a gating run destroys them at
     * the moment of detection, so the dump has to be taken by the detector itself.
     * <p>
     * <b>Membership is by thread name, and it is exact.</b> PC names every thread it owns with the
     * instance's {@code myId} as a suffix - the worker pool, the control thread and the broker-poll
     * thread all end {@code -PC-<instanceId>}, which {@code ManagedPCInstance#start} sets. The match is
     * {@code endsWith}, not {@code contains}: {@code -PC-1} is a suffix of nothing but instance 1,
     * whereas a substring match would fold instance 14's threads into it.
     * <p>
     * <b>An empty result is reported as such, never as an empty string.</b> No matching threads means
     * either the instance's threads have already exited or the naming does not match, and both are
     * findings a reader must not mistake for "nothing was happening" -
     * {@code docs/solutions/best-practices/silence-from-an-instrument-that-could-not-have-spoken-is-not-evidence.md}.
     * For the same reason a bean that throws yields a marker rather than propagating: this runs on
     * the sampler thread, and the violation it accompanies has already been recorded.
     * <p>
     * Package-private so {@code InstanceStallProbeIT} can pin the exact-suffix rule and the
     * explicit-absence marker broker-free. The naming itself is PC's contract, not this file's:
     * {@code CloseInterruptLivelockTest} finds a real PC's control thread by
     * {@code "pc-control-" + myId}, so a rename there fails that test before it silently empties
     * this dump.
     */
    static String instanceThreadDump(int instanceId) {
        String suffix = "-PC-" + instanceId;
        try {
            long[] ids = Thread.getAllStackTraces().keySet().stream()
                    .filter(t -> t.getName().endsWith(suffix))
                    .mapToLong(Thread::getId)
                    .toArray();
            if (ids.length == 0) {
                return "  (no threads named *" + suffix + " exist - the instance's threads have exited, or "
                        + "the naming contract this dump relies on has changed)";
            }
            java.lang.management.ThreadMXBean bean = java.lang.management.ManagementFactory.getThreadMXBean();
            StringBuilder out = new StringBuilder();
            for (java.lang.management.ThreadInfo info : bean.getThreadInfo(ids, true, true)) {
                if (info == null) continue; // exited between the name scan and the bean read
                out.append("  \"").append(info.getThreadName()).append("\" ").append(info.getThreadState());
                if (info.getLockName() != null) {
                    out.append(" waiting on ").append(info.getLockName());
                    if (info.getLockOwnerName() != null) {
                        out.append(" held by \"").append(info.getLockOwnerName()).append("\"");
                    }
                }
                out.append('\n');
                StackTraceElement[] frames = info.getStackTrace();
                int shown = Math.min(frames.length, STALL_DUMP_FRAMES);
                for (int i = 0; i < shown; i++) {
                    out.append("      at ").append(frames[i]).append('\n');
                }
                if (frames.length > shown) {
                    out.append("      ... ").append(frames.length - shown).append(" more\n");
                }
                for (var monitor : info.getLockedMonitors()) {
                    out.append("      holds monitor ").append(monitor).append('\n');
                }
                for (var sync : info.getLockedSynchronizers()) {
                    out.append("      holds ").append(sync).append('\n');
                }
            }
            return out.toString();
        } catch (RuntimeException e) {
            return "  (thread dump unreadable: " + e + ")";
        }
    }
}
