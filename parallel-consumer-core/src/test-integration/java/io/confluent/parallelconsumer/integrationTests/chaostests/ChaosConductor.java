package io.confluent.parallelconsumer.integrationTests.chaostests;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import io.confluent.parallelconsumer.integrationTests.chaostests.scenario.FleetControl;
import io.confluent.parallelconsumer.integrationTests.chaostests.scenario.MembershipAction;
import io.confluent.parallelconsumer.integrationTests.chaostests.scenario.Scenario;
import io.confluent.parallelconsumer.integrationTests.chaostests.scenario.ScenarioAction;
import io.confluent.parallelconsumer.integrationTests.chaostests.scenario.ScenarioContext;
import io.confluent.parallelconsumer.integrationTests.chaostests.scenario.ScenarioRunner;
import io.confluent.parallelconsumer.integrationTests.chaostests.scenario.WorkloadControl;
import io.confluent.parallelconsumer.integrationTests.utils.ManagedPCInstance;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

/**
 * The fleet half of the scenario driver: it owns a fleet of PC instances, an authoritative per-instance
 * state machine, and the timeline - and it exposes them to a {@link Scenario} as a {@link FleetControl}.
 * The tick loop itself now lives in {@link ScenarioRunner}, so the same driver runs the Chaos Pain Suite
 * (see {@code scenario.ChaosScenarios}) and any other scenario declared against the framework.
 * <p>
 * Originally the Chaos Pain Suite's seeded chaos scheduler (Phase 1 - see
 * {@code docs/plans/2026-07-30-002-feat-chaos-pain-suite-phase1-plan.md}), generalised in place per KTD14
 * of {@code docs/plans/2026-08-07-002-feat-embedded-web-dashboard-plan.md}.
 * <p>
 * <b>Determinism is unchanged by the generalisation.</b> Each tick consumes one complete
 * {@code ScenarioTick} from the seeded RNG - up front and in full, regardless of live fleet state - so the
 * random stream is a pure function of (seed, tick index). Same seed = same draw sequence, always. The
 * REALIZED action/target is the draw filtered through live fleet state (the join-after-drain bias only
 * fires after a successful STOP_DRAIN; a target roll resolves against the instances currently in the
 * wanted state), so wall-clock timing pins what a draw lands on - never which draws occur, and never how
 * many. See {@code scenario.SeededPlanSource} for the draw contract and
 * {@code scenario.PlanSourceSeedStabilityTest} for the golden values that guard it.
 * <p>
 * Every executed action is appended to a timestamped {@link #getTimeline() timeline}, logged at execution
 * and dumped wholesale by {@link #stop()} - a failing run's first artifact.
 * <p>
 * Membership action set: {@link MembershipAction}. The conductor deliberately supports a
 * <b>join-after-stopDrain bias</b>, declared on the scenario phase as its follow-on action: the
 * zombie-drain defect class bites hardest when a member joins while another is mid-drain.
 * <p>
 * Lifecycle notes: drives instances via {@link ManagedPCInstance#start(ExecutorService)},
 * {@link ManagedPCInstance#stopAsync()} (DONT_DRAIN) and - because the managed API only exposes
 * DONT_DRAIN closes - raw {@code getParallelConsumer().closeDrainFirst()} on a background thread for
 * DRAIN stops. Drain threads are tracked and joined (bounded by {@link #DRAINER_JOIN_BUDGET}) inside
 * {@link #stop()}, so callers see a quiesced fleet - no drainer races the test's settle/teardown close.
 * The conductor keeps its own authoritative per-instance state machine and never calls {@code toggle()},
 * so the managed {@code started} flag (which only gates toggle) stays out of play.
 */
@Slf4j
public class ChaosConductor implements ScenarioContext, FleetControl {

    /** Conductor's authoritative view of each instance it manages. Public: referenced across the
     * chaostests package (probe, scenarios, assertions). */
    public enum InstanceState {RUNNING, DRAINING, STOPPED}

    /** Drain threads must not outlive {@link #stop()} - budget must exceed the heaviest legitimate
     * drain (a heavy-tail record legitimately blocks its drain for the full dwell). Leftovers are
     * recorded loudly in the timeline rather than silently leaked into teardown. */
    public static final Duration DRAINER_JOIN_BUDGET = Duration.ofSeconds(60);

    private final long seed;
    private final Scenario scenario;
    private final int maxFleetSize;
    private final ExecutorService pcExecutor;
    private final Supplier<ManagedPCInstance> instanceFactory;
    /** Never touched by chaos - the survivor that guarantees the group always has a healthy member. */
    private final ManagedPCInstance protectedInstance;
    /** Optional: only scenarios with workload actions need one. */
    private final WorkloadControl workload;

    private final List<ManagedPCInstance> fleet = new CopyOnWriteArrayList<>();
    private final Map<Integer, InstanceState> states = new ConcurrentHashMap<>();
    private final List<Thread> drainers = new CopyOnWriteArrayList<>();
    private final AtomicInteger disturbanceCount = new AtomicInteger();
    @Getter
    private final List<String> timeline = Collections.synchronizedList(new ArrayList<>());
    private final ScenarioRunner runner;
    private Thread conductorThread;
    private Instant startedAt;
    /** Observer hook for the probe: called with (instanceId, action) as each action executes. */
    private final ChaosObserver observer;

    public interface ChaosObserver {
        void onAction(int instanceId, ScenarioAction action);

        /** A STOP_DRAIN's background close finished (successfully or not) for this instance. */
        default void onDrainComplete(int instanceId) {
        }
    }

    @Builder
    public ChaosConductor(long seed,
                          Scenario scenario,
                          String replayCommand,
                          int maxFleetSize,
                          ExecutorService pcExecutor,
                          Supplier<ManagedPCInstance> instanceFactory,
                          ManagedPCInstance protectedInstance,
                          WorkloadControl workload,
                          List<ManagedPCInstance> initialFleet,
                          ChaosObserver observer) {
        if (scenario == null) {
            throw new IllegalArgumentException("a conductor needs a scenario - see scenario.ChaosScenarios "
                    + "for the suite's W1 and W4 declarations");
        }
        this.seed = seed;
        this.scenario = scenario;
        this.maxFleetSize = maxFleetSize;
        this.pcExecutor = pcExecutor;
        this.instanceFactory = instanceFactory;
        this.protectedInstance = protectedInstance;
        this.workload = workload;
        this.observer = observer == null ? (id, a) -> { } : observer;
        if (initialFleet != null) {
            for (ManagedPCInstance pc : initialFleet) {
                fleet.add(pc);
                states.put(pc.getInstanceId(), InstanceState.RUNNING);
            }
        }
        this.runner = ScenarioRunner.builder()
                .scenario(scenario)
                .seed(seed)
                .mode(ScenarioRunner.Mode.LOOP)
                .context(this)
                .replayCommand(replayCommand)
                .build();
    }

    // --- ScenarioContext ---

    @Override
    public FleetControl fleet() {
        return this;
    }

    @Override
    public WorkloadControl workload() {
        if (workload == null) {
            return ScenarioContext.super.workload(); // named failure, not a silent no-op
        }
        return workload;
    }

    @Override
    public void record(String what, int instanceId) {
        String entry = "t=+" + (startedAt == null ? "?" : Duration.between(startedAt, Instant.now()).toMillis() + "ms")
                + " " + what + (instanceId >= 0 ? " -> instance " + instanceId : "");
        timeline.add(entry);
        log.info("[chaos] {}", entry);
    }

    // --- lifecycle ---

    public void start() {
        startedAt = Instant.now();
        record("CONDUCTOR START seed=" + seed + " scenario='" + scenario.getName() + "'", -1);
        conductorThread = new Thread(runner::run, "chaos-conductor");
        conductorThread.setDaemon(true);
        conductorThread.start();
    }

    public void stop() {
        runner.stop();
        if (conductorThread != null) {
            conductorThread.interrupt();
            try {
                conductorThread.join(5_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        joinDrainers();
        record("CONDUCTOR STOP - timeline follows", -1);
        synchronized (timeline) {
            for (String entry : timeline) {
                log.info("[chaos-timeline] {}", entry);
            }
        }
    }

    /** Postcondition failures the scenario's phases reported. Empty for the unbounded chaos scenarios,
     * which declare none. */
    public List<String> getScenarioFailures() {
        return runner.getFailures();
    }

    private void joinDrainers() {
        Instant deadline = Instant.now().plus(DRAINER_JOIN_BUDGET);
        for (Thread drainer : drainers) {
            long left = Duration.between(Instant.now(), deadline).toMillis();
            if (left <= 0) break;
            try {
                drainer.join(left);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
        long alive = drainers.stream().filter(Thread::isAlive).count();
        if (alive > 0) {
            record("CONDUCTOR STOP: " + alive + " drain thread(s) still running after "
                    + DRAINER_JOIN_BUDGET.getSeconds() + "s join budget - teardown may race them", -1);
        }
    }

    /** All instances the conductor knows about, INCLUDING the protected instance (only chaos targeting
     * excludes it - see {@link #pickInState}; the probe needs to see the whole fleet). */
    public List<ManagedPCInstance> getFleet() {
        return new ArrayList<>(fleet);
    }

    /** Disturbances (STOP_DRAIN + STOP_NO_DRAIN + RESTART) that actually fired - the authoritative
     * input for the ledger's duplicate allowance, vs parsing the human-readable timeline. */
    public int getDisturbanceCount() {
        return disturbanceCount.get();
    }

    // --- FleetControl: the actions themselves ---

    @Override
    public boolean stopDrain(int targetRoll) {
        ManagedPCInstance victim = pickInState(InstanceState.RUNNING, targetRoll);
        if (victim == null) return false;
        states.put(victim.getInstanceId(), InstanceState.DRAINING);
        disturbanceCount.incrementAndGet();
        record("STOP_DRAIN", victim.getInstanceId());
        observer.onAction(victim.getInstanceId(), MembershipAction.STOP_DRAIN);
        var pc = victim.getParallelConsumer();
        Thread drainer = new Thread(() -> {
            try {
                pc.closeDrainFirst();
            } catch (Exception e) {
                log.warn("Drain-close of instance {} threw (classified on restart or by the end-of-run sweep): {}",
                        victim.getInstanceId(), e.getMessage());
            } finally {
                states.put(victim.getInstanceId(), InstanceState.STOPPED);
                record("DRAIN_COMPLETE", victim.getInstanceId());
                observer.onDrainComplete(victim.getInstanceId());
            }
        }, "chaos-drain-" + victim.getInstanceId());
        drainer.setDaemon(true);
        drainers.add(drainer);
        drainer.start();
        return true; // candidate for join-after-drain bias
    }

    @Override
    public void stopNoDrain(int targetRoll) {
        ManagedPCInstance victim = pickInState(InstanceState.RUNNING, targetRoll);
        if (victim == null) return;
        states.put(victim.getInstanceId(), InstanceState.STOPPED);
        disturbanceCount.incrementAndGet();
        record("STOP_NO_DRAIN", victim.getInstanceId());
        observer.onAction(victim.getInstanceId(), MembershipAction.STOP_NO_DRAIN);
        victim.stopAsync();
    }

    @Override
    public void restart(int targetRoll) {
        ManagedPCInstance target = pickInState(InstanceState.STOPPED, targetRoll);
        if (target == null) return;
        // start() first: it can throw (the unexpected-failure-cause canary), in which case the
        // instance must stay STOPPED and untargetable rather than lying as RUNNING
        target.start(pcExecutor);
        states.put(target.getInstanceId(), InstanceState.RUNNING);
        disturbanceCount.incrementAndGet();
        record("RESTART", target.getInstanceId());
        observer.onAction(target.getInstanceId(), MembershipAction.RESTART);
    }

    @Override
    public void joinNew() {
        if (fleet.size() >= maxFleetSize) return;
        ManagedPCInstance recruit = instanceFactory.get();
        recruit.start(pcExecutor);
        fleet.add(recruit);
        states.put(recruit.getInstanceId(), InstanceState.RUNNING);
        record("JOIN_NEW", recruit.getInstanceId());
        observer.onAction(recruit.getInstanceId(), MembershipAction.JOIN_NEW);
    }

    private ManagedPCInstance pickInState(InstanceState wanted, int targetRoll) {
        List<ManagedPCInstance> candidates = new ArrayList<>();
        for (ManagedPCInstance pc : fleet) {
            if (pc == protectedInstance) continue;
            if (states.get(pc.getInstanceId()) == wanted) candidates.add(pc);
        }
        if (candidates.isEmpty()) return null;
        return candidates.get(targetRoll % candidates.size());
    }
}
