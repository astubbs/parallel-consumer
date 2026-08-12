package io.confluent.parallelconsumer.integrationTests.chaostests;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import io.confluent.parallelconsumer.integrationTests.utils.ManagedPCInstance;
import lombok.Builder;
import lombok.Getter;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

/**
 * Seeded, replayable chaos scheduler for the Chaos Pain Suite (Phase 1 - see
 * {@code docs/plans/2026-07-30-002-feat-chaos-pain-suite-phase1-plan.md}).
 * <p>
 * Replaces the ad-hoc {@code Math.random()} chaos monkey with a deterministic draw stream: each tick
 * consumes one complete {@link TickDraws} from the seeded RNG - up front and in full, regardless of
 * live fleet state - so the random stream is a pure function of (seed, tick index). Same seed = same
 * draw sequence, always. The REALIZED action/target is the draw filtered through live fleet state
 * (the join-after-drain bias only fires after a successful STOP_DRAIN; a target roll resolves against
 * the instances currently in the wanted state), so wall-clock timing pins what a draw lands on - never
 * which draws occur, and never how many. Every executed action is appended to a timestamped
 * {@link #getTimeline() timeline}, logged at execution and dumped wholesale by {@link #stop()} - a
 * failing run's first artifact.
 * <p>
 * Phase 1 action set (W1 churn storm): {@link ChaosAction#STOP_DRAIN}, {@link ChaosAction#STOP_NO_DRAIN},
 * {@link ChaosAction#RESTART}, {@link ChaosAction#JOIN_NEW}. The conductor deliberately supports a
 * <b>join-after-stopDrain bias</b>: the zombie-drain defect class bites hardest when a member joins while
 * another is mid-drain, so after each STOP_DRAIN the next action is forced to JOIN_NEW with probability
 * {@code joinAfterDrainBias} - the calibration tuning knob (probes get tuned via the conductor, never
 * loosened).
 * <p>
 * Lifecycle notes: drives instances via {@link ManagedPCInstance#start(ExecutorService)},
 * {@link ManagedPCInstance#stopAsync()} (DONT_DRAIN) and - because the managed API only exposes
 * DONT_DRAIN closes - raw {@code getParallelConsumer().closeDrainFirst()} on a background thread for
 * DRAIN stops. Drain threads are tracked and joined (bounded by {@link #DRAINER_JOIN_BUDGET}) inside
 * {@link #stop()}, so callers see a quiesced fleet - no drainer races the test's settle/teardown
 * close. The conductor keeps its own authoritative per-instance state machine and never calls
 * {@code toggle()}, so the managed {@code started} flag (which only gates toggle) stays out of play.
 * Because {@code stopAsync()} returns before the close completes, the conductor can mark an instance
 * STOPPED and redraw a RESTART while the previous restart is still queued; {@code start()} refuses
 * that second submission and the conductor leaves the instance STOPPED rather than recording a
 * disturbance that never happened.
 */
@Slf4j
public class ChaosConductor {

    public enum ChaosAction {STOP_DRAIN, STOP_NO_DRAIN, RESTART, JOIN_NEW}

    /** Conductor's authoritative view of each instance it manages. Public: referenced across the
     * chaostests package (probe, scenarios, assertions). */
    public enum InstanceState {RUNNING, DRAINING, STOPPED}

    /** Drain threads must not outlive {@link #stop()} - budget must exceed the heaviest legitimate
     * drain (a heavy-tail record legitimately blocks its drain for the full dwell). Leftovers are
     * recorded loudly in the timeline rather than silently leaked into teardown. */
    public static final Duration DRAINER_JOIN_BUDGET = Duration.ofSeconds(60);

    /**
     * One tick's complete draw set, consumed from the seeded RNG before the tick acts. Consuming every
     * draw every tick - used or not - is what makes the stream replayable: without it, the number of
     * draws would depend on live fleet state (bias armed? candidates available?), which is mutated by
     * wall-clock-timed drain completions, and two runs of the same seed could silently desynchronise.
     */
    @Value
    public static class TickDraws {
        long tickMs;
        double biasRoll;
        ChaosAction action;
        int targetRoll;
    }

    private final long seed;
    private final Random random;
    private final Duration minTick;
    private final Duration maxTick;
    private final Map<ChaosAction, Integer> weights;
    private final double joinAfterDrainBias;
    private final int maxFleetSize;
    private final ExecutorService pcExecutor;
    private final Supplier<ManagedPCInstance> instanceFactory;
    /** Never touched by chaos - the survivor that guarantees the group always has a healthy member. */
    private final ManagedPCInstance protectedInstance;

    private final List<ManagedPCInstance> fleet = new CopyOnWriteArrayList<>();
    private final Map<Integer, InstanceState> states = new ConcurrentHashMap<>();
    private final List<Thread> drainers = new CopyOnWriteArrayList<>();
    private final AtomicInteger disturbanceCount = new AtomicInteger();
    @Getter
    private final List<String> timeline = Collections.synchronizedList(new ArrayList<>());
    private final AtomicBoolean running = new AtomicBoolean(false);
    private Thread conductorThread;
    private Instant startedAt;
    /** Observer hook for the probe: called with (instanceId, action) as each action executes. */
    private final ChaosObserver observer;

    public interface ChaosObserver {
        void onAction(int instanceId, ChaosAction action);

        /** A STOP_DRAIN's background close finished (successfully or not) for this instance. */
        default void onDrainComplete(int instanceId) {
        }
    }

    @Builder
    public ChaosConductor(long seed,
                          Duration minTick,
                          Duration maxTick,
                          Map<ChaosAction, Integer> weights,
                          double joinAfterDrainBias,
                          int maxFleetSize,
                          ExecutorService pcExecutor,
                          Supplier<ManagedPCInstance> instanceFactory,
                          ManagedPCInstance protectedInstance,
                          List<ManagedPCInstance> initialFleet,
                          ChaosObserver observer) {
        this.seed = seed;
        this.random = new Random(seed);
        this.minTick = minTick == null ? Duration.ofSeconds(1) : minTick;
        this.maxTick = maxTick == null ? Duration.ofSeconds(3) : maxTick;
        // defensive EnumMap copy: deterministic iteration order is what makes weightedPick
        // seed-replayable - a caller-supplied HashMap would iterate in identity-hash order
        this.weights = weights == null ? defaultW1Weights() : new EnumMap<>(weights);
        this.joinAfterDrainBias = joinAfterDrainBias;
        this.maxFleetSize = maxFleetSize;
        this.pcExecutor = pcExecutor;
        this.instanceFactory = instanceFactory;
        this.protectedInstance = protectedInstance;
        this.observer = observer == null ? (id, a) -> { } : observer;
        if (initialFleet != null) {
            for (ManagedPCInstance pc : initialFleet) {
                fleet.add(pc);
                states.put(pc.getInstanceId(), InstanceState.RUNNING);
            }
        }
    }

    /**
     * W1 churn-storm defaults: drain-heavy, steady joins, some hard stops. EnumMap on purpose:
     * deterministic iteration order is what makes the weighted pick seed-replayable.
     */
    public static Map<ChaosAction, Integer> defaultW1Weights() {
        Map<ChaosAction, Integer> w = new EnumMap<>(ChaosAction.class);
        w.put(ChaosAction.STOP_DRAIN, 4);
        w.put(ChaosAction.STOP_NO_DRAIN, 2);
        w.put(ChaosAction.RESTART, 3);
        w.put(ChaosAction.JOIN_NEW, 1);
        return w;
    }

    /**
     * W4 revoke-under-work defaults: NO drain stops at all - hard stops, restarts and joins only. The
     * point is to force partition REVOCATIONS while heavy work is in flight without ever opening a
     * Class 1 drain-zombie window, isolating the protocol-invisible Class 2 stall mechanism (a member
     * that keeps heartbeating while its partitions' committed offsets freeze). EnumMap for seed-replayable
     * iteration order, same as W1.
     */
    public static Map<ChaosAction, Integer> defaultW4Weights() {
        Map<ChaosAction, Integer> w = new EnumMap<>(ChaosAction.class);
        w.put(ChaosAction.STOP_NO_DRAIN, 3);
        w.put(ChaosAction.RESTART, 3);
        w.put(ChaosAction.JOIN_NEW, 2);
        return w;
    }

    /**
     * The single draw path - used by BOTH the live {@link #loop()} and {@link #planTicks}, so the
     * determinism regression test ({@code ChaosConductorPlanIT}) exercises the exact production draw
     * sequence, bias and target rolls included.
     */
    static TickDraws drawTick(Random random, Duration minTick, Duration maxTick, Map<ChaosAction, Integer> weights) {
        long tickMs = minTick.toMillis()
                + (long) (random.nextInt(1000) / 1000.0 * (maxTick.toMillis() - minTick.toMillis()));
        double biasRoll = random.nextDouble();
        ChaosAction action = weightedPick(random, weights);
        int targetRoll = random.nextInt(Integer.MAX_VALUE);
        return new TickDraws(tickMs, biasRoll, action, targetRoll);
    }

    /** Pure function of the seed: the exact draw sequence a conductor with this seed consumes. */
    public static List<TickDraws> planTicks(long seed, int steps, Duration minTick, Duration maxTick,
                                            Map<ChaosAction, Integer> weights) {
        Random r = new Random(seed);
        List<TickDraws> plan = new ArrayList<>();
        for (int i = 0; i < steps; i++) {
            plan.add(drawTick(r, minTick, maxTick, weights));
        }
        return plan;
    }

    public void start() {
        running.set(true);
        startedAt = Instant.now();
        record("CONDUCTOR START seed=" + seed + " weights=" + weights + " tick=" + minTick + ".." + maxTick
                + " joinAfterDrainBias=" + joinAfterDrainBias, -1);
        conductorThread = new Thread(this::loop, "chaos-conductor");
        conductorThread.setDaemon(true);
        conductorThread.start();
    }

    public void stop() {
        running.set(false);
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

    private void loop() {
        boolean forceJoinNext = false;
        while (running.get()) {
            try {
                TickDraws draws = drawTick(random, minTick, maxTick, weights);
                Thread.sleep(draws.getTickMs());

                ChaosAction action = forceJoinNext && draws.getBiasRoll() < joinAfterDrainBias
                        ? ChaosAction.JOIN_NEW
                        : draws.getAction();
                forceJoinNext = false;

                switch (action) {
                    case STOP_DRAIN -> forceJoinNext = doStopDrain(draws.getTargetRoll());
                    case STOP_NO_DRAIN -> doStopNoDrain(draws.getTargetRoll());
                    case RESTART -> doRestart(draws.getTargetRoll());
                    case JOIN_NEW -> doJoin();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (Exception e) {
                record("CONDUCTOR ERROR: " + e, -1);
                log.error("Chaos conductor action failed", e);
            }
        }
    }

    private boolean doStopDrain(int targetRoll) {
        ManagedPCInstance victim = pickInState(InstanceState.RUNNING, targetRoll);
        if (victim == null) return false;
        states.put(victim.getInstanceId(), InstanceState.DRAINING);
        disturbanceCount.incrementAndGet();
        record("STOP_DRAIN", victim.getInstanceId());
        observer.onAction(victim.getInstanceId(), ChaosAction.STOP_DRAIN);
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

    /**
     * The conductor's own state for an instance. Package-private: a test asserting a refused restart
     * leaves the instance redrawable has to be able to see this, and {@code states} is otherwise
     * private with no accessor.
     */
    InstanceState stateOf(int instanceId) {
        return states.get(instanceId);
    }

    // doStopNoDrain and doRestart are package-private rather than private so ChaosConductorRestartRefusalIT
    // can drive them directly. loop() picks actions from a seeded RNG on its own thread, so reaching a
    // specific action from a test any other way means racing the draw sequence.
    void doStopNoDrain(int targetRoll) {
        ManagedPCInstance victim = pickInState(InstanceState.RUNNING, targetRoll);
        if (victim == null) return;
        states.put(victim.getInstanceId(), InstanceState.STOPPED);
        disturbanceCount.incrementAndGet();
        record("STOP_NO_DRAIN", victim.getInstanceId());
        observer.onAction(victim.getInstanceId(), ChaosAction.STOP_NO_DRAIN);
        victim.stopAsync();
    }

    void doRestart(int targetRoll) {
        ManagedPCInstance target = pickInState(InstanceState.STOPPED, targetRoll);
        if (target == null) return;
        // start() first: it can throw (the unexpected-failure-cause canary), in which case the
        // instance must stay STOPPED and untargetable rather than lying as RUNNING
        if (!target.start(pcExecutor)) {
            // an earlier restart is still queued behind its close-wait - leave it STOPPED and
            // redrawable rather than double-submitting it (see ManagedPCInstance#startInFlight)
            log.debug("[chaos] RESTART of instance {} refused - start still in flight", target.getInstanceId());
            return;
        }
        states.put(target.getInstanceId(), InstanceState.RUNNING);
        disturbanceCount.incrementAndGet();
        record("RESTART", target.getInstanceId());
        observer.onAction(target.getInstanceId(), ChaosAction.RESTART);
    }

    private void doJoin() {
        if (fleet.size() >= maxFleetSize) return;
        ManagedPCInstance recruit = instanceFactory.get();
        recruit.start(pcExecutor);
        fleet.add(recruit);
        states.put(recruit.getInstanceId(), InstanceState.RUNNING);
        record("JOIN_NEW", recruit.getInstanceId());
        observer.onAction(recruit.getInstanceId(), ChaosAction.JOIN_NEW);
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

    private static ChaosAction weightedPick(Random r, Map<ChaosAction, Integer> weights) {
        int total = weights.values().stream().mapToInt(Integer::intValue).sum();
        int pick = r.nextInt(total);
        int acc = 0;
        for (Map.Entry<ChaosAction, Integer> e : weights.entrySet()) {
            acc += e.getValue();
            if (pick < acc) return e.getKey();
        }
        return ChaosAction.RESTART; // unreachable
    }

    private void record(String what, int instanceId) {
        String entry = "t=+" + (startedAt == null ? "?" : Duration.between(startedAt, Instant.now()).toMillis() + "ms")
                + " " + what + (instanceId >= 0 ? " -> instance " + instanceId : "");
        timeline.add(entry);
        log.info("[chaos] {}", entry);
    }
}
