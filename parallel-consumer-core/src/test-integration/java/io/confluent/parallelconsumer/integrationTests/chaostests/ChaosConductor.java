package io.confluent.parallelconsumer.integrationTests.chaostests;

/*-
 * Copyright (C) 2020-2026 Confluent, Inc. and contributors
 */

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
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/**
 * Seeded, replayable chaos scheduler for the Chaos Pain Suite (Phase 1 - see
 * {@code docs/plans/2026-07-30-002-feat-chaos-pain-suite-phase1-plan.md}).
 * <p>
 * Replaces the ad-hoc {@code Math.random()} chaos monkey with a deterministic action plan: same seed =
 * same action/target sequence (wall-clock interleaving still varies - the seed pins WHAT happens, load
 * pins exactly WHEN). Every executed action is appended to a timestamped {@link #getTimeline() timeline},
 * logged at execution and dumped wholesale by {@link #stop()} - a failing run's first artifact.
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
 * DRAIN stops. The conductor keeps its own authoritative per-instance state machine and never calls
 * {@code toggle()}, so the managed {@code started} flag (which only gates toggle) stays out of play.
 */
@Slf4j
public class ChaosConductor {

    public enum ChaosAction {STOP_DRAIN, STOP_NO_DRAIN, RESTART, JOIN_NEW}

    /** Conductor's authoritative view of each instance it manages. (Public: the truth-assertion
     * generator emits cross-package accessors for nested types it discovers.) */
    public enum InstanceState {RUNNING, DRAINING, STOPPED}

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
    @Getter
    private final List<String> timeline = Collections.synchronizedList(new ArrayList<>());
    private final AtomicBoolean running = new AtomicBoolean(false);
    private Thread conductorThread;
    private Instant startedAt;
    /** Observer hook for the probe: called with (instanceId, action) as each action executes. */
    private final ChaosObserver observer;

    public interface ChaosObserver {
        void onAction(int instanceId, ChaosAction action);
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
        this.weights = weights == null ? defaultW1Weights() : weights;
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
        Map<ChaosAction, Integer> w = new java.util.EnumMap<>(ChaosAction.class);
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
        Map<ChaosAction, Integer> w = new java.util.EnumMap<>(ChaosAction.class);
        w.put(ChaosAction.STOP_NO_DRAIN, 3);
        w.put(ChaosAction.RESTART, 3);
        w.put(ChaosAction.JOIN_NEW, 2);
        return w;
    }

    /**
     * Pure function of the seed: the action sequence a conductor with this seed would draw (targets not
     * resolved - target choice depends on live fleet state). Used by the determinism regression test.
     */
    public static List<ChaosAction> planActions(long seed, int steps, Map<ChaosAction, Integer> weights) {
        Random r = new Random(seed);
        List<ChaosAction> plan = new ArrayList<>();
        for (int i = 0; i < steps; i++) {
            r.nextInt(1000); // consumes the tick draw, mirroring the live loop's draw order
            plan.add(weightedPick(r, weights));
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
        record("CONDUCTOR STOP - timeline follows", -1);
        synchronized (timeline) {
            for (String entry : timeline) {
                log.info("[chaos-timeline] {}", entry);
            }
        }
    }

    /** All instances the conductor knows about (excluding the protected instance). */
    public List<ManagedPCInstance> getFleet() {
        return new ArrayList<>(fleet);
    }

    private void loop() {
        boolean forceJoinNext = false;
        while (running.get()) {
            try {
                long tickMs = minTick.toMillis()
                        + (long) (random.nextInt(1000) / 1000.0 * (maxTick.toMillis() - minTick.toMillis()));
                Thread.sleep(tickMs);

                ChaosAction action;
                if (forceJoinNext && random.nextDouble() < joinAfterDrainBias) {
                    action = ChaosAction.JOIN_NEW;
                } else {
                    action = weightedPick(random, weights);
                }
                forceJoinNext = false;

                switch (action) {
                    case STOP_DRAIN -> forceJoinNext = doStopDrain();
                    case STOP_NO_DRAIN -> doStopNoDrain();
                    case RESTART -> doRestart();
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

    private boolean doStopDrain() {
        ManagedPCInstance victim = pickInState(InstanceState.RUNNING);
        if (victim == null) return false;
        states.put(victim.getInstanceId(), InstanceState.DRAINING);
        record("STOP_DRAIN", victim.getInstanceId());
        observer.onAction(victim.getInstanceId(), ChaosAction.STOP_DRAIN);
        var pc = victim.getParallelConsumer();
        Thread drainer = new Thread(() -> {
            try {
                pc.closeDrainFirst();
            } catch (Exception e) {
                log.warn("Drain-close of instance {} threw (classified by restart path): {}",
                        victim.getInstanceId(), e.getMessage());
            } finally {
                states.put(victim.getInstanceId(), InstanceState.STOPPED);
                record("DRAIN_COMPLETE", victim.getInstanceId());
                observer.onAction(victim.getInstanceId(), null); // null action = drain finished marker
            }
        }, "chaos-drain-" + victim.getInstanceId());
        drainer.setDaemon(true);
        drainer.start();
        return true; // candidate for join-after-drain bias
    }

    private void doStopNoDrain() {
        ManagedPCInstance victim = pickInState(InstanceState.RUNNING);
        if (victim == null) return;
        states.put(victim.getInstanceId(), InstanceState.STOPPED);
        record("STOP_NO_DRAIN", victim.getInstanceId());
        observer.onAction(victim.getInstanceId(), ChaosAction.STOP_NO_DRAIN);
        victim.stopAsync();
    }

    private void doRestart() {
        ManagedPCInstance target = pickInState(InstanceState.STOPPED);
        if (target == null) return;
        states.put(target.getInstanceId(), InstanceState.RUNNING);
        record("RESTART", target.getInstanceId());
        observer.onAction(target.getInstanceId(), ChaosAction.RESTART);
        target.start(pcExecutor);
    }

    private void doJoin() {
        if (fleet.size() >= maxFleetSize) return;
        ManagedPCInstance recruit = instanceFactory.get();
        fleet.add(recruit);
        states.put(recruit.getInstanceId(), InstanceState.RUNNING);
        record("JOIN_NEW", recruit.getInstanceId());
        observer.onAction(recruit.getInstanceId(), ChaosAction.JOIN_NEW);
        recruit.start(pcExecutor);
    }

    private ManagedPCInstance pickInState(InstanceState wanted) {
        List<ManagedPCInstance> candidates = new ArrayList<>();
        for (ManagedPCInstance pc : fleet) {
            if (pc == protectedInstance) continue;
            if (states.get(pc.getInstanceId()) == wanted) candidates.add(pc);
        }
        if (candidates.isEmpty()) return null;
        return candidates.get(random.nextInt(candidates.size()));
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
