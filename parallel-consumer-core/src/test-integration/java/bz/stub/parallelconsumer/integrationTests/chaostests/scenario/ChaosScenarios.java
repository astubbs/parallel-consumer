package bz.stub.parallelconsumer.integrationTests.chaostests.scenario;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import java.time.Duration;
import java.util.EnumMap;
import java.util.Map;

/**
 * The Chaos Pain Suite's own configurations, expressed as single-phase {@link Scenario}s. This is what
 * makes the chaos suite a CONSUMER of the scenario framework rather than a special case inside the
 * driver: the weights, tick range and follow-on bias that used to be chained onto the conductor's builder
 * are declared here, in one place, next to the reasoning for their values.
 * <p>
 * Both phases are unbounded ({@code duration == null}): the chaos scenarios run until their own test's
 * await completes and then stop the driver, exactly as they always have. That also means they are
 * {@link ScenarioRunner.Mode#LOOP}-only, which is correct - a chaos run is not a single deterministic
 * sweep.
 * <p>
 * <b>The weight maps are {@link EnumMap}s on purpose.</b> Deterministic iteration order is half of what
 * makes the weighted pick seed-replayable (the other half is the total). Changing a weight, or the
 * declaration order of {@link MembershipAction}, re-maps every previously recorded chaos seed onto a
 * different schedule.
 */
public final class ChaosScenarios {

    private ChaosScenarios() {
    }

    /** W1 churn-storm defaults: drain-heavy, steady joins, some hard stops. */
    public static Map<MembershipAction, Integer> w1Weights() {
        Map<MembershipAction, Integer> w = new EnumMap<>(MembershipAction.class);
        w.put(MembershipAction.STOP_DRAIN, 4);
        w.put(MembershipAction.STOP_NO_DRAIN, 2);
        w.put(MembershipAction.RESTART, 3);
        w.put(MembershipAction.JOIN_NEW, 1);
        return w;
    }

    /**
     * W4 revoke-under-work defaults: NO drain stops at all - hard stops, restarts and joins only. The
     * point is to force partition REVOCATIONS while heavy work is in flight without ever opening a Class 1
     * drain-zombie window, isolating the protocol-invisible Class 2 stall mechanism (a member that keeps
     * heartbeating while its partitions' committed offsets freeze).
     */
    public static Map<MembershipAction, Integer> w4Weights() {
        Map<MembershipAction, Integer> w = new EnumMap<>(MembershipAction.class);
        w.put(MembershipAction.STOP_NO_DRAIN, 3);
        w.put(MembershipAction.RESTART, 3);
        w.put(MembershipAction.JOIN_NEW, 2);
        return w;
    }

    /**
     * W1: membership churn with a deliberate join-after-stopDrain bias at 0.9 - the zombie-drain defect
     * class bites hardest when a member joins while another is mid-drain, so the bias is the calibration
     * tuning knob (probes get tuned via the scenario, never loosened).
     */
    public static Scenario churnStorm() {
        return Scenario.of("W1 churn storm", ScenarioPhase.builder()
                .description("membership churn: drain stops with a join-after-drain bias, hard stops, restarts, joins")
                .minTick(Duration.ofMillis(500))
                .maxTick(Duration.ofMillis(1500))
                .weights(w1Weights())
                .followOnAction(MembershipAction.JOIN_NEW)
                .followOnProbability(0.9)
                .build());
    }

    /**
     * W4: faster ticks than W1 - more rebalances per run means more revoke-under-work collisions - and no
     * follow-on bias, because there are no drains to bias after.
     */
    public static Scenario revokeUnderWork() {
        return Scenario.of("W4 revoke under work", ScenarioPhase.builder()
                .description("revocation storm: hard stops, restarts and joins only - no drains, ever")
                .minTick(Duration.ofMillis(300))
                .maxTick(Duration.ofMillis(1000))
                .weights(w4Weights())
                .followOnProbability(0)
                .build());
    }
}
