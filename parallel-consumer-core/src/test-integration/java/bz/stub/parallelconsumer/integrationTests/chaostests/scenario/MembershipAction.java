package io.confluent.parallelconsumer.integrationTests.chaostests.scenario;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

/**
 * The consumer-group membership action set the Chaos Pain Suite has always had, now expressed as
 * {@link ScenarioAction}s so workload actions can sit beside them in the same weight map.
 * <p>
 * <b>Declaration order is load-bearing and must not change.</b> The chaos weight maps are
 * {@link java.util.EnumMap}s, so they iterate in the order below, and
 * {@link SeededPlanSource#weightedPick} walks that order to turn one {@code nextInt(total)} draw into an
 * action. Reordering these constants would silently re-map every previously recorded chaos seed onto a
 * different schedule - which no test would go red for. See {@code PlanSourceSeedStabilityTest}.
 */
public enum MembershipAction implements ScenarioAction {

    /** Finish in-flight work, then close. The only action that arms the follow-on bias. */
    STOP_DRAIN {
        @Override
        public boolean apply(ScenarioContext context, int targetRoll) {
            return context.fleet().stopDrain(targetRoll);
        }
    },
    /** Hard stop - abandons in-flight work, forcing revocation while work is running. */
    STOP_NO_DRAIN {
        @Override
        public boolean apply(ScenarioContext context, int targetRoll) {
            context.fleet().stopNoDrain(targetRoll);
            return false;
        }
    },
    RESTART {
        @Override
        public boolean apply(ScenarioContext context, int targetRoll) {
            context.fleet().restart(targetRoll);
            return false;
        }
    },
    JOIN_NEW {
        @Override
        public boolean apply(ScenarioContext context, int targetRoll) {
            context.fleet().joinNew();
            return false;
        }
    };

    static {
        ScenarioActions.registerAll(values());
    }
}
