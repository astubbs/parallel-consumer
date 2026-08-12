package io.confluent.parallelconsumer.integrationTests.chaostests.scenario;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

/**
 * Everything a {@link ScenarioAction} is allowed to touch. The driver implements it; the action set is
 * written against it, which is what lets membership actions and workload actions coexist without either
 * knowing about the other's plumbing.
 * <p>
 * The control surfaces are deliberately unimplemented by default and throw a NAMED error rather than
 * returning null or a silent no-op: a scenario that schedules a workload action against a driver with no
 * workload wired should fail loudly at the first tick, not quietly demonstrate nothing.
 */
public interface ScenarioContext {

    /** Fleet membership control - stop, restart, join. */
    default FleetControl fleet() {
        throw new UnsupportedOperationException("this scenario context provides no fleet control - a membership "
                + "action was scheduled against a driver that manages no instances");
    }

    /** Workload control - publish rate, induced failures, user-function delay. */
    default WorkloadControl workload() {
        throw new UnsupportedOperationException("this scenario context provides no workload control - a workload "
                + "action was scheduled against a driver with no publisher or scripted function wired");
    }

    /**
     * Append to the run's timeline (and its log). A failing run's first artifact.
     *
     * @param instanceId the instance the entry concerns, or negative for a fleet-wide entry
     */
    void record(String what, int instanceId);
}
