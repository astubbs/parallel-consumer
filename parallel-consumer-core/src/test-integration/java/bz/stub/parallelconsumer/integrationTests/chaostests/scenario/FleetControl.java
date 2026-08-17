package bz.stub.parallelconsumer.integrationTests.chaostests.scenario;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

/**
 * The membership half of a scenario driver's control surface - what {@link MembershipAction}s call.
 * Implemented by {@code ChaosConductor}, which owns the authoritative per-instance state machine; the
 * {@code targetRoll} arguments are the tick's seeded target draw, resolved against whichever instances
 * are currently in the wanted state.
 */
public interface FleetControl {

    /**
     * Stop an instance draining first (finish in-flight work, then close).
     *
     * @return {@code true} if a stop actually fired - the join-after-drain bias only arms on a real drain
     */
    boolean stopDrain(int targetRoll);

    /** Stop an instance without draining - a hard stop that abandons in-flight work. */
    void stopNoDrain(int targetRoll);

    /** Restart a stopped instance. */
    void restart(int targetRoll);

    /** Add a brand-new member to the group, up to the driver's fleet ceiling. */
    void joinNew();
}
