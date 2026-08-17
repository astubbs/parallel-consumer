package bz.stub.parallelconsumer.integrationTests.chaostests.scenario;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

/**
 * One thing a scenario can do to a running system on a tick: stop an instance, join a new one, change
 * the publish rate, start failing a key, slow the user function.
 * <p>
 * This is the extension point that replaced the fixed {@code ChaosConductor.ChaosAction} enum. Anything
 * implementing it can appear in a {@link ScenarioPhase}'s weight map alongside the built-in
 * {@link MembershipAction}s, so a scenario declared entirely outside this package needs no change to
 * the framework (R30). Built-ins live in {@link MembershipAction} and {@link WorkloadActions}.
 * <p>
 * <b>Seed stability</b>: an action's identity never affects the draw stream - the number and order of
 * random draws per tick is fixed by {@link SeededPlanSource#drawTick}, and the weighted pick consumes
 * exactly one {@code nextInt(totalWeight)} regardless of which actions are in the map. What DOES affect
 * the stream is the map's total weight and its iteration order, so weight maps must have a deterministic
 * order (see {@link ScenarioPhase}).
 */
public interface ScenarioAction {

    /** Stable identifier, used in the timeline, the registry and scripted plans. Enums get this free. */
    String name();

    /**
     * Perform the action.
     *
     * @param targetRoll the tick's target draw - actions that choose among several candidates must
     *                   resolve it as {@code candidates.get(targetRoll % candidates.size())} so the
     *                   choice is a function of the seed, not of wall-clock ordering
     * @return {@code true} if this action arms the phase's follow-on bias for the next tick (see
     * {@link ScenarioPhase#getFollowOnAction()}). The chaos suite's join-after-drain bias is exactly
     * this: {@link MembershipAction#STOP_DRAIN} returns true when it actually fired.
     */
    boolean apply(ScenarioContext context, int targetRoll);
}
