package io.confluent.parallelconsumer.integrationTests.chaostests.scenario;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

/**
 * Where a phase's ticks come from. Two implementations: {@link SeededPlanSource}, which draws them from
 * a seeded RNG (the chaos suite's replayable schedule), and {@link ScriptedPlanSource}, which replays a
 * fixed list (a captured schedule, or a hand-written one for a test).
 * <p>
 * This is the seam that was previously welded shut inside the conductor's loop. Splitting it is what
 * lets a scenario script its shape while still drawing its detail from a seed.
 */
public interface PlanSource {

    /** The next tick's complete draw set. Never null; a source that can run dry must say so by throwing. */
    ScenarioTick nextTick();

    /** One line for the run log, e.g. the weights and tick range, or the length of the scripted list. */
    String describe();
}
