package bz.stub.parallelconsumer.integrationTests.chaostests.scenario;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import lombok.Value;

/**
 * One tick's complete draw set, consumed from the plan source before the tick acts. (Formerly
 * {@code ChaosConductor.TickDraws} - same four fields, same order, same meaning.)
 * <p>
 * Consuming every draw every tick - used or not - is what makes a seeded stream replayable: without it
 * the number of draws would depend on live system state (bias armed? candidates available?), which is
 * mutated by wall-clock-timed completions, and two runs of the same seed could silently desynchronise.
 * The REALIZED action/target is the draw filtered through live state, so wall-clock timing pins what a
 * draw lands on - never which draws occur, and never how many.
 */
@Value
public class ScenarioTick {

    /** How long to wait before acting on this tick. */
    long tickMs;

    /** Drawn in {@code [0,1)}; compared against the phase's follow-on bias probability. */
    double biasRoll;

    /** The action drawn from the phase's weights, before the follow-on bias may override it. */
    ScenarioAction action;

    /** Non-negative; actions resolve it modulo their candidate count. */
    int targetRoll;
}
