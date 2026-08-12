package io.confluent.parallelconsumer.integrationTests.chaostests.scenario;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * A plan source that replays a fixed list of ticks - the scripted counterpart to
 * {@link SeededPlanSource}. Use it to replay a captured schedule exactly, or to pin a specific sequence
 * in a test where a seeded draw would only add noise.
 * <p>
 * Not thread-safe by design: a plan source is consumed by the single scenario thread.
 */
public class ScriptedPlanSource implements PlanSource {

    private final List<ScenarioTick> ticks;
    /** True: wrap around at the end. False: running dry is an error, not a silent stop. */
    private final boolean cycle;
    private int cursor;

    public ScriptedPlanSource(List<ScenarioTick> ticks, boolean cycle) {
        if (ticks == null || ticks.isEmpty()) {
            throw new IllegalArgumentException("a scripted plan needs at least one tick");
        }
        this.ticks = new ArrayList<>(ticks);
        this.cycle = cycle;
    }

    public static ScriptedPlanSource cycling(ScenarioTick... ticks) {
        return new ScriptedPlanSource(Arrays.asList(ticks), true);
    }

    public static ScriptedPlanSource once(ScenarioTick... ticks) {
        return new ScriptedPlanSource(Arrays.asList(ticks), false);
    }

    @Override
    public ScenarioTick nextTick() {
        if (cursor >= ticks.size()) {
            if (!cycle) {
                throw new NoSuchElementException("scripted plan exhausted after " + ticks.size()
                        + " ticks - give the phase a shorter duration, more ticks, or make the plan cycling");
            }
            cursor = 0;
        }
        return ticks.get(cursor++);
    }

    @Override
    public String describe() {
        return "scripted " + ticks.size() + " tick(s)" + (cycle ? " cycling" : " once");
    }
}
