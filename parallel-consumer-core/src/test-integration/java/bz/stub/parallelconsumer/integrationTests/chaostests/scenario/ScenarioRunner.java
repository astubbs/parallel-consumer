package bz.stub.parallelconsumer.integrationTests.chaostests.scenario;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import lombok.Builder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

/**
 * Walks a {@link Scenario}'s phases and applies their drawn actions through a {@link ScenarioContext}.
 * This is the tick loop that used to live inside {@code ChaosConductor}, generalised: the conductor now
 * supplies the fleet control surface and this drives it.
 * <p>
 * Two modes:
 * <ul>
 *   <li>{@link Mode#LOOP} repeats the phase list until {@link #stop()} - the demonstration mode, and the
 *       shape the chaos suite uses (one unbounded phase, stopped by the test).</li>
 *   <li>{@link Mode#ONCE} performs a single pass and reports failure if any phase's postcondition did not
 *       hold. This is what makes a scenario a test rather than only a demo.</li>
 * </ul>
 * Both modes log the seed and a replay command on start, matching the chaos suite's convention: a raw CI
 * log must be self-sufficient to reproduce the run.
 */
@Slf4j
public class ScenarioRunner {

    public enum Mode {
        /** Repeat the phase list until stopped. */
        LOOP,
        /** One pass; a failed postcondition is a run failure. */
        ONCE
    }

    /** Ceiling on retained postcondition failures - see {@link #checkPostcondition}. */
    private static final int FAILURE_LIMIT = 500;

    private final Scenario scenario;
    private final long seed;
    private final Mode mode;
    private final ScenarioContext context;
    /** Printed alongside the seed so a log reader can reproduce the run without knowing the harness. */
    private final String replayCommand;

    /** Postcondition failures, in phase order. Empty after a clean pass. */
    private final List<String> failures = Collections.synchronizedList(new ArrayList<>());
    @Getter
    private volatile boolean running;
    private volatile Thread runnerThread;

    @Builder
    public ScenarioRunner(Scenario scenario, long seed, Mode mode, ScenarioContext context, String replayCommand) {
        if (scenario == null) throw new IllegalArgumentException("a runner needs a scenario");
        if (context == null) throw new IllegalArgumentException("a runner needs a scenario context to act through");
        this.scenario = scenario;
        this.seed = seed;
        this.mode = mode == null ? Mode.LOOP : mode;
        this.context = context;
        this.replayCommand = replayCommand == null ? "-Dscenario.seed=" + seed : replayCommand;
        if (this.mode == Mode.ONCE && scenario.hasUnboundedPhase()) {
            throw new IllegalArgumentException("scenario '" + scenario.getName() + "' has an unbounded phase, so "
                    + "ONCE mode would never finish - give every phase a duration, or run it in LOOP mode");
        }
    }

    public List<String> getFailures() {
        synchronized (failures) {
            return new ArrayList<>(failures);
        }
    }

    /**
     * Run to completion (ONCE) or until {@link #stop()} (LOOP), on the calling thread.
     *
     * @return the postcondition failures; empty means the run produced everything it declared
     */
    public List<String> run() {
        running = true;
        runnerThread = Thread.currentThread();
        Random random = new Random(seed);
        context.record("SCENARIO START '" + scenario.getName() + "' mode=" + mode + " seed=" + seed
                + " phases=" + scenario.getPhases().size(), -1);
        log.info("=== scenario '{}' mode={} seed={} (replay: {}) ===", scenario.getName(), mode, seed, replayCommand);
        try {
            do {
                for (ScenarioPhase phase : scenario.getPhases()) {
                    if (!running) break;
                    runPhase(phase, random);
                }
            } while (mode == Mode.LOOP && running);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            running = false;
            runnerThread = null;
        }
        List<String> result = getFailures();
        context.record("SCENARIO END '" + scenario.getName() + "' seed=" + seed
                + (result.isEmpty() ? " - all postconditions held" : " - FAILURES: " + result), -1);
        if (!result.isEmpty()) {
            log.error("Scenario '{}' failed {} postcondition(s) (replay: {}): {}",
                    scenario.getName(), result.size(), replayCommand, result);
        }
        return result;
    }

    /** Stop after the current tick. Safe to call from another thread; interrupts a sleeping runner. */
    public void stop() {
        running = false;
        Thread t = runnerThread;
        if (t != null) t.interrupt();
    }

    private void runPhase(ScenarioPhase phase, Random random) throws InterruptedException {
        PlanSource plan = phase.newPlanSource(random);
        context.record("PHASE START: " + phase.getDescription()
                + " (" + (phase.getDuration() == null ? "until stopped" : phase.getDuration().toString())
                + ", " + plan.describe() + ")", -1);
        if (phase.getOnEnter() != null) {
            phase.getOnEnter().accept(context);
        }

        Instant end = phase.getDuration() == null ? null : Instant.now().plus(phase.getDuration());
        boolean biasArmed = false;
        while (running && (end == null || Instant.now().isBefore(end))) {
            try {
                ScenarioTick tick = plan.nextTick();
                Thread.sleep(clampToPhase(tick.getTickMs(), end));

                ScenarioAction action = biasArmed
                        && phase.getFollowOnAction() != null
                        && tick.getBiasRoll() < phase.getFollowOnProbability()
                        ? phase.getFollowOnAction()
                        : tick.getAction();
                // cleared BEFORE applying, deliberately: if apply throws, the bias must not stay armed
                biasArmed = false;
                biasArmed = action.apply(context, tick.getTargetRoll());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw e;
            } catch (Exception e) {
                context.record("SCENARIO ERROR: " + e, -1);
                log.error("Scenario action failed in phase '{}'", phase.getDescription(), e);
            }
        }

        checkPostcondition(phase);
    }

    /**
     * A tick longer than the phase's remaining time would overrun into the next phase, which for a short
     * demonstration phase means it never gets to act at all. Unbounded phases (the chaos suite's shape)
     * are untouched, so the historical timing is unchanged.
     */
    private long clampToPhase(long tickMs, Instant end) {
        if (end == null) return tickMs;
        long remaining = Duration.between(Instant.now(), end).toMillis();
        return Math.max(0, Math.min(tickMs, remaining));
    }

    private void checkPostcondition(ScenarioPhase phase) {
        if (phase.getPostcondition() == null) return;
        List<String> problems;
        try {
            problems = phase.getPostcondition().check(context);
        } catch (Exception e) {
            problems = Collections.singletonList("postcondition threw: " + e);
        }
        if (problems == null || problems.isEmpty()) {
            context.record("PHASE OK: " + phase.getDescription(), -1);
            return;
        }
        for (String problem : problems) {
            String failure = "phase '" + phase.getDescription() + "': " + problem;
            // bounded: a LOOP-mode demo runs for hours, and an always-failing postcondition would
            // otherwise grow this list without limit
            if (failures.size() < FAILURE_LIMIT) failures.add(failure);
            context.record("PHASE POSTCONDITION FAILED - " + failure, -1);
            log.error("Postcondition failed (seed={}, replay: {}): {}", seed, replayCommand, failure);
        }
    }
}
