package bz.stub.parallelconsumer.conformance;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */


import bz.stub.parallelconsumer.proxy.harness.HarnessScenario;
import bz.stub.parallelconsumer.proxy.harness.ConformanceHarness;
import java.time.Duration;

/**
 * One named product behaviour, complete: what the engine seeds, what the client is prescribed to do about
 * it, and what must then be true.
 * <p>
 * <b>All three halves live here, on the JVM, in one place.</b> The seeds come from the engine-side
 * {@link HarnessScenario} that already owns them; the prescription is a closed {@link RunnerBehaviour}
 * token; the assertion is Java code with the harness in hand. A client's job shrinks to <em>doing what the
 * scenario says</em> - which is what makes ten languages agreeing with each other evidence rather than
 * coincidence, because none of them was asked what "correct" means.
 *
 * @author Antony Stubbs
 * @see ConformanceScenarios
 */
/* A plain class rather than a record - LanguageRunner's header says why, for the whole module. */
public final class ConformanceScenario {

    private final HarnessScenario harnessScenario;

    private final RunnerBehaviour behaviour;

    private final int expectedDispatches;

    private final int maxConcurrency;

    private final Duration runnerBudget;

    private final Assertion assertion;

    public ConformanceScenario(HarnessScenario harnessScenario, RunnerBehaviour behaviour,
                               int expectedDispatches, int maxConcurrency, Duration runnerBudget,
                               Assertion assertion) {
        this.harnessScenario = harnessScenario;
        this.behaviour = behaviour;
        this.expectedDispatches = expectedDispatches;
        this.maxConcurrency = maxConcurrency;
        this.runnerBudget = runnerBudget;
        this.assertion = assertion;
    }

    /** What the engine seeds for this scenario. */
    public HarnessScenario harnessScenario() {
        return harnessScenario;
    }

    /** What the binding is prescribed to do with each delivery. */
    public RunnerBehaviour behaviour() {
        return behaviour;
    }

    /** How many deliveries the scenario prescribes before the binding is finished. */
    public int expectedDispatches() {
        return expectedDispatches;
    }

    /** The in-flight ceiling the session is configured with. */
    public int maxConcurrency() {
        return maxConcurrency;
    }

    /** The binding's whole wall-clock budget for carrying out the prescription. */
    public Duration runnerBudget() {
        return runnerBudget;
    }

    /** What must be true once it has. */
    public Assertion assertion() {
        return assertion;
    }

    /**
     * A scenario whose in-flight ceiling is its own dispatch count - which is a ceiling nothing can reach,
     * and so no constraint at all.
     * <p>
     * <b>Every scenario written before the ceiling was testable is this one</b>, and the value is not
     * arbitrary: a scenario that holds a record must not deadlock on an executor count smaller than its own
     * shape, so its ceiling has to be at least as large as the number of records it prescribes. Making the
     * ceiling explicit is what let a scenario finally set it SMALLER and ask a client to prove it.
     */
    public ConformanceScenario(HarnessScenario harnessScenario, RunnerBehaviour behaviour,
                               int expectedDispatches, Duration runnerBudget, Assertion assertion) {
        this(harnessScenario, behaviour, expectedDispatches, expectedDispatches, runnerBudget, assertion);
    }

    /**
     * What must be true once the runner has done what it was told. It receives the harness - so it can read
     * engine state no client can see, which for the mock lane is the committed offset and the produced
     * records - and the transcript, so it can read what the client actually saw.
     * <p>
     * A real broker generalises the first half without changing this shape: the committed offset comes from
     * the Kafka <b>Admin API</b> ({@code listConsumerGroupOffsets}) and the produced records from a
     * verification consumer, in place of the mock consumer's commit history. Nothing on the client side of
     * the contract changes with it.
     */
    @FunctionalInterface
    public interface Assertion {
        void check(ConformanceHarness harness, RunnerTranscript transcript);
    }

    /** The scenario's stable name: its identity on the runner's command line, in the harness, and in every language's tests. */
    public String name() {
        return harnessScenario.name();
    }
}
