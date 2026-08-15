package bz.stub.parallelconsumer.conformance;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.proxy.harness.HarnessScenario;
import bz.stub.parallelconsumer.proxy.harness.ProxyHarness;
import com.github.bsideup.jabel.Desugar;

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
@Desugar // Jabel requires the annotation on every record, even in this module where release=17 makes it a no-op
public record ConformanceScenario(HarnessScenario harnessScenario, RunnerBehaviour behaviour,
                                  int expectedDispatches, Duration runnerBudget, Assertion assertion) {

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
        void check(ProxyHarness harness, RunnerTranscript transcript);
    }

    /** The scenario's stable name: its identity on the runner's command line, in the harness, and in every language's tests. */
    public String name() {
        return harnessScenario.name();
    }
}
