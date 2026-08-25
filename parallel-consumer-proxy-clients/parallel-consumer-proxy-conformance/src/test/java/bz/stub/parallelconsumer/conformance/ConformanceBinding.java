package bz.stub.parallelconsumer.conformance;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.proxy.harness.ProxyHarness;

/**
 * One thing a scenario can be run against: the engine driven directly by a Java function, or a foreign
 * client driven through its conformance runner.
 * <p>
 * <b>One scenario definition, many bindings.</b> A scenario's seeds, prescription and assertions are written
 * once in {@link ConformanceScenarios} and executed once per binding. The same assertion executing many
 * times is the goal; the same assertion being <em>written</em> many times is the thing this interface exists
 * to prevent - eleven copies of "correct" would disagree, and the first time two of them did, nobody could
 * say which was wrong.
 * <p>
 * <b>{@link CoreBinding} is the control arm</b>, and that is the reason this interface is not simply
 * "a language runner". A scenario that fails against the engine driven by a plain Java function is a
 * <em>wrong scenario</em>, not a broken client - and without that arm the first suspect for a red Ruby run is
 * always Ruby.
 *
 * @author Antony Stubbs
 * @see ConformanceDriver
 */
public interface ConformanceBinding {

    /** How this binding is named in the matrix, in a failure message, and on the selector's command line. */
    String name();

    /**
     * Builds or locates whatever this binding needs, before the harness exists. Failing here FAILS the
     * scenario; there is no skip, because absence looks exactly like agreement.
     */
    default void ensureAvailable() {
    }

    /**
     * Runs the scenario's prescription against a started harness, and returns the run - which stays open
     * until the assertions have been made.
     */
    Run execute(ProxyHarness harness, ConformanceScenario scenario);

    /**
     * One binding's execution of one scenario, open for as long as the assertions need it.
     * <p>
     * <b>Closing is not cleanup, it is the end of the observation window.</b> A binding may still be holding
     * a record when the transcript is ready - {@code report-nothing} prescribes exactly that - and the
     * assertions have to run while it is held, or "the offset never advanced" is a claim about a client that
     * had already gone away. A foreign runner buys that window with the contract's 3s hold before it exits;
     * an in-process binding buys it by staying blocked until this is closed.
     */
    interface Run extends AutoCloseable {

        /** What the binding observed, and the verdict it exited with. */
        RunnerTranscript transcript();

        /** Ends the observation window: releases anything the binding is still holding. */
        @Override
        void close();
    }
}
