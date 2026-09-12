package bz.stub.parallelconsumer.fluent;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.apache.kafka.common.annotation.InterfaceStability;

/**
 * The values of the {@code outcome} tag on the per-route record counter: the terminal outcomes of R7, plus the stop
 * request of R24, which is not a terminal outcome of the record but is counted beside them.
 * <p>
 * An enum rather than four string constants and an array of them, because the set is closed and the compiler can
 * say so: {@link #values()} is the list every route's counters are pre-registered from, and a call site cannot ask
 * for a tag value that has no counter.
 * <p>
 * <b>Public because an enum in this module cannot be anything else.</b> The Truth assertion generator sweeps every
 * enum in core - {@code ConsumerOwnership.Phase} and the four already in this package all have a generated
 * {@code Subject} - and writes a {@code that(...)} overload for each into {@code ManagedTruth} and
 * {@code ManagedSubjectBuilder}, which live in the parent package. A package-private enum, or one nested in a
 * package-private class such as {@code FluentMeters}, is therefore named by generated code that cannot see it, and
 * the build fails in {@code ManagedSubjectBuilder} with a name clash rather than with the real access error.
 * Measured on this branch, both ways, 2026-09-11. The plugin has no exclusion parameter, and narrowing its
 * recursion is not an option - it would collapse the generated set and break chained assertions across the suite.
 * <p>
 * Being public is no loss: these four strings are what a dashboard queries on, so naming them is a contract this
 * package owes its users rather than an implementation detail leaking out.
 */
@InterfaceStability.Unstable
public enum OutcomeTag {

    /**
     * The route processed the record ({@link Outcome#succeeded()}, and the produce outcomes, which succeed once
     * their records have been sent).
     */
    SUCCEEDED("succeeded"),

    /**
     * Records the route deliberately skipped ({@link Outcome#filtered()}). Counted apart from {@link #SUCCEEDED}
     * although both complete and commit, because "nothing happened to it" and "it was processed" are different
     * answers to the only question an operator is asking (R8).
     */
    FILTERED("filtered"),

    /**
     * Records that ran out of attempts and park cycles, or that a function parked outright. This is the count of
     * park <em>events</em> and it never goes down; the size of the set an operator can act on is a gauge.
     */
    PARKED("parked"),

    /**
     * Counted against the record whose route asked the instance to stop (R24). Not a terminal outcome of that
     * record - it is left incomplete - but it is counted here because the tag answers "what became of a record on
     * this topic", and an instance that stopped has exactly one of these.
     */
    STOPPED("stopped");

    /**
     * What a dashboard sees. Carried on the constant rather than derived from {@link #name()} so that renaming a
     * constant cannot silently rename a tag somebody is querying on.
     */
    private final String tagValue;

    OutcomeTag(String tagValue) {
        this.tagValue = tagValue;
    }

    /**
     * @return the {@code outcome} tag value this constant is counted under
     */
    public String tagValue() {
        return tagValue;
    }
}
