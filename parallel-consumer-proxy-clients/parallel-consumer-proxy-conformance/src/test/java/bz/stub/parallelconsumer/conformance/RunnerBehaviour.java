package bz.stub.parallelconsumer.conformance;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

/**
 * What a scenario prescribes a runner to do with each record it is handed.
 * <p>
 * <b>The behaviour belongs to the scenario, not to the runner.</b> A runner free to invent its own
 * processing logic could satisfy any assertion by doing something adjacent to what was asked - succeeding
 * where the scenario needed a failure, reporting where it needed silence - and the assertion would then
 * be measuring the runner's imagination rather than the client's conformance. So the set is closed here,
 * a scenario names exactly one member, and a runner handed a token outside this set exits
 * {@link RunnerContract#EXIT_USAGE} rather than guessing.
 *
 * @author Antony Stubbs
 */
public enum RunnerBehaviour {

    /** Report every delivery as a success, with no produced output. */
    SUCCEED("succeed"),

    /**
     * Report nothing at all: take the delivery, never return an outcome for it, and abandon the session.
     * The record stays in flight with a worker that has gone away - which is what makes the negative
     * control a real one rather than a client that simply did nothing.
     */
    REPORT_NOTHING("report-nothing"),

    /**
     * Fail the first attempt with {@link RunnerContract#PRESCRIBED_FAILURE_REASON}, then succeed on every
     * later attempt. The redelivery is what carries the failure history the suite asserts on.
     */
    FAIL_THEN_SUCCEED("fail-then-succeed"),

    /**
     * Hold the first delivery - do not report it - until a second delivery arrives, then succeed both and
     * everything after. Holding is the instrument: what the engine dispatches while one record is
     * outstanding is exactly what the ordering guarantee is a claim about.
     */
    HOLD_FIRST_UNTIL_SECOND("hold-first-until-second"),

    /**
     * Hold every delivery until {@code --max-concurrency} of them are held at once; keep the full group held
     * for {@link RunnerContract#CEILING_SETTLE}; then report all of them as successes and begin the next
     * group. If a group never fills within the budget, the run has failed - {@link RunnerContract#EXIT_BEHAVIOUR_FAILED}.
     * <p>
     * <b>It is a two-sided instrument, and both sides are load-bearing.</b> Filling the group is what forces
     * the ceiling to be REACHED, so "never exceeded" is not the vacuous truth it would be for a client that
     * quietly ran one record at a time or stopped asking for work; the settle window is what makes an excess
     * VISIBLE rather than a race. A client that cannot get {@code max_concurrency} records into its user
     * function at once therefore times out and says so, and one that gets more prints the extra lines while
     * the group is still held.
     * <p>
     * A runner also releases whatever it holds once it has observed every dispatch the scenario prescribed,
     * so a final short group cannot deadlock a scenario whose record count is not a multiple of its ceiling.
     */
    HOLD_UNTIL_CEILING_FULL("hold-until-ceiling-full");

    private final String token;

    RunnerBehaviour(String token) {
        this.token = token;
    }

    /** The kebab-case spelling that travels on the command line, identical in every language. */
    public String token() {
        return token;
    }
}
