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
    HOLD_FIRST_UNTIL_SECOND("hold-first-until-second");

    private final String token;

    RunnerBehaviour(String token) {
        this.token = token;
    }

    /** The kebab-case spelling that travels on the command line, identical in every language. */
    public String token() {
        return token;
    }
}
