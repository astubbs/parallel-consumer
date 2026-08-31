package bz.stub.parallelconsumer.conformance;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import java.time.Duration;

/**
 * The cross-language runner contract, in one place: the flags a conformance runner takes, the exit
 * statuses it may return, the single line it prints per delivery, and the two fixed literals a scenario's
 * behaviour depends on.
 * <p>
 * <b>Every language implements exactly this.</b> The prose form, written for someone about to write the
 * next runner, is this module's {@code README.md}; these constants are the same contract in the form the
 * suite can actually hold itself to, so a drift between the two shows up as a failing scenario rather
 * than as documentation nobody re-read.
 * <p>
 * <b>The verdict channel is the exit status.</b> There is deliberately no results file, no report message
 * and no per-language codegen for test data: carrying test results over a wire is the whole wire problem
 * again, multiplied by ten languages, to say something an exit status already says. Everything the suite
 * knows about engine state - committed offsets, produced records - it reads from the harness it is
 * hosting, in this JVM.
 *
 * @author Antony Stubbs
 * @see ConformanceScenarios
 */
public final class RunnerContract {

    /** The scenario's name, which is also the topic the runner subscribes to. */
    public static final String FLAG_SCENARIO = "--scenario";

    /** Which prescribed behaviour to apply to each delivery - see {@link RunnerBehaviour}. */
    public static final String FLAG_BEHAVIOUR = "--behaviour";

    /** The absolute path of the sidecar command the client library must spawn. */
    public static final String FLAG_SIDECAR = "--sidecar";

    /** How many deliveries the scenario prescribes before the runner exits. */
    public static final String FLAG_EXPECT_DISPATCHES = "--expect-dispatches";

    /**
     * The in-flight ceiling the runner configures on its session, and the only thing it may set
     * {@code max_concurrency} from.
     * <p>
     * <b>It is a flag rather than a runner's own choice because a scenario has to be able to make the ceiling
     * BIND.</b> Every runner used to derive it from {@link #FLAG_EXPECT_DISPATCHES}, which by construction is
     * a ceiling no scenario can ever reach - so no scenario could ask a client to prove it respected one.
     * Passing it explicitly costs the four older scenarios nothing: they are driven with it equal to their
     * expected dispatch count, which is exactly what the runners hard-coded before.
     */
    public static final String FLAG_MAX_CONCURRENCY = "--max-concurrency";

    /** The runner's whole wall-clock budget; exceeding it without completing is {@link #EXIT_BEHAVIOUR_FAILED}. */
    public static final String FLAG_TIMEOUT_SECONDS = "--timeout-seconds";

    /** The prescribed behaviour completed. The suite's own assertions then decide whether the scenario passed. */
    public static final int EXIT_OK = 0;

    /** The runner could not do what was prescribed: it failed to connect, or the budget elapsed first. */
    public static final int EXIT_BEHAVIOUR_FAILED = 1;

    /** The caller asked for something this runner does not do: a missing flag, an unknown behaviour token. */
    public static final int EXIT_USAGE = 2;

    /** Every delivery produces one stdout line beginning with this. Anything else on stdout is ignored. */
    public static final String DISPATCH_LINE_PREFIX = "dispatch ";

    /**
     * Every delivery whose outcome the runner decided produces a second line, beginning with this, at the
     * moment it was decided. A record the prescription never resolves - {@link RunnerBehaviour#REPORT_NOTHING}
     * is the whole of that case - never gets one, which is what makes its absence meaningful.
     * <p>
     * <b>It is what lets the suite see OVERLAP rather than only arrival.</b> A dispatch line alone says a
     * record turned up; a dispatch and a settled line together bound the window in which it was
     * <em>unresolved</em>, and the running difference between the two counts, read in line order, is the
     * number of records outstanding at that instant. No clock is involved: stdout is one serialized stream,
     * so the order of the lines IS the order of the events - which the ordering scenario already relied on
     * for its dispatch lines.
     */
    public static final String SETTLED_LINE_PREFIX = "settled ";

    /**
     * The exact failure text {@link RunnerBehaviour#FAIL_THEN_SUCCEED} reports, so the suite can assert the
     * redelivery carried it back verbatim. A fixed literal of the contract in every language - never a
     * message a runner composes, because a composed message is one each language would compose differently
     * and the assertion would have to weaken to accommodate them.
     */
    public static final String PRESCRIBED_FAILURE_REASON = "conformance-prescribed-failure";

    /**
     * Fixed session tunables every runner sets. They are contract, not runner judgement: they exist only so
     * scenarios converge at unit-test speed against the engine's production defaults (a 5s commit interval,
     * a 1s retry delay), and a runner free to pick its own would make the suite's convergence budgets mean
     * something different in each language.
     */
    public static final Duration COMMIT_INTERVAL = Duration.ofMillis(100);

    /** @see #COMMIT_INTERVAL */
    public static final Duration RETRY_DELAY = Duration.ofMillis(50);

    /**
     * How long {@link RunnerBehaviour#HOLD_UNTIL_CEILING_FULL} keeps a full group held after it has filled,
     * before releasing it.
     * <p>
     * <b>It is what turns "the ceiling was never exceeded" from a race into a measurement.</b> Release the
     * group the instant it fills and a client that told the engine a larger ceiling still passes: its extra
     * records arrive a few milliseconds later, by which time the group has gone and the outstanding count
     * has already fallen back. Holding the full ceiling still for a fixed window means the extra dispatch a
     * broken client provokes arrives INSIDE that window, prints its line while every other record is still
     * unresolved, and shows up as a peak above the ceiling. A correct engine cannot dispatch anything during
     * the window at all - the ceiling is full - so the wait costs a conforming client nothing but time.
     */
    public static final Duration CEILING_SETTLE = Duration.ofMillis(250);

    private RunnerContract() {
    }
}
