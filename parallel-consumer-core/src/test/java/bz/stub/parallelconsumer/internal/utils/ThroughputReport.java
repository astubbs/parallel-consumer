/*
 * Copyright (C) 2026 Antony Stubbs and contributors
 */
package bz.stub.parallelconsumer.internal.utils;

import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.time.Instant;
import java.util.function.IntSupplier;
import java.util.function.Supplier;

/**
 * Emits one comparable throughput figure per performance run, on success <b>and</b> on failure.
 *
 * <p><b>Why this exists.</b> The performance suite gates on wall-clock deadlines, and a deadline is a
 * coin flip under load: the same tree passes or fails depending on what else the machine is doing, so
 * a red run says "slower than the bound here, today" and nothing more. That is not a property you can
 * compare between two trees - which is the only question a performance lane is actually asked.
 *
 * <p>It was not a hypothetical. A four-to-tenfold throughput regression sat in this repo's required
 * `Performance Tests` check for weeks, reported the whole time, and read as flakiness every time -
 * because a red timing lane and a busy runner produce the same signal. The lane detected it; the
 * lane's OUTPUT could not express it.
 *
 * <p><b>Report on green runs too.</b> A number recorded only when something fails cannot establish
 * what normal looks like, and a future recalibration has nothing to read. This repo states the rule
 * as "suppress the violation, never the measurement" -
 * {@code docs/solutions/best-practices/a-timing-bound-used-as-a-correctness-gate-manufactures-its-own-evidence.md}.
 *
 * <p><b>WARN, not INFO.</b> Both test log profiles pin {@code bz.stub.parallelconsumer} to
 * {@code warn}, so a figure emitted at INFO is invisible in exactly the CI run someone is trying to
 * read - see {@code docs/logging.md}. A measurement nobody can see has not been reported.
 *
 * <p>The line is deliberately one grep away: {@code grep PC-THROUGHPUT}, with {@code key=value}
 * fields so a collector does not have to parse prose.
 */
@UtilityClass
@Slf4j
public class ThroughputReport {

    /** The token a collector greps for. Changing it breaks any harness reading these runs. */
    public static final String MARKER = "PC-THROUGHPUT";

    /**
     * @param label     what was measured - conventionally the test or scenario name
     * @param processed records actually completed, whatever the outcome
     * @param expected  records the run was aiming at, so a partial run is visible as partial
     * @param started   when the measured window opened; the window closes now
     * @param context   free-form {@code key=value} fields naming the configuration, since a rate is
     *                  meaningless without the commit mode and ordering that produced it
     */
    public static void report(String label, long processed, long expected, Instant started, String context) {
        long elapsedMs = Duration.between(started, Instant.now()).toMillis();
        // Guard the divide rather than reporting a fiction: a run that finished inside the clock's
        // resolution has no meaningful rate, and -1 is easier to spot in a table than a huge number.
        long perSecond = elapsedMs > 0 ? (processed * 1000L) / elapsedMs : -1;
        log.warn("{} test={} processed={} expected={} elapsedMs={} recordsPerSecond={} {}",
                MARKER, label, processed, expected, elapsedMs, perSecond, context);
    }

    /**
     * Runs {@code wait} and reports the rate on BOTH exits - {@code outcome=PASSED} when it returns,
     * {@code outcome=FAILED} when it throws, with the failure rethrown unchanged.
     * <p>
     * <b>Extracted because four performance classes had grown the same fifteen lines.</b> Each captured
     * an {@link Instant}, wrapped its wait in a try, called a private {@code reportThroughput} on every
     * exit, and rethrew - and the file-similarity gate measured two of them converging from 54.8% to
     * 63.6% as the fourth copy landed. The shape is identical everywhere because the requirement is:
     * <em>a run that missed its deadline is the one whose rate a collector most wants</em>, and that is
     * a property of the measurement rather than of any one test.
     * <p>
     * <b>It catches {@link Throwable}, deliberately.</b> Awaitility's two failure exits -
     * {@code ConditionTimeoutException} and {@code TerminalFailureException} - are unrelated siblings,
     * and a test that catches only the timeout reports nothing for the failure mode that actually has a
     * named cause. An {@code AssertionError} from a {@code fail(...)} inside the wait is a third. Naming
     * them individually is how the gap reappears; catching the supertype is what makes "report on both
     * exits" true rather than aspirational.
     *
     * @param processed read AFTER the wait, so a failing run reports the count it actually reached
     */
    public static void reporting(String label, long expected, IntSupplier processed,
                                 Supplier<String> context, Runnable wait) {
        Instant started = Instant.now();
        try {
            wait.run();
        } catch (Throwable failure) {
            report(label, processed.getAsInt(), expected, started, context.get() + " outcome=FAILED");
            throw failure;
        }
        report(label, processed.getAsInt(), expected, started, context.get() + " outcome=PASSED");
    }
}
