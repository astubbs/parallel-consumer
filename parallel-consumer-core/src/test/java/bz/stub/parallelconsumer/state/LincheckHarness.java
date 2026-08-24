package bz.stub.parallelconsumer.state;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.lincheck.LincheckAssertionError;
import org.jetbrains.lincheck.datastructures.Options;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Shared plumbing for the Lincheck lane, so that the individual harnesses hold only the operations of the
 * class under test and the bounds they were run at.
 * <p>
 * The one non-obvious job here is {@link #runExpectingViolation}. Lincheck signals BOTH a real violation and
 * <em>its own internal crash</em> by throwing {@link LincheckAssertionError}, so a naive
 * {@code assertThrows(LincheckAssertionError.class, ...)} passes when Lincheck fell over having verified
 * nothing - the exact false-positive this whole evaluation exists to avoid. This method separates them.
 * <p>
 * <b>There are deliberately no {@code ManagedStrategyGuarantee} helpers here.</b> Three were written during
 * the proof of concept - excluding this project's Lombok value types, the micrometer stack, and the two
 * {@code PartitionState} accessors built on {@code parallelStream()} - and every one of them was removed with
 * its measurement, because no model-checking arm over the product classes survived into this lane at all.
 * What each was for and what it bought is in {@code docs/plans/2026-08-25-001-test-lincheck-poc-plan.md};
 * bring one back with the arm that needs it, not before.
 *
 * @author Antony Stubbs
 */
@Slf4j
@UtilityClass
public class LincheckHarness {

    /**
     * Lincheck's banner when it crashes inside itself rather than finding a bug in the code under test.
     */
    private static final String INTERNAL_CRASH_BANNER = "You've caught a bug in Lincheck";

    /**
     * Every failure report Lincheck produces opens with a {@code = ... =} headline naming the failure kind
     * ("Invalid execution results", "The execution failed with an unexpected exception", and so on). Its
     * presence is what distinguishes a real verdict from a stack trace.
     */
    private static final String VERDICT_HEADLINE = "\n= ";

    /**
     * The report headline Lincheck prints for a linearizability violation.
     */
    private static final String INVALID_RESULTS_HEADLINE = "= Invalid execution results =";

    /**
     * Runs a configured Lincheck check that is EXPECTED to find a violation, and returns the report.
     * <p>
     * Lincheck can deliver a found violation down TWO paths, and the second is easy to mistake for a tool
     * failure:
     * <ol>
     *   <li>the clean one - {@link LincheckAssertionError} carrying the minimal interleaving;</li>
     *   <li>an {@link IllegalStateException} reading "Non-determinism found", when the model checker replayed
     *       the failing interleaving and got a DIFFERENT wrong answer the second time. Its message still
     *       contains both Invalid-execution-results reports and the full trace, so the violation is real and
     *       fully evidenced - Lincheck simply refuses to minimise a scenario it cannot replay. Two different
     *       wrong answers from one interleaving is, if anything, a stronger statement about a torn read.</li>
     * </ol>
     * Anything else - above all Lincheck's own internal crash, which is thrown as a
     * {@link LincheckAssertionError} too - must NOT count as a finding, because it means nothing was verified.
     *
     * @param label what is being checked, for the log line that carries the interleaving
     * @param check the configured {@code new ModelCheckingOptions()...check(TheTest.class)} call
     * @return Lincheck's failure report, including the minimal reproducing interleaving
     */
    public static String runExpectingViolation(String label, Runnable check) {
        Throwable thrown = assertThrows(Throwable.class, check::run,
                label + ": Lincheck completed WITHOUT finding a violation. On this tree that means either the "
                        + "bug is gone (invert this test) or the harness is not exercising it.");

        boolean cleanVerdict = thrown instanceof LincheckAssertionError;
        boolean verdictInsideNonDeterminismAbort = thrown instanceof IllegalStateException
                && String.valueOf(thrown.getMessage()).contains(INVALID_RESULTS_HEADLINE);
        assertWithMessage("%s: Lincheck threw something that is not a verdict at all, so nothing was verified",
                label)
                .that(cleanVerdict || verdictInsideNonDeterminismAbort)
                .isTrue();

        String report = thrown.getMessage();
        assertThat(report).doesNotContain(INTERNAL_CRASH_BANNER);
        assertThat(report).contains(VERDICT_HEADLINE);

        // Logged, not asserted: the interleaving is the deliverable of a run like this, and it is only
        // readable when the harness is turned up - bin/lincheck-test.sh passes -Dpc.log.level=info.
        log.info("{} - Lincheck found a violation ({}):\n{}", label,
                cleanVerdict ? "clean verdict" : "reported via the non-determinism abort path", report);
        return report;
    }

    /**
     * Lincheck's own {@code Options.check} throws whatever the JUnit assertion mechanism is; wrapping it in a
     * {@link Runnable} keeps the call sites reading as one expression.
     */
    public static Runnable check(Options<?, ?> options, Class<?> testClass) {
        return () -> options.check(testClass);
    }
}
