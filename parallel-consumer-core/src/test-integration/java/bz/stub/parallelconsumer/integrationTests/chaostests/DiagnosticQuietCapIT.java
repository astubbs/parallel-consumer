package bz.stub.parallelconsumer.integrationTests.chaostests;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.time.Instant;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Coverage for {@link AbstractRevokeUnderWorkScenario#effectiveDiagnosticQuietCap}, the arithmetic
 * that stops {@code -Dchaos.diagnoseStallRecovery=true} promising a longer watch than the scenario's
 * own {@code @Timeout} can deliver.
 * <p>
 * <b>Why this needs a test rather than a careful reading.</b> The bug it fixes was silent by
 * construction: a 20-minute default under a 600s annotation meant JUnit killed the run
 * mid-observation, and a killed run is neither of the two outcomes the diagnostic exists to
 * distinguish. A regression here does not go red - it produces an uninterpretable diagnostic run,
 * which is exactly how the original defect survived being documented in its own constant's javadoc.
 * <p>
 * Broker-free: the method reads an annotation and subtracts {@link Duration}s, so the fixtures below
 * only need to be real annotated subclasses. Deliberately NOT tagged {@code chaos}, so it runs in
 * every default integration build, matching {@link Class2ObservationIT} and {@link InstanceStallProbeIT}.
 */
class DiagnosticQuietCapIT {

    /** The property the production path reads; the requested cap is resolved from it at class-init. */
    private static final Duration REQUESTED = Duration.ofMinutes(
            Integer.getInteger("chaos.diagnosticQuietCapMinutes", 20));
    private static final Duration TEARDOWN_RESERVE = Duration.ofSeconds(90);

    /** Carries the same 600s ceiling every real chaos scenario uses. */
    @Timeout(600)
    private static final class TimedScenario extends AbstractRevokeUnderWorkScenario {
        @Override
        protected String scenarioLabel() {
            return "diagnostic-cap-fixture";
        }

        @Override
        protected boolean useCooperativeAssignor() {
            return false;
        }
    }

    /** No ceiling to fit inside - the requested watch must stand unshortened. */
    private static final class UntimedScenario extends AbstractRevokeUnderWorkScenario {
        @Override
        protected String scenarioLabel() {
            return "diagnostic-cap-fixture-untimed";
        }

        @Override
        protected boolean useCooperativeAssignor() {
            return false;
        }
    }

    @Test
    void aFreshRunIsShortenedToWhatTheTimeoutActuallyAllows() {
        Duration cap = new TimedScenario().effectiveDiagnosticQuietCap(Instant.now());

        assertWithMessage("the 20-minute default cannot fit under a 600s @Timeout, so it must be cut - "
                + "leaving it whole is the original defect")
                .that(cap).isLessThan(REQUESTED);
        assertWithMessage("what is left must still be a usable watch, not a token remainder")
                .that(cap).isGreaterThan(Duration.ofMinutes(6));
        assertWithMessage("the watch must end inside the annotation, leaving the teardown reserve")
                .that(cap.plus(TEARDOWN_RESERVE)).isLessThan(Duration.ofSeconds(600));
    }

    @Test
    void anAlreadySpentBudgetRefusesToRunRatherThanWatchNothing() {
        Instant longAgo = Instant.now().minus(Duration.ofSeconds(595));

        IllegalStateException thrown = assertThrows(IllegalStateException.class,
                () -> new TimedScenario().effectiveDiagnosticQuietCap(longAgo));

        assertWithMessage("a diagnostic with no time left must say so rather than run and report "
                + "an outcome it never observed")
                .that(thrown).hasMessageThat().contains("no time left to watch");
        assertWithMessage("the message must name the number to raise @Timeout to, or the operator "
                + "is told it failed without being told the fix")
                .that(thrown).hasMessageThat().contains("Raise this scenario's @Timeout");
    }

    /**
     * The clamp must never silently extend a watch either - an absent ceiling means the request
     * stands exactly, which is what makes the shortening attributable to the annotation.
     */
    @Test
    void withNoTimeoutTheRequestedWatchStandsExactly() {
        Duration cap = new UntimedScenario().effectiveDiagnosticQuietCap(Instant.now());

        assertThat(cap).isEqualTo(REQUESTED);
    }
}
