package bz.stub.parallelconsumer.integrationTests.chaostests;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Coverage for {@link DiagnosticQuietCap#within}, the arithmetic that stops
 * {@code -Dchaos.diagnoseStallRecovery=true} promising a longer watch than the scenario's own
 * {@code @Timeout} can deliver.
 * <p>
 * <b>Why this needs a test rather than a careful reading.</b> The bug it fixes was silent by
 * construction: a 20-minute default under a 600s annotation meant JUnit killed the run
 * mid-observation, and a killed run is neither of the two outcomes the diagnostic exists to
 * distinguish. A regression here does not go red - it produces an uninterpretable diagnostic run,
 * which is exactly how the original defect survived being documented in its own constant's javadoc.
 * <p>
 * <b>Genuinely broker-free, and that took a code change to be true.</b> Everything else in this
 * package descends from {@code BrokerIntegrationTest}, whose static initialiser calls
 * {@code kafkaContainer.start()}, so instantiating any scenario subclass boots Kafka under
 * Testcontainers. {@link DiagnosticQuietCap} is deliberately outside that hierarchy, so touching
 * it reaches none of that - see its javadoc for the two failed attempts that established a static
 * method on the scenario is NOT enough (initialising a subclass initialises its superclasses).
 * Deliberately untagged, so it gates every default integration build.
 */
class DiagnosticQuietCapIT {

    /** The 600s every real chaos scenario carries. */
    private static final Duration CEILING = Duration.ofSeconds(600);
    /** Held back for teardown by the production constant; the assertions below only rely on it being non-zero. */
    private static final Duration TEARDOWN_RESERVE = Duration.ofSeconds(90);
    private static final String SCENARIO = "FixtureScenario";

    @Test
    void aFreshRunIsShortenedToWhatTheTimeoutActuallyAllows() {
        Duration requested = Duration.ofMinutes(20);
        Duration cap = DiagnosticQuietCap.within(CEILING, Duration.ofSeconds(30), SCENARIO);

        assertWithMessage("a 20-minute watch cannot fit under a 600s @Timeout, so it must be cut - "
                + "returning it whole is the original defect this method exists to prevent")
                .that(cap).isLessThan(requested);
        assertWithMessage("the watch plus what was already spent plus the teardown reserve must fit "
                + "inside the ceiling, or JUnit still kills the run")
                .that(Duration.ofSeconds(30).plus(cap).plus(TEARDOWN_RESERVE)).isAtMost(CEILING);
        assertWithMessage("exactly the remaining budget should be used - a shorter watch than the "
                + "ceiling allows throws away observation time for nothing")
                .that(cap).isEqualTo(CEILING.minus(Duration.ofSeconds(30)).minus(TEARDOWN_RESERVE));
    }

    @Test
    void anAlreadySpentBudgetRefusesToRunRatherThanWatchNothing() {
        IllegalStateException thrown = assertThrows(IllegalStateException.class,
                () -> DiagnosticQuietCap.within(CEILING, Duration.ofSeconds(595), SCENARIO));

        assertWithMessage("a diagnostic with no time left must say so rather than run and report an "
                + "outcome it never observed")
                .that(thrown).hasMessageThat().contains("no time left to watch");
        assertWithMessage("the message must name the number to raise @Timeout to, or the operator is "
                + "told it failed without being told the fix")
                .that(thrown).hasMessageThat().contains("Raise this scenario's @Timeout");
    }

    /**
     * The boundary: a budget that lands exactly on zero is refused too. Zero seconds of observation
     * produces the same uninterpretable "neither drained nor did not drain" result the throw exists
     * to prevent, so {@code isZero} must be handled alongside {@code isNegative}.
     */
    @Test
    void aBudgetOfExactlyZeroIsRefusedRatherThanRunForNoTime() {
        Duration everythingButTheReserve = CEILING.minus(TEARDOWN_RESERVE);

        assertThrows(IllegalStateException.class,
                () -> DiagnosticQuietCap.within(CEILING, everythingButTheReserve, SCENARIO));
    }

    /**
     * No ceiling means nothing to fit inside, so the request stands untouched - which is what makes
     * any shortening attributable to the annotation rather than to this method having an opinion.
     */
    @Test
    void withNoTimeoutTheRequestedWatchStandsExactly() {
        Duration cap = DiagnosticQuietCap.within(null, Duration.ofSeconds(30), SCENARIO);

        assertThat(cap).isEqualTo(Duration.ofMinutes(
                Integer.getInteger("chaos.diagnosticQuietCapMinutes", 20)));
    }
}
