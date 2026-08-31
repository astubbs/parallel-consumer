package bz.stub.parallelconsumer.examples.streams.pc;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.junit.jupiter.api.Test;

import static bz.stub.parallelconsumer.examples.streams.pc.LatencyScenario.Expectation.NO_MATERIAL_DIFFERENCE;
import static bz.stub.parallelconsumer.examples.streams.pc.LatencyScenario.Expectation.PC_MUCH_FASTER;
import static bz.stub.parallelconsumer.examples.streams.pc.LatencyScenario.contradiction;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Pins the check that decides whether the demo may print its own verdict.
 * <p>
 * <b>This is the one decision in the demo that a healthy run never exercises.</b> Every arm passing means
 * every contradiction branch is dead code at runtime, and the demo lives behind {@code -Pdemo} so the
 * ordinary build never runs it either. The consequence is not hypothetical: the first version tested
 * {@code ratio > 1.0} for the headline, and a deliberately sabotaged run - the seam left off in the arm that
 * was meant to enable it - measured 1.01x, satisfied the check, and printed "a fast record no longer waits
 * for an unrelated slow one" over a run in which every fast record had waited. The cases below are that
 * failure, pinned.
 * <p>
 * Deliberately free of Docker and of Kafka, like {@link ClasspathGuardTest}.
 *
 * @author Antony Stubbs
 */
class LatencyScenarioTest {

    /**
     * The regression. A ratio barely above parity is what a run with the dispatch switch off measures, and
     * it must not be allowed to stand in for the headline claim.
     */
    @Test
    void aRatioBarelyAboveParityDoesNotSatisfyTheHeadlineClaim() {
        assertThat(contradiction(PC_MUCH_FASTER, 1.01))
                .isNotNull()
                .contains("1.01x");
    }

    @Test
    void pcLosingOutrightContradictsTheHeadlineClaim() {
        assertThat(contradiction(PC_MUCH_FASTER, 0.85)).isNotNull();
    }

    /**
     * The real measurements sit an order of magnitude above the threshold, so this is the case that must not
     * be flagged - a check that fired on a good run would be discarded by its readers within two runs.
     */
    @Test
    void aRealHeadOfLineResultSatisfiesTheHeadlineClaim() {
        assertThat(contradiction(PC_MUCH_FASTER, 13.27)).isNull();
    }

    /** Both sides of parity pass, because which arm wins a tie is decided by the machine. */
    @Test
    void theControlAcceptsEitherSideOfParity() {
        assertThat(contradiction(NO_MATERIAL_DIFFERENCE, 0.99)).isNull();
        assertThat(contradiction(NO_MATERIAL_DIFFERENCE, 1.01)).isNull();
        assertThat(contradiction(NO_MATERIAL_DIFFERENCE, 1.19)).isNull();
    }

    /**
     * The control has to flag a PC WIN as loudly as a PC loss. A win here would mean something is acting
     * that key concurrency cannot explain, which would void the headline rather than strengthen it - and an
     * asymmetric check is how that gets missed.
     */
    @Test
    void theControlFlagsALargeDifferenceInEitherDirection() {
        assertThat(contradiction(NO_MATERIAL_DIFFERENCE, 1.60)).isNotNull();
        assertThat(contradiction(NO_MATERIAL_DIFFERENCE, 0.60)).isNotNull();
    }
}
