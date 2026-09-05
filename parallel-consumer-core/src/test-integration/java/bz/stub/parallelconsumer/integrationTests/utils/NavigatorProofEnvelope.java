package bz.stub.parallelconsumer.integrationTests.utils;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.navigator.ResourceContract;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.data.Percentage.withPercentage;

/**
 * The partition-share proof's acceptance envelope (the plan's R11, U5 step 1): every number that turns "about
 * 1Hz" into a pass or a fail, declared in ONE place before any assertion was written, and read by the asserted
 * twin ({@code NavigatorPartitionShareIT}), the churn ladder (U6) and the two-JVM demo (U7) alike - so the three
 * cannot drift into three meanings of "about".
 * <p>
 * <b>Calibration status (KTD13).</b> Calibrated on the implementing machine (an Apple Silicon laptop, one
 * TestContainers broker, three to five child JVMs); every value below is written as the CI runner's
 * number-to-confirm, and the first hosted-runner run of the lane is where it is confirmed or re-derived. If a
 * value has to move, it moves HERE, with the observation that moved it in this javadoc - never in a test.
 * <p>
 * <b>Why a count tolerance is honest at all.</b> The demo policy phase-locks firings to quantum starts: an
 * instance spends its share the moment the quantum mints it. A window anchored at an observed firing therefore
 * contains {@code W/q} to {@code W/q + 1} quantum starts, and a share whose remainder rotates (three quarters
 * mints 2,2,1,1) is exact only over whole rotation periods - which is why {@link #WINDOW_QUANTA} is a common
 * multiple of every partition total the proof uses. The residual jitter is one credit for the anchor's own
 * quantum plus one for a credit lost to a control-loop pause spanning a boundary; the tolerance must absorb
 * that at the SMALLEST expected count in the proof, and must still separate every adjacent hypothesis at
 * every count. The table the tolerance was chosen against (window 12 quanta, rate 2/s):
 * <pre>
 *   shape            expected  band at 30%   adjacent hypotheses excluded
 *   half share           12    [8.4, 15.6]   full rate 24, quarter 6, unthrottled hundreds
 *   full rate (survivor) 24    [16.8, 31.2]  stuck at the old half 12
 *   three quarters       18    [12.6, 23.4]  equal split 12, full rate 24
 *   one quarter           6    [4.2, 7.8]    equal split 12, starved 0
 *   one third             8    [5.6, 10.4]   old half 12, zero 0
 * </pre>
 * 40% - the in-process twin's value - would admit an equal split (12) inside three quarters' band, so this
 * envelope is tighter than its predecessor by design; 25% would leave the one-quarter band one lost credit wide.
 *
 * @author Antony Stubbs
 * @see FiringLedger#countIn
 */
public final class NavigatorProofEnvelope {

    private NavigatorProofEnvelope() {
    }

    // ------------------------------------------------------------------
    // The demo policy (the predecessor plan's KTD7) - the contract every lane and demo tags
    // ------------------------------------------------------------------

    /** The tagged resource's name. */
    public static final String RESOURCE = "api-x";

    /** Credits per second the fleet may collectively spend. */
    public static final double RATE_PER_SECOND = 2.0;

    /** The quantum: one second, so credits per quantum equal the rate and a window in quanta is a window in seconds. */
    public static final Duration QUANTUM = Duration.ofSeconds(1);

    /** The fleet-wide overdraft budget, divided by share (R2). */
    public static final int BURST = 2;

    /** The contract as the children register it. */
    public static final ResourceContract CONTRACT = new ResourceContract(RESOURCE, RATE_PER_SECOND, BURST, QUANTUM);

    // ------------------------------------------------------------------
    // The envelope (R11)
    // ------------------------------------------------------------------

    /**
     * Rate tolerance: an observed count passes when within this percentage of its expected count. Chosen
     * against the table in the class javadoc - the largest value that still separates every adjacent hypothesis
     * at every shape the proof asserts.
     */
    public static final int RATE_TOLERANCE_PERCENT = 30;

    /**
     * The observation window, in quanta: a common multiple of every partition total the proof uses (2, 3 and
     * 4), so every fractional share's rotation period divides it and the expected count is exact - and long
     * enough that the smallest expected count clears {@link #MIN_EXPECTED_SAMPLES}.
     */
    public static final int WINDOW_QUANTA = 12;

    /** {@link #WINDOW_QUANTA} as a duration. */
    public static final Duration WINDOW = QUANTUM.multipliedBy(WINDOW_QUANTA);

    /**
     * Minimum sample count: the smallest EXPECTED count a window may be asserted against. A percentage of a
     * count of two discriminates nothing, so {@link #assertCountWithinTolerance} refuses an expectation below
     * this as a design error in the calling scenario rather than a finding about the mechanism. Six is the
     * one-quarter share over the window - the smallest shape in the proof.
     */
    public static final double MIN_EXPECTED_SAMPLES = 6;

    /**
     * The session timeout the children run at: the broker's floor (KTD10), so a killed member is rebalanced
     * away as fast as the broker allows.
     */
    public static final Duration SESSION_TIMEOUT = Duration.ofMillis(ChildPcOptions.BROKER_MIN_SESSION_TIMEOUT_MS);

    /**
     * The rebalance allowance: how long, beyond the session timeout, the group may take to report the survivors
     * stable with the departed member's partitions. The harness self-test observed a killed member gone about
     * 5.9s after the kill under the 6s floor - the session timeout itself - with stability following inside a
     * second on this machine; five seconds is that observation with room for a hosted runner's slower
     * heartbeat round trips. Re-confirm against the latencies the lane REPORTS on CI.
     */
    public static final Duration REBALANCE_ALLOWANCE = Duration.ofSeconds(5);

    /**
     * The convergence deadline after a kill or a join (R11, KTD10): session timeout + rebalance allowance + one
     * quantum, the last because a moved share is first minted at the quantum boundary AFTER its assignment
     * (R4). The post-transition window opens exactly this long after the transition's broker-time anchor, so
     * "converged within the deadline" is a count over a window the deadline fixes - never a gate on how long the
     * rebalance took, which the lane observes and reports.
     */
    public static final Duration CONVERGENCE_DEADLINE = SESSION_TIMEOUT.plus(REBALANCE_ALLOWANCE).plus(QUANTUM);

    /**
     * How far below rate the fleet may run across a join, in seconds of rate (F3, AE10): the rebalance itself,
     * during which the moving partitions' share mints but has no holder to spend it, plus the one quantum the
     * moved share waits for its next boundary. No session timeout term - a joining member is a rebalance the
     * coordinator starts at once.
     */
    public static final Duration JOIN_UNDERSHOOT_SPAN = REBALANCE_ALLOWANCE.plus(QUANTUM);

    // ------------------------------------------------------------------
    // Derived bounds - arithmetic on the policy, never tuned
    // ------------------------------------------------------------------

    /** Expected firings for a share over the window: {@code rate * fraction * window seconds}. */
    public static double expectedFirings(double shareFraction) {
        return RATE_PER_SECOND * shareFraction * WINDOW.getSeconds();
    }

    /**
     * The R8 bound on aggregate firings over an anchored span (zero clock offset): an unaligned span of S
     * seconds intersects at most {@code floor(S/q) + 1} quanta, each minting at most {@code rate * q}, plus
     * overdraft bounded by burst - {@code rate * (S + q) + burst}. Per KTD13's pre-registered rule, on the
     * zero-offset shapes this is asserted with no re-derivation, and any crossing is a defect.
     */
    public static double overshootBound(Duration span) {
        return RATE_PER_SECOND * (span.toMillis() / 1000.0 + QUANTUM.getSeconds()) + BURST;
    }

    /**
     * The least the fleet may fire over a span that contains one join: the steady expectation minus the
     * priced undershoot ({@link #JOIN_UNDERSHOOT_SPAN} of rate) minus one credit of anchor-phase slack.
     */
    public static double joinUndershootFloor(Duration span) {
        return RATE_PER_SECOND * (span.toMillis() / 1000.0 - JOIN_UNDERSHOOT_SPAN.getSeconds()) - 1;
    }

    /**
     * The fleet-conservation tolerance (R10, AE7). "Summed shares" is the harness's sampled sum of each child's
     * {@code creditsPerQuantum} per quantum index - a rotation-AVERAGED share (1.5 for three quarters), sampled
     * from before the first mint to after the last - so a child can legitimately mint up to one credit more than
     * its sampled sum: the partial rotation at each end of its life (a three-quarter holder mints 2,2 across a
     * rotation whose sampled average is 1.5, 1.5), and a sampler pass that straddled a boundary. One credit per
     * child, on top of rounding the sum up.
     */
    public static long conservationSlack(int children) {
        return children;
    }

    // ------------------------------------------------------------------
    // The one assertion shape every rate claim uses
    // ------------------------------------------------------------------

    /**
     * {@code count} is within {@link #RATE_TOLERANCE_PERCENT} of {@code expected}; refuses an expectation
     * below {@link #MIN_EXPECTED_SAMPLES} as the calling scenario's design error.
     */
    public static void assertCountWithinTolerance(long count, double expected, String claim) {
        assertThat(expected)
                .as("envelope: the expected count for '%s' must clear the minimum sample count, or the "
                        + "tolerance discriminates nothing - lengthen the window or the share", claim)
                .isGreaterThanOrEqualTo(MIN_EXPECTED_SAMPLES);
        assertThat((double) count)
                .as("%s: %s firings against an expected %s within %s%%", claim, count, expected,
                        RATE_TOLERANCE_PERCENT)
                .isCloseTo(expected, withPercentage(RATE_TOLERANCE_PERCENT));
    }
}
