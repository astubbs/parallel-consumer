package bz.stub.parallelconsumer.integrationTests.chaostests;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.junit.jupiter.api.Test;

import org.apache.kafka.common.ConsumerGroupState;

import java.time.Duration;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;

/**
 * The contract behind {@link ProgressProbe#disableRebalanceDwellViolation()}: it suppresses the
 * VIOLATION and never the MEASUREMENT.
 * <p>
 * <b>Why this needed a test rather than a reading.</b> `ChaosKeyOrderIT` disables the Class 1 detector
 * because its own disturbances cross the bound with no member actually being a zombie - seed
 * 1838980910098175839 replays that RED deterministically on plain master at 15.4-15.7s against a
 * 15000ms bound. The obvious way to check that fix is to replay the seed and watch it go green, and
 * that check is WORTHLESS: the crossing is probabilistic even on a fixed seed, so a green run may
 * simply not have reached the condition (the first such replay peaked at 10062ms - it would have
 * passed unfixed). A green there is an absence, which is the weakest evidence this repo recognises.
 * <p>
 * So the toggle is asserted directly instead, in both directions and on both halves. The
 * peak-still-measured half matters most: a scenario that suppressed the measurement as well would
 * quietly delete the very numbers a future re-calibration needs, and nothing would go red to say so.
 * {@code docs/inflight/test-chaos-phase2.md} records this invariant as having had no fast coverage.
 * <p>
 * Untagged deliberately, so it gates every default integration build.
 */
class RebalanceDwellToggleIT {

    private static final Duration OVER = ProgressProbe.REBALANCE_DWELL_BOUND.plusSeconds(1);
    private static final Duration UNDER = ProgressProbe.REBALANCE_DWELL_BOUND.minusSeconds(1);

    private static ProgressProbe probe() {
        return ProgressProbe.forSeamTest("dwell-toggle-group", "dwell-toggle-topic");
    }

    @Test
    void armedIsTheControl_aCrossingViolatesAndAsksToReArm() {
        ProgressProbe probe = probe();

        boolean violated = probe.recordRebalanceDwell(OVER, "g", ConsumerGroupState.PREPARING_REBALANCE);

        assertWithMessage("without this arm the disabled case below proves nothing - a detector that "
                + "never fires either way would pass it")
                .that(violated).isTrue();
        assertThat(probe.getViolations()).hasSize(1);
        assertThat(probe.getViolations().get(0)).contains("ZOMBIE_MEMBER/REBALANCE_BLOCKED");
    }

    @Test
    void disabledSuppressesTheViolationOnTheSameCrossing() {
        ProgressProbe probe = probe().disableRebalanceDwellViolation();

        boolean violated = probe.recordRebalanceDwell(OVER, "g", ConsumerGroupState.PREPARING_REBALANCE);

        assertThat(violated).isFalse();
        assertWithMessage("this is the whole point of the toggle")
                .that(probe.getViolations()).isEmpty();
    }

    /**
     * The half that is easy to break and impossible to notice: suppressing the finding must not
     * suppress the number behind it.
     */
    @Test
    void disabledStillMeasuresThePeak() {
        ProgressProbe probe = probe().disableRebalanceDwellViolation();

        probe.recordRebalanceDwell(OVER, "g", ConsumerGroupState.PREPARING_REBALANCE);

        assertWithMessage("the peak is what a future re-calibration reads - losing it would delete the "
                + "evidence silently, with nothing going red to say so")
                .that(probe.getPeakRebalanceDwellMs()).isEqualTo(OVER.toMillis());
    }

    @Test
    void aDwellUnderTheBoundIsMeasuredButNeverViolates() {
        ProgressProbe probe = probe();

        boolean violated = probe.recordRebalanceDwell(UNDER, "g", ConsumerGroupState.PREPARING_REBALANCE);

        assertThat(violated).isFalse();
        assertThat(probe.getViolations()).isEmpty();
        assertThat(probe.getPeakRebalanceDwellMs()).isEqualTo(UNDER.toMillis());
    }

    /** The peak is a maximum, not a last-value: a later smaller dwell must not erase a larger one. */
    @Test
    void thePeakKeepsTheLargestDwellSeen() {
        ProgressProbe probe = probe().disableRebalanceDwellViolation();

        probe.recordRebalanceDwell(OVER, "g", ConsumerGroupState.PREPARING_REBALANCE);
        probe.recordRebalanceDwell(UNDER, "g", ConsumerGroupState.PREPARING_REBALANCE);

        assertThat(probe.getPeakRebalanceDwellMs()).isEqualTo(OVER.toMillis());
    }
}
