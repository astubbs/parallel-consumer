package bz.stub.parallelconsumer.streams.benchmark;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Pins the margin rule the benchmark's headline assertions rest on, and it runs in the ordinary build.
 * <p>
 * <b>The benchmarks themselves are tagged {@code performance} and need a broker</b>, so the decision they make
 * about what counts as a result would otherwise be exercised only on a machine somebody remembered to run them
 * on. The rule is a pure function of two numbers, so it does not have to be.
 * <p>
 * The case that matters is the first one: a sabotaged run - the seam neutered, so both arms are stock - lands
 * at parity, and a check that only asks "did the ratio beat one" is satisfied by it. That is not a
 * hypothetical; it is what happened on astubbs/parallel-consumer#391, where a run in which every fast record
 * had waited printed the claim that a fast record no longer waits.
 *
 * @author Antony Stubbs
 */
class NoiseFloorTest {

    @Test
    void aRunAtParityDoesNotBeatTheFloorEvenWhenTheFloorIsAsTightAsItCanBe() {
        NoiseFloor tight = NoiseFloor.between(100d, 100d);

        assertThat(tight.getRatio()).isEqualTo(1.0d);
        assertThat(tight.beatenBy(1.01d))
                .as("the shape of a sabotaged run: the mechanism is gone, the two arms do the same work, and "
                        + "the ratio lands just above one. A direction check passes it")
                .isFalse();
    }

    @Test
    void aRealResultSitsWellOutsideAFloorThatIsItselfWide() {
        // The floor a real machine produces when it is busy, and the magnitude this suite's backlog
        // experiment measures. The threshold lives in the empty space between them, which is the whole design.
        NoiseFloor wide = NoiseFloor.between(10.0d, 12.0d);

        assertThat(wide.getRatio()).isEqualTo(1.2d);
        assertThat(wide.beatenBy(3.7d)).isTrue();
        assertThat(wide.beatenBy(1.19d))
                .as("inside the floor, so the run measured nothing however good the number looks")
                .isFalse();
    }

    @Test
    void theFloorIsOrderIndependentBecauseWhichArmRanFirstIsNotAFinding() {
        assertThat(NoiseFloor.between(10d, 12d).getRatio())
                .isEqualTo(NoiseFloor.between(12d, 10d).getRatio());
    }

    @Test
    void anArmThatDidNotRunIsRefusedRatherThanTurnedIntoAFloorOfInfinity() {
        assertThatThrownBy(() -> NoiseFloor.between(10d, 0d))
                .as("a zero arm makes the floor infinite, which silently passes nothing and fails everything, "
                        + "or zero, which passes everything - both disable the check without saying so")
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> NoiseFloor.between(Double.NaN, 10d))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void theDescriptionTellsAReaderWhatToDivideBy() {
        assertThat(NoiseFloor.between(10d, 12d).describe())
                .contains("1.20x")
                .contains("identical work");
    }
}
