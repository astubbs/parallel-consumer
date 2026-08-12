package io.confluent.parallelconsumer;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import io.confluent.parallelconsumer.integrationTests.CompletionCeiling;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static com.google.common.truth.Truth.assertThat;
import static java.time.Duration.ofSeconds;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The whole contract of the scaling ceiling is "the gating size keeps exactly the deadline it always
 * had". That claim is what makes it safe to put in front of four integration tests, and until now it
 * was only ever checked by running those tests - each of which needs Docker and minutes. This pins it
 * in milliseconds.
 * <p>
 * It also pins the case that motivated the helper: {@code LoadTest}'s original inline formula agreed
 * with its documented anchor at the gating value and diverged above it, which is precisely the shape
 * of error a gating-value-only check cannot see.
 */
class CompletionCeilingTest {

    /** Mirrors the real call sites, so a change to any of their constants breaks here first. */
    @Test
    void eachSiteKeepsTheDeadlineItAlwaysHadAtItsOwnGatingSize() {
        assertThat(ceiling(4_000, 4_000, ofSeconds(60))).isEqualTo(ofSeconds(60));          // LoadTest
        assertThat(ceiling(1_000_000, 1_000_000, ofSeconds(120))).isEqualTo(ofSeconds(120)); // VeryLargeMessageVolume
        assertThat(ceiling(3_000_000, 3_000_000, ofSeconds(60))).isEqualTo(ofSeconds(60));   // MultiInstanceHighVolume
    }

    @Test
    void scalesProportionallyAboveTheGatingSize() {
        assertThat(ceiling(400_000, 4_000, ofSeconds(60))).isEqualTo(ofSeconds(6_000));
        assertThat(ceiling(2_000_000, 1_000_000, ofSeconds(120))).isEqualTo(ofSeconds(240));
        assertThat(ceiling(10_000_000, 3_000_000, ofSeconds(60))).isEqualTo(ofSeconds(200));
    }

    @Test
    void neverReturnsLessThanTheGatingDeadline() {
        // below the gating size the run is smaller, but the deadline is a ceiling and does not tighten
        assertThat(ceiling(8, 4_000, ofSeconds(60))).isEqualTo(ofSeconds(60));
        assertThat(ceiling(1, 4_000, ofSeconds(60))).isEqualTo(ofSeconds(60));
        assertThat(ceiling(0, 4_000, ofSeconds(60))).isEqualTo(ofSeconds(60));
    }

    @Test
    void rejectsAMisconfiguredKnobRatherThanFailingAsArithmetic() {
        assertThatThrownBy(() -> ceiling(4_000, 0, ofSeconds(60)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("gatingUnits");

        // a mistyped -Dvolume.messages with extra digits would otherwise surface as a
        // Duration overflow from java.time internals, which reads like an environment fault
        assertThatThrownBy(() -> ceiling(Long.MAX_VALUE / 1_000, 1_000_000, ofSeconds(120)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("too large");
    }

    /**
     * Deliberately names {@link CompletionCeiling} and never {@code BrokerIntegrationTest}: the
     * latter initialises a Kafka container in a static field, so routing through it would start
     * Docker to test arithmetic - the exact cost this class was split out to avoid.
     */
    private static Duration ceiling(long units, long gatingUnits, Duration ceilingAtGating) {
        return CompletionCeiling.completionCeiling(units, gatingUnits, ceilingAtGating);
    }
}
