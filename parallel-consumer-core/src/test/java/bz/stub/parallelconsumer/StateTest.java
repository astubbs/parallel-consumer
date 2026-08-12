package io.confluent.parallelconsumer;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.EnumSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Covers the public contract of {@link State}: the up/down predicate a health check folds into its verdict, and the
 * numeric values published through the {@code pc.status} and {@code pc.poller.status} gauges.
 *
 * @author Antony Stubbs
 */
class StateTest {

    /**
     * The states that mean "not shutting down". Held here as an independent restatement of the production switch, so
     * that a change to either side has to be a deliberate change to both.
     */
    private static final Set<State> EXPECTED_UP = EnumSet.of(State.UNUSED, State.RUNNING, State.PAUSED);

    @ParameterizedTest
    @EnumSource(State.class)
    void isRunningOrPausedClassifiesEveryState(State state) {
        assertThat(state.isRunningOrPaused())
                .as("%s should be classified as up=%s", state, EXPECTED_UP.contains(state))
                .isEqualTo(EXPECTED_UP.contains(state));
    }

    /**
     * A state added later must be classified deliberately rather than falling through to a default. The production
     * switch throws on an unmapped constant, so this proves no constant is currently unmapped.
     */
    @ParameterizedTest
    @EnumSource(State.class)
    void everyStateIsMappedByThePredicate(State state) {
        assertThatCode(state::isRunningOrPaused)
                .as("%s is not classified by State#isRunningOrPaused", state)
                .doesNotThrowAnyException();
    }

    /**
     * These integers are a published contract: they are what the {@code pc.status} and {@code pc.poller.status} gauges
     * report, and the gauge description enumerates them. Asserted per-constant rather than by looping over
     * {@link State#values()}, because a loop over ordinals would happily agree with a renumbering.
     */
    @Test
    void gaugeValuesAreUnchanged() {
        assertThat(State.UNUSED.getValue()).isZero();
        assertThat(State.RUNNING.getValue()).isEqualTo(1);
        assertThat(State.PAUSED.getValue()).isEqualTo(2);
        assertThat(State.DRAINING.getValue()).isEqualTo(3);
        assertThat(State.CLOSING.getValue()).isEqualTo(4);
        assertThat(State.CLOSED.getValue()).isEqualTo(5);
    }

    /**
     * Guards the constant set itself. Adding or removing a state changes the gauge's published description, so it
     * should be a deliberate edit here too.
     */
    @Test
    void theStateSetIsUnchanged() {
        assertThat(State.values()).containsExactly(
                State.UNUSED,
                State.RUNNING,
                State.PAUSED,
                State.DRAINING,
                State.CLOSING,
                State.CLOSED);
    }
}
