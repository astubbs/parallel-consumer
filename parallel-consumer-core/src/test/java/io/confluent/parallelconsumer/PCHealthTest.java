package io.confluent.parallelconsumer;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Covers the public contract of {@link PCHealth}: how the controller state, the broker-poller state and the failure
 * cause fold into the single liveness verdict, and which of them are deliberately excluded from it.
 *
 * @author Antony Stubbs
 */
class PCHealthTest {

    private static final Exception FAILURE = new IllegalStateException("control loop died");

    @Test
    void runningWithoutFailureIsHealthy() {
        var health = PCHealth.builder()
                .controllerState(State.RUNNING)
                .pollerState(State.RUNNING)
                .build();

        assertThat(health.isHealthy()).isTrue();
        assertThat(health.getFailureCause()).isEmpty();
    }

    /**
     * A crashed control loop can leave the state field untouched, so a present failure has to override an otherwise
     * healthy-looking state.
     */
    @Test
    void aPresentFailureOverridesARunningState() {
        var health = PCHealth.builder()
                .controllerState(State.RUNNING)
                .pollerState(State.RUNNING)
                .failureCause(FAILURE)
                .build();

        assertThat(health.isHealthy()).isFalse();
        assertThat(health.getFailureCause()).contains(FAILURE);
    }

    /**
     * The clean-shutdown-versus-crash distinction: not healthy, but with nothing to blame it on.
     */
    @Test
    void aCleanShutdownIsNotHealthyAndHasNoFailureCause() {
        var health = PCHealth.builder()
                .controllerState(State.CLOSED)
                .pollerState(State.CLOSED)
                .build();

        assertThat(health.isHealthy()).isFalse();
        assertThat(health.getFailureCause()).isEmpty();
    }

    /**
     * A deliberate pause is not a reason to restart the process.
     */
    @Test
    void aPausedControllerIsHealthy() {
        var health = PCHealth.builder()
                .controllerState(State.PAUSED)
                .pollerState(State.RUNNING)
                .build();

        assertThat(health.isHealthy()).isTrue();
    }

    @Test
    void failureCauseIsAnEmptyOptionalNeverNull() {
        var health = PCHealth.builder()
                .controllerState(State.RUNNING)
                .pollerState(State.RUNNING)
                .build();

        assertThat(health.getFailureCause()).isNotNull();
        assertThat(health.getFailureCause()).isEmpty();
    }

    /**
     * The two subsystems must be separately readable - {@code pauseIfRunning()} moves only the controller, so the
     * snapshot has to report the divergence rather than collapsing it onto one value.
     */
    @Test
    void controllerAndPollerStatesAreNotConflated() {
        var health = PCHealth.builder()
                .controllerState(State.PAUSED)
                .pollerState(State.RUNNING)
                .build();

        assertThat(health.getControllerState()).isEqualTo(State.PAUSED);
        assertThat(health.getPollerState()).isEqualTo(State.RUNNING);
    }

    /**
     * The poller state is diagnostic only. Proven with a poller state that would flip the verdict if it participated.
     */
    @Test
    void aDownPollerDoesNotMakeARunningControllerUnhealthy() {
        var health = PCHealth.builder()
                .controllerState(State.RUNNING)
                .pollerState(State.CLOSED)
                .build();

        assertThat(State.CLOSED.isRunningOrPaused())
                .as("test premise: the poller state chosen must be one that would flip the verdict if it counted")
                .isFalse();
        assertThat(health.isHealthy()).isTrue();
    }

    /**
     * Both states are required. Rejecting at {@code build()} means a caller sees the omission where they made it,
     * rather than as an unexplained NPE later when the verdict is computed.
     */
    @Test
    void bothStatesAreRequiredAtBuildTime() {
        assertThatThrownBy(() -> PCHealth.builder().pollerState(State.RUNNING).build())
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("controllerState");

        assertThatThrownBy(() -> PCHealth.builder().controllerState(State.RUNNING).build())
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("pollerState");
    }

    /**
     * Cheap, but it is what an operator sees in a log line.
     */
    @Test
    void toStringCarriesBothStatesAndSurvivesAnAbsentFailure() {
        var health = PCHealth.builder()
                .controllerState(State.PAUSED)
                .pollerState(State.DRAINING)
                .build();

        assertThatCode(health::toString).doesNotThrowAnyException();
        assertThat(health.toString())
                .contains("PAUSED")
                .contains("DRAINING");
    }
}
