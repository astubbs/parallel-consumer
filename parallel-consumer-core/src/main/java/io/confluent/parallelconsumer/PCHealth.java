package io.confluent.parallelconsumer;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import lombok.AccessLevel;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;
import org.apache.kafka.common.annotation.InterfaceStability;

import java.util.Optional;

/**
 * An immutable snapshot of the health of a {@link ParallelConsumer} instance, taken at the moment it was requested.
 * <p>
 * Carries the run {@link State} of the control loop, the run {@link State} of the broker poller, the failure cause if
 * the instance failed, and a single derived verdict - {@link #isHealthy()} - that a container orchestrator can act on
 * without interpreting the state enum itself.
 *
 * <h2>What healthy means, and what it does not</h2>
 * <p>
 * <strong>The verdict is liveness-scoped.</strong> It answers "does this instance need restarting", not "is this
 * instance consuming". A healthy verdict means only that the control loop is not shutting down and that no failure has
 * been recorded. <strong>It never means the instance is making progress.</strong> A consumer that is processing
 * nothing at all - because the broker is unreachable, because every record is failing, or because there is simply
 * nothing to read - still reports {@link State#RUNNING} and still reports healthy.
 * <p>
 * To observe progress, use the {@code pc.*} Micrometer meters instead (see
 * {@link io.confluent.parallelconsumer.metrics.PCMetricsDef}): the processed-record and offset-commit meters move when
 * work is being done, and a rate derived from them is the signal a progress check needs. This snapshot deliberately
 * does not attempt to derive one.
 *
 * <h2>The poller state is diagnostic</h2>
 * <p>
 * {@link #getPollerState()} is reported for diagnosis and is deliberately <em>excluded</em> from
 * {@link #isHealthy()}. The two states diverge during entirely normal operation - {@link ParallelConsumer#pauseIfRunning()}
 * moves only the control loop, leaving the broker poller {@link State#RUNNING}, and before {@code poll()} is ever
 * called the control loop reads {@link State#UNUSED} while the poller already reads {@link State#RUNNING}. Folding the
 * poller state into the verdict would turn those into false alarms.
 *
 * <h2>Retention</h2>
 * <p>
 * The snapshot holds a reference to the failure {@link Exception}, and therefore to its whole cause chain and every
 * object those exceptions reference. A caller that retains snapshots - a ring buffer of health history, for example -
 * retains that object graph too.
 *
 * @author Antony Stubbs
 * @see ParallelConsumer#getHealth()
 * @see State
 */
@Getter
@Builder
@ToString
@EqualsAndHashCode
@InterfaceStability.Evolving
public class PCHealth {

    /**
     * The run state of the control loop at the moment the snapshot was taken.
     * <p>
     * This is the state that feeds {@link #isHealthy()}, through {@link State#isRunningOrPaused()}.
     */
    @NonNull
    private final State controllerState;

    /**
     * The run state of the broker poller at the moment the snapshot was taken.
     * <p>
     * Diagnostic only - it does not participate in {@link #isHealthy()}. See the class documentation for why.
     */
    @NonNull
    private final State pollerState;

    /**
     * The exception that killed the instance, or {@code null} if it has not failed. Read through
     * {@link #getFailureCause()}, which reports the absence explicitly.
     * <p>
     * A single unattributed exception sourced from the control loop - it is not attributed to a subsystem.
     */
    @Getter(AccessLevel.NONE)
    private final Exception failureCause;

    /**
     * @return the exception that killed this instance, or {@link Optional#empty()} if it has not failed - never
     *         {@code null}
     */
    public Optional<Exception> getFailureCause() {
        return Optional.ofNullable(failureCause);
    }

    /**
     * The single derived liveness verdict: true when the control loop is not shutting down
     * ({@link State#isRunningOrPaused()}) <em>and</em> no failure cause is present.
     * <p>
     * A present failure cause forces this false regardless of the state, because a control loop that died can leave
     * the state field untouched.
     * <p>
     * {@link #getPollerState()} is not consulted - see the class documentation.
     * <p>
     * <strong>True does not mean the instance is making progress</strong>, only that it is neither shut down nor
     * failed. Observe progress through the {@code pc.*} Micrometer meters.
     *
     * @return true if this instance does not need restarting
     */
    public boolean isHealthy() {
        return controllerState.isRunningOrPaused() && !getFailureCause().isPresent();
    }
}
