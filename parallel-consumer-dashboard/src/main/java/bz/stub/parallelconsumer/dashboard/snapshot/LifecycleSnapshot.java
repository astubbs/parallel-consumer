package bz.stub.parallelconsumer.dashboard.snapshot;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import lombok.Builder;
import lombok.Value;
import org.apache.kafka.common.annotation.InterfaceStability;

/**
 * Run state of the controller and of the broker poller, plus the small amount of identity read directly off the PC
 * instance.
 * <p>
 * The two states arrive as numbers ({@code pc.status} and {@code pc.poller.status} carry
 * {@code bz.stub.parallelconsumer.internal.State#getValue()}), and are mapped back to the enum name here so the
 * page never has to know the numeric mapping. Both the name and the raw number are carried: the number is what a
 * step-function chart plots, the name is what a human reads. If the number does not correspond to any known state -
 * which happens if core adds a state this module has not been rebuilt against - the name is left absent rather than
 * guessed, and the number still renders.
 * <p>
 * <strong>Absent is not zero.</strong> Every field is boxed; {@code null} means unknown, not "off". In particular
 * {@code null} state is meaningfully different from {@code UNUSED}.
 * <p>
 * Experimental: the dashboard module is opt-in and its API may change without notice.
 */
@InterfaceStability.Unstable
@Value
public class LifecycleSnapshot {

    /**
     * Controller run state name, e.g. {@code RUNNING}. From {@code pc.status}.
     */
    String state;

    /**
     * Raw numeric value behind {@link #getState()}, as published by the meter.
     */
    Integer stateValue;

    /**
     * Broker poller state name. From {@code pc.poller.status}.
     */
    String pollerState;

    /**
     * Raw numeric value behind {@link #getPollerState()}.
     */
    Integer pollerStateValue;

    /**
     * From {@code AbstractParallelEoSStreamProcessor#isClosedOrFailed()}, read directly on the control thread.
     * {@code null} when no PC instance was supplied (a registry-only sampler).
     */
    Boolean closedOrFailed;

    /**
     * The instance's optional ID, from {@code AbstractParallelEoSStreamProcessor#getMyId()}. {@code null} when unset
     * or when no PC instance was supplied.
     */
    String instanceId;

    @Builder(toBuilder = true)
    LifecycleSnapshot(String state,
                      Integer stateValue,
                      String pollerState,
                      Integer pollerStateValue,
                      Boolean closedOrFailed,
                      String instanceId) {
        this.state = state;
        this.stateValue = stateValue;
        this.pollerState = pollerState;
        this.pollerStateValue = pollerStateValue;
        this.closedOrFailed = closedOrFailed;
        this.instanceId = instanceId;
    }
}
