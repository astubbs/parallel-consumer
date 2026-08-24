package bz.stub.parallelconsumer.internal.admission;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import lombok.Value;

/**
 * The engine signals sampled ONCE at a window boundary - never per control-loop pass - and stamped onto the
 * {@link ClosedAdmissionWindow} they close. They exist so the closed window can carry an honest binding
 * classification (the design's R2/KTD1): whether the admission limit was actually the constraint when the window
 * closed, and when it was not, which starvation cause was.
 * <p>
 * Sampled by {@code AbstractParallelEoSStreamProcessor#sampleAdmissionBoundarySignals()} on the control thread,
 * pulled by {@link AdmissionController#tick(java.util.function.Supplier)} only when a window is actually due - so
 * the O(shards) cost of the ordering-aware selectable-work bound is paid once per window (about once a second),
 * not once per pass.
 * <p>
 * Pure data: the classification arithmetic lives on {@link ClosedAdmissionWindow#bindingClassification()}, where
 * the decision layer reads it.
 */
@Value
public class AdmissionBoundarySignals {

    /**
     * The no-signals sentinel for callers with no engine to sample (the no-arg {@link AdmissionController#tick()}
     * used by tests, and any pre-plumbing path). All-zero: it classifies as
     * {@link ClosedAdmissionWindow.BindingClassification#NO_WORK}, never as limit-bound - an unsampled boundary
     * must not fabricate evidence that the limit binds.
     */
    public static final AdmissionBoundarySignals UNSAMPLED =
            new AdmissionBoundarySignals(0, 0, false, 0, 0, false, false);

    /**
     * Active user-function tasks (slots) at the boundary - {@code UserFunctionTaskAccounting#getActive()}. The
     * KTD1 binding verdict compares this against {@link #targetSlots}: achieved slots, not commanded, because the
     * target lies exactly when the shards cannot fill it.
     */
    int activeTasks;

    /**
     * The commanded admission target in slots at the boundary - {@code PCModule#admissionTargetSlots()}.
     */
    int targetSlots;

    /**
     * Whether the engine's last work request came back SHORT (the inverse of
     * {@code AbstractParallelEoSStreamProcessor#lastWorkRequestWasFulfilled}). Carried as corroborating data for
     * the decision layer; the classification itself turns on the work-presence figures below.
     */
    boolean dispatchUnderServed;

    /**
     * The ordering-aware upper bound on selectable work at the boundary -
     * {@code WorkManager#getUpperBoundOnSelectableWork()}. Zero with {@link #bufferedShardWork} present is the
     * ordering-starved signature: work exists but no shard could yield it.
     */
    long selectableWorkUpperBound;

    /**
     * Work queued in shards awaiting selection at the boundary -
     * {@code WorkManager#getNumberOfWorkQueuedInShardsAwaitingSelection()}. Distinguishes an EMPTY buffer
     * (no work anywhere - {@code NO_WORK}) from a buffer whose work the shards cannot yield
     * ({@code ORDERING_STARVED}); the selectable upper bound alone cannot tell them apart.
     */
    long bufferedShardWork;

    /**
     * Whether the broker poller had paused itself for throttling at the boundary -
     * {@code BrokerPollSystem#isPausedForThrottling()}. Self-inflicted starvation: a window closed under it must
     * never be read as evidence of anything.
     */
    boolean pollerSelfThrottled;

    /**
     * Whether ANY assigned partition was blocked by offset-encoding back-pressure at the boundary
     * ({@code PartitionState#isBlocked()} via {@code WorkManager#isAnyPartitionBlocked()}). Plumbed for the law's
     * absolute brake (the design's R8); this unit only carries it.
     */
    boolean offsetBackPressure;
}
