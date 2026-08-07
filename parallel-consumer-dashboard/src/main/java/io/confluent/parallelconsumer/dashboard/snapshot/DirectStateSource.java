package io.confluent.parallelconsumer.dashboard.snapshot;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import io.confluent.parallelconsumer.internal.AbstractParallelEoSStreamProcessor;
import org.apache.kafka.common.annotation.InterfaceStability;

/**
 * Reads state straight off the live Parallel Consumer instance, for the things a meter cannot carry.
 * <p>
 * <strong>Deliberately minimal in this phase.</strong> The seam exists because the registry is a lossy projection -
 * it cannot hold the full incomplete-offset set, per-shard detail, or a state transition that happens between
 * scrapes - and those reads are what makes an in-process dashboard able to show what an external tool cannot. But
 * everything phase 1 draws is already published as a meter, and the richer reads need read-only accessors that do
 * not exist on core's classes yet. So this reads only what is already public, already cheap, and already safe:
 * identity and the closed/failed flag.
 * <p>
 * <strong>Called on the control thread only</strong>, from {@link SnapshotPublisher}'s loop-end callback. That is
 * also the constraint that governs what may ever be added here. Specifically forbidden, each for a recorded reason:
 * <ul>
 *     <li>{@code RetryQueue.size()} - reads a plain {@code HashMap} without the lock every other method on that
 *     class takes, so it is a genuine data race rather than a theoretical one.</li>
 *     <li>Iterating a {@code RetryQueue} - its iterator holds the read lock until closed, so a caller that forgets
 *     to close it deadlocks the control loop permanently.</li>
 *     <li>Anything reaching {@code KafkaConsumer}, including {@code ConsumerManager.assignment()} and
 *     {@code paused()}, which pass straight through to it. {@code KafkaConsumer} is single-threaded and belongs to
 *     the poll thread.</li>
 *     <li>Anything using {@code parallelStream()}, such as {@code PartitionState.getAllIncompleteOffsets()} - it
 *     runs on the common ForkJoinPool, so it takes the control loop's latency out of the control loop's hands.</li>
 * </ul>
 * <p>
 * Experimental: the dashboard module is opt-in and its API may change without notice.
 */
@InterfaceStability.Unstable
public class DirectStateSource {

    private final AbstractParallelEoSStreamProcessor<?, ?> pc;

    /**
     * @param pc the live instance, or null for a registry-only sampler (which is how the snapshot model is unit
     *           tested, and how a caller with a registry but no handle on the processor can still get a page)
     */
    public DirectStateSource(AbstractParallelEoSStreamProcessor<?, ?> pc) {
        this.pc = pc;
    }

    /**
     * Whether a live instance was supplied. False means every field this source contributes is absent, and the page
     * should say "not available" rather than draw a default.
     */
    public boolean isAvailable() {
        return pc != null;
    }

    /**
     * Adds this source's fields to a lifecycle builder already carrying the meter-derived ones.
     *
     * @return the same builder, for chaining
     */
    public LifecycleSnapshot.LifecycleSnapshotBuilder contributeTo(LifecycleSnapshot.LifecycleSnapshotBuilder builder) {
        if (pc == null) {
            return builder;
        }
        return builder
                .closedOrFailed(pc.isClosedOrFailed())
                .instanceId(pc.getMyId().orElse(null));
    }
}
