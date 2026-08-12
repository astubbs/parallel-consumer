package bz.stub.parallelconsumer.dashboard.snapshot;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import lombok.Builder;
import lombok.Value;
import org.apache.kafka.common.annotation.InterfaceStability;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Everything the dashboard page needs about a Parallel Consumer instance, as it was at one instant.
 * <p>
 * <strong>Deeply immutable, and holds no reference to any live PC object.</strong> That is the whole point: this
 * value is built on the control thread by {@link StateSampler}, published through
 * {@link SnapshotPublisher}, and then read by arbitrary other threads. Nothing reachable from here can be mutated
 * afterwards, and nothing reachable from here can drag a reader onto a {@code KafkaConsumer}, a {@code RetryQueue}
 * or a Micrometer gauge supplier. See {@code SnapshotPublisher}'s class javadoc for the recorded incident that
 * makes this non-negotiable.
 * <p>
 * <strong>Absent is not zero.</strong> Every optional quantity in this tree is a boxed type whose {@code null} means
 * "no meter supplied this", never "the value is zero". A dashboard that draws zero for a missing meter is asserting
 * something it does not know; the page renders absence as absence.
 * <p>
 * <strong>Confidence.</strong> {@link #getSampleSequence()} and {@link #isRegistryPopulated()} exist so a
 * just-started dashboard can degrade honestly. A first sample taken microseconds after start, from a registry that
 * has not had its meters bound yet, is a legitimate reading of "nothing known yet" - and it must be distinguishable
 * from a confident reading of an idle consumer.
 * <p>
 * Experimental: the dashboard module is opt-in and its API may change without notice.
 */
@InterfaceStability.Unstable
@Value
public class PcSnapshot {

    /**
     * When this snapshot was captured, in epoch milliseconds. Staleness display and every rate derived between two
     * snapshots depend on this - a rate must be divided by measured elapsed time, never by a nominal interval.
     */
    long captureEpochMillis;

    /**
     * 1-based count of samples the publisher has taken, this one included. {@code 1} means the page is looking at the
     * very first reading and has no history behind it.
     */
    long sampleSequence;

    /**
     * Whether the meter registry contained any of Parallel Consumer's own meters when this was captured. False means
     * the absences below are "not published yet", not "measured as nothing".
     */
    boolean registryPopulated;

    /**
     * Controller and poller run state. Never null; all-absent if nothing could be read.
     */
    LifecycleSnapshot lifecycle;

    /**
     * Instance-wide work state. Never null; all-absent if nothing could be read.
     */
    WorkSnapshot work;

    /**
     * Offset-encoding health. Never null; all-absent if nothing could be read.
     */
    EncodingSnapshot encoding;

    /**
     * One row per topic-partition, ordered by topic then partition number so the page's table does not reshuffle
     * between ticks. Unmodifiable, never null, possibly empty.
     */
    List<PartitionSnapshot> partitions;

    @Builder(toBuilder = true)
    PcSnapshot(long captureEpochMillis,
               long sampleSequence,
               boolean registryPopulated,
               LifecycleSnapshot lifecycle,
               WorkSnapshot work,
               EncodingSnapshot encoding,
               List<PartitionSnapshot> partitions) {
        this.captureEpochMillis = captureEpochMillis;
        this.sampleSequence = sampleSequence;
        this.registryPopulated = registryPopulated;
        // never null: a page that has to null-check every branch of this tree grows a null-check bug instead of an
        // absence-rendering rule. An all-absent sub-snapshot says the same thing and says it uniformly.
        this.lifecycle = lifecycle == null ? LifecycleSnapshot.builder().build() : lifecycle;
        this.work = work == null ? WorkSnapshot.builder().build() : work;
        this.encoding = encoding == null ? EncodingSnapshot.builder().build() : encoding;
        this.partitions = partitions == null
                ? Collections.<PartitionSnapshot>emptyList()
                : Collections.unmodifiableList(new ArrayList<>(partitions));
    }

    /**
     * Whether this snapshot carries nothing to draw - no PC meters were registered and no partition rows were found.
     * This is the honest "nothing known yet" state, and the page says so rather than rendering a confident zero.
     */
    public boolean isEmpty() {
        return !registryPopulated && partitions.isEmpty();
    }

    /**
     * How old this snapshot is, in milliseconds, relative to the supplied wall-clock instant. Negative results are
     * clamped to zero so a clock that steps backwards cannot render a snapshot from the future.
     */
    public long ageMillis(long nowEpochMillis) {
        long age = nowEpochMillis - captureEpochMillis;
        return age < 0 ? 0 : age;
    }
}
