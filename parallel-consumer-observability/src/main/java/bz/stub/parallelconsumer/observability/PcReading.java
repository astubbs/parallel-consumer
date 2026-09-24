package bz.stub.parallelconsumer.observability;
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
 * Everything a reader needs about a Parallel Consumer instance, as it was at one instant. The web dashboard and the
 * MCP tools are two such readers; an application wiring this to its own monitoring is a third.
 * <p>
 * <strong>Deeply immutable, and holds no reference to any live PC object.</strong> That is the whole point: this
 * value is built on the control thread by {@link StateSampler}, published through
 * {@link ReadingPublisher}, and then read by arbitrary other threads. Nothing reachable from here can be mutated
 * afterwards, and nothing reachable from here can drag a reader onto a {@code KafkaConsumer}, a {@code RetryQueue}
 * or a Micrometer gauge supplier. See {@code ReadingPublisher}'s class javadoc for the recorded incident that
 * makes this non-negotiable.
 * <p>
 * <strong>Absent is not zero.</strong> Every optional quantity in this tree is a boxed type whose {@code null} means
 * "no meter supplied this", never "the value is zero". A reader that renders zero for a missing meter is asserting
 * something it does not know; absence is carried as absence so it does not have to.
 * <p>
 * <strong>Confidence.</strong> {@link #getSampleSequence()} and {@link #isRegistryPopulated()} exist so a
 * just-started reader can degrade honestly. A first sample taken microseconds after start, from a registry that
 * has not had its meters bound yet, is a legitimate reading of "nothing known yet" - and it must be distinguishable
 * from a confident reading of an idle consumer.
 * <p>
 * Experimental: this module is opt-in and its API may change without notice.
 */
@InterfaceStability.Unstable
@Value
public class PcReading {

    /**
     * When this reading was captured, in epoch milliseconds. Staleness display and every rate derived between two
     * readings depend on this - a rate must be divided by measured elapsed time, never by a nominal interval.
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
    LifecycleReading lifecycle;

    /**
     * Instance-wide work state. Never null; all-absent if nothing could be read.
     */
    WorkReading work;

    /**
     * Offset-encoding health. Never null; all-absent if nothing could be read.
     */
    EncodingReading encoding;

    /**
     * One row per topic-partition, ordered by topic then partition number so the page's table does not reshuffle
     * between ticks. Unmodifiable, never null, possibly empty.
     */
    List<PartitionReading> partitions;

    @Builder(toBuilder = true)
    PcReading(long captureEpochMillis,
               long sampleSequence,
               boolean registryPopulated,
               LifecycleReading lifecycle,
               WorkReading work,
               EncodingReading encoding,
               List<PartitionReading> partitions) {
        this.captureEpochMillis = captureEpochMillis;
        this.sampleSequence = sampleSequence;
        this.registryPopulated = registryPopulated;
        // never null: a page that has to null-check every branch of this tree grows a null-check bug instead of an
        // absence-rendering rule. An all-absent sub-reading says the same thing and says it uniformly.
        this.lifecycle = lifecycle == null ? LifecycleReading.builder().build() : lifecycle;
        this.work = work == null ? WorkReading.builder().build() : work;
        this.encoding = encoding == null ? EncodingReading.builder().build() : encoding;
        this.partitions = partitions == null
                ? Collections.<PartitionReading>emptyList()
                : Collections.unmodifiableList(new ArrayList<>(partitions));
    }

    /**
     * Whether this reading carries nothing to draw - no PC meters were registered and no partition rows were found.
     * This is the honest "nothing known yet" state, and the page says so rather than rendering a confident zero.
     */
    public boolean isEmpty() {
        return !registryPopulated && partitions.isEmpty();
    }

    /**
     * How old this reading is, in milliseconds, relative to the supplied wall-clock instant. Negative results are
     * clamped to zero so a clock that steps backwards cannot render a reading from the future.
     */
    public long ageMillis(long nowEpochMillis) {
        long age = nowEpochMillis - captureEpochMillis;
        return age < 0 ? 0 : age;
    }
}
