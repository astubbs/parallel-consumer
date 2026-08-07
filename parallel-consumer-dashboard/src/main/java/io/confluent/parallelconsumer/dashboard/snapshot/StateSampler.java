package io.confluent.parallelconsumer.dashboard.snapshot;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import io.confluent.parallelconsumer.internal.AbstractParallelEoSStreamProcessor;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.Getter;
import org.apache.kafka.common.annotation.InterfaceStability;

/**
 * Composes the two state sources into one {@link PcSnapshot}, so nothing downstream has to care which source a value
 * came from.
 * <p>
 * <strong>Control thread only.</strong> {@link #sample()} evaluates Micrometer gauges, and gauge evaluation runs the
 * gauge's supplier on the calling thread - see {@link MeterSource} for the incident that makes this a rule rather
 * than a preference. {@link SnapshotPublisher} is what enforces it; calling {@code sample()} from anywhere else
 * defeats the entire design.
 * <p>
 * Not final, and {@link #sample()} is overridable, so a test can substitute a sampler that fails and prove the
 * publisher isolates it.
 * <p>
 * Experimental: the dashboard module is opt-in and its API may change without notice.
 */
@InterfaceStability.Unstable
public class StateSampler {

    @Getter
    private final MeterSource meterSource;

    @Getter
    private final DirectStateSource directStateSource;

    /**
     * Samples taken so far. Only ever incremented on the control thread; volatile so a reader that wants the count
     * without a snapshot in hand sees a current value.
     */
    private volatile long sampleCount;

    public StateSampler(MeterSource meterSource, DirectStateSource directStateSource) {
        this.meterSource = meterSource;
        this.directStateSource = directStateSource;
    }

    /**
     * Convenience for the usual composition: read the meters from {@code registry}, read identity and the
     * closed/failed flag from {@code pc}. Either may be null.
     */
    public StateSampler(AbstractParallelEoSStreamProcessor<?, ?> pc, MeterRegistry registry) {
        this(new MeterSource(registry), new DirectStateSource(pc));
    }

    /**
     * Builds one immutable snapshot of everything the page needs.
     * <p>
     * Must be called on the control thread. The result holds no reference back to any live PC object, so it is safe
     * to hand to any number of other threads.
     */
    public PcSnapshot sample() {
        long sequence = ++sampleCount;
        LifecycleSnapshot lifecycle = directStateSource.contributeTo(meterSource.sampleLifecycle()).build();
        return PcSnapshot.builder()
                .captureEpochMillis(System.currentTimeMillis())
                .sampleSequence(sequence)
                .registryPopulated(meterSource.isPopulated())
                .lifecycle(lifecycle)
                .work(meterSource.sampleWork())
                .encoding(meterSource.sampleEncoding())
                .partitions(meterSource.samplePartitions())
                .build();
    }

    /**
     * How many samples this sampler has taken. Feeds {@link PcSnapshot#getSampleSequence()}, which is how a
     * just-started dashboard knows it has no history behind it.
     */
    public long getSampleCount() {
        return sampleCount;
    }
}
