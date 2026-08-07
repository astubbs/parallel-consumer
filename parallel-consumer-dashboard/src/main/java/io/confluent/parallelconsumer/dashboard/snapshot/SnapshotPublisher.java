package io.confluent.parallelconsumer.dashboard.snapshot;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import io.confluent.parallelconsumer.internal.AbstractParallelEoSStreamProcessor;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.annotation.InterfaceStability;

import java.util.concurrent.TimeUnit;

/**
 * Samples Parallel Consumer's state on the control thread and publishes the result for other threads to read.
 * <p>
 * <strong>Why the sampling has to happen here.</strong> Micrometer evaluates a gauge on whichever thread reads it.
 * confluentinc/parallel-consumer#618 records a Prometheus scrape thread evaluating a PC gauge and getting
 * {@code ConcurrentModificationException: KafkaConsumer is not safe for multi-threaded access}. So this class
 * registers a {@link Runnable} through
 * {@link AbstractParallelEoSStreamProcessor#addLoopEndCallBack(Runnable) addLoopEndCallBack}, which is already
 * public and already runs on the control loop, and every gauge evaluation therefore happens where it already legally
 * happens. HTTP handlers never sample; they only read {@link #getCurrent()}.
 * <p>
 * <strong>Why current and previous are kept.</strong> Rate charts and the pipeline animation both need a delta.
 * Computing it once here beats every reader recomputing it, and both snapshots are published through a
 * <em>single</em> volatile reference to an immutable pair - two separate volatile fields would let a reader see a
 * current from one tick beside a previous from another, and every rate derived from that pair would be wrong.
 * <p>
 * That is also why {@link #getSnapshots()} exists, and why a reader that needs <em>both</em> must use it:
 * {@link #getCurrent()} and {@link #getPrevious()} are two separate volatile reads, so a publish landing between them
 * hands back a mismatched pair. That is not hypothetical - it is what the concurrency test in
 * {@code SnapshotPublisherTest} caught on the first run, and the reason the pair is an exposed value type rather than
 * two independent getters.
 * <p>
 * <strong>Fault isolation.</strong> A dashboard bug must not be able to stop a consumer. {@link #sampleOnce()}
 * catches {@link Throwable}, leaves the previously published pair in place, and logs at WARN with repeat
 * suppression - the control loop runs continuously, so an unsuppressed log line here would be a log flood, which is
 * its own outage.
 * <p>
 * Experimental: the dashboard module is opt-in and its API may change without notice.
 */
@InterfaceStability.Unstable
@Slf4j
public class SnapshotPublisher {

    /**
     * At most one warning per this interval for a repeating failure. Long enough that a persistent fault cannot
     * flood the log from a loop that runs thousands of times a minute, short enough that an operator watching a live
     * log still sees it recur.
     */
    private static final long FAILURE_LOG_INTERVAL_MS = TimeUnit.MINUTES.toMillis(5);

    /**
     * A failure with a <em>different</em> signature is worth surfacing sooner than the repeat interval - but not
     * unconditionally, because two faults alternating would otherwise defeat suppression entirely and flood the log
     * exactly as a single unsuppressed one would.
     */
    private static final long NEW_FAULT_LOG_FLOOR_MS = TimeUnit.SECONDS.toMillis(10);

    private final StateSampler sampler;

    /**
     * The single publication point. Immutable pair, volatile reference: a reader either sees the whole previous pair
     * or the whole new one, never a mixture and never a half-built snapshot.
     */
    private volatile Snapshots snapshots = new Snapshots(null, null);

    /**
     * Total failed sampling attempts since construction. Volatile because it is written on the control thread and
     * read from anywhere - it is what a health endpoint would surface.
     */
    private volatile long sampleFailureCount;

    // failure-log suppression bookkeeping - touched only on the control thread
    private String lastFailureSignature;

    private long lastFailureLoggedAtEpochMillis;

    private long suppressedFailureCount;

    public SnapshotPublisher(StateSampler sampler) {
        this.sampler = sampler;
    }

    /**
     * The usual construction: sample {@code registry} for the meters and {@code pc} for identity, and register the
     * loop-end callback immediately so the first snapshot appears on the next control loop iteration.
     */
    public static SnapshotPublisher createAndRegister(AbstractParallelEoSStreamProcessor<?, ?> pc,
                                                      MeterRegistry registry) {
        SnapshotPublisher publisher = new SnapshotPublisher(new StateSampler(pc, registry));
        publisher.registerWith(pc);
        return publisher;
    }

    /**
     * Registers sampling as a control-loop end callback. This is the only supported way to drive sampling in
     * production - see the class javadoc.
     */
    public void registerWith(AbstractParallelEoSStreamProcessor<?, ?> pc) {
        pc.addLoopEndCallBack(this::sampleOnce);
    }

    /**
     * Takes one sample and publishes it, rotating the previous current into previous.
     * <p>
     * This is the callback body. It never throws: a failure leaves the published pair untouched, so the page keeps
     * showing the last good reading (visibly ageing, because {@link PcSnapshot#getCaptureEpochMillis()} does not
     * move) instead of going blank.
     */
    public void sampleOnce() {
        PcSnapshot fresh;
        try {
            fresh = sampler.sample();
        } catch (Throwable t) {
            recordFailure(t);
            return;
        }
        // read-then-write of a field only ever written from the control thread, so no CAS is needed; the volatile
        // write is what makes the new pair visible to readers
        this.snapshots = new Snapshots(fresh, this.snapshots.getCurrent());
    }

    /**
     * The current and previous snapshots as one consistent pair, read from the volatile exactly once.
     * <p>
     * <strong>Any reader that needs both must use this</strong> rather than calling {@link #getCurrent()} and
     * {@link #getPrevious()} in turn - those are two separate volatile reads, and a publish landing between them
     * returns a current and a previous from different ticks, which silently corrupts every rate derived from them.
     * Never null; its fields are null until enough samples have been taken.
     */
    public Snapshots getSnapshots() {
        return snapshots;
    }

    /**
     * The most recently captured snapshot, or null if none has been taken yet. For a reader that needs the previous
     * one as well, use {@link #getSnapshots()}.
     */
    public PcSnapshot getCurrent() {
        return snapshots.getCurrent();
    }

    /**
     * The snapshot captured immediately before {@link #getCurrent()}, or null until at least two have been taken. For
     * a reader that needs both, use {@link #getSnapshots()} - see its javadoc for why.
     */
    public PcSnapshot getPrevious() {
        return snapshots.getPrevious();
    }

    /**
     * How many sampling attempts have failed since construction. Zero is the expected value; anything else means the
     * page is showing a stale reading and something in the sampler needs looking at.
     */
    public long getSampleFailureCount() {
        return sampleFailureCount;
    }

    private void recordFailure(Throwable t) {
        sampleFailureCount++;
        String signature = t.getClass().getName() + ": " + t.getMessage();
        long now = System.currentTimeMillis();
        boolean isNewFault = !signature.equals(lastFailureSignature);
        long sinceLastLog = now - lastFailureLoggedAtEpochMillis;
        if ((isNewFault && sinceLastLog >= NEW_FAULT_LOG_FLOOR_MS) || sinceLastLog >= FAILURE_LOG_INTERVAL_MS) {
            log.warn("Dashboard state sampling failed and was suppressed - the previously published snapshot is "
                            + "left in place and the consumer is unaffected. {} identical failure(s) were not logged "
                            + "since the last message; {} sampling attempts have failed in total.",
                    suppressedFailureCount, sampleFailureCount, t);
            lastFailureSignature = signature;
            lastFailureLoggedAtEpochMillis = now;
            suppressedFailureCount = 0;
        } else {
            suppressedFailureCount++;
        }
    }

    /**
     * The current/previous pair, published as one immutable object so the two can never be seen out of step.
     * <p>
     * {@link #getPrevious()} is always exactly one sample behind {@link #getCurrent()}, or null when only one sample
     * has been taken - which is the property that makes a delta computed from this pair meaningful.
     * <p>
     * Experimental: the dashboard module is opt-in and its API may change without notice.
     */
    @InterfaceStability.Unstable
    @Value
    public static class Snapshots {
        PcSnapshot current;

        PcSnapshot previous;

        /**
         * Whether both ends of the pair are present, i.e. whether a delta can be computed at all. A page with only
         * one sample shows the state but no rate, rather than a rate derived from a single point.
         */
        public boolean hasDelta() {
            return current != null && previous != null;
        }

        /**
         * Measured elapsed time between the two captures, in milliseconds, or null if there is no delta. A rate must
         * be divided by this, never by a nominal poll interval - dividing by the nominal interval is exactly how a
         * well-known dashboard's realtime graph came to read roughly double.
         */
        public Long elapsedMillis() {
            if (!hasDelta()) {
                return null;
            }
            return current.getCaptureEpochMillis() - previous.getCaptureEpochMillis();
        }
    }
}
