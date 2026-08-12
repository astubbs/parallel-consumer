package bz.stub.parallelconsumer.dashboard.snapshot;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.internal.AbstractParallelEoSStreamProcessor;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.annotation.InterfaceStability;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
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
 * <strong>Why publication is announced rather than polled for.</strong> A reader that wants every sample used to
 * have no choice but to re-read {@link #getCurrent()} on a timer of its own, which is a poll: it arrives late by up
 * to its own period, it runs when nothing has been published, and its period becomes an accidental floor on how
 * fresh anything downstream can be. {@link #addListener(SnapshotListener)} removes the timer - a sample is announced
 * at the instant it is published.
 * <p>
 * <strong>The listener contract is deliberately austere, and it is a hard constraint rather than advice.</strong>
 * Listeners run on the control thread, inside the loop-end callback, so whatever a listener does is latency added to
 * every single control loop iteration. A listener must therefore do nothing but hand off: set a flag, offer to a
 * queue, wake another thread. It must not serialise anything, must not touch a socket, must not block on a lock, and
 * must not wait on another thread. It is also handed no snapshot - only the fact that one exists - precisely so that
 * the tempting shortcut of reading the value on this thread and carrying it somewhere is not the path of least
 * resistance; a listener reads {@link #getSnapshots()} later, from wherever it does its real work.
 * <p>
 * <strong>Fault isolation.</strong> A dashboard bug must not be able to stop a consumer. {@link #sampleOnce()}
 * catches {@link Throwable} from both sampling and notification, leaves the previously published pair in place, and
 * logs at WARN with repeat suppression - the control loop runs continuously, so an unsuppressed log line here would
 * be a log flood, which is its own outage. A listener that throws is isolated from the control loop and from the
 * other listeners.
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
     * Copy-on-write because the control thread iterates this on every single loop iteration while registration
     * happens a handful of times in a process's life: iteration must be allocation-free and lock-free, and paying
     * for that with an array copy per registration is the right way round.
     */
    private final List<SnapshotListener> listeners = new CopyOnWriteArrayList<>();

    /**
     * Sampling failures, kept separately from notification failures because they mean different things: a sampling
     * failure means the page is showing a stale reading, a notification failure means a subscriber is broken while
     * the reading itself is fine.
     */
    private final FailureLog samplingFailures = new FailureLog(
            "Dashboard state sampling failed and was suppressed - the previously published snapshot is left in "
                    + "place and the consumer is unaffected.");

    private final FailureLog notificationFailures = new FailureLog(
            "A dashboard snapshot listener threw and was suppressed - the snapshot was still published, the other "
                    + "listeners still ran, and the consumer is unaffected.");

    /**
     * Set by {@link #stopSampling()}; checked at the top of {@link #sampleOnce()}.
     * <p>
     * <strong>This is the only way sampling can be stopped, because core has no {@code removeLoopEndCallBack}.</strong>
     * A registered callback lives as long as the {@link AbstractParallelEoSStreamProcessor} does, so without this flag
     * a closed dashboard would keep walking every meter in the registry and allocating a snapshot on the user's
     * control loop, forever - and every start/stop cycle would add another one. Making the callback a cheap
     * volatile-read-and-return is the most this module can do from outside core; adding
     * {@code removeLoopEndCallBack} there is the durable fix and would let the callback be deregistered outright.
     * <p>
     * Volatile because {@link #stopSampling()} is called from whichever thread closes the server and the flag is read
     * on the control thread.
     */
    private volatile boolean samplingStopped;

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
        if (samplingStopped) {
            // the callback cannot be deregistered from here, so this is where it becomes free - see samplingStopped
            return;
        }
        PcSnapshot fresh;
        try {
            fresh = sampler.sample();
        } catch (Throwable t) {
            samplingFailures.record(t);
            return;
        }
        // read-then-write of a field only ever written from the control thread, so no CAS is needed; the volatile
        // write is what makes the new pair visible to readers
        this.snapshots = new Snapshots(fresh, this.snapshots.getCurrent());
        // AFTER the publish, never before: a listener that reacts by reading getSnapshots() must find the sample it
        // was told about, not the one before it
        notifyListeners();
    }

    /**
     * Stops sampling permanently. Idempotent, and safe to call from any thread.
     * <p>
     * After this returns, the loop-end callback registered by {@link #registerWith} does a volatile read and returns.
     * The last published pair is left in place, so a reader that still holds this publisher sees the final reading
     * ageing rather than a null.
     * <p>
     * There is no {@code startSampling()} counterpart on purpose: this is a shutdown, and a publisher that could be
     * resumed would need to answer what happens to the gap in the middle. Construct a new one.
     */
    public void stopSampling() {
        samplingStopped = true;
    }

    /**
     * Whether {@link #stopSampling()} has been called. Exists so a shutdown can be asserted to have actually taken
     * effect - the alternative is counting samples and waiting, which proves nothing about a slow control loop.
     */
    public boolean isSamplingStopped() {
        return samplingStopped;
    }

    /**
     * Registers a listener to be told, on the control thread, that a new snapshot has been published. Read the
     * class javadoc before writing one - the constraints on what it may do are not stylistic.
     * <p>
     * Registering the same listener twice registers it twice; this is a list, not a set, because a caller that
     * genuinely wants two independent subscriptions from one object should be able to have them, and silently
     * dropping a registration is a worse failure than an obvious duplicate.
     */
    public void addListener(SnapshotListener listener) {
        if (listener != null) {
            listeners.add(listener);
        }
    }

    /**
     * Deregisters a listener, if it is registered. A subscriber that is shutting down must call this: the publisher
     * outlives the dashboard server, so a listener left behind is a leak that keeps calling into a closed object on
     * every control loop iteration.
     */
    public void removeListener(SnapshotListener listener) {
        listeners.remove(listener);
    }

    /**
     * How many listeners are registered. Exists so a shutdown path can be asserted to have actually deregistered,
     * which is the only way a leak of this kind is visible from outside.
     */
    public int getListenerCount() {
        return listeners.size();
    }

    private void notifyListeners() {
        for (SnapshotListener listener : listeners) {
            try {
                listener.onSnapshotPublished();
            } catch (Throwable t) {
                // one broken listener must not cost the others their notification, and none of them may cost the
                // control loop anything at all
                notificationFailures.record(t);
            }
        }
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
        return samplingFailures.getCount();
    }

    /**
     * How many listener notifications have thrown since construction. Zero is the expected value; anything else
     * means a subscriber is broken while the published snapshot itself is fine.
     */
    public long getListenerFailureCount() {
        return notificationFailures.getCount();
    }

    /**
     * What a listener is told when a snapshot is published. Not a {@link Runnable} so that the constraints below have
     * somewhere to live where an implementer will read them.
     * <p>
     * <strong>Runs on the control thread.</strong> Whatever this method does is added to the latency of every
     * control loop iteration, on a loop that runs thousands of times a minute. So it must hand off and return: set a
     * flag, offer to a non-blocking queue, wake an event loop. It must not serialise, must not do I/O, must not
     * block, and must not wait on another thread.
     * <p>
     * It is handed no snapshot on purpose. Read {@link #getSnapshots()} from the thread that does the real work -
     * see the class javadoc for why carrying a value off this thread is the shortcut worth designing out.
     * <p>
     * Experimental: the dashboard module is opt-in and its API may change without notice.
     */
    @InterfaceStability.Unstable
    public interface SnapshotListener {

        void onSnapshotPublished();
    }

    /**
     * Repeat-suppressed WARN logging for a fault that recurs on every control loop iteration, with a count of what
     * it swallowed.
     * <p>
     * One class used by both failure paths rather than two copies of the bookkeeping: the suppression rules are the
     * interesting part and they are identical, so a second copy would be a second place for them to drift.
     * <p>
     * Not thread-safe, and does not need to be: every field is touched only from the control thread, except the
     * count, which is volatile because a health endpoint reads it from anywhere.
     */
    private static final class FailureLog {

        private final String what;

        private volatile long count;

        private String lastSignature;

        private long lastLoggedAtEpochMillis;

        private long suppressedSinceLastLog;

        private FailureLog(String what) {
            this.what = what;
        }

        long getCount() {
            return count;
        }

        void record(Throwable t) {
            count++;
            String signature = t.getClass().getName() + ": " + t.getMessage();
            long now = System.currentTimeMillis();
            boolean isNewFault = !signature.equals(lastSignature);
            long sinceLastLog = now - lastLoggedAtEpochMillis;
            if ((isNewFault && sinceLastLog >= NEW_FAULT_LOG_FLOOR_MS) || sinceLastLog >= FAILURE_LOG_INTERVAL_MS) {
                log.warn("{} {} identical failure(s) were not logged since the last message; {} failure(s) in "
                        + "total.", what, suppressedSinceLastLog, count, t);
                lastSignature = signature;
                lastLoggedAtEpochMillis = now;
                suppressedSinceLastLog = 0;
            } else {
                suppressedSinceLastLog++;
            }
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
