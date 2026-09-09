package bz.stub.parallelconsumer.integrationTests.chaostests;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import lombok.Getter;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.ConsumerGroupState;
import org.apache.kafka.common.TopicPartition;

import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * The per-partition liveness gate ({@code UNCOMMITTED_COMPLETIONS/COMMIT_NOT_LANDING}) that closes
 * the gap {@code docs/inflight/test-per-shard-liveness-has-no-gate.md} opened when
 * {@link ProgressProbe#LAG_STAGNATION_BOUND} was demoted to an observation on 2026-08-25.
 * <p>
 * <b>The uncovered case, restated.</b> {@link InstanceStallDetector#INSTANCE_STALL_BOUND} is
 * per-INSTANCE and is re-armed by any successful work result, so an instance whose other shards keep
 * completing never fires it however long one partition's commits have been frozen. The fleet-wide
 * {@code NO_PROGRESS} watermark is coarser still. {@code CLASS2_STALL} is the only detector that
 * looks at a partition at all, and it does not gate - because what it measures, elapsed stagnation of
 * a committed offset, is crossed just as readily by a slow record as by a wedge.
 * <p>
 * <b>What this detector asks instead, and why it is not that bound wearing a new name.</b> The
 * quantity here is not an elapsed time but the DIFFERENCE BETWEEN TWO POSITIONS: the instance's own
 * next-offset-to-commit for a partition ({@link InstanceProgressView#localOffsetToCommit}, which is
 * {@code offsetHighestSequentialSucceeded + 1} - what PC would commit if it committed now) against
 * what the group has actually committed. A positive difference means work is finished locally and no
 * commit has carried it to the broker; a restart at that instant redelivers exactly those records.
 * <p>
 * <b>That difference excludes the false positive structurally rather than by calibration, which is
 * the whole point.</b> The case that made the Class 2 bound uninformative - one heavy or repeatedly
 * redelivered record pinning a partition's committed offset while the shard behind it works normally
 * - pins {@code offsetHighestSequentialSucceeded} at exactly the same place, because that offset is
 * defined as one below the LOWEST INCOMPLETE offset. So on the two replay seeds the demotion was
 * argued from ({@code 6825864417772979246} and {@code 4044221734199516240}, whose pinned partitions
 * were pinned by an in-flight record) the difference this detector reads is zero, and it is silent no
 * matter how long the stagnation runs. No threshold sits between a healthy peak and a defect peak
 * here; there is nothing for load to push across.
 * <p>
 * <b>What the sample count is for, and what it is not.</b> A commit is periodic
 * ({@code ParallelConsumerOptions#DEFAULT_COMMIT_INTERVAL}), so a positive difference is the NORMAL
 * state of a busy partition between two commit cycles. {@link #COMMIT_NOT_LANDING_SAMPLES} therefore
 * requires the difference to survive that many consecutive samples <em>with the committed offset not
 * moving at all</em> - it is a debounce for the commit cadence, not a tolerance for slowness. Any
 * commit that lands re-arms it, so an arbitrarily slow but progressing commit path can never
 * accumulate; only a commit path that has stopped can.
 * <p>
 * <b>The clock only runs while the group is STABLE.</b> A rebalance legitimately defers commits, and
 * an instance mid-stop or mid-restart holds torn-down state - both re-arm every partition, the same
 * rule {@link InstanceStallDetector#sample} keeps for a member that is not live. The cost is a real
 * blind spot in the churn scenarios, whose groups are rarely stable for long: this detector is armed
 * in the stable stretches and says nothing about the churning ones. That is a narrower claim than
 * the one it replaces, and it is deliberate - see the note above for the reasoning.
 *
 * @see ProgressProbe#withInstanceProgress
 */
@Slf4j
class UncommittedCompletionDetector {

    /**
     * How many CONSECUTIVE lag samples a partition must show locally-completed-but-uncommitted work,
     * with its committed offset standing still and the group stable, before this gates.
     * <p>
     * Sized against the commit CADENCE rather than against any measured defect signature, which is
     * what makes it a debounce rather than a calibrated bound. {@link ProgressProbe} samples this
     * detector on its lag cadence (every 5s), so six samples is ~30s: six commit cycles at the
     * shipped default interval, or three hundred at the transactional default. A healthy partition
     * with something to commit lands a commit inside one of those and re-arms; the number only has to
     * clear the widest legitimate gap between two commit attempts, and it is not required to sit
     * between two measured peaks because - see the class javadoc - the signal itself excludes the
     * slow case.
     */
    static final int COMMIT_NOT_LANDING_SAMPLES = 6;

    /** One partition's uncommitted-completion stretch: what the group had committed when it began,
     * which instance is reporting the completed work, and how many consecutive samples have held. */
    @Value
    private static class UncommittedStretch {
        long committedWhenStretchBegan;
        int instanceId;
        Object incarnation;
        int consecutiveSamples;
    }

    private final Map<TopicPartition, UncommittedStretch> stretches = new ConcurrentHashMap<>();
    /** Partitions already reported in their CURRENT stretch - one finding per stretch, not per sample. */
    private final java.util.Set<TopicPartition> reportedThisStretch = ConcurrentHashMap.newKeySet();

    /**
     * The widest local-minus-committed gap seen on any sample, in records, for the end-of-run peaks
     * line.
     * <p>
     * <b>Measured on every sample that has a reporter, not only on the stretches that gate</b> - so a
     * healthy run's peak is the largest gap its commit cadence legitimately opened, which is exactly
     * the number a future re-calibration of {@link #COMMIT_NOT_LANDING_SAMPLES} needs. That is the
     * same invariant {@code ProgressProbe#recordLagStagnation} states and the reason a demoted
     * detector still earns its keep: suppressing a finding must never lose the measurement.
     */
    @Getter
    private volatile long peakUncommittedCompletions = 0;

    /** Fleet supplier, shared with {@link InstanceStallDetector}; null = not wired (ambient mode). */
    private volatile Supplier<List<InstanceProgressView>> instanceProgressSupplier;

    private final InstanceStallDetector.FindingSink sink;

    UncommittedCompletionDetector(InstanceStallDetector.FindingSink sink) {
        this.sink = sink;
    }

    /** Arms the detector with the same live fleet view the instance-stall detector watches. */
    void watch(Supplier<List<InstanceProgressView>> fleetSupplier) {
        this.instanceProgressSupplier = fleetSupplier;
    }

    /**
     * One sample for one partition - see the class javadoc for the property and
     * {@link #COMMIT_NOT_LANDING_SAMPLES} for why the count exists.
     * <p>
     * Package-private and taking the group state explicitly so {@code UncommittedCompletionProbeIT}
     * can drive it deterministically, broker-free, in both directions; {@link ProgressProbe} calls it
     * from the lag sampler, which has already paid for the committed-offset round trip.
     *
     * @param committed  what the GROUP has committed for this partition, read from the broker
     * @param lag        the partition's current lag, for the finding's text only
     * @param groupState the group's state at this sample; anything but {@code STABLE} re-arms
     * @return whether the detector fired on this sample
     */
    boolean sample(TopicPartition tp, long committed, long lag, ConsumerGroupState groupState) {
        var supplier = instanceProgressSupplier;
        if (supplier == null) return false; // not wired (ambient mode, or a scenario predating this probe)
        if (groupState != ConsumerGroupState.STABLE) {
            // a rebalance legitimately defers commits - re-arm rather than accumulate through it
            clear(tp);
            return false;
        }

        // Which LIVE member reports finished work above what the group has committed? A member that is
        // not live holds torn-down state, and one that does not own the partition answers empty, so
        // neither can accuse.
        InstanceProgressView reporter = null;
        long widestLocalNext = committed;
        for (InstanceProgressView view : supplier.get()) {
            if (!view.isLive()) continue;
            OptionalLong localNext = view.localOffsetToCommit(tp);
            if (!localNext.isPresent()) continue;
            if (localNext.getAsLong() > widestLocalNext) {
                widestLocalNext = localNext.getAsLong();
                reporter = view;
            }
        }
        if (reporter == null) {
            // nothing finished-but-uncommitted anywhere: the commit path is level with the work
            clear(tp);
            return false;
        }

        long uncommittedCompletions = widestLocalNext - committed;
        if (uncommittedCompletions > peakUncommittedCompletions) {
            peakUncommittedCompletions = uncommittedCompletions;
        }

        UncommittedStretch previous = stretches.get(tp);
        boolean continues = previous != null
                && previous.getCommittedWhenStretchBegan() == committed
                && previous.getInstanceId() == reporter.instanceId()
                && previous.getIncarnation() == reporter.incarnationMarker();
        int samples = continues ? previous.getConsecutiveSamples() + 1 : 1;
        stretches.put(tp, new UncommittedStretch(committed, reporter.instanceId(),
                reporter.incarnationMarker(), samples));
        if (!continues) {
            // the committed offset moved, or a different member (or incarnation) is now the reporter:
            // a fresh stretch, which earns its own finding
            reportedThisStretch.remove(tp);
        }
        if (samples < COMMIT_NOT_LANDING_SAMPLES || !reportedThisStretch.add(tp)) {
            return false;
        }
        sink.violate("UNCOMMITTED_COMPLETIONS/COMMIT_NOT_LANDING: partition " + tp + " - instance "
                + reporter.instanceId() + " has locally completed every offset below " + widestLocalNext
                + " (" + uncommittedCompletions + " records) while the group's committed offset has stood at "
                + committed + " across " + samples + " consecutive samples with the group STABLE, lag=" + lag
                + ". The records are finished and the commit that carries them has not landed, so a restart "
                + "redelivers exactly those records. This is NOT the Class 2 false positive: an in-flight or "
                + "redelivered record pins offsetHighestSequentialSucceeded too, which would make this "
                + "difference zero - see UncommittedCompletionDetector's javadoc and "
                + "docs/inflight/test-per-shard-liveness-has-no-gate.md");
        return true;
    }

    /**
     * Re-arm one partition. Separate from {@link #stretches} removal alone because the
     * report-once-per-stretch marker has to go with it, or a partition that recovers and wedges again
     * reports only the first time.
     */
    private void clear(TopicPartition tp) {
        stretches.remove(tp);
        reportedThisStretch.remove(tp);
    }
}
