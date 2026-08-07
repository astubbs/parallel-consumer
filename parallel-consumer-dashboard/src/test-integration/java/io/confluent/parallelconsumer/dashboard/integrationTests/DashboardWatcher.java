package io.confluent.parallelconsumer.dashboard.integrationTests;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import io.confluent.parallelconsumer.dashboard.snapshot.PartitionSnapshot;
import io.confluent.parallelconsumer.dashboard.snapshot.PcSnapshot;
import io.confluent.parallelconsumer.dashboard.snapshot.WorkSnapshot;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

/**
 * Watches the <em>same state document the dashboard page draws from</em> and folds it into the handful of
 * observations {@link ShowcaseScenario}'s postconditions ask about.
 * <p>
 * This is what makes the showcase scenario an assertion rather than a slideshow. A phase that was supposed to
 * strand a band of completed work behind a failing offset, and did not, is a failed run (R33) - and the only
 * honest way to know is to read the document the panels read, not the workload's own bookkeeping. If the
 * numbers here never move, neither will the graphs.
 * <p>
 * Sampling is a background poll of {@code SnapshotPublisher#getCurrent()}, which the PC control loop refreshes
 * once per cycle. Nothing here touches PC internals, and nothing here runs on the control thread.
 * <p>
 * Observations come in two scopes: whole-run (a peak or a count since construction) and phase-scoped, rebased
 * by {@link #beginPhase(String)} from the phase's {@code onEnter}. Phase scope is what lets "the assignment
 * epoch changed <em>while an instance was joining</em>" be asserted, rather than "changed at some point".
 */
@Slf4j
public class DashboardWatcher implements AutoCloseable {

    /**
     * Fast relative to the publisher's own refresh (one per control-loop cycle), so a transient peak - the
     * whole point of the in-flight observation - is not sampled straight past.
     */
    private static final Duration POLL_INTERVAL = Duration.ofMillis(200);

    /** One partition's stranded band, as captured at its widest. */
    public static final class StrandedBand {
        private final String partitionKey;
        private final long floorOffset;
        private final long topOffset;
        private final Long committedAtCapture;

        StrandedBand(String partitionKey, long floorOffset, long topOffset, Long committedAtCapture) {
            this.partitionKey = partitionKey;
            this.floorOffset = floorOffset;
            this.topOffset = topOffset;
            this.committedAtCapture = committedAtCapture;
        }

        public String getPartitionKey() {
            return partitionKey;
        }

        /**
         * The highest offset below which nothing was incomplete - i.e. the offset the failure was pinning the
         * commit to. Everything above it, up to {@link #getTopOffset()}, was finished work that could not be
         * committed.
         */
        public long getFloorOffset() {
            return floorOffset;
        }

        /** The highest COMPLETED offset while the band existed: the top of the stranded work. */
        public long getTopOffset() {
            return topOffset;
        }

        /** How many offsets of finished work were stranded above the commit point. */
        public long getWidth() {
            return topOffset - floorOffset;
        }

        public Long getCommittedAtCapture() {
            return committedAtCapture;
        }

        @Override
        public String toString() {
            return "partition " + partitionKey + ": " + getWidth() + " offsets of completed work stranded in ("
                    + floorOffset + ".." + topOffset + "] (committed then: " + committedAtCapture + ")";
        }
    }

    private final Supplier<PcSnapshot> source;
    private volatile boolean running;
    private Thread thread;

    private final AtomicLong samplesTaken = new AtomicLong();
    private final AtomicLong peakInflight = new AtomicLong();
    private final AtomicLong peakShards = new AtomicLong();
    private final AtomicLong peakProcessedRecords = new AtomicLong();
    private final AtomicLong assignmentEpochChanges = new AtomicLong();

    /** Last assignment epoch seen per partition - a change in any of them is a re-assignment. */
    private final Map<String, Long> lastEpoch = new ConcurrentHashMap<>();
    /** Highest committed offset ever seen per partition - the proof a commit later moved past a strand. */
    private final Map<String, Long> highestCommitted = new ConcurrentHashMap<>();

    private volatile StrandedBand widestBand;
    /**
     * The first band the commit was SEEN to move past, latched forever once it happens.
     * <p>
     * Sticky on purpose. A band is any completed work sitting above an incomplete offset, and a busy
     * partition grows a transient one simply from records in flight - so a later phase can produce a WIDER
     * band than the deliberately stranded one, replace {@link #widestBand}, and make a re-derived "did the
     * commit recover?" answer flip back to false long after the recovery genuinely happened. Latching records
     * the event rather than recomputing a conclusion from state that has moved on.
     */
    private volatile StrandedBand recoveredBand;
    /** Set when the stranding phase ends - see {@link #freezeStrandedBandCapture()}. */
    private volatile boolean strandedBandCaptureFrozen;

    // phase-scoped: rebased on beginPhase
    private volatile String phaseDescription = "(before the first phase)";
    private volatile long phasePeakInflight;
    private volatile long phasePeakShards;
    private volatile long phasePeakPartitions;
    private volatile long phaseMinPartitions = Long.MAX_VALUE;
    private volatile long phaseEpochChangeBaseline;
    private volatile long phaseSampleBaseline;

    public DashboardWatcher(Supplier<PcSnapshot> source) {
        if (source == null) throw new IllegalArgumentException("a watcher needs a snapshot source to read");
        this.source = source;
    }

    public synchronized void start() {
        if (running) return;
        running = true;
        thread = new Thread(this::pollLoop, "dashboard-demo-watcher");
        thread.setDaemon(true);
        thread.start();
    }

    @Override
    public synchronized void close() {
        running = false;
        Thread t = thread;
        if (t != null) {
            t.interrupt();
            try {
                t.join(5_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            thread = null;
        }
    }

    /** Rebase the phase-scoped observations. Called from a phase's {@code onEnter}. */
    public void beginPhase(String description) {
        this.phaseDescription = description;
        this.phasePeakInflight = 0;
        this.phasePeakShards = 0;
        this.phasePeakPartitions = 0;
        this.phaseMinPartitions = Long.MAX_VALUE;
        this.phaseEpochChangeBaseline = assignmentEpochChanges.get();
        this.phaseSampleBaseline = samplesTaken.get();
    }

    private void pollLoop() {
        while (running) {
            try {
                Thread.sleep(POLL_INTERVAL.toMillis());
                observe(source.get());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (Exception e) {
                // never fail the run from the observer - a failed read is one missing sample, and the
                // postconditions already fail loudly if too few samples arrived
                log.warn("Dashboard watcher sample failed (continuing): {}", e.toString());
            }
        }
    }

    private void observe(PcSnapshot snapshot) {
        if (snapshot == null || snapshot.isEmpty()) return;
        samplesTaken.incrementAndGet();

        WorkSnapshot work = snapshot.getWork();
        if (work != null) {
            if (work.getInflightRecords() != null) {
                long inflight = work.getInflightRecords();
                raise(peakInflight, inflight);
                if (inflight > phasePeakInflight) phasePeakInflight = inflight;
            }
            if (work.getShards() != null) {
                long shards = work.getShards();
                raise(peakShards, shards);
                if (shards > phasePeakShards) phasePeakShards = shards;
            }
        }

        List<PartitionSnapshot> partitions = snapshot.getPartitions();
        if (partitions == null) return;
        long assigned = partitions.size();
        if (assigned > phasePeakPartitions) phasePeakPartitions = assigned;
        if (assigned < phaseMinPartitions) phaseMinPartitions = assigned;

        long processed = 0;
        for (PartitionSnapshot partition : partitions) {
            String key = partition.getKey();
            if (partition.getProcessedRecords() != null) processed += partition.getProcessedRecords().longValue();
            if (partition.getAssignmentEpoch() != null) {
                Long previous = lastEpoch.put(key, partition.getAssignmentEpoch());
                if (previous != null && !previous.equals(partition.getAssignmentEpoch())) {
                    assignmentEpochChanges.incrementAndGet();
                }
            }
            if (partition.getLastCommittedOffset() != null) {
                highestCommitted.merge(key, partition.getLastCommittedOffset(), Math::max);
            }
            recordBandIfWidest(partition, key);
        }
        raise(peakProcessedRecords, processed);
        latchRecoveryIfCommitPassedTheBand();
    }

    /**
     * Latch the moment the commit is first seen ABOVE THE TOP of the band - not merely above its floor.
     * <p>
     * The distinction is the whole assertion. PC reports the committed offset as the next offset to consume,
     * so it already sits one above the floor the instant the band forms; a floor comparison therefore latches
     * immediately and reports recovery in the very phase that is supposed to be BLOCKING it. Measured: a band
     * of 129 offsets above floor 1990 reported "committed 1991" while the key was still failing. Requiring the
     * commit to clear the top offset is the only version that means the stranded work actually got committed.
     */
    private void latchRecoveryIfCommitPassedTheBand() {
        if (recoveredBand != null) return;
        StrandedBand band = widestBand;
        if (band == null) return;
        Long committed = highestCommitted.get(band.getPartitionKey());
        if (committed != null && committed > band.getTopOffset()) {
            recoveredBand = band;
            log.info("Commit cleared the stranded band: {} (committed {})", band, committed);
        }
    }

    /**
     * A band exists when a partition holds offsets it has NOT completed below offsets it HAS - which is
     * exactly what one repeatedly-failing key produces, and exactly what the offset ribbon is for. Recorded at
     * its widest, with the commit position at that instant, so the recovery phase has something concrete to
     * prove it moved past.
     * <p>
     * Ignored once {@link #freezeStrandedBandCapture()} has been called: a busy partition grows a transient
     * band from records merely in flight, so a later phase could otherwise replace the deliberately stranded
     * one with a bigger accident and move the target the recovery phase is aiming at.
     */
    private void recordBandIfWidest(PartitionSnapshot partition, String key) {
        if (strandedBandCaptureFrozen) return;
        Long incomplete = partition.getIncompleteOffsets();
        Long completed = partition.getHighestCompletedOffset();
        Long sequential = partition.getHighestSequentialSucceededOffset();
        if (incomplete == null || incomplete <= 0 || completed == null || sequential == null) return;
        long width = completed - sequential;
        StrandedBand current = widestBand;
        if (width > 0 && (current == null || width > current.getWidth())) {
            widestBand = new StrandedBand(key, sequential, completed, partition.getLastCommittedOffset());
        }
    }

    /**
     * Stop replacing the recorded band. Called when the phase that deliberately stranded work ends, so the
     * recovery phase has one fixed thing to prove the commit cleared.
     */
    public void freezeStrandedBandCapture() {
        this.strandedBandCaptureFrozen = true;
        log.info("Stranded-band capture frozen at: {}", widestBand);
    }

    /**
     * Forget the recorded band and the recovery latch, and start capturing again.
     * <p>
     * Called on entering the stranding phase, which matters in {@code LOOP} mode: without it, the second pass
     * would inherit the first pass's band and its recovery, and both phases would pass on evidence they did
     * not produce - the same "silently stops demonstrating it" failure R33 exists to prevent.
     */
    public void resetStrandedBandCapture() {
        this.widestBand = null;
        this.recoveredBand = null;
        this.strandedBandCaptureFrozen = false;
    }

    private static void raise(AtomicLong holder, long candidate) {
        long seen;
        while (candidate > (seen = holder.get())) {
            if (holder.compareAndSet(seen, candidate)) return;
        }
    }

    // --- whole-run observations ---

    public long getSamplesTaken() {
        return samplesTaken.get();
    }

    public long getPeakInflight() {
        return peakInflight.get();
    }

    public long getPeakShards() {
        return peakShards.get();
    }

    public long getPeakProcessedRecords() {
        return peakProcessedRecords.get();
    }

    public long getAssignmentEpochChanges() {
        return assignmentEpochChanges.get();
    }

    /** The widest stranded band this run produced, or null if no partition ever held one. */
    public StrandedBand getWidestStrandedBand() {
        return widestBand;
    }

    /**
     * Whether the commit was ever seen to clear the top of a stranded band. False when no band was ever
     * observed - "nothing to recover from" must not read as "recovered".
     */
    public boolean hasCommittedPastStrandedBand() {
        return recoveredBand != null;
    }

    /** The band the commit was seen to move past, or null if that never happened. */
    public StrandedBand getRecoveredStrandedBand() {
        return recoveredBand;
    }

    /** Highest committed offset seen on the partition that held the widest band, or null. */
    public Long getHighestCommittedOnStrandedPartition() {
        StrandedBand band = widestBand;
        return band == null ? null : highestCommitted.get(band.getPartitionKey());
    }

    // --- phase-scoped observations ---

    public long getPhaseSamples() {
        return samplesTaken.get() - phaseSampleBaseline;
    }

    public long getPhasePeakInflight() {
        return phasePeakInflight;
    }

    /** Most shards seen holding work at one instant during this phase - occupancy, not a running total. */
    public long getPhasePeakShards() {
        return phasePeakShards;
    }

    public long getPhaseAssignmentEpochChanges() {
        return assignmentEpochChanges.get() - phaseEpochChangeBaseline;
    }

    /** True if this instance was assigned more partitions at some point in the phase than at its narrowest. */
    public boolean phasePartitionCountGrew() {
        long min = phaseMinPartitions;
        return min != Long.MAX_VALUE && phasePeakPartitions > min;
    }

    /** Everything worth printing in a completion summary, in one line per observation. */
    public List<String> summarise() {
        List<String> lines = new ArrayList<>();
        lines.add("state-document samples read: " + getSamplesTaken());
        lines.add("peak in-flight records:      " + getPeakInflight());
        lines.add("peak shards:                 " + getPeakShards());
        lines.add("records processed (peak sum):" + getPeakProcessedRecords());
        lines.add("assignment epoch changes:    " + getAssignmentEpochChanges());
        StrandedBand band = widestBand;
        lines.add("widest stranded band:        " + (band == null ? "none observed" : band.toString()));
        StrandedBand recovered = recoveredBand;
        lines.add("band the commit cleared:     " + (recovered == null
                ? "none - the commit never got above a stranded band's top offset"
                : recovered + ", highest committed there: " + highestCommitted.get(recovered.getPartitionKey())));
        lines.add("last phase entered:          " + phaseDescription);
        return lines;
    }
}
