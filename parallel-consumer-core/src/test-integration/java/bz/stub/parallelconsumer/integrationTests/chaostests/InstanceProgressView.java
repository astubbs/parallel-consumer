package bz.stub.parallelconsumer.integrationTests.chaostests;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.integrationTests.utils.ManagedPCInstance;

import java.time.Duration;

/**
 * What the instance-progress probe samples from one fleet member. An interface rather than
 * {@code ManagedPCInstance} directly so the detector's decision logic is broker-free testable
 * against fake views ({@code InstanceStallProbeIT}) - the same pure-replay pattern as
 * {@code ProgressProbe#ledger} and {@code KeyOrderLedger#check}.
 */
public interface InstanceProgressView {
    int instanceId();

    /**
     * Started, not mid-stop/restart, and its PC is up and not failed. The chaos harness stops and
     * restarts members constantly; a stopped or restarting instance holds torn-down state and must
     * never be reported as stalled.
     */
    boolean isLive();

    /** Work queued in this instance's shards awaiting selection ({@code WorkManager}'s own count). */
    long queuedInShards();

    /** Records this instance currently has out for processing ({@code WorkManager}'s own count). */
    long outForProcessing();

    /**
     * One line of engine counters for a stall dump, read straight off the live {@code WorkManager}
     * - what the instance holds and whether any of it is parked for retry, which the three
     * progress counters above cannot say. Default empty so a scripted view need not fake it.
     */
    default String engineSnapshot() {
        return "";
    }

    /**
     * Workers running user code right now - the difference between a member that is stalled and
     * one that is working. {@link InstanceStallDetector#BUSY_WORKERS_UNKNOWN} when the view cannot say, which the
     * detector treats as "assume none": the pre-2026-09-07 rule, kept for scripted views.
     * <p>
     * <b>Why the detector needs it.</b> The instance-stall line on {@code ChaosChurnStormIT} was
     * classified as a wedge and turned out to be all ten workers asleep in the scenario's heavy
     * dwell, on records revoked out from under them - a working member, with a control loop that
     * had nothing to finish ({@code docs/inflight/test-857-churn-storm-async-stalls.md},
     * "DIAGNOSED, 2026-09-07"). Held work plus a frozen completion count is the detector's whole
     * signal, and a member produces it for as long as ANY of its user functions run: one long
     * function freezes the count, and PC's own backpressure counts records rather than workers,
     * so a free worker beside a busy one proves nothing. Only a member holding work with NO
     * worker in user code is making a claim about PC - its results are with nobody.
     * <p>
     * A busy count rather than an idle one because idle needs a capacity to subtract from, and a
     * pool that has not created its last thread has a free slot no thread shows - the first cut
     * of this rule read such a member as "one idle" and would have accused it.
     */
    default int busyWorkers() {
        return InstanceStallDetector.BUSY_WORKERS_UNKNOWN;
    }

    /** Monotone count of work results returned - see {@code ManagedPCInstance#workResultsReturned}. */
    long workResultsReturned();

    /**
     * Identity that changes when the instance brings up a NEW PC (a restart). A fresh incarnation
     * gets a fresh full bound-window rather than inheriting the old PC's silence.
     */
    Object incarnationMarker();

    /** The live adapter over a real fleet member, reading PC's own {@code WorkManager} state. */
    static InstanceProgressView of(ManagedPCInstance pc) {
        return new InstanceProgressView() {
            @Override
            public int instanceId() {
                return pc.getInstanceId();
            }

            @Override
            public boolean isLive() {
                var parallelConsumer = pc.getParallelConsumer();
                return pc.isStarted() && !pc.isClosePending()
                        && parallelConsumer != null && !parallelConsumer.isClosedOrFailed();
            }

            @Override
            public long queuedInShards() {
                var parallelConsumer = pc.getParallelConsumer();
                // the count can be transiently negative by its own javadoc (counter races) - floor it
                return parallelConsumer == null ? 0
                        : Math.max(0, parallelConsumer.getWm().getNumberOfWorkQueuedInShardsAwaitingSelection());
            }

            @Override
            public long outForProcessing() {
                var parallelConsumer = pc.getParallelConsumer();
                return parallelConsumer == null ? 0
                        : Math.max(0, parallelConsumer.getWm().getNumberRecordsOutForProcessing());
            }

            @Override
            public long workResultsReturned() {
                return pc.getWorkResultsReturnedCount();
            }

            @Override
            public Object incarnationMarker() {
                return pc.getParallelConsumer();
            }

            @Override
            public int busyWorkers() {
                return pc.getParallelConsumer() == null ? InstanceStallDetector.BUSY_WORKERS_UNKNOWN
                        : InstanceStallDetector.busyWorkersOf(pc.getInstanceId());
            }

            @Override
            public String engineSnapshot() {
                var parallelConsumer = pc.getParallelConsumer();
                if (parallelConsumer == null) {
                    return "no PC";
                }
                var wm = parallelConsumer.getWm();
                var sm = wm.getSm();
                return "closedOrFailed=" + parallelConsumer.isClosedOrFailed()
                        + " incompleteOffsets=" + wm.getNumberOfIncompleteOffsets()
                        + " recordsInShards=" + sm.getNumberOfRecordsInShards()
                        + " parkedForRetry=" + sm.getNumberOfRecordsParkedForRetry()
                        + " lowestRetryIn=" + wm.getLowestRetryTime().map(Duration::toString).orElse("none");
            }
        };
    }
}
