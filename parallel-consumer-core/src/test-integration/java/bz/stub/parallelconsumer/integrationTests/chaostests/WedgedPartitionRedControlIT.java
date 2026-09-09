package bz.stub.parallelconsumer.integrationTests.chaostests;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.ParallelEoSStreamProcessor;
import lombok.extern.slf4j.Slf4j;
import bz.stub.parallelconsumer.internal.utils.LongPollingMockConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.ConsumerGroupState;
import org.apache.kafka.common.TopicPartition;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static pl.tlinkowski.unij.api.UniLists.of;

/**
 * <b>The RED CONTROL for the per-partition liveness gap</b>, and the reason
 * {@link UncommittedCompletionDetector} was allowed to exist at all.
 * <p>
 * {@code docs/inflight/test-per-shard-liveness-has-no-gate.md} states the bar in its own words: the
 * bound this replaces "was itself green-calibrated and argued for, and was wrong for three months",
 * so before anything gates, an injected fault must freeze one partition's commits while its siblings
 * keep completing, every existing gating detector must be shown to stay GREEN on it, and the new one
 * must be shown to fire. That is what the three arms below do, in one JVM, with no broker and no
 * wall-clock waiting on a bound.
 * <p>
 * <b>The fault is a real defect class, not an invented one.</b> {@link BlackHolingMockConsumer}
 * acknowledges every commit in full and applies only the partitions that are not black-holed - a
 * coordinator that answers and drops, which is the shape
 * {@code MockConsumerAsyncCommitCallbackDroppedTest} covers from the other side and the shape
 * astubbs#470 fixed inside PC (an async commit counted as committed when it was SENT rather than when
 * the broker answered). PC's control loop is entirely undisturbed by it, which is precisely the
 * point: nothing in the engine notices, so nothing that watches the engine can.
 * <p>
 * <b>The arms, and what each establishes:</b>
 * <ul>
 *   <li>{@link #wedgedPartitionIsInvisibleToEveryExistingGateAndVisibleToTheNewOne()} - RED. One
 *   partition's commits are dropped; its siblings drain and commit normally. Every record reaches the
 *   user function, so the correctness ledger balances; the instance keeps returning results, so
 *   {@code INSTANCE_STALL} re-arms throughout and the settled instance holds nothing, so it stays
 *   silent across twenty times its bound while an armed control fires; the Class 2 detector sees the frozen watermark and files an OBSERVATION, which does not
 *   gate. The new detector fires.</li>
 *   <li>{@link #aHealthyRunNeverFiresTheNewDetector()} - GREEN control. Same harness, nothing
 *   black-holed.</li>
 *   <li>{@link #aPartitionPinnedByAnIncompleteRecordNeverFiresTheNewDetector()} - the GREEN control
 *   that matters, because it is the false positive the Class 2 bound could never separate: a
 *   partition whose committed offset is frozen by one record still in the user function, with real
 *   lag behind it and its siblings completing. Class 2 observes it; the new detector is silent,
 *   because an incomplete record pins {@code offsetHighestSequentialSucceeded} at exactly the same
 *   place and the difference the detector reads is therefore zero.</li>
 * </ul>
 * <b>Broker-free and deliberately not tagged {@code chaos}</b>, so it gates every default integration
 * build - the {@link InstanceStallProbeIT} / {@link ProgressProbeLedgerIT} pattern. A chaos scenario
 * would have needed a fleet, a broker and a fault injector to establish the same thing, and would
 * have paid a bound's worth of wall clock to do it.
 * <p>
 * <b>Why the harness is here rather than reused.</b> {@code MockConsumerTestBase} is the shared home
 * for raw-{@link MockConsumer} scenarios and would be the right parent, but it is package-private in
 * {@code bz.stub.parallelconsumer} and single-partition by construction (one {@code topicPartition}
 * field, one rebalance, one offset store) - and a wedge that hides behind healthy siblings cannot be
 * expressed with one partition. Widening that base for a probe test in another package would change
 * the wiring under eight commit scenarios whose subject is that wiring.
 */
@Slf4j
@Timeout(120)
class WedgedPartitionRedControlIT {

    private static final String TOPIC = "wedged-partition-red-control";
    private static final TopicPartition HEALTHY_A = new TopicPartition(TOPIC, 0);
    private static final TopicPartition WEDGED = new TopicPartition(TOPIC, 1);
    private static final TopicPartition HEALTHY_B = new TopicPartition(TOPIC, 2);
    private static final List<TopicPartition> ALL = of(HEALTHY_A, WEDGED, HEALTHY_B);
    /** Enough per partition that the frozen one carries real lag, and small enough to drain in seconds. */
    private static final int RECORDS_PER_PARTITION = 60;
    /** Well under the run's own duration, so several commit cycles happen while the fault is active. */
    private static final Duration COMMIT_INTERVAL = Duration.ofMillis(200);
    /**
     * Which offset the false-positive arm holds open in the user function. NOT offset 0 and not the
     * last one: under {@code PARTITION} ordering one incomplete record stops its whole partition, so a
     * record chosen at either end leaves either no completed work behind it or no lag in front of it,
     * and the arm would then differ from the red control in two terms rather than one. Nine leaves
     * nine committed offsets behind it and {@code RECORDS_PER_PARTITION - 9} of real lag ahead, which
     * clears {@link ProgressProbe#LAG_STAGNATION_MIN_LAG} - so Class 2 genuinely cannot tell the two
     * arms apart, which is the comparison this arm exists to make.
     */
    private static final int BLOCKED_OFFSET = 9;
    /** Cadence of the run's own engine-counter trace - dense enough that a fast drain still records one. */
    private static final Duration TRACE_SAMPLE_INTERVAL = Duration.ofMillis(20);

    /** Synthetic replay clock - see {@link #assertInstanceStallCannotFireOnTheSettledWedge}. */
    private static final Instant REPLAY_T0 = Instant.parse("2026-01-01T00:00:00Z");

    private BlackHolingMockConsumer mockConsumer;
    private ParallelEoSStreamProcessor<String, String> parallelConsumer;
    private CountDownLatch blockedRecordRelease;

    /**
     * A consumer that ANSWERS every commit and APPLIES only some of it. The offsets for a black-holed
     * partition are dropped on the floor while the callback reports the whole offer as successful, so
     * PC marks the partition clean and never re-sends it - the broker's committed offset for it is
     * frozen for the life of the run and PC has no way to know.
     * <p>
     * <b>Only {@code commitAsync} is overridden, and that is not a gap.</b> {@link MockConsumer}
     * implements {@code commitSync(Map)} by delegating to {@code commitAsync(offsets, null)}, so both
     * of PC's commit paths funnel through this one method - and overriding the other as well made the
     * pair mutually recursive, which showed up as a {@code StackOverflowError} inside the broker-poll
     * thread rather than as anything resembling a wrong commit.
     */
    private static final class BlackHolingMockConsumer extends LongPollingMockConsumer<String, String> {
        private final Set<TopicPartition> blackHoled = new HashSet<>();

        BlackHolingMockConsumer() {
            super(OffsetResetStrategy.EARLIEST);
        }

        void blackHole(TopicPartition tp) {
            blackHoled.add(tp);
        }

        private Map<TopicPartition, OffsetAndMetadata> applicable(Map<TopicPartition, OffsetAndMetadata> offsets) {
            Map<TopicPartition, OffsetAndMetadata> applied = new LinkedHashMap<>();
            offsets.forEach((tp, offsetAndMetadata) -> {
                if (blackHoled.contains(tp)) {
                    log.info("Black-holing the commit of {} for {} - it is answered but never applied", offsetAndMetadata, tp);
                } else {
                    applied.put(tp, offsetAndMetadata);
                }
            });
            return applied;
        }

        @Override
        public synchronized void commitAsync(Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback) {
            // the offer is APPLIED filtered, with no callback of its own...
            super.commitAsync(applicable(offsets), null);
            if (callback != null) {
                // ...and ACKNOWLEDGED whole, black-holed partitions included. That is the fault.
                callback.onComplete(offsets, null);
            }
        }
    }

    /** One sample of the engine counters {@link InstanceStallDetector} reads, taken during the live run. */
    private static final class EngineSample {
        final long queued;
        final long outForProcessing;
        final long workResultsReturned;

        EngineSample(long queued, long outForProcessing, long workResultsReturned) {
            this.queued = queued;
            this.outForProcessing = outForProcessing;
            this.workResultsReturned = workResultsReturned;
        }
    }

    @AfterEach
    void releaseAnyBlockedRecordAndClose() {
        if (blockedRecordRelease != null) {
            // before the close, or the close waits on a user function this test is holding shut
            blockedRecordRelease.countDown();
        }
        Awaitility.reset();
        if (parallelConsumer != null && !parallelConsumer.isClosedOrFailed()) {
            parallelConsumer.close();
        }
    }

    // --- the arms ---

    @Test
    void wedgedPartitionIsInvisibleToEveryExistingGateAndVisibleToTheNewOne() {
        List<EngineSample> trace = runFleetOfOne(WEDGED, /* blockOneRecordOn */ null);

        // 1. THE WEDGE IS REAL, read from the engine and from the broker-side offset store.
        assertWithMessage("the black-holed partition's committed offset must never have moved")
                .that(brokerCommitted(WEDGED)).isEqualTo(0L);
        assertWithMessage("its siblings must have committed normally - a run where nothing committed "
                + "would prove nothing about a partition-scoped fault")
                .that(brokerCommitted(HEALTHY_A)).isEqualTo(RECORDS_PER_PARTITION);
        assertThat(brokerCommitted(HEALTHY_B)).isEqualTo(RECORDS_PER_PARTITION);
        assertWithMessage("PC has locally completed every record on the wedged partition - the work is "
                + "DONE and only the commit is missing, which is the whole shape of this fault")
                .that(localOffsetToCommit(WEDGED)).isEqualTo(OptionalLong.of(RECORDS_PER_PARTITION));

        // 2. EVERY EXISTING GATING DETECTOR STAYS GREEN ON IT.
        assertInstanceStallCannotFireOnTheSettledWedge(trace);

        ProgressProbe probe = ProgressProbe.forSeamTest("red-control-group", TOPIC);
        // the fleet-wide progress watermark: the consumed count advanced to the end, so it cannot fire
        assertWithMessage("every record reached the user function, so the fleet consumed count advanced "
                + "to completion - NO_PROGRESS has nothing to fire on")
                .that(consumedKeys).hasSize(ALL.size() * RECORDS_PER_PARTITION);
        // the correctness ledger: it counts records PROCESSED, not offsets durably committed
        assertWithMessage("the correctness ledger balances on a run that would redeliver %s records on "
                + "restart - it counts what was processed, not what was committed", RECORDS_PER_PARTITION)
                .that(ProgressProbe.ledger(new HashSet<>(consumedKeys), consumedKeys, 1, 5_000)).isEmpty();
        // Class 2 sees the frozen watermark - and files an observation, which does not gate
        boolean class2Fired = probe.recordLagStagnation(WEDGED, brokerCommitted(WEDGED), RECORDS_PER_PARTITION,
                ProgressProbe.LAG_STAGNATION_BOUND.multipliedBy(10).toMillis());
        assertWithMessage("Class 2 is the only existing detector that can see this at all")
                .that(class2Fired).isTrue();
        assertWithMessage("...and since 2026-08-25 what it files does not gate")
                .that(probe.getViolations()).isEmpty();
        assertThat(probe.getObservations()).hasSize(1);

        // 3. THE NEW DETECTOR FIRES ON IT.
        ProgressProbe gated = armedProbe();
        boolean fired = sampleUncommittedCompletionsUntilItFires(gated, WEDGED);
        assertWithMessage("the new detector must fire on the red control, or it is decoration")
                .that(fired).isTrue();
        assertThat(gated.getViolations()).hasSize(1);
        assertThat(gated.getViolations().get(0)).contains("UNCOMMITTED_COMPLETIONS/COMMIT_NOT_LANDING");
        assertThat(gated.getViolations().get(0)).contains(WEDGED.toString());
        assertWithMessage("the peak is measured whether or not anything gated")
                .that(gated.getPeakUncommittedCompletions()).isEqualTo(RECORDS_PER_PARTITION);
    }

    @Test
    void aHealthyRunNeverFiresTheNewDetector() {
        runFleetOfOne(/* blackHole */ null, /* blockOneRecordOn */ null);

        for (TopicPartition tp : ALL) {
            assertWithMessage("every partition commits on a healthy run")
                    .that(brokerCommitted(tp)).isEqualTo(RECORDS_PER_PARTITION);
        }

        ProgressProbe probe = armedProbe();
        boolean fired = sampleUncommittedCompletionsUntilItFires(probe, WEDGED);
        assertWithMessage("a detector that fires on a healthy run would be disabled within a week")
                .that(fired).isFalse();
        assertThat(probe.getViolations()).isEmpty();
    }

    @Test
    void aPartitionPinnedByAnIncompleteRecordNeverFiresTheNewDetector() {
        runFleetOfOne(/* blackHole */ null, /* blockOneRecordOn */ WEDGED);

        // The pinned partition looks, to the BROKER, exactly like the red control: a frozen committed
        // offset with real lag behind it. This is the case the Class 2 bound could never separate.
        long pinnedAt = brokerCommitted(WEDGED);
        assertWithMessage("the blocked record pins its partition's committed offset at exactly itself")
                .that(pinnedAt).isEqualTo(BLOCKED_OFFSET);
        assertWithMessage("its siblings drained and committed, so the instance was busy throughout")
                .that(brokerCommitted(HEALTHY_A)).isEqualTo(RECORDS_PER_PARTITION);

        ProgressProbe class2 = ProgressProbe.forSeamTest("false-positive-group", TOPIC);
        assertWithMessage("Class 2 cannot tell this from the red control, which is why it was demoted")
                .that(class2.recordLagStagnation(WEDGED, pinnedAt, RECORDS_PER_PARTITION - pinnedAt,
                        ProgressProbe.LAG_STAGNATION_BOUND.multipliedBy(10).toMillis())).isTrue();

        ProgressProbe probe = armedProbe();
        boolean fired = sampleUncommittedCompletionsUntilItFires(probe, WEDGED);
        assertWithMessage("the new detector separates them STRUCTURALLY: an incomplete record pins "
                + "offsetHighestSequentialSucceeded at the same offset the broker holds, so the "
                + "difference it reads is zero however long the stagnation runs")
                .that(fired).isFalse();
        assertThat(probe.getViolations()).isEmpty();
        assertWithMessage("and the peak stays at zero, so the measurement agrees with the verdict")
                .that(probe.getPeakUncommittedCompletions()).isEqualTo(0L);
    }

    // --- harness ---

    private final ConcurrentLinkedQueue<String> consumedKeys = new ConcurrentLinkedQueue<>();
    private final AtomicLong workResultsReturned = new AtomicLong();

    /**
     * Runs one PC over three partitions until the work settles, and returns the trace of engine
     * counters sampled while it ran.
     *
     * @param blackHole        partition whose commits are answered but dropped, or null for a healthy run
     * @param blockOneRecordOn partition whose FIRST record never returns from the user function, or null
     */
    private List<EngineSample> runFleetOfOne(TopicPartition blackHole, TopicPartition blockOneRecordOn) {
        mockConsumer = new BlackHolingMockConsumer();
        if (blackHole != null) {
            mockConsumer.blackHole(blackHole);
        }
        blockedRecordRelease = new CountDownLatch(1);

        parallelConsumer = new ParallelEoSStreamProcessor<>(ParallelConsumerOptions.<String, String>builder()
                .consumer(mockConsumer)
                // PARTITION ordering so one shard IS one partition: the note's uncovered case is stated
                // per partition, which is also the only granularity the broker's committed offset has.
                .ordering(ParallelConsumerOptions.ProcessingOrder.PARTITION)
                .commitInterval(COMMIT_INTERVAL)
                .maxConcurrency(ALL.size() * 2)
                .build());
        parallelConsumer.subscribe(of(TOPIC));
        // MockConsumer assigns nothing on subscribe: LongPollingMockConsumer does the rebalance and the
        // beginning offsets, and PC is told about the assignment separately - the same manual dance
        // MockConsumerTestBase documents, done through the shared harness rather than by hand.
        mockConsumer.subscribeWithRebalanceAndAssignment(of(TOPIC), ALL.size());
        parallelConsumer.onPartitionsAssigned(ALL);

        parallelConsumer.getWm().addSuccessfulWorkListener(ignored -> workResultsReturned.incrementAndGet());

        for (TopicPartition tp : ALL) {
            for (int offset = 0; offset < RECORDS_PER_PARTITION; offset++) {
                mockConsumer.addRecord(new ConsumerRecord<>(TOPIC, tp.partition(), offset,
                        tp.partition() + "-" + offset, "value"));
            }
        }

        parallelConsumer.poll(context -> context.forEach(recordContext -> {
            boolean isTheBlockedRecord = blockOneRecordOn != null
                    && recordContext.partition() == blockOneRecordOn.partition()
                    && recordContext.offset() == BLOCKED_OFFSET;
            if (isTheBlockedRecord) {
                log.info("Holding {} in the user function for the whole run - this is the FALSE POSITIVE arm",
                        recordContext.offset());
                try {
                    blockedRecordRelease.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                return; // never recorded as consumed: it is the record that does not finish
            }
            consumedKeys.add(recordContext.key());
        }));

        // under PARTITION ordering the blocked record stops its whole partition, so everything at and
        // above it stays unprocessed - not merely the one record
        int expected = blockOneRecordOn == null
                ? ALL.size() * RECORDS_PER_PARTITION
                : (ALL.size() - 1) * RECORDS_PER_PARTITION + BLOCKED_OFFSET;
        // Sampled on a thread of its own rather than inside the awaitility condition below: the
        // condition is evaluated only until it holds, so on a run that drains in two polls the trace
        // had THREE entries and the arm that reads it asserted on almost nothing. Its density must not
        // depend on how long the thing it is watching takes.
        List<EngineSample> trace = Collections.synchronizedList(new ArrayList<>());
        Thread sampler = new Thread(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                trace.add(sampleEngine());
                try {
                    Thread.sleep(TRACE_SAMPLE_INTERVAL.toMillis());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }, TOPIC + "-trace-sampler");
        sampler.setDaemon(true);
        sampler.start();
        try {
            Awaitility.await().atMost(Duration.ofSeconds(60)).pollInterval(Duration.ofMillis(50))
                    .untilAsserted(() -> {
                        // at info because this package is pinned to info by both test log profiles -
                        // without it a wedged harness and a wedged engine are indistinguishable in the
                        // failure output
                        log.info("Draining: consumed={}/{} committed={}",
                                consumedKeys.size(), expected, committedSnapshot());
                        assertThat(consumedKeys).hasSize(expected);
                    });
            // let several commit cycles run past the last completion, so a commit that was going to land
            // has landed - without this the arms would differ by timing rather than by the fault
            Awaitility.await().atMost(Duration.ofSeconds(30)).pollInterval(COMMIT_INTERVAL)
                    .untilAsserted(() -> {
                        assertThat(brokerCommitted(HEALTHY_A)).isEqualTo(RECORDS_PER_PARTITION);
                        assertThat(brokerCommitted(HEALTHY_B)).isEqualTo(RECORDS_PER_PARTITION);
                    });
        } finally {
            sampler.interrupt();
        }
        log.info("Run settled: consumed={} trace samples={} committed={}",
                consumedKeys.size(), trace.size(), committedSnapshot());
        return trace;
    }

    private EngineSample sampleEngine() {
        var wm = parallelConsumer.getWm();
        return new EngineSample(Math.max(0, wm.getNumberOfWorkQueuedInShardsAwaitingSelection()),
                Math.max(0, wm.getNumberRecordsOutForProcessing()), workResultsReturned.get());
    }

    private Map<TopicPartition, Long> committedSnapshot() {
        Map<TopicPartition, Long> snapshot = new LinkedHashMap<>();
        ALL.forEach(tp -> snapshot.put(tp, brokerCommitted(tp)));
        return snapshot;
    }

    /** What the BROKER-side offset store holds - never what PC believes it committed. */
    private long brokerCommitted(TopicPartition tp) {
        OffsetAndMetadata committed = mockConsumer.committed(Collections.singleton(tp)).get(tp);
        return committed == null ? 0L : committed.offset();
    }

    private OptionalLong localOffsetToCommit(TopicPartition tp) {
        return liveView().localOffsetToCommit(tp);
    }

    /**
     * The live engine as the probe sees a fleet member. Not {@code InstanceProgressView.of}, which
     * adapts a {@code ManagedPCInstance} - a chaos harness this test deliberately does not stand up -
     * but reading the same engine counters through the same interface, so what is replayed below is
     * the real run's readings and not a script.
     */
    private InstanceProgressView liveView() {
        return new InstanceProgressView() {
            @Override
            public int instanceId() {
                return 0;
            }

            @Override
            public boolean isLive() {
                return !parallelConsumer.isClosedOrFailed();
            }

            @Override
            public long queuedInShards() {
                return Math.max(0, parallelConsumer.getWm().getNumberOfWorkQueuedInShardsAwaitingSelection());
            }

            @Override
            public long outForProcessing() {
                return Math.max(0, parallelConsumer.getWm().getNumberRecordsOutForProcessing());
            }

            @Override
            public long workResultsReturned() {
                return workResultsReturned.get();
            }

            @Override
            public Object incarnationMarker() {
                return parallelConsumer;
            }

            @Override
            public OptionalLong localOffsetToCommit(TopicPartition tp) {
                // through the shared helper, not a second copy of the arithmetic - the automated
                // review on astubbs#491 flagged this pair, and the quantity is the detector's premise
                return InstanceProgressView.localOffsetToCommitOf(parallelConsumer.getWm(), tp);
            }
        };
    }

    /** A chaos-mode probe armed with the live engine as its one fleet member. */
    private ProgressProbe armedProbe() {
        return ProgressProbe.forSeamTest("red-control-group", TOPIC)
                .withInstanceProgress(() -> of(liveView()));
    }

    /**
     * Drives {@link ProgressProbe#sampleUncommittedCompletions} with a STABLE group for well past
     * {@link UncommittedCompletionDetector#COMMIT_NOT_LANDING_SAMPLES}, against the state the run
     * actually ended in.
     *
     * @return whether the detector fired at any point
     */
    private boolean sampleUncommittedCompletionsUntilItFires(ProgressProbe probe, TopicPartition tp) {
        probe.withGroupStateForSeamTest(ConsumerGroupState.STABLE);
        long committed = brokerCommitted(tp);
        long lag = RECORDS_PER_PARTITION - committed;
        log.info("Driving the per-partition detector for {}: committed={} lag={} localOffsetToCommit={}",
                tp, committed, lag, localOffsetToCommit(tp));
        boolean fired = false;
        for (int sample = 0; sample < UncommittedCompletionDetector.COMMIT_NOT_LANDING_SAMPLES * 3; sample++) {
            fired |= probe.sampleUncommittedCompletions(tp, committed, lag);
        }
        return fired;
    }

    /**
     * Shows that {@code INSTANCE_STALL/NO_WORK_COMPLETED} cannot fire on the red control, <b>with an
     * ARMED control first so the green half cannot pass vacuously</b> - the shape
     * {@code docs/solutions/best-practices/a-timing-bound-used-as-a-correctness-gate-manufactures-its-own-evidence.md}
     * prescribes and {@code RebalanceDwellToggleIT} already follows here.
     * <p>
     * Two claims, and they are different in kind:
     * <ul>
     *   <li><b>During the run</b> the instance completed work continuously - asserted on the recorded
     *   trace, whose completion count rises monotonically to the full expected total. Any successful
     *   result re-arms the detector, so no stretch of that trace can accumulate. This is a statement
     *   about the run, not about a replay: replaying a dense 50ms trace at bound-sized spacing would
     *   manufacture stalls that never happened, which is why the trace is READ here rather than
     *   replayed.</li>
     *   <li><b>After it settles</b> the instance holds no work at all, so the detector re-arms on
     *   every sample for as long as the process lives - replayed here across twenty times the bound
     *   and still silent, while the armed control fires on the first sample past it.</li>
     * </ul>
     */
    private void assertInstanceStallCannotFireOnTheSettledWedge(List<EngineSample> trace) {
        assertWithMessage("a trace of one sample would prove nothing").that(trace.size()).isAtLeast(3);
        long previous = -1;
        for (EngineSample sample : trace) {
            assertWithMessage("the completion count must never go backwards - a trace that did would "
                    + "mean the counter, not the detector, is what this arm is reading")
                    .that(sample.workResultsReturned).isAtLeast(previous);
            previous = sample.workResultsReturned;
        }
        assertWithMessage("the instance completed work throughout the run, which is what re-arms "
                + "INSTANCE_STALL on every one of those samples")
                .that(trace.get(trace.size() - 1).workResultsReturned)
                .isGreaterThan(trace.get(0).workResultsReturned);

        // ARMED: the same detector, the same spacing, a member that DOES hold work with a frozen count
        ProgressProbe armed = ProgressProbe.forSeamTest("red-control-group", TOPIC);
        // built ONCE, outside the supplier: the detector re-arms on a changed incarnation marker, so a
        // supplier that minted a fresh view per sample would hand it a fresh incarnation every time and
        // the armed control would silently never fire - which is exactly what it is here to rule out
        InstanceProgressView frozenMember = scriptedMember(/* queued */ 1, /* out */ 0, /* results */ 7);
        armed.withInstanceProgress(() -> of(frozenMember));
        replayAcross(armed, 4);
        assertWithMessage("the armed control must fire, or the silent arm below proves nothing")
                .that(armed.getViolations()).isNotEmpty();

        // GREEN: the settled red control, read live off the engine
        ProgressProbe onTheWedge = ProgressProbe.forSeamTest("red-control-group", TOPIC);
        onTheWedge.withInstanceProgress(() -> of(liveView()));
        assertWithMessage("the settled instance holds nothing: every record is finished, and only the "
                + "commit for one partition is missing")
                .that(liveView().queuedInShards() + liveView().outForProcessing()).isEqualTo(0L);
        replayAcross(onTheWedge, 20);
        assertWithMessage("INSTANCE_STALL is per-instance and re-armed by an idle or completing member, "
                + "so a partition-scoped commit fault is invisible to it however long the run lasts")
                .that(onTheWedge.getViolations()).isEmpty();
    }

    /** Drives the instance-progress sampler over {@code bounds} times {@link InstanceStallDetector#INSTANCE_STALL_BOUND}. */
    private void replayAcross(ProgressProbe probe, int bounds) {
        for (int i = 0; i <= bounds; i++) {
            probe.sampleInstanceProgress(REPLAY_T0.plus(InstanceStallDetector.INSTANCE_STALL_BOUND.multipliedBy(i)));
        }
    }

    /**
     * A member frozen in the one shape {@code INSTANCE_STALL} exists to accuse: holding work, no worker
     * in user code, and a completion count that never moves.
     */
    private InstanceProgressView scriptedMember(long queued, long outForProcessing, long results) {
        return new InstanceProgressView() {
            private final Object incarnation = new Object();

            @Override
            public int instanceId() {
                return 0;
            }

            @Override
            public boolean isLive() {
                return true;
            }

            @Override
            public long queuedInShards() {
                return queued;
            }

            @Override
            public long outForProcessing() {
                return outForProcessing;
            }

            @Override
            public long workResultsReturned() {
                return results;
            }

            @Override
            public Object incarnationMarker() {
                return incarnation;
            }

            @Override
            public int busyWorkers() {
                return 0;
            }
        };
    }
}
