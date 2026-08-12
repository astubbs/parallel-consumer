package bz.stub.parallelconsumer.connect;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.connect.PcSinkTaskDurabilityBarrier.ConfirmationRule;
import bz.stub.parallelconsumer.streams.PcTaskDispatcher;
import bz.stub.parallelconsumer.streams.PcTaskDispatcher.CompletionHandle;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The experiment that settles U3 of {@code docs/plans/2026-08-10-001-investigate-connect-offset-composition.md}:
 * when one partition's records are split across several {@code SinkTask} lanes, can their per-task
 * {@code preCommit} watermarks compose into a per-partition frontier that is safe to commit?
 *
 * <p>The answer under test is candidate C3 - do not compose the watermarks. Read each lane's watermark
 * against <em>that lane's own record stream</em>, turn it into per-record durability facts, and let Parallel
 * Consumer's frontier machinery compose those instead. What makes this testable rather than assertable is
 * {@link ConfirmationRule#HIGHEST_ACROSS_LANES}: the same probe with the composition deliberately inverted,
 * which must be caught. A probe that cannot fail proves nothing.
 *
 * <p>Throwaway by design - deleted or promoted once the verdict lands. See the plan's Definition of Done.
 */
class OffsetCompositionProbeTest {

    private static final String TOPIC = "connect-sink-in";
    private static final TopicPartition PARTITION = new TopicPartition(TOPIC, 0);

    private PcTaskDispatcher dispatcher;

    @AfterEach
    void closeDispatcher() {
        if (dispatcher != null) {
            dispatcher.close();
            dispatcher = null;
        }
    }

    // ---------- the decisive interleaving -----------------------------------------------------------

    /**
     * The interleaving U2 named as decisive: lane A's low-offset record becomes durable <em>after</em> lane
     * B's higher-offset records. A watermark scheme cannot express this - a high-water mark has no way to
     * say "100 is written but 50 is not". The frontier must sit at the lowest incomplete offset regardless
     * of how far the other lane has run ahead.
     */
    @Test
    @DisplayName("lane B running ahead never advances the frontier past lane A's unwritten record")
    void aLaneRunningAheadNeverAdvancesTheFrontierPastAnotherLanesUnwrittenRecord() {
        final ScriptedSinkTask taskA = new ScriptedSinkTask();
        final ScriptedSinkTask taskB = new ScriptedSinkTask();
        final Fixture fixture = new Fixture(taskA, taskB, ConfirmationRule.OWNING_LANE);

        // Offset 0 to lane A, offsets 1..3 to lane B.
        fixture.stage(0, taskA);
        fixture.stage(1, taskB);
        fixture.stage(2, taskB);
        fixture.stage(3, taskB);
        fixture.deliverAll();

        // Lane B declares everything it holds durable. Lane A has written nothing.
        taskB.declareDurableThrough(4);
        taskA.declareDurableThrough(0);
        fixture.runCycle();

        assertThat(fixture.completedOffsets())
                .as("lane B's records are durable, so they may complete - out of order is fine, that is the point")
                .containsExactlyInAnyOrder(1L, 2L, 3L);
        assertThat(fixture.frontier())
                .as("but the frontier is the LOWEST INCOMPLETE offset, and offset 0 is still in lane A's buffer")
                .isEqualTo(0L);

        // Now lane A writes offset 0, and only now may the frontier jump the whole contiguous run.
        taskA.declareDurableThrough(1);
        fixture.runCycle();
        assertThat(fixture.frontier())
                .as("0..3 are now all durable, so the frontier is 4")
                .isEqualTo(4L);
    }

    // ---------- the negative control ----------------------------------------------------------------

    /**
     * The same load with the composition inverted: a record is confirmed against the highest watermark
     * <em>any</em> lane returned rather than its own lane's. This is the mistake the sound rule exists to
     * avoid, and the probe must catch it - otherwise the assertion above is satisfied by a detector that
     * cannot fire.
     *
     * <p>Varying exactly one term, per {@code docs/solutions/best-practices/control-arms-vary-exactly-one-term.md}:
     * identical records, identical lanes, identical watermarks. Only {@link ConfirmationRule} differs.
     */
    @Test
    @DisplayName("negative control: composing on the highest watermark over-commits, and the probe catches it")
    void negativeControlComposingOnTheHighestWatermarkOverCommitsAndIsDetected() {
        final ScriptedSinkTask taskA = new ScriptedSinkTask();
        final ScriptedSinkTask taskB = new ScriptedSinkTask();
        final Fixture fixture = new Fixture(taskA, taskB, ConfirmationRule.HIGHEST_ACROSS_LANES);

        fixture.stage(0, taskA);
        fixture.stage(1, taskB);
        fixture.stage(2, taskB);
        fixture.stage(3, taskB);
        fixture.deliverAll();

        taskB.declareDurableThrough(4);
        taskA.declareDurableThrough(0);
        fixture.runCycle();

        assertThat(fixture.completedOffsets())
                .as("the inverted rule lets lane B's watermark speak for lane A's record - offset 0 was never written")
                .contains(0L);
        assertThat(fixture.frontier())
                .as("so the frontier passes an offset nothing has durably written: this is the over-commit, "
                        + "and a crash here loses offset 0")
                .isEqualTo(4L);
        // Not asserted on what lane A RECEIVED - it did receive offset 0. Receiving is the buffering step,
        // and treating it as durability is the very conflation under investigation. What proves the
        // over-commit is lane A's own claim: it never declared offset 0 durable, and offset 0 completed anyway.
        assertThat(taskA.received())
                .as("lane A was handed the record - buffering is not the thing in question")
                .contains(0L);
        assertThat(taskA.durableThrough())
                .as("but lane A's own durability claim covers nothing, so offset 0 completed on a watermark "
                        + "that was never about it")
                .isEqualTo(0L);
    }

    // ---------- the remaining plan scenarios --------------------------------------------------------

    @Test
    @DisplayName("a lane returning a watermark below what it consumed is respected, not overridden")
    void aWatermarkBelowWhatTheLaneConsumedIsRespected() {
        final ScriptedSinkTask task = new ScriptedSinkTask();
        final Fixture fixture = new Fixture(task, ConfirmationRule.OWNING_LANE);
        fixture.stage(0, task);
        fixture.stage(1, task);
        fixture.stage(2, task);
        fixture.deliverAll();

        // Legitimate per Connect: the task consumed 0..2 but has only durably written 0..1.
        task.declareDurableThrough(2);
        fixture.runCycle();

        assertThat(fixture.completedOffsets()).containsExactlyInAnyOrder(0L, 1L);
        assertThat(fixture.frontier())
                .as("the frontier follows the task's own claim, not what we handed it")
                .isEqualTo(2L);
    }

    @Test
    @DisplayName("a lane omitting the partition does not advance the frontier on its behalf")
    void aLaneOmittingThePartitionDoesNotAdvanceTheFrontier() {
        final ScriptedSinkTask task = new ScriptedSinkTask();
        final Fixture fixture = new Fixture(task, ConfirmationRule.OWNING_LANE);
        fixture.stage(0, task);
        fixture.stage(1, task);
        fixture.deliverAll();

        task.omitPartition();
        fixture.runCycle();

        assertThat(fixture.completedOffsets()).isEmpty();
        assertThat(fixture.frontier())
                .as("silence is not a durability claim")
                .isEqualTo(0L);
    }

    @Test
    @DisplayName("an empty returned map completes nothing")
    void anEmptyReturnedMapCompletesNothing() {
        final ScriptedSinkTask task = new ScriptedSinkTask();
        final Fixture fixture = new Fixture(task, ConfirmationRule.OWNING_LANE);
        fixture.stage(0, task);
        fixture.deliverAll();

        task.returnEmpty();
        fixture.runCycle();

        assertThat(fixture.completedOffsets()).isEmpty();
        assertThat(fixture.frontier()).isEqualTo(0L);
    }

    @Test
    @DisplayName("a record whose put threw is never confirmed durable by a later cycle")
    void aRecordWhosePutThrewIsNeverConfirmedByALaterCycle() {
        final ScriptedSinkTask task = new ScriptedSinkTask();
        final Fixture fixture = new Fixture(task, ConfirmationRule.OWNING_LANE);
        fixture.stage(0, task);
        fixture.stage(1, task);
        // Offset 0's put threw, so it is never promoted - mirroring WorkerSinkTask skipping the
        // origOffsets -> currentOffsets promotion when task.put throws.
        fixture.deliver(1);

        task.declareDurableThrough(2);
        fixture.runCycle();

        assertThat(fixture.claimedThrough(task))
                .as("the lane's own prefix stops at the gap, so it claims nothing - a scalar cannot say "
                        + "'I have 1 but not 0', and claiming 2 would tell the sink to flush a record it "
                        + "never took")
                .isNull();
        assertThat(fixture.completedOffsets())
                .as("and with nothing claimed there is nothing to confirm - offset 0 above all, but offset 1 "
                        + "is unreachable too while the gap below it is open")
                .isEmpty();
    }

    /**
     * The over-report the sound rule did <em>not</em> catch on its own: within a single lane. Distinct keys
     * share a lane by hash, and {@link PcSinkTaskLane}'s lock gives exclusion with no ordering claim, so the
     * higher offset can return from {@code put} first. Taking the numeric maximum then claims a prefix that
     * has a hole in it.
     */
    @Test
    @DisplayName("out of order inside one lane: the map handed to preCommit never claims an undelivered offset")
    void outOfOrderDeliveryInsideOneLaneNeverClaimsAnUndeliveredOffset() {
        final ScriptedSinkTask task = new ScriptedSinkTask();
        final Fixture fixture = new Fixture(task, ConfirmationRule.OWNING_LANE);
        fixture.stage(10, task);
        fixture.stage(15, task);

        fixture.deliver(15);
        assertThat(fixture.claimedThrough(task))
                .as("15 is in the sink's hands and 10 is not; claiming 16 here would instruct a connector "
                        + "that overrides flush to flush an offset it was never given")
                .isNull();

        fixture.deliver(10);
        assertThat(fixture.claimedThrough(task))
                .as("both received, so the lane may claim its whole prefix - 15 is the top of it, and the "
                        + "offsets between are simply not this lane's")
                .isEqualTo(16L);
    }

    /**
     * The other half of {@code failed()}: an offset that reached the sink and is only awaiting confirmation.
     * Unlike the pre-delivery case it must be <em>dropped</em>, or a later watermark could still confirm a
     * record PC has already been told failed - completing it twice, in opposite directions.
     */
    @Test
    @DisplayName("failing an already-delivered record drops it, so no later watermark can confirm it")
    void failingAnAlreadyDeliveredRecordDropsItFromConfirmation() {
        final ScriptedSinkTask task = new ScriptedSinkTask();
        final Fixture fixture = new Fixture(task, ConfirmationRule.OWNING_LANE);
        fixture.stage(0, task);
        fixture.stage(1, task);
        fixture.deliverAll();

        fixture.fail(0);
        assertThat(fixture.failedOffsets())
                .as("the record's own handle must be told, exactly once")
                .containsExactly(0L);

        // A watermark that would otherwise have covered both.
        task.declareDurableThrough(2);
        fixture.runCycle();

        assertThat(fixture.completedOffsets())
                .as("offset 0 already failed, so the watermark may not also succeed it - only 1 completes")
                .containsExactly(1L);
    }

    // ---------- the exhaustive arm ------------------------------------------------------------------

    /**
     * Two lanes, four offsets, every order in which the lanes can declare durability. Small enough to
     * exhaust, and exhausting it is the point: the arms above test the interleaving U2 already imagined, so
     * a blind spot in U2 would be invisible to them by construction.
     *
     * <p>The invariant asserted at every step is the one U2 stated: a record is complete only if its own
     * lane's watermark covers it, and the frontier never exceeds the lowest incomplete offset.
     */
    @Test
    @DisplayName("exhaustive: every completion order across two lanes and four offsets upholds the invariant")
    void everyCompletionOrderAcrossTwoLanesAndFourOffsetsUpholdsTheInvariant() {
        int casesRun = 0;
        // Which lane owns each of offsets 0..3: every assignment except the two that put all four in one
        // lane (those are not split partitions and cannot exercise the question).
        for (int assignment = 1; assignment < 15; assignment++) {
            // Every order of declaring the two lanes' watermarks, at every depth each can reach.
            for (int aStep = 0; aStep <= 4; aStep++) {
                for (int bStep = 0; bStep <= 4; bStep++) {
                    for (boolean aFirst : new boolean[]{true, false}) {
                        runExhaustiveCase(assignment, aStep, bStep, aFirst);
                        casesRun++;
                    }
                }
            }
        }
        assertThat(casesRun)
                .as("the enumeration must actually have run - a silently empty loop would pass vacuously")
                .isEqualTo(14 * 5 * 5 * 2);
    }

    private void runExhaustiveCase(final int assignment, final int aStep, final int bStep,
                                   final boolean aFirst) {
        final ScriptedSinkTask taskA = new ScriptedSinkTask();
        final ScriptedSinkTask taskB = new ScriptedSinkTask();
        final Fixture fixture = new Fixture(taskA, taskB, ConfirmationRule.OWNING_LANE);

        final Map<Long, ScriptedSinkTask> owner = new HashMap<>();
        for (long offset = 0; offset < 4; offset++) {
            final ScriptedSinkTask task = ((assignment >> offset) & 1) == 0 ? taskA : taskB;
            owner.put(offset, task);
            fixture.stage(offset, task);
        }
        fixture.deliverAll();

        // Each lane declares durability up to its own step, expressed in ITS OWN offset sequence position.
        final ScriptedSinkTask first = aFirst ? taskA : taskB;
        final ScriptedSinkTask second = aFirst ? taskB : taskA;
        final int firstStep = aFirst ? aStep : bStep;
        final int secondStep = aFirst ? bStep : aStep;

        declareThroughNthOwnedOffset(fixture, first, owner, firstStep);
        fixture.runCycle();
        assertInvariant(fixture, owner, assignment, aStep, bStep, aFirst);

        declareThroughNthOwnedOffset(fixture, second, owner, secondStep);
        fixture.runCycle();
        assertInvariant(fixture, owner, assignment, aStep, bStep, aFirst);
    }

    /** Declares the lane durable through its own {@code n}th owned offset, in offset order. */
    private void declareThroughNthOwnedOffset(final Fixture fixture, final ScriptedSinkTask task,
                                              final Map<Long, ScriptedSinkTask> owner, final int n) {
        final List<Long> owned = new ArrayList<>();
        for (long offset = 0; offset < 4; offset++) {
            if (owner.get(offset) == task) {
                owned.add(offset);
            }
        }
        if (n == 0 || owned.isEmpty()) {
            task.declareDurableThrough(0);
            return;
        }
        final int index = Math.min(n, owned.size()) - 1;
        task.declareDurableThrough(owned.get(index) + 1);
    }

    private void assertInvariant(final Fixture fixture, final Map<Long, ScriptedSinkTask> owner,
                                 final int assignment, final int aStep, final int bStep,
                                 final boolean aFirst) {
        final String scenario = String.format(
                "assignment=%s aStep=%d bStep=%d aFirst=%s", Integer.toBinaryString(assignment), aStep, bStep, aFirst);

        for (final long completed : fixture.completedOffsets()) {
            assertThat(owner.get(completed).durableThrough())
                    .as("%s: offset %d completed, so its OWN lane's watermark must cover it", scenario, completed)
                    .isGreaterThan(completed);
        }

        long lowestIncomplete = 0;
        final Set<Long> done = fixture.completedOffsets();
        while (lowestIncomplete < 4 && done.contains(lowestIncomplete)) {
            lowestIncomplete++;
        }
        assertThat(fixture.frontier())
                .as("%s: the frontier must never exceed the lowest incomplete offset", scenario)
                .isLessThanOrEqualTo(lowestIncomplete);
    }

    // ---------- the end-to-end arm through the real dispatcher --------------------------------------

    /**
     * The arms above drive the barrier directly. This one runs the same question through a live
     * {@link PcTaskDispatcher}, so the frontier being asserted is the one PC would actually commit - not a
     * number this test computed.
     */
    @Test
    @DisplayName("end to end: a real dispatcher's collected commit data stops at the unwritten record")
    void endToEndTheDispatchersCollectedCommitDataStopsAtTheUnwrittenRecord() {
        final ScriptedSinkTask taskA = new ScriptedSinkTask();
        final ScriptedSinkTask taskB = new ScriptedSinkTask();
        final PcSinkTaskLane laneA = new PcSinkTaskLane(taskA);
        final PcSinkTaskLane laneB = new PcSinkTaskLane(taskB);
        final PcSinkTaskLaneRouter router = new PcSinkTaskLaneRouter(
                Arrays.asList(laneA, laneB), OffsetCompositionProbeTest::project);

        dispatcher = new PcTaskDispatcher("connect-probe", Collections.singleton(PARTITION), 4);

        // Four records with four distinct keys; ask the router which lane each landed in rather than
        // assuming, so the assertions below describe the real routing and not an imagined one.
        final List<ConsumerRecord<byte[], byte[]>> batch = new ArrayList<>();
        for (long offset = 0; offset < 4; offset++) {
            batch.add(record(offset, "key-" + offset));
        }
        dispatcher.registerRecords(PARTITION, batch);
        dispatcher.pumpUntilQuiescent(router, Duration.ofSeconds(30));

        assertThat(dispatcher.collectCommitData())
                .as("every record is buffered but none is durable, so there is no frontier to commit at all")
                .doesNotContainKey(PARTITION);

        // Whichever lane owns offset 0, hold it back and let the other lane declare everything durable.
        final PcSinkTaskLane ownerOfZero = router.laneFor(batch.get(0));
        for (final PcSinkTaskLane lane : Arrays.asList(laneA, laneB)) {
            final ScriptedSinkTask task = (ScriptedSinkTask) lane.getTask();
            task.declareDurableThrough(lane == ownerOfZero ? 0 : 4);
        }
        router.runDurabilityCycle();
        dispatcher.pumpUntilQuiescent(router, Duration.ofSeconds(30));

        final Map<TopicPartition, OffsetAndMetadata> collected = dispatcher.collectCommitData();
        if (collected.containsKey(PARTITION)) {
            assertThat(collected.get(PARTITION).offset())
                    .as("offset 0's lane wrote nothing, so PC's own frontier cannot pass it")
                    .isEqualTo(0L);
        }

        // Release it, and the frontier may finally cover the whole run.
        ((ScriptedSinkTask) ownerOfZero.getTask()).declareDurableThrough(4);
        router.runDurabilityCycle();
        dispatcher.pumpUntilQuiescent(router, Duration.ofSeconds(30));

        assertThat(dispatcher.collectCommitData().get(PARTITION).offset())
                .as("all four durable, so PC commits 4 - the next offset to resume from")
                .isEqualTo(4L);
    }

    // ---------- fixtures ----------------------------------------------------------------------------

    /**
     * Drives barriers directly, without a worker pool, so completion order is the test's to choose rather
     * than the scheduler's. The frontier it reports is computed the way PC computes one - lowest incomplete
     * offset - which the end-to-end arm above cross-checks against a real dispatcher.
     */
    private final class Fixture {

        private final Map<ScriptedSinkTask, PcSinkTaskLane> laneByTask = new HashMap<>();
        private final Map<ScriptedSinkTask, PcSinkTaskDurabilityBarrier> barriers = new HashMap<>();
        private final Map<Long, ScriptedSinkTask> taskByOffset = new HashMap<>();
        private final Collection<Long> completed = new ConcurrentLinkedQueue<>();
        private final Collection<Long> failed = new ConcurrentLinkedQueue<>();
        private final ConfirmationRule rule;

        Fixture(final ScriptedSinkTask task, final ConfirmationRule rule) {
            this(rule, task);
        }

        Fixture(final ScriptedSinkTask a, final ScriptedSinkTask b, final ConfirmationRule rule) {
            this(rule, a, b);
        }

        private Fixture(final ConfirmationRule rule, final ScriptedSinkTask... tasks) {
            this.rule = rule;
            for (final ScriptedSinkTask task : tasks) {
                final PcSinkTaskLane lane = new PcSinkTaskLane(task);
                laneByTask.put(task, lane);
                barriers.put(task, new PcSinkTaskDurabilityBarrier(lane));
            }
        }

        void stage(final long offset, final ScriptedSinkTask task) {
            taskByOffset.put(offset, task);
            barriers.get(task).staged(PARTITION, offset, new CompletionHandle() {
                @Override
                public void succeeded() {
                    completed.add(offset);
                }

                @Override
                public void failed(final Throwable cause) {
                    failed.add(offset);
                }
            });
        }

        /** Fails one record through its barrier, as the router does when {@code put} throws. */
        void fail(final long offset) {
            final ScriptedSinkTask task = taskByOffset.get(offset);
            barriers.get(task).failed(PARTITION, offset, new IllegalStateException("scripted failure"));
        }

        Set<Long> failedOffsets() {
            return new HashSet<>(failed);
        }

        /** Runs the lane's {@code put} and promotes the offset, as the worker call would. */
        void deliver(final long offset) {
            final ScriptedSinkTask task = taskByOffset.get(offset);
            laneByTask.get(task).put(Collections.singletonList(sinkRecord(offset)));
            barriers.get(task).delivered(PARTITION, offset);
        }

        void deliverAll() {
            new ArrayList<>(taskByOffset.keySet()).stream().sorted().forEach(this::deliver);
        }

        /** Gathers every lane's watermark before confirming any of them - see the router's own two phases. */
        void runCycle() {
            final Map<PcSinkTaskDurabilityBarrier, Map<TopicPartition, OffsetAndMetadata>> polled =
                    new HashMap<>();
            final Map<TopicPartition, Long> ceiling = new HashMap<>();
            for (final PcSinkTaskDurabilityBarrier barrier : barriers.values()) {
                final Map<TopicPartition, OffsetAndMetadata> returned = barrier.pollWatermarks();
                polled.put(barrier, returned);
                returned.forEach((tp, offset) -> ceiling.merge(tp, offset.offset(), Math::max));
            }
            polled.forEach((barrier, returned) -> barrier.confirm(rule, returned, ceiling));
        }

        Set<Long> completedOffsets() {
            return new HashSet<>(completed);
        }

        /**
         * What this lane's barrier would hand {@code preCommit} for {@link #PARTITION} right now, or null if
         * it would claim nothing. Asserted on directly, because the claim is the thing under test: a
         * scripted task can return any watermark it likes, so asserting only on completions would let an
         * over-claim through whenever the task did not act on it.
         */
        Long claimedThrough(final ScriptedSinkTask task) {
            final OffsetAndMetadata claim = barriers.get(task).snapshotDeliveredThrough().get(PARTITION);
            return claim == null ? null : claim.offset();
        }

        /** PC's frontier: the lowest offset not yet complete. */
        long frontier() {
            final Set<Long> done = completedOffsets();
            long frontier = 0;
            while (done.contains(frontier)) {
                frontier++;
            }
            return frontier;
        }
    }

    /**
     * A sink task whose durability claim the test dictates. It records what it was actually handed, so an
     * assertion about over-commit can be checked against what the sink really wrote rather than against the
     * test's own model of it.
     */
    private static final class ScriptedSinkTask extends SinkTask {

        private final Set<Long> received = new LinkedHashSet<>();
        private final AtomicLong durableThrough = new AtomicLong();
        private volatile Mode mode = Mode.WATERMARK;

        private enum Mode { WATERMARK, OMIT_PARTITION, EMPTY }

        @Override
        public void put(final Collection<SinkRecord> records) {
            records.forEach(record -> received.add(record.kafkaOffset()));
        }

        @Override
        public Map<TopicPartition, OffsetAndMetadata> preCommit(
                final Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
            switch (mode) {
                case EMPTY:
                    return Collections.emptyMap();
                case OMIT_PARTITION:
                    return new HashMap<>();
                case WATERMARK:
                default:
                    return Collections.singletonMap(PARTITION, new OffsetAndMetadata(durableThrough.get()));
            }
        }

        void declareDurableThrough(final long offset) {
            mode = Mode.WATERMARK;
            durableThrough.set(offset);
        }

        long durableThrough() {
            return durableThrough.get();
        }

        void omitPartition() {
            mode = Mode.OMIT_PARTITION;
        }

        void returnEmpty() {
            mode = Mode.EMPTY;
        }

        Set<Long> received() {
            return new LinkedHashSet<>(received);
        }

        @Override
        public String version() {
            return "probe";
        }

        @Override
        public void start(final Map<String, String> props) {
            // nothing to start - this task exists only to answer preCommit
        }

        @Override
        public void stop() {
            // nothing to stop
        }
    }

    private static SinkRecord sinkRecord(final long offset) {
        return new SinkRecord(TOPIC, 0, null, "key-" + offset, null, "value-" + offset, offset);
    }

    private static SinkRecord project(final ConsumerRecord<byte[], byte[]> record) {
        return new SinkRecord(record.topic(), record.partition(), null,
                new String(record.key(), StandardCharsets.UTF_8), null,
                new String(record.value(), StandardCharsets.UTF_8), record.offset());
    }

    private static ConsumerRecord<byte[], byte[]> record(final long offset, final String key) {
        return new ConsumerRecord<>(TOPIC, 0, offset,
                key.getBytes(StandardCharsets.UTF_8),
                ("value-" + offset).getBytes(StandardCharsets.UTF_8));
    }
}
