package bz.stub.parallelconsumer.state;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.internal.PCModuleTestEnv;
import bz.stub.parallelconsumer.offsets.OffsetMapCodecManager.HighestOffsetAndIncompletes;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import pl.tlinkowski.unij.api.UniLists;

import java.util.ArrayList;
import java.util.List;
import java.util.SortedSet;

import static com.google.common.truth.Truth.assertWithMessage;

/**
 * Pins the no-op contract of {@link RemovedPartitionState} for the incomplete-offsets accessors, and - more to the
 * point - pins it in a way that can actually go red.
 * <p>
 * <b>Why the obvious version of this test proves nothing.</b> Asserting that the shipped singleton returns an empty
 * set does not discriminate the override at all: the singleton's underlying incomplete-offsets map is empty, so the
 * <em>base</em> filter returns an empty set too. Delete the override and that test still passes. So the state here is
 * arranged so that reaching the base implementation would visibly differ - records are added through the base
 * {@link PartitionState#addNewIncompleteRecord} path, which {@link RemovedPartitionState} does not override.
 * <p>
 * <b>The two entry points need different techniques, because they are not guarded the same way.</b> The bounded
 * overload is guarded once, by its own override, so a populated instance plus an explicit bound discriminates it.
 * The convenience method is guarded <em>twice</em> - it reaches the bounded override, but
 * {@link RemovedPartitionState#getOffsetHighestSucceeded()} is also overridden to
 * {@link PartitionState#KAFKA_OFFSET_ABSENCE}, so the bound it passes is below every offset and the base filter would
 * return empty regardless. No arrangement of state can tell those apart, so the delegation claim is pinned with an
 * observable dispatch seam instead of pretending state does it.
 *
 * @author Antony Stubbs
 */
class RemovedPartitionStateTest {

    private static final String TOPIC = "topic";

    private static final List<Long> POPULATED_OFFSETS = UniLists.of(1L, 2L, 3L, 4L, 5L);

    private final ModelUtils mu = new ModelUtils(new PCModuleTestEnv());

    private final TopicPartition tp = new TopicPartition(TOPIC, 0);

    /**
     * A removed state that records every bound the base class routes through the overridden method, so the
     * delegation itself is observable rather than inferred.
     */
    private static final class DispatchRecordingRemovedState extends RemovedPartitionState<String, String> {

        private final List<Long> boundsReceived = new ArrayList<>();

        @Override
        public SortedSet<Long> getIncompleteOffsetsBelow(long highestSucceededBound) {
            boundsReceived.add(highestSucceededBound);
            return super.getIncompleteOffsetsBelow(highestSucceededBound);
        }
    }

    /**
     * The bounded entry point - the one the offset encoder calls directly - must be a no-op even when the base
     * filter, if it were reached, would return a non-empty set. Deleting the override turns this red.
     */
    @Test
    void boundedEntryPointIsANoOpEvenWhenTheBaseFilterWouldReturnOffsets() {
        // Control arm: the identical arrangement on a live partition DOES return the offsets, which is what makes
        // the subject below a discriminating test rather than empty-equals-empty.
        PartitionState<String, String> live = new PartitionState<>(1, mu.getModule(), tp, HighestOffsetAndIncompletes.of());
        populate(live);

        assertWithMessage("control: on a live partition this exact arrangement must return the offsets - if it does "
                + "not, the subject assertion below proves nothing and this test has quietly gone tautological")
                .that(live.getIncompleteOffsetsBelow(Long.MAX_VALUE))
                .containsExactlyElementsIn(POPULATED_OFFSETS);

        RemovedPartitionState<String, String> removed = new RemovedPartitionState<>();
        populate(removed);

        assertWithMessage("a removed partition reports no incomplete offsets even though the base filter would "
                + "return %s for this bound - if this fails, the no-op override was lost", POPULATED_OFFSETS)
                .that(removed.getIncompleteOffsetsBelow(Long.MAX_VALUE))
                .isEmpty();
    }

    private void populate(PartitionState<String, String> state) {
        for (long offset : POPULATED_OFFSETS) {
            state.addNewIncompleteRecord(recordAt(offset));
        }
    }

    /**
     * The convenience entry point must reach the no-op contract by dispatching through the bounded override, which is
     * the claim {@link RemovedPartitionState}'s javadoc makes and the reason only one override is needed.
     */
    @Test
    void convenienceEntryPointDispatchesThroughTheBoundedOverride() {
        DispatchRecordingRemovedState removed = new DispatchRecordingRemovedState();

        SortedSet<Long> result = removed.getIncompleteOffsetsBelowHighestSucceeded();

        assertWithMessage("the convenience method must route through the bounded overload - that delegation is what "
                + "lets a single override guard both entry points; a base class that filtered inline instead would "
                + "bypass the guard silently")
                .that(removed.boundsReceived)
                .hasSize(1);

        assertWithMessage("a removed partition has no incomplete offsets to report")
                .that(result)
                .isEmpty();
    }

    /**
     * The shipped singleton, across bounds that would be nonsense on a live partition. Cheap, and it covers the
     * instance production actually hands out.
     */
    @Test
    void theSingletonIsANoOpForAnyBound() {
        PartitionState<String, String> singleton = RemovedPartitionState.getSingleton();

        for (long bound : new long[]{Long.MIN_VALUE, PartitionState.KAFKA_OFFSET_ABSENCE, 0L, 5L, Long.MAX_VALUE}) {
            assertWithMessage("the singleton reports no incomplete offsets whatever bound is asked for (bound: %s)",
                    bound)
                    .that(singleton.getIncompleteOffsetsBelow(bound))
                    .isEmpty();
        }
    }

    private ConsumerRecord<String, String> recordAt(long offset) {
        return new ConsumerRecord<>(TOPIC, 0, offset, "key", "value");
    }
}
