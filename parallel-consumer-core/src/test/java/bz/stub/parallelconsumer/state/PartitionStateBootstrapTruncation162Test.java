package bz.stub.parallelconsumer.state;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.internal.EpochAndRecordsMap;
import bz.stub.parallelconsumer.internal.PCModuleTestEnv;
import bz.stub.parallelconsumer.offsets.OffsetMapCodecManager.HighestOffsetAndIncompletes;
import lombok.extern.slf4j.Slf4j;
import one.util.streamex.LongStreamEx;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import pl.tlinkowski.unij.api.UniLists;
import pl.tlinkowski.unij.api.UniMaps;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;

/**
 * The two bootstrap branches of {@code PartitionState#maybeTruncateBelowOrAbove}, as a pair of arms differing in
 * one term, plus the round trip that says whether PC can reach the lower one on its own.
 * <p>
 * <b>What this settles, for
 * <a href="https://github.com/astubbs/parallel-consumer/issues/162">astubbs#162</a> /
 * <a href="https://github.com/confluentinc/parallel-consumer/issues/546">confluentinc#546</a>.</b> That thread's
 * second warning - {@code Bootstrap polled offset has been reset to an earlier offset} - was never diagnosed
 * upstream, and this fork's note for it proposed, untested, that PC's own {@code consumer.committed()} call races
 * the consumer's resolution of the fetch position for a newly assigned partition. <b>That hypothesis is refuted by
 * the client's own ordering</b>, and the refutation needs no test because it is not a race:
 * {@code ClassicKafkaConsumer.updateAssignmentMetadataIfNeeded} runs {@code coordinator.poll} - which invokes the
 * rebalance listener, and so PC's {@code committed()} - and only afterwards {@code updateFetchPositions}, whose
 * {@code ConsumerCoordinator.initWithCommittedOffsetsIfNeeded} is what gives an initializing partition its
 * position. PC's read therefore strictly precedes the fetcher's, so a commit landing between the two is the one
 * the FETCHER sees: the polled offset comes out at or above PC's expectation, which is the OTHER branch. The named
 * race cannot produce the warning it was proposed to explain.
 * <p>
 * What is left, and what {@link #aCommitPcWroteDecodesBackToTheOffsetItWasCommittedAt} covers, is the only route to
 * the lower branch that belongs to PC rather than to the broker: a commit whose encoded payload decodes to an
 * expectation ABOVE the offset that commit was filed under. Keep that invariant and a consumer starting where PC
 * asked it to start can never take the branch; break it - which is the shape
 * {@code fix(core) astubbs#121: commit the offset the encoded payload was written against} closed - and the branch
 * fires with a gap equal to the inconsistency, replaying everything above it.
 *
 * @author Antony Stubbs
 * @see PartitionStateCommittedOffsetTest for the same two branches driven from deliberate truncation - a compacted
 *         topic and a committed offset moved either way - rather than from what PC itself wrote
 */
@Slf4j
@Execution(ExecutionMode.SAME_THREAD)
class PartitionStateBootstrapTruncation162Test {

    static final String TOPIC = "topic";

    final TopicPartition tp = new TopicPartition(TOPIC, 0);

    final ModelUtils mu = new ModelUtils(new PCModuleTestEnv());

    /**
     * The offset the loaded commit data was filed under, and so the offset PC expects its first poll to start at:
     * {@code getOffsetToCommit()} is the lowest incomplete whenever any incomplete is loaded.
     */
    static final long expectedBootstrapOffset = 11L;

    /**
     * An incomplete high enough that the bootstrap poll batches below do not reach it. It is the one offset whose
     * fate differs between the two arms, so it is what makes them a controlled pair rather than two tests.
     */
    static final long incompleteAboveEveryBootstrapBatch = 60L;

    final List<Long> loadedIncompletes = UniLists.of(expectedBootstrapOffset, 15L, 20L, incompleteAboveEveryBootstrapBatch);

    PartitionState<String, String> stateLoadedFromCommitData() {
        var offsetData = new HighestOffsetAndIncompletes(Optional.of(101L), new TreeSet<>(loadedIncompletes));
        var state = new PartitionState<String, String>(0, mu.getModule(), tp, offsetData);
        assertWithMessage("fixture: the loaded state expects its first poll at the lowest loaded incomplete")
                .that(state.getOffsetToCommit()).isEqualTo(expectedBootstrapOffset);
        return state;
    }

    void bootstrapPollFrom(PartitionState<String, String> state, long fromOffset, long toOffset) {
        var batch = new PolledTestBatch(mu, tp, fromOffset, toOffset);
        state.maybeRegisterNewPollBatchAsWork(batch.polledRecordBatch.records(state.getTp()));
    }

    /**
     * ARM A - the branch the issue thread never had explained. The first poll arrives BELOW the expectation, so the
     * state is thrown away wholesale and rebuilt from what was polled.
     * <p>
     * The cost is duplicate processing and not loss, and the two halves of that claim are asserted separately. No
     * tracked incomplete can be stranded BELOW the polled offset, because the expectation this branch fires against
     * IS the lowest tracked incomplete - so anything discarded sits above the position the fetcher is about to read
     * from, and comes back. What does get discarded is every record between the polled offset and the old
     * expectation that a previous owner had already completed and committed: they are re-registered as work.
     */
    @Test
    void resetToEarlierDiscardsEveryLoadedIncompleteIncludingThoseAboveTheBatch() {
        var state = stateLoadedFromCommitData();
        long resetTo = expectedBootstrapOffset - 5L;

        bootstrapPollFrom(state, resetTo, 30L);

        assertWithMessage("the reset rewinds the commit frontier to where the poll actually started")
                .that(state.getOffsetToCommit()).isEqualTo(resetTo);

        var stillTracked = state.getAllIncompleteOffsets();
        assertWithMessage("the loaded state is discarded entirely - only what was polled is tracked, so the "
                + "incomplete at %s is no longer known to PC and is replayed when the fetch reaches it",
                incompleteAboveEveryBootstrapBatch)
                .that(stillTracked)
                .containsExactlyElementsIn(LongStreamEx.rangeClosed(resetTo, 30L).boxed().toList());

        assertWithMessage("no tracked offset is stranded below the polled offset - the branch fires against the "
                + "lowest tracked incomplete, so nothing tracked can lie beneath it")
                .that(stillTracked.stream().filter(offset -> offset < resetTo).collect(Collectors.toList()))
                .isEmpty();

        assertWithMessage("records the previous owner completed and committed are replayed - this is the "
                + "duplicate processing the branch costs")
                .that(stillTracked).containsAtLeastElementsIn(LongStreamEx.range(resetTo, expectedBootstrapOffset).boxed().toList());
    }

    /**
     * ARM B - the control. Same fixture, same shaped batch, one term moved: the first poll arrives ABOVE the
     * expectation instead of below it. The outcome flips exactly where the branch says it should - the incomplete
     * out of the batch's reach survives here and does not in Arm A.
     */
    @Test
    void pollAboveExpectedPrunesOnlyBeneathItAndKeepsTheRest() {
        var state = stateLoadedFromCommitData();
        long truncateTo = 20L;

        bootstrapPollFrom(state, truncateTo, 30L);

        assertWithMessage("the truncation moves the commit frontier up to the first offset that still exists")
                .that(state.getOffsetToCommit()).isEqualTo(truncateTo);

        assertThat(state.getAllIncompleteOffsets())
                .containsAtLeast(truncateTo, incompleteAboveEveryBootstrapBatch);

        assertWithMessage("only the incompletes the broker no longer has are dropped")
                .that(state.getAllIncompleteOffsets())
                .containsNoneOf(expectedBootstrapOffset, 15L);
    }

    /**
     * The decisive arm: drive a commit PC itself wrote back through the real assignment path - a
     * {@link MockConsumer} holding it as the group's committed offset, {@code WorkManager#onPartitionsAssigned},
     * {@code OffsetMapCodecManager#loadPartitionStateForAssignment} and the decoders - and read off the expectation
     * the rebuilt state bootstraps with.
     * <p>
     * A consumer resolves its fetch position from the same committed offset, so as long as this equality holds a
     * partition PC hands over to itself cannot take either truncation branch. Every shape here is one PC can
     * genuinely commit: a fully drained partition (no payload at all), a contiguous tail of incompletes, a sparse
     * map with a completed island above the frontier, and a single incomplete at the very base.
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("commitShapes")
    void aCommitPcWroteDecodesBackToTheOffsetItWasCommittedAt(String shapeName, Collection<Long> offsetsToSucceed) {
        var mockConsumer = new MockConsumer<String, String>(OffsetResetStrategy.EARLIEST);
        var options = ParallelConsumerOptions.<String, String>builder()
                .ordering(ParallelConsumerOptions.ProcessingOrder.UNORDERED)
                .consumer(mockConsumer)
                .build();
        var module = new PCModuleTestEnv(options);
        var wm = module.workManager();
        module.setWorkManager(wm);

        // MockConsumer#committed hands back a bare OffsetAndMetadata(0) - indistinguishable from a real commit at
        // offset 0 - for any partition its own SubscriptionState does not hold, and MockConsumer#assign clears the
        // committed map, so the assignment has to come first or the round trip silently measures the
        // never-committed path instead. This is the arm proving itself: without it every shape below reported an
        // expectation of 0 and read as a product defect.
        mockConsumer.assign(UniLists.of(tp));

        wm.onPartitionsAssigned(UniLists.of(tp));
        wm.registerWork(new EpochAndRecordsMap<>(ModelUtils.pollOf(tp, LongStreamEx.rangeClosed(0L, 9L).toArray()), wm.getPm()));

        for (var work : wm.getWorkIfAvailable()) {
            if (offsetsToSucceed.contains(work.offset())) {
                wm.onSuccessResult(work);
            }
        }

        var commitData = wm.collectCommitDataForDirtyPartitions();
        OffsetAndMetadata commit = commitData.get(tp);
        assertWithMessage("fixture: %s must produce a commit to round trip", shapeName).that(commit).isNotNull();
        mockConsumer.commitSync(UniMaps.of(tp, commit));

        // hand the partition back to ourselves - the same path a rebalance takes
        wm.onPartitionsRevoked(UniLists.of(tp));
        wm.onPartitionsAssigned(UniLists.of(tp));

        long bootstrapExpectation = wm.getPm().getPartitionState(tp).getOffsetToCommit();

        assertWithMessage("%s: the bootstrap expectation must equal the offset the commit was filed under, or a "
                + "consumer starting at that offset takes a truncation branch against state that is correct",
                shapeName)
                .that(bootstrapExpectation).isEqualTo(commit.offset());
    }

    static Stream<Arguments> commitShapes() {
        return Stream.of(
                Arguments.of("fully drained - nothing incomplete, so no payload is written at all",
                        LongStreamEx.rangeClosed(0L, 9L).boxed().toList()),
                Arguments.of("contiguous tail incomplete", LongStreamEx.rangeClosed(0L, 4L).boxed().toList()),
                Arguments.of("sparse - a completed island above the commit frontier",
                        LongStreamEx.rangeClosed(0L, 9L).filter(offset -> offset != 3L && offset != 7L).boxed().toList()),
                Arguments.of("only the base offset incomplete", LongStreamEx.rangeClosed(1L, 9L).boxed().toList())
        );
    }


}
