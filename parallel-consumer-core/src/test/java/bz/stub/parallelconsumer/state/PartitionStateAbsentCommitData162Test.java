package bz.stub.parallelconsumer.state;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions.InvalidOffsetMetadataHandlingPolicy;
import bz.stub.parallelconsumer.internal.PCModuleTestEnv;
import bz.stub.parallelconsumer.internal.utils.LogCapture;
import bz.stub.parallelconsumer.offsets.OffsetMapCodecManager;
import bz.stub.parallelconsumer.offsets.OffsetMapCodecManager.HighestOffsetAndIncompletes;
import ch.qos.logback.classic.Level;
import lombok.extern.slf4j.Slf4j;
import one.util.streamex.LongStreamEx;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.mockito.Mockito;
import pl.tlinkowski.unij.api.UniLists;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;

/**
 * A partition with <b>no commit data</b> must start quietly - the live case of
 * <a href="https://github.com/astubbs/parallel-consumer/issues/162">astubbs#162</a> /
 * <a href="https://github.com/confluentinc/parallel-consumer/issues/546">confluentinc#546</a>.
 * <p>
 * <b>The defect.</b> {@code PartitionState#maybeTruncateBelowOrAbove} took its expectation from
 * {@code getOffsetToCommit()} and never asked whether commit data existed. With none, the state is the codec
 * manager's default entry, {@code offsetHighestSucceeded} is {@code KAFKA_OFFSET_ABSENCE} and the expectation
 * computes to 0 - so any first poll above offset 0 took the above-expected branch and logged
 * {@code Truncating state - removing records lower than ...}, having pruned nothing, because there was nothing
 * loaded to prune. False in both halves, and it is the shape reported upstream on 0.5.2.7 alongside a broker CLI
 * screenshot showing an empty {@code CURRENT-OFFSET} for exactly those partitions.
 * <p>
 * <b>Why the sentinel is not the question to ask, which is what this test's control arm pins.</b> The obvious
 * cheap check - {@code offsetHighestSucceeded == KAFKA_OFFSET_ABSENCE} - is WRONG, because a real commit at
 * offset 0 with no payload decodes to {@code highestSeen = 0 - 1 = -1}: the same sentinel, an expectation of 0,
 * and commit data that genuinely existed. Suppressing the warning on the sentinel would silence the true
 * truncation report for exactly the partition that has processed nothing yet, which is the partition most likely
 * to have been caught by retention. The absence is instead carried from where it is KNOWN - the empty
 * {@link HighestOffsetAndIncompletes#getHighestSeenOffset()} the default entry is built with - which is the
 * engine {@code AGENTS.md}'s collapse-parallel-state rule applied to a fact rather than to a cache.
 *
 * @author Antony Stubbs
 * @see PartitionStateBootstrapTruncation162Test for the same method's two genuine truncation branches, and the
 *         round trip refuting the third defect on that issue thread
 * @see PartitionStateCommittedOffsetTest for deliberate truncation - compaction and a moved committed offset
 */
@Slf4j
@Execution(ExecutionMode.SAME_THREAD)
class PartitionStateAbsentCommitData162Test {

    /**
     * The line an operator alerts on, and the thing that must not be logged for a partition that has nothing to
     * truncate. Matched as a substring rather than reproduced whole, so a reword of the sentence does not fail
     * this test while a return of the branch does.
     */
    static final String TRUNCATION_WARNING = "Truncating state";

    final ModelUtils mu = new ModelUtils(new PCModuleTestEnv());

    /**
     * A partition per test. {@link LogCapture} attaches to the shared {@link PartitionState} logger, so every
     * read has to be narrowed to lines this test's own state produced - the second obligation in that class's
     * javadoc.
     */
    TopicPartition partitionFor(String testName) {
        return new TopicPartition("absent-commit-data-" + testName, 0);
    }

    /**
     * The codec manager's default entry, built exactly as
     * {@code OffsetMapCodecManager#loadPartitionStateForAssignment} builds it for an assignment with no commit
     * history: {@link HighestOffsetAndIncompletes#of()}, whose highest-seen offset is empty.
     */
    PartitionState<String, String> stateWithNoCommitData(TopicPartition tp) {
        return new PartitionState<>(0, mu.getModule(), tp, HighestOffsetAndIncompletes.of());
    }

    /**
     * Commit data that DID exist, and that decodes to the same {@code KAFKA_OFFSET_ABSENCE} sentinel the
     * absent case leaves behind: a commit filed at offset 0 with no offset map, which
     * {@code OffsetMapCodecManager#decodeCompressedOffsets} decodes as highest-seen {@code 0 - 1}.
     */
    PartitionState<String, String> stateFromACommitAtOffsetZero(TopicPartition tp) {
        var decoded = new HighestOffsetAndIncompletes(Optional.of(-1L), new TreeSet<>());
        return new PartitionState<>(0, mu.getModule(), tp, decoded);
    }

    void bootstrapPollFrom(PartitionState<String, String> state, long fromOffset, long toOffset) {
        var batch = new PolledTestBatch(mu, state.getTp(), fromOffset, toOffset);
        state.maybeRegisterNewPollBatchAsWork(batch.polledRecordBatch.records(state.getTp()));
    }

    /**
     * The shape reported upstream: nothing committed, and the first poll lands high up the partition because the
     * consumer's reset policy sent it there. RED before the fix - this is where the false
     * {@code Truncating state} warning fired.
     */
    @Test
    void firstPollAboveZeroWithNoCommitDataDoesNotWarn() {
        var tp = partitionFor("above-zero");
        var state = stateWithNoCommitData(tp);

        try (var logs = LogCapture.of(PartitionState.class, Level.INFO)) {
            bootstrapPollFrom(state, 55674910L, 55674919L);

            assertWithMessage("no commit data was loaded and nothing was pruned, so the truncation warning is "
                    + "false in both halves and must not be logged")
                    .that(logs.messagesAt(Level.WARN, tp.topic()))
                    .isEmpty();

            String started = logs.onlyMessageAt(Level.INFO, tp.topic());
            assertWithMessage("the replacement line has to say what actually happened, so an operator reading it "
                    + "is not left looking for state that was removed")
                    .that(started).contains("55674910");
            assertThat(started).doesNotContain(TRUNCATION_WARNING);
        }

        assertWithMessage("the partition bootstraps at the offset the fetcher actually started from")
                .that(state.getOffsetToCommit()).isEqualTo(55674910L);
        assertWithMessage("every polled record is tracked - nothing was truncated away")
                .that(state.getAllIncompleteOffsets())
                .containsExactlyElementsIn(LongStreamEx.rangeClosed(55674910L, 55674919L).boxed().toList());
    }

    /**
     * The other shape the note names: a genuinely new group on a partition that starts at 0. It was already
     * quiet - the comparison is strict, so {@code 0 > 0} is false - and it stays quiet, which is what makes this
     * arm worth keeping: the fix must not turn the silent case into a noisy one on its way to fixing the other.
     */
    @Test
    void firstPollAtZeroWithNoCommitDataDoesNotWarn() {
        var tp = partitionFor("at-zero");
        var state = stateWithNoCommitData(tp);

        try (var logs = LogCapture.of(PartitionState.class, Level.INFO)) {
            bootstrapPollFrom(state, 0L, 9L);

            assertWithMessage("a brand new consumer group starting at the beginning of the partition is not an "
                    + "event worth a warning")
                    .that(logs.messagesAt(Level.WARN, tp.topic()))
                    .isEmpty();

            assertThat(logs.onlyMessageAt(Level.INFO, tp.topic())).contains(tp.topic());
        }

        assertThat(state.getOffsetToCommit()).isEqualTo(0L);
    }

    /**
     * THE CONTROL ARM. Same sentinel, same expectation of 0, same first polled offset - one term differs, and it
     * is the only one that should matter: commit data existed. The warning must still fire, because here the
     * broker really has dropped every record between the committed offset and the poll.
     * <p>
     * This is what a check on {@code offsetHighestSucceeded == KAFKA_OFFSET_ABSENCE} would break, silently.
     */
    @Test
    void aCommitAtOffsetZeroStillWarnsWhenTheFirstPollIsAboveIt() {
        var tp = partitionFor("commit-at-zero");
        var state = stateFromACommitAtOffsetZero(tp);

        assertWithMessage("fixture: this arm must present the same expectation as the absent case, or it is not "
                + "a control for it")
                .that(state.getOffsetToCommit()).isEqualTo(0L);

        try (var logs = LogCapture.of(PartitionState.class, Level.INFO)) {
            bootstrapPollFrom(state, 500L, 509L);

            assertWithMessage("commit data existed and the records it was filed against are gone - that is a "
                    + "genuine truncation and the operator is entitled to the warning")
                    .that(logs.messagesAt(Level.WARN, tp.topic(), TRUNCATION_WARNING))
                    .hasSize(1);
        }
    }

    /**
     * The same quiet path, reached the way production reaches it: through
     * {@code OffsetMapCodecManager#loadPartitionStateForAssignment} with a consumer that reports no committed
     * offset for the partition - a {@code null} against the partition key, which is what
     * {@code Consumer#committed(Set)} returns for a group that has never committed there.
     */
    @Test
    void theAssignmentPathBuildsAPartitionThatStartsQuietly() {
        var tp = partitionFor("assignment-path");
        var module = new PCModuleTestEnv();
        Consumer<String, String> consumer = module.options().getConsumer();
        Map<TopicPartition, OffsetAndMetadata> noCommitForThisPartition = Collections.singletonMap(tp, null);
        Mockito.when(consumer.committed(Mockito.<Set<TopicPartition>>any()))
                .thenReturn(noCommitForThisPartition);

        var wm = module.workManager();
        wm.onPartitionsAssigned(UniLists.of(tp));
        var state = wm.getPm().getPartitionState(tp);

        assertWithMessage("fixture: the assignment must have produced the codec manager's default entry")
                .that(state.getOffsetToCommit()).isEqualTo(0L);

        try (var logs = LogCapture.of(PartitionState.class, Level.INFO)) {
            var batch = new PolledTestBatch(new ModelUtils(module), tp, 900L, 909L);
            state.maybeRegisterNewPollBatchAsWork(batch.polledRecordBatch.records(tp));

            assertWithMessage("a partition assigned with no committed offset must not report truncating state")
                    .that(logs.messagesAt(Level.WARN, tp.topic()))
                    .isEmpty();
        }

        assertThat(state.getOffsetToCommit()).isEqualTo(900L);
    }

    /**
     * The astubbs#217 route, asserted at what it actually does rather than at what the inflight note claimed.
     * <p>
     * The note said unreadable commit metadata lands on the same default entry through
     * {@code catch (OffsetDecodingError)} and so inherits the false warning. It does not, and has not since
     * astubbs#217: under the runtime-default {@code IGNORE} policy,
     * {@code EncodedOffsetPair#handleUnreadableMetadata} returns {@code of(baseOffset - 1)} - commit data,
     * carrying the committed offset - so the partition bootstraps with a real expectation and the truncation
     * branches remain correct for it. That is the right outcome: the group HAS committed, and a poll above the
     * committed offset there really does mean records were removed.
     */
    @Test
    void unreadableMetadataKeepsTheCommittedOffsetRatherThanBecomingAbsentCommitData() throws Exception {
        long committedOffset = 400L;
        var decoded = OffsetMapCodecManager.deserialiseIncompleteOffsetMapFromBase64(
                committedOffset,
                // base64 of "Zunreadable-by-this-build": 'Z' is a magic byte no OffsetEncoding claims, which is
                // the forward-compatibility case the policy exists for
                "WnVucmVhZGFibGUtYnktdGhpcy1idWlsZA==",
                InvalidOffsetMetadataHandlingPolicy.IGNORE);

        assertWithMessage("IGNORE keeps the committed offset, so this is NOT the absent-commit-data case")
                .that(decoded.getHighestSeenOffset()).isEqualTo(Optional.of(committedOffset - 1));

        var tp = partitionFor("unreadable-metadata");
        var state = new PartitionState<String, String>(0, mu.getModule(), tp, decoded);
        assertThat(state.getOffsetToCommit()).isEqualTo(committedOffset);

        try (var logs = LogCapture.of(PartitionState.class, Level.INFO)) {
            bootstrapPollFrom(state, committedOffset, committedOffset + 9);

            assertWithMessage("polling exactly where the recovered commit said to poll is neither truncation "
                    + "nor an absent commit, so nothing is reported either way")
                    .that(logs.messagesAt(Level.WARN, tp.topic()))
                    .isEmpty();
            assertThat(logs.messagesAt(Level.INFO, tp.topic())).isEmpty();
        }
    }

}
