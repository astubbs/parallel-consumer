package io.confluent.csid.utils;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.confluent.csid.utils.KafkaTestUtils.collapseRepeatedCommits;
import static org.assertj.core.api.Assertions.assertThat;
import static pl.tlinkowski.unij.api.UniLists.of;

/**
 * Covers the commit-sequence normalisation that {@link KafkaTestUtils#assertCommits} relies on.
 * <p>
 * The distinction under test is between a repeat commit of the same base offset - which PC emits legitimately
 * when a record completes that cannot advance the highest sequentially succeeded offset - and a committed
 * offset moving backwards, which would be a real defect and must still fail.
 */
class KafkaTestUtilsTest {

    private static final TopicPartition P0 = new TopicPartition("input", 0);
    private static final TopicPartition P1 = new TopicPartition("input", 1);

    @Test
    void repeatedCommitOfTheSameOffsetCollapses() {
        assertThat(collapseRepeatedCommits(of(1, 1, 2))).containsExactly(1, 2);
        assertThat(collapseRepeatedCommits(of(1, 1, 1, 2, 2))).containsExactly(1, 2);
    }

    @Test
    void distinctOffsetsAreUntouched() {
        assertThat(collapseRepeatedCommits(of(1, 2, 3))).containsExactly(1, 2, 3);
        assertThat(collapseRepeatedCommits(of())).isEmpty();
        assertThat(collapseRepeatedCommits(of(1))).containsExactly(1);
    }

    @Test
    void anOffsetGoingBackwardsSurvivesAndStillFails() {
        // not adjacent repeats - the offset regressed, which is a real defect, so the sequence must be kept
        assertThat(collapseRepeatedCommits(of(1, 2, 1))).containsExactly(1, 2, 1);
        assertThat(collapseRepeatedCommits(of(1, 2, 1, 2))).containsExactly(1, 2, 1, 2);
    }

    /**
     * The regression that {@code astubbs#101} fixed - a commit that never happened - produced {@code [2, 2]}
     * where {@code [1, 2]} was expected. Collapsing must not turn that into a pass.
     */
    @Test
    void aMissingCommitIsStillCaught() {
        assertThat(collapseRepeatedCommits(of(2, 2))).containsExactly(2);
        assertThat(collapseRepeatedCommits(of(2, 2))).isNotEqualTo(of(1, 2));
    }

    /**
     * A commit history is flattened across partitions, so two partitions committing the same offset in one
     * round sit next to each other. Collapsing on the offset alone would merge them and hide a partition that
     * never committed.
     */
    @Test
    void thesameOffsetOnTwoPartitionsIsNotAMergeableRepeat() {
        assertThat(collapseRepeatedCommits(of(P0, P1), of(6, 6))).containsExactly(6, 6);
        assertThat(collapseRepeatedCommits(of(P0, P0), of(6, 6))).containsExactly(6);
    }

    /**
     * Only a repeat of that partition's own previous commit collapses, so a round-major history stays in
     * emission order.
     */
    @Test
    void eachPartitionsRepeatsCollapseIndependently() {
        // rounds: {p0:1, p1:0}, {p0:1, p1:0}, {p0:2, p1:0}
        assertThat(collapseRepeatedCommits(of(P0, P1, P0, P1, P0, P1), of(1, 0, 1, 0, 2, 0)))
                .containsExactly(1, 0, 2);
    }

    /**
     * The genesis commit is a race, so it is trimmed - but trimming before the collapse would turn an offset
     * that went backwards through 0 into an adjacent repeat and swallow it. Guards the ordering inside
     * {@link KafkaTestUtils#assertCommits}.
     */
    @Test
    void anOffsetRegressingThroughGenesisSurvivesTheCollapse() {
        List<Integer> collapsedThenTrimmed =
                KafkaTestUtils.trimAllGenesisOffset(collapseRepeatedCommits(of(P0, P0, P0), of(1, 0, 1)));
        assertThat(collapsedThenTrimmed)
                .as("the regression must survive to fail an of(1) expectation")
                .containsExactly(1, 1);

        List<Integer> trimmedThenCollapsed =
                collapseRepeatedCommits(KafkaTestUtils.trimAllGenesisOffset(of(1, 0, 1)));
        assertThat(trimmedThenCollapsed)
                .as("trimming first would have swallowed it - this is the ordering the helper must avoid")
                .containsExactly(1);
    }
}
