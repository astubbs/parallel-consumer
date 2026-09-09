package bz.stub.parallelconsumer.integrationTests.chaostests;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.apache.kafka.common.ConsumerGroupState;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.util.OptionalLong;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static pl.tlinkowski.unij.api.UniLists.of;

/**
 * Non-vacuity regression for {@link UncommittedCompletionDetector} in BOTH directions - a detector
 * that cannot fire is decoration, and one that fires on healthy members would be disabled within a
 * week. The same contract {@link InstanceStallProbeIT} holds for the instance-stall detector, and the
 * same shape: constructed views and explicit inputs, pure replay, no broker and no sampler thread.
 * <p>
 * <b>This is the rule-level cover; {@link WedgedPartitionRedControlIT} is the red control.</b> That
 * test runs a real engine into a real wedge and is what says the property is worth detecting; this
 * one pins the re-arm rules that a real run cannot reach on demand - a rebalance mid-stretch, a
 * member that has lost the partition, a commit that lands on the last sample before the threshold.
 * Neither substitutes for the other, and neither is tagged {@code chaos}, so both gate every default
 * integration build.
 */
class UncommittedCompletionProbeIT {

    private static final String TOPIC = "uncommitted-completion-probe";
    private static final TopicPartition TP = new TopicPartition(TOPIC, 0);
    private static final long COMMITTED = 100;
    private static final long LOCALLY_DONE_TO = 140;
    private static final long LAG = 500;
    private static final int SAMPLES = UncommittedCompletionDetector.COMMIT_NOT_LANDING_SAMPLES;

    /** Mutable scripted view - the test flips its fields between samples. */
    private static final class FakeMember implements InstanceProgressView {
        final int id;
        boolean live = true;
        Object incarnation = new Object();
        /** Absent = this member does not own the partition. */
        OptionalLong localOffsetToCommit = OptionalLong.of(LOCALLY_DONE_TO);

        FakeMember(int id) {
            this.id = id;
        }

        @Override
        public int instanceId() {
            return id;
        }

        @Override
        public boolean isLive() {
            return live;
        }

        @Override
        public long queuedInShards() {
            return 0;
        }

        @Override
        public long outForProcessing() {
            return 0;
        }

        @Override
        public long workResultsReturned() {
            return 0;
        }

        @Override
        public Object incarnationMarker() {
            return incarnation;
        }

        @Override
        public OptionalLong localOffsetToCommit(TopicPartition tp) {
            return TP.equals(tp) ? localOffsetToCommit : OptionalLong.empty();
        }
    }

    private final FakeMember member = new FakeMember(0);

    private ProgressProbe probeWatching(FakeMember... fleet) {
        ProgressProbe probe = ProgressProbe.forSeamTest("uncommitted-completion-group", TOPIC)
                .withInstanceProgress(() -> of((InstanceProgressView[]) fleet));
        probe.withGroupStateForSeamTest(ConsumerGroupState.STABLE);
        return probe;
    }

    private boolean sampleTimes(ProgressProbe probe, int times) {
        boolean fired = false;
        for (int i = 0; i < times; i++) {
            fired |= probe.sampleUncommittedCompletions(TP, COMMITTED, LAG);
        }
        return fired;
    }

    @Test
    void firesOnlyAfterTheStretchSurvivesTheFullSampleCount() {
        ProgressProbe probe = probeWatching(member);

        assertWithMessage("one sample short of the count must NOT fire - a positive difference is the "
                + "normal state of a busy partition between two commit cycles")
                .that(sampleTimes(probe, SAMPLES - 1)).isFalse();
        assertThat(probe.getViolations()).isEmpty();

        assertWithMessage("the sample that completes the count fires")
                .that(sampleTimes(probe, 1)).isTrue();
        assertThat(probe.getViolations()).hasSize(1);
        assertThat(probe.getViolations().get(0)).contains("UNCOMMITTED_COMPLETIONS/COMMIT_NOT_LANDING");
        assertThat(probe.getViolations().get(0)).contains(TP.toString());
    }

    @Test
    void reportsOncePerStretchRatherThanOncePerSample() {
        ProgressProbe probe = probeWatching(member);
        sampleTimes(probe, SAMPLES * 5);
        assertWithMessage("a wedge that persists must not fill the run's output with one finding per sample")
                .that(probe.getViolations()).hasSize(1);
    }

    @Test
    void aCommitThatLandsReArmsTheStretch() {
        ProgressProbe probe = probeWatching(member);
        sampleTimes(probe, SAMPLES - 1);
        // the commit lands: the group's committed offset moves, which starts a fresh stretch
        assertThat(probe.sampleUncommittedCompletions(TP, COMMITTED + 10, LAG)).isFalse();
        assertWithMessage("and the old stretch's remaining samples must not carry over into the new one")
                .that(sampleTimes(probe, SAMPLES - 1)).isFalse();
        assertThat(probe.getViolations()).isEmpty();
    }

    @Test
    void aRebalanceReArmsTheStretch() {
        ProgressProbe probe = probeWatching(member);
        sampleTimes(probe, SAMPLES - 1);
        probe.withGroupStateForSeamTest(ConsumerGroupState.PREPARING_REBALANCE);
        assertWithMessage("a rebalance legitimately defers commits, so it must re-arm rather than accumulate")
                .that(probe.sampleUncommittedCompletions(TP, COMMITTED, LAG)).isFalse();
        probe.withGroupStateForSeamTest(ConsumerGroupState.STABLE);
        assertThat(sampleTimes(probe, SAMPLES - 1)).isFalse();
        assertThat(probe.getViolations()).isEmpty();
    }

    @Test
    void aRestartReArmsTheStretch() {
        ProgressProbe probe = probeWatching(member);
        sampleTimes(probe, SAMPLES - 1);
        member.incarnation = new Object(); // a fresh PC on the same instance id
        assertWithMessage("a new incarnation inherits nothing from the old one's silence")
                .that(sampleTimes(probe, 1)).isFalse();
        assertThat(sampleTimes(probe, SAMPLES - 2)).isFalse();
        assertThat(probe.getViolations()).isEmpty();
    }

    @Test
    void aMemberThatDoesNotOwnThePartitionNeverAccuses() {
        member.localOffsetToCommit = OptionalLong.empty();
        ProgressProbe probe = probeWatching(member);
        assertWithMessage("a removed or never-assigned partition answers empty, and empty is not a reading")
                .that(sampleTimes(probe, SAMPLES * 3)).isFalse();
        assertThat(probe.getViolations()).isEmpty();
        assertWithMessage("nothing was read, so nothing was measured either")
                .that(probe.getPeakUncommittedCompletions()).isEqualTo(0L);
    }

    @Test
    void aMemberThatIsNotLiveNeverAccuses() {
        member.live = false;
        ProgressProbe probe = probeWatching(member);
        assertWithMessage("a stopped or mid-restart member holds torn-down state")
                .that(sampleTimes(probe, SAMPLES * 3)).isFalse();
        assertThat(probe.getViolations()).isEmpty();
    }

    @Test
    void aMemberLevelWithTheGroupNeverAccuses() {
        member.localOffsetToCommit = OptionalLong.of(COMMITTED);
        ProgressProbe probe = probeWatching(member);
        assertWithMessage("nothing is finished-but-uncommitted: this is the healthy state, and it is also "
                + "the shape of the Class 2 false positive, where an incomplete record pins the local "
                + "watermark at the very offset the broker holds")
                .that(sampleTimes(probe, SAMPLES * 3)).isFalse();
        assertThat(probe.getViolations()).isEmpty();
    }

    @Test
    void theWidestReporterIsNamedWhenTwoMembersDisagree() {
        FakeMember behind = new FakeMember(1);
        behind.localOffsetToCommit = OptionalLong.of(COMMITTED + 1);
        ProgressProbe probe = probeWatching(behind, member);
        assertThat(sampleTimes(probe, SAMPLES)).isTrue();
        assertWithMessage("the finding must name the member with the most uncommitted completed work, "
                + "not whichever the fleet supplier happened to list first")
                .that(probe.getViolations().get(0)).contains("instance " + member.id);
    }

    @Test
    void thePeakIsMeasuredEvenWhileNothingGates() {
        ProgressProbe probe = probeWatching(member);
        sampleTimes(probe, SAMPLES - 1);
        assertThat(probe.getViolations()).isEmpty();
        assertWithMessage("suppressing a finding must never lose the measurement - the invariant "
                + "recordLagStagnation states, and the reason a demoted detector still earns its keep")
                .that(probe.getPeakUncommittedCompletions()).isEqualTo(LOCALLY_DONE_TO - COMMITTED);
    }

    @Test
    void anUnwiredProbeNeverFires() {
        ProgressProbe probe = ProgressProbe.forSeamTest("uncommitted-completion-group", TOPIC);
        probe.withGroupStateForSeamTest(ConsumerGroupState.STABLE);
        assertWithMessage("ambient mode has no fleet to read, and a scenario predating this detector "
                + "must not start gating on it by accident")
                .that(sampleTimes(probe, SAMPLES * 3)).isFalse();
        assertThat(probe.getViolations()).isEmpty();
    }
}
