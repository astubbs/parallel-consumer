package bz.stub.parallelconsumer;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;
import org.junit.jupiter.api.Test;
import pl.tlinkowski.unij.api.UniMaps;

import java.time.Duration;
import java.util.Map;

import static bz.stub.parallelconsumer.CommitFailureHandler.CommitFailureDecision.CONTINUE;
import static bz.stub.parallelconsumer.CommitFailureHandler.CommitFailureDecision.SHUT_DOWN;
import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * The canned {@link CommitFailurePolicies} are pure functions of the {@link CommitFailureContext} they receive, so
 * every graduation clock here is driven by synthetic contexts - no threads, no wall clock.
 *
 * @author Antony Stubbs
 * @see CommitFailurePolicies
 * @see CommitFailureHandler
 */
class CommitFailurePoliciesTest {

    private static final TopicPartition PARTITION = new TopicPartition("input-topic", 0);

    private static final Duration FOREVER = Duration.ofDays(365);

    /**
     * A bound high enough that the test exercising one bound never trips the others.
     */
    private static final int NEVER_COUNT = Integer.MAX_VALUE;

    private CommitFailureContext context(int consecutiveExhaustedBudgets, Duration timeSinceLastSuccessfulCommit) {
        return context(consecutiveExhaustedBudgets, timeSinceLastSuccessfulCommit, 0);
    }

    private CommitFailureContext context(int consecutiveExhaustedBudgets,
                                         Duration timeSinceLastSuccessfulCommit,
                                         long assignmentEpoch) {
        return CommitFailureContext.builder()
                .failure(new TimeoutException("simulated broker commit timeout"))
                .offsets(UniMaps.of(PARTITION, new OffsetAndMetadata(42L)))
                .attemptsMade(3)
                .elapsed(Duration.ofSeconds(30))
                .consecutiveExhaustedBudgets(consecutiveExhaustedBudgets)
                .timeSinceLastSuccessfulCommit(timeSinceLastSuccessfulCommit)
                .commitMode(ParallelConsumerOptions.CommitMode.PERIODIC_CONSUMER_SYNC)
                .assignmentEpoch(assignmentEpoch)
                .build();
    }

    @Test
    void shutDownAlwaysDecidesShutDown() {
        var policy = CommitFailurePolicies.shutDown();

        assertThat(policy.onCommitFailure(context(1, Duration.ofSeconds(1)))).isEqualTo(SHUT_DOWN);
        assertThat(policy.onCommitFailure(context(1, Duration.ZERO))).isEqualTo(SHUT_DOWN);
    }

    /**
     * Load-bearing, not cosmetic: {@link ParallelConsumerOptions#validate()} detects "the user configured a
     * non-default handler" by identity against this instance.
     */
    @Test
    void shutDownIsASharedInstance() {
        assertThat(CommitFailurePolicies.shutDown()).isSameInstanceAs(CommitFailurePolicies.shutDown());
    }

    @Test
    void continueUnboundedNeverGraduates() {
        var policy = CommitFailurePolicies.continueUnbounded();

        for (int i = 1; i <= 1000; i++) {
            var context = context(i, FOREVER);
            assertWithMessage("exhaustion #%s", i)
                    .that(policy.onCommitFailure(context)).isEqualTo(CONTINUE);
        }
    }

    @Test
    void boundedGraduatesOnConsecutiveExhaustedBudgets() {
        var policy = CommitFailurePolicies.continueBounded(3, FOREVER, NEVER_COUNT, FOREVER);

        assertThat(policy.onCommitFailure(context(1, Duration.ofSeconds(5)))).isEqualTo(CONTINUE);
        assertThat(policy.onCommitFailure(context(2, Duration.ofSeconds(10)))).isEqualTo(CONTINUE);
        assertThat(policy.onCommitFailure(context(3, Duration.ofSeconds(15)))).isEqualTo(SHUT_DOWN);
    }

    @Test
    void boundedGraduatesOnTimeSinceLastSuccessfulCommit() {
        var atTheBound = CommitFailurePolicies.continueBounded();
        assertThat(atTheBound.onCommitFailure(context(1, Duration.ofMinutes(5)))).isEqualTo(SHUT_DOWN);

        var underTheBound = CommitFailurePolicies.continueBounded();
        assertThat(underTheBound.onCommitFailure(context(1, Duration.ofMinutes(5).minusMillis(1))))
                .isEqualTo(CONTINUE);
    }

    @Test
    void defaultTimeBoundIsFiveMinutes() {
        assertThat(CommitFailurePolicies.DEFAULT_MAX_TIME_SINCE_LAST_SUCCESSFUL_COMMIT)
                .isEqualTo(Duration.ofMinutes(5));
    }

    /**
     * A flapping broker that lets one commit through per twenty exhaustions resets both the consecutive counter and
     * the time-since-last-success clock every time - only the rolling window still graduates.
     */
    @Test
    void rollingWindowCatchesFlappingBroker() {
        var policy = CommitFailurePolicies.continueBounded(100, Duration.ofHours(1), 50, Duration.ofHours(1));

        int event = 0;
        // batches of twenty exhaustions, an intervening success after each batch
        for (int batch = 0; batch < 3; batch++) {
            for (int consecutive = 1; consecutive <= 20; consecutive++) {
                event++;
                // exhaustions spaced five seconds apart within the batch; the success resets the clock
                var context = context(consecutive, Duration.ofSeconds(5L * consecutive));
                var decision = policy.onCommitFailure(context);
                if (event < 50) {
                    assertWithMessage("exhaustion #%s should not yet graduate", event)
                            .that(decision).isEqualTo(CONTINUE);
                } else {
                    assertWithMessage("exhaustion #%s is the 50th inside the window", event)
                            .that(decision).isEqualTo(SHUT_DOWN);
                    return;
                }
            }
        }
    }

    /**
     * Exhaustions older than the window fall out of it - sparse failures never accumulate to the trigger.
     */
    @Test
    void rollingWindowEvictsOldExhaustions() {
        var policy = CommitFailurePolicies.continueBounded(NEVER_COUNT, FOREVER, 3, Duration.ofMinutes(1));

        // one long failing stretch, exhaustions two minutes apart - never more than one in any one-minute window
        for (int i = 1; i <= 10; i++) {
            var context = context(i, Duration.ofMinutes(2L * i));
            assertWithMessage("sparse exhaustion #%s", i)
                    .that(policy.onCommitFailure(context)).isEqualTo(CONTINUE);
        }
    }

    /**
     * The epoch rule: when no commit has ever succeeded, {@link CommitFailureContext#getTimeSinceLastSuccessfulCommit()}
     * is measured from assignment start - the bound still fires, there is no immortal "never succeeded" state.
     */
    @Test
    void noSuccessfulCommitEverStillGraduatesOnTheTimeBound() {
        var policy = CommitFailurePolicies.continueBounded();

        var noCommitEverSucceeded = context(2, Duration.ofMinutes(6));
        assertThat(policy.onCommitFailure(noCommitEverSucceeded)).isEqualTo(SHUT_DOWN);
    }

    @Test
    void graduationLatches() {
        var policy = CommitFailurePolicies.continueBounded(2, FOREVER, NEVER_COUNT, FOREVER);

        assertThat(policy.onCommitFailure(context(1, Duration.ZERO))).isEqualTo(CONTINUE);
        assertThat(policy.onCommitFailure(context(2, Duration.ZERO))).isEqualTo(SHUT_DOWN);
        // a later, milder context does not un-graduate a policy that already decided to shut down
        assertThat(policy.onCommitFailure(context(1, Duration.ZERO))).isEqualTo(SHUT_DOWN);
    }

    @Test
    void newAssignmentEpochResetsTheWindow() {
        var policy = CommitFailurePolicies.continueBounded(NEVER_COUNT, FOREVER, 5, Duration.ofHours(1));

        for (int i = 1; i <= 4; i++) {
            assertThat(policy.onCommitFailure(context(i, Duration.ofSeconds(5L * i), 0))).isEqualTo(CONTINUE);
        }

        // rebalance - history from the previous assignment does not count against the new one
        for (int i = 1; i <= 4; i++) {
            assertWithMessage("epoch 1 exhaustion #%s", i)
                    .that(policy.onCommitFailure(context(i, Duration.ofSeconds(5L * i), 1))).isEqualTo(CONTINUE);
        }
        assertThat(policy.onCommitFailure(context(5, Duration.ofSeconds(25), 1))).isEqualTo(SHUT_DOWN);
    }

    @Test
    void boundedRejectsNonPositiveBounds() {
        assertThrows(IllegalArgumentException.class,
                () -> CommitFailurePolicies.continueBounded(0, FOREVER, NEVER_COUNT, FOREVER));
        assertThrows(IllegalArgumentException.class,
                () -> CommitFailurePolicies.continueBounded(-1, FOREVER, NEVER_COUNT, FOREVER));
        assertThrows(IllegalArgumentException.class,
                () -> CommitFailurePolicies.continueBounded(1, Duration.ZERO, NEVER_COUNT, FOREVER));
        assertThrows(IllegalArgumentException.class,
                () -> CommitFailurePolicies.continueBounded(1, Duration.ofSeconds(-1), NEVER_COUNT, FOREVER));
        assertThrows(IllegalArgumentException.class,
                () -> CommitFailurePolicies.continueBounded(1, FOREVER, 0, FOREVER));
        assertThrows(IllegalArgumentException.class,
                () -> CommitFailurePolicies.continueBounded(1, FOREVER, 1, Duration.ZERO));
        assertThrows(NullPointerException.class,
                () -> CommitFailurePolicies.continueBounded(1, null, NEVER_COUNT, FOREVER));
        assertThrows(NullPointerException.class,
                () -> CommitFailurePolicies.continueBounded(1, FOREVER, NEVER_COUNT, null));
    }

    @Test
    void contextExposesEveryField() {
        var failure = new TimeoutException("the broker went away");
        Map<TopicPartition, OffsetAndMetadata> offsets = UniMaps.of(PARTITION, new OffsetAndMetadata(101L));

        var context = CommitFailureContext.builder()
                .failure(failure)
                .offsets(offsets)
                .attemptsMade(7)
                .elapsed(Duration.ofSeconds(90))
                .consecutiveExhaustedBudgets(4)
                .timeSinceLastSuccessfulCommit(Duration.ofMinutes(3))
                .commitMode(ParallelConsumerOptions.CommitMode.PERIODIC_TRANSACTIONAL_PRODUCER)
                .assignmentEpoch(2)
                .build();

        assertThat(context.getFailure()).isSameInstanceAs(failure);
        assertThat(context.getOffsets()).isEqualTo(offsets);
        assertThat(context.getAttemptsMade()).isEqualTo(7);
        assertThat(context.getElapsed()).isEqualTo(Duration.ofSeconds(90));
        assertThat(context.getConsecutiveExhaustedBudgets()).isEqualTo(4);
        assertThat(context.getTimeSinceLastSuccessfulCommit()).isEqualTo(Duration.ofMinutes(3));
        assertThat(context.getCommitMode())
                .isEqualTo(ParallelConsumerOptions.CommitMode.PERIODIC_TRANSACTIONAL_PRODUCER);
        assertThat(context.getAssignmentEpoch()).isEqualTo(2);
    }
}
