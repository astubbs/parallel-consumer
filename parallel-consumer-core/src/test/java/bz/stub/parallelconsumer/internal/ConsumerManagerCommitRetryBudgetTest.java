package bz.stub.parallelconsumer.internal;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import bz.stub.parallelconsumer.OffsetCommitBudgetExceededException;
import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.apache.kafka.common.errors.TimeoutException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static bz.stub.parallelconsumer.internal.utils.ThreadUtils.sleepOrFail;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static pl.tlinkowski.unij.api.UniMaps.of;

/**
 * {@code offsetCommitTimeout} must bound the <b>whole</b> commit, not each individual attempt.
 * <p>
 * This is the "the broker is actually down" case behind the commit-response timeout reports (astubbs#177,
 * confluentinc#833). {@link ConsumerManager#commitSync(Map)} retries Kafka's
 * {@link TimeoutException} and is documented as honouring the user's {@code offsetCommitTimeout}
 * before giving up. It measured that budget from a start instant captured <em>inside</em> the retry
 * loop, so every attempt reset it: whenever a single {@code commitSync} attempt failed faster than the
 * budget - which is the normal shape of a fast connection failure, and is guaranteed whenever
 * {@code default.api.timeout.ms} is below {@code offsetCommitTimeout} - the comparison could never
 * become false and the loop retried forever, with no backoff. PC then neither committed nor failed:
 * the broker-poll thread spun inside one {@code commitSync} call while the control thread waited out
 * its commit response, which is the reported symptom arriving by a different road.
 * <p>
 * It only <em>looked</em> bounded because the shipped defaults hide it - a single attempt blocks for
 * {@code default.api.timeout.ms} (60s), which already exceeds the default 10s budget, so the first
 * comparison fails and it gives up after one try.
 * <p>
 * {@link ConsumerManager#poll(Duration)} got the same pattern right, capturing {@code pollStarted}
 * outside its retry loop, which is the strongest evidence the difference was accidental.
 */
@Slf4j
@Timeout(60)
class ConsumerManagerCommitRetryBudgetTest {

    private static final TopicPartition TP = new TopicPartition("ConsumerManagerCommitRetryBudgetTest", 0);

    /**
     * Deliberately 20x {@link #ATTEMPT_DURATION} rather than the 5x a smaller budget would give.
     * Both bounds below are load-sensitive in opposite directions, and the ratio is what separates
     * them: the lower bound only fails if the FIRST attempt stretches past the whole budget, so 20x
     * means that needs a 20-fold dilation rather than a five-fold one. Core's unit tests run under
     * heavy thread parallelism, and a 5x stretch of a 100ms sleep there is not far-fetched - it is
     * the signature of the load-tightness family in docs/inflight/test-load-tightness-flakes.md.
     */
    private static final Duration COMMIT_BUDGET = Duration.ofSeconds(2);

    /** Each failing attempt burns this much of the budget. */
    private static final Duration ATTEMPT_DURATION = Duration.ofMillis(100);

    /**
     * Escape hatch so an unbounded loop <b>fails</b> rather than hangs: past this, the mock starts
     * succeeding. Far more attempts than the budget can pay for, so reaching it means the budget was
     * never enforced.
     */
    private static final int ATTEMPTS_BEFORE_MOCK_RELENTS = 50;

    /**
     * Generous ceiling on {@code COMMIT_BUDGET / ATTEMPT_DURATION} (= 20). Only an upper bound, so a
     * loaded machine taking longer per attempt just uses fewer of them - it cannot flake THIS bound.
     * The lower bound is the one load can reach, which is what {@link #COMMIT_BUDGET}'s ratio buys.
     */
    private static final int ATTEMPT_LIMIT = 40;

    @Test
    void commitSyncGivesUpOnceTheWholeOffsetCommitTimeoutIsSpent() {
        final AtomicInteger attempts = new AtomicInteger();

        var mockConsumer = new MockConsumer<String, String>(OffsetResetStrategy.EARLIEST) {
            @Override
            public synchronized void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
                if (attempts.incrementAndGet() > ATTEMPTS_BEFORE_MOCK_RELENTS) {
                    log.warn("Mock relenting after {} attempts - the retry budget was never enforced", attempts.get());
                    super.commitSync(offsets);
                    return;
                }
                sleepOrFail(ATTEMPT_DURATION, "Interrupted while mocking a slow commit");
                throw new TimeoutException("Broker unreachable (mocking)");
            }
        };

        var consumerManager = new ConsumerManager<>(mockConsumer,
                COMMIT_BUDGET,
                Duration.ofSeconds(30), // sasl budget - not under test, kept clear of the commit budget
                Duration.ofMillis(10));

        var offsets = of(TP, new OffsetAndMetadata(1L));

        var thrown = assertThrows(OffsetCommitBudgetExceededException.class, () -> consumerManager.commitSync(offsets),
                "a permanently timing-out commit must surface a failure once the budget is spent, not retry forever");

        // the broker's own exception is never discarded - it is the cause
        assertThat(thrown).hasCauseThat().isInstanceOf(TimeoutException.class);

        // and the message has to be actionable: which budget ran out, and what to turn
        assertThat(thrown).hasMessageThat().contains("offsetCommitTimeout");
        assertThat(thrown).hasMessageThat().contains(COMMIT_BUDGET.toString());
        // the shutdown is a known limit with a home, not an accident the reader has to guess at
        assertThat(thrown).hasMessageThat().contains("issues/317");

        // retries WERE reachable here, so the single-attempt diagnostic must not fire
        assertThat(thrown).hasMessageThat().doesNotContain("Only ONE attempt");

        assertThat(attempts.get()).isAtMost(ATTEMPT_LIMIT);
        assertThat(attempts.get()).isAtLeast(2); // it must still RETRY - giving up on the first attempt is the other failure
    }

    /**
     * The other half of the give-up message: when only ONE attempt was made, that attempt outlived the
     * whole budget, so no retry was ever reachable. Saying so is the check Kafka would have made at
     * construction - it refuses a total budget below one attempt - which PC cannot make, because
     * {@link org.apache.kafka.clients.consumer.Consumer} exposes no configuration to compare against.
     * <p>
     * This is the shipped default's shape, not an exotic one: {@code offsetCommitTimeout} defaults to
     * 10s while a single {@code consumer.commitSync} runs to {@code default.api.timeout.ms}, 60s. So out
     * of the box the retry loop cannot be entered, and until this message existed nothing said why.
     */
    @Test
    void oneAttemptOutlivingTheWholeBudgetSaysNoRetryWasReachable() {
        final Duration budget = Duration.ofMillis(100);
        final AtomicInteger attempts = new AtomicInteger();

        var mockConsumer = new MockConsumer<String, String>(OffsetResetStrategy.EARLIEST) {
            @Override
            public synchronized void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
                attempts.incrementAndGet();
                // one attempt, longer than the entire budget - the default configuration's shape
                sleepOrFail(budget.multipliedBy(3), "Interrupted while mocking an attempt that outlives the budget");
                throw new TimeoutException("Broker unreachable (mocking)");
            }
        };

        var consumerManager = new ConsumerManager<>(mockConsumer, budget,
                Duration.ofSeconds(30), Duration.ofMillis(10));

        var thrown = assertThrows(OffsetCommitBudgetExceededException.class,
                () -> consumerManager.commitSync(of(TP, new OffsetAndMetadata(1L))));

        assertThat(attempts.get()).isEqualTo(1);
        // the diagnostic that only fires here, and names the consumer setting PC cannot read
        assertThat(thrown).hasMessageThat().contains("Only ONE attempt");
        assertThat(thrown).hasMessageThat().contains("default.api.timeout.ms");
    }

    /**
     * SASL gives up against its OWN budget, and says which one ran out.
     * <p>
     * Two things this pins that reading cannot. The budget is
     * {@code saslAuthenticationRetryTimeout}, not {@code offsetCommitTimeout} - here the commit budget
     * is set generously so a message naming it would be wrong. And the clock starts at the first SASL
     * failure rather than at the start of the call, so time spent elsewhere in the commit cannot spend
     * an authentication budget.
     */
    @Test
    void saslGivesUpAgainstItsOwnBudgetAndNamesIt() {
        final Duration saslBudget = Duration.ofMillis(100);
        final AtomicInteger attempts = new AtomicInteger();

        var mockConsumer = new MockConsumer<String, String>(OffsetResetStrategy.EARLIEST) {
            @Override
            public synchronized void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
                attempts.incrementAndGet();
                throw new SaslAuthenticationException("LDAP unavailable (mocking)");
            }
        };

        var consumerManager = new ConsumerManager<>(mockConsumer,
                Duration.ofMinutes(5), // commit budget, deliberately generous - it must NOT be what ends this
                saslBudget,
                Duration.ofMillis(10)); // backoff between SASL retries

        var thrown = assertThrows(OffsetCommitBudgetExceededException.class,
                () -> consumerManager.commitSync(of(TP, new OffsetAndMetadata(1L))));

        assertThat(thrown).hasCauseThat().isInstanceOf(SaslAuthenticationException.class);
        assertThat(thrown).hasMessageThat().contains("saslAuthenticationRetryTimeout");
        assertThat(thrown).hasMessageThat().contains(saslBudget.toString());
        // the commit budget is not what ended this, so it must not be named
        assertThat(thrown).hasMessageThat().doesNotContain("offsetCommitTimeout");
        // and it retried rather than giving up on the first failure - the backoff path ran
        assertThat(attempts.get()).isAtLeast(2);
    }
}
