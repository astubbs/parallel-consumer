package bz.stub.parallelconsumer.state;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.FakeRuntimeException;
import bz.stub.parallelconsumer.PCRetriableException;
import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.RecordContext;
import bz.stub.parallelconsumer.internal.PCModuleTestEnv;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import static com.google.common.truth.Truth.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * What a thrown {@link PCRetriableException} can say about the hand-back it is making, and what the container does
 * with each fact (KTD14).
 *
 * <h2>Why these three facts and not a wrapper around the failure path</h2>
 * A throw is the only way a processing function can return a record, so it is also the only place it can say what it
 * meant. Before this, a caller that needed a different delay had to install a {@code retryDelayProvider} and leave
 * itself a note for it to find, and a caller that needed a hand-back which was <em>not</em> a failure of the user's
 * work had no way to say so at all - the facade kept a second attempt count beside the engine's for exactly that
 * reason, and this is what deletes it.
 * <p>
 * The contract each test below pins:
 * <ul>
 *     <li>A throw that says nothing behaves exactly as it always has - one attempt, the configured delay. That is
 *     the arm that keeps every classic user unaffected.</li>
 *     <li>A carried delay is read <b>before</b> the provider, and a throw with no carried delay still reaches it.</li>
 *     <li>Never-due is a state, not a large number: no arithmetic reads it, so nothing can degrade it.</li>
 * </ul>
 */
class WorkContainerHandbackTest {

    private static final Duration ENGINE_DEFAULT = ParallelConsumerOptions.DEFAULT_STATIC_RETRY_DELAY;

    private final List<RecordContext<String, String>> providerAskedAbout = new ArrayList<>();

    private WorkContainer<String, String> container() {
        return containerWithProvider(null);
    }

    private WorkContainer<String, String> containerWithProvider(
            Function<RecordContext<String, String>, Duration> provider) {
        return containerAt(5, provider);
    }

    /**
     * @param offset distinct per container where two are in one queue - {@link RetryQueue} de-duplicates by topic,
     *               partition and offset, so two containers at one offset are one entry
     */
    private WorkContainer<String, String> containerAt(long offset,
                                                      Function<RecordContext<String, String>, Duration> provider) {
        var options = ParallelConsumerOptions.<String, String>builder()
                .retryDelayProvider(provider == null ? null : context -> {
                    providerAskedAbout.add(context);
                    return provider.apply(context);
                })
                .build();
        return new WorkContainer<>(0, new ConsumerRecord<>("orders", 0, offset, "key", "value"),
                new PCModuleTestEnv(options));
    }

    /**
     * The control arm for every test below: an ordinary failure, saying nothing.
     */
    @Test
    void aThrowThatCarriesNothingCountsAnAttemptAndTakesTheConfiguredDelay() {
        var container = container();

        container.onUserFunctionFailure(new FakeRuntimeException("failed"));

        assertThat(container.getNumberOfFailedAttempts()).isEqualTo(1);
        assertThat(container.isParked()).isFalse();
        assertThat(container.getParkedReason()).isNull();
        assertThat(container.getDelayUntilRetryDue()).isAtMost(ENGINE_DEFAULT);
    }

    @Test
    void aCarriedDelayIsAppliedInsteadOfTheConfiguredOne() {
        var container = container();

        container.onUserFunctionFailure(new PCRetriableException("busy").retryAfter(Duration.ofMinutes(7)));

        // Within a tick of seven minutes: the deadline is failure time plus the delay, and the clock has moved.
        assertThat(container.getDelayUntilRetryDue()).isGreaterThan(Duration.ofMinutes(6));
        assertThat(container.getDelayUntilRetryDue()).isAtMost(Duration.ofMinutes(7));
        assertThat(container.getNumberOfFailedAttempts()).isEqualTo(1);
    }

    /**
     * The consistency rule: the carried delay wins where there is one, and a throw that carries none still consults
     * the user's provider - so installing a provider is not made pointless by this feature existing.
     */
    @Test
    void theProviderIsConsultedOnlyWhenTheThrowCarriedNoDelay() {
        var container = containerWithProvider(context -> Duration.ofMinutes(3));

        container.onUserFunctionFailure(new PCRetriableException("busy").retryAfter(Duration.ofSeconds(30)));
        assertThat(providerAskedAbout).isEmpty();

        container.onUserFunctionFailure(new FakeRuntimeException("failed for an ordinary reason"));
        assertThat(providerAskedAbout).hasSize(1);
        assertThat(container.getDelayUntilRetryDue()).isGreaterThan(Duration.ofMinutes(2));
    }

    @Test
    void aNotAnAttemptThrowLeavesTheFailureCountWhereItWas() {
        var container = container();
        container.onUserFunctionFailure(new FakeRuntimeException("a real failure"));

        container.onUserFunctionFailure(new PCRetriableException("nothing was attempted").notAnAttempt());

        assertThat(container.getNumberOfFailedAttempts()).isEqualTo(1);
        // Still a failure for every other purpose: it has a deadline and it is claimable again once that passes.
        assertThat(container.getLastFailedAt()).isPresent();
    }

    /**
     * The documented shape of {@code notAnAttempt}, on a record's <b>first</b> delivery: the function was never
     * run, or was withheld, and it asks to be handed back in thirty seconds.
     * <p>
     * The regression this pins is that the deadline survives the suppressed attempt. "Has this record an
     * artificial delay at all" used to be answered from the attempt counter, which {@code notAnAttempt} is
     * defined not to move - so a first delivery wrote a deadline nothing then consulted, the record was
     * immediately claimable again, and repeated withholding became a hot loop at control-loop frequency against
     * whatever the function was backing off from. That is the exact failure {@code computeRetryDueAt}'s javadoc
     * says the design exists to prevent.
     * <p>
     * {@code aNotAnAttemptThrowLeavesTheFailureCountWhereItWas} cannot see it: it throws a real failure first, so
     * the counter is already non-zero and the broken branch is never entered.
     */
    @Test
    void aWithheldFirstDeliveryStillWaitsTheDelayItAskedFor() {
        var container = container();

        container.onUserFunctionFailure(new PCRetriableException("throttled")
                .retryAfter(Duration.ofSeconds(30))
                .notAnAttempt());

        // The withhold cost no attempt, which is the whole point of saying so...
        assertThat(container.getNumberOfFailedAttempts()).isEqualTo(0);
        assertThat(container.hasPreviouslyFailed()).isFalse();
        // ...and the delay it asked for is still what decides when the record comes back.
        assertThat(container.hasRetryDeadline()).isTrue();
        assertThat(container.isDelayPassed()).isFalse();
        assertThat(container.isAvailableToTakeAsWork()).isFalse();
        assertThat(container.getDelayUntilRetryDue()).isGreaterThan(Duration.ofSeconds(29));
        assertThat(container.getDelayUntilRetryDue()).isAtMost(Duration.ofSeconds(30));
    }

    /**
     * The park is a state. Nothing computes it, so nothing can lose it - which is the whole reason it is not a
     * hundred-year retry delay.
     */
    @Test
    void aParkedRecordIsNeverDueAndCarriesItsReason() {
        var container = container();

        container.onUserFunctionFailure(new PCRetriableException("out of attempts")
                .park("it ran out of attempts"));

        assertThat(container.isParked()).isTrue();
        assertThat(container.getParkedReason()).isEqualTo("it ran out of attempts");
        assertThat(container.isDelayPassed()).isFalse();
        assertThat(container.isAvailableToTakeAsWork()).isFalse();
        // No due time at all, which is why every engine caller asks isParked() first rather than reaching for this.
        assertThat(container.getRetryDueAt()).isEqualTo(java.time.Instant.MAX);
    }

    /**
     * A park spends no attempt when it says so - the permanent decode failure's shape, where the payload will never
     * be readable and there was nothing to attempt.
     */
    @Test
    void aParkMayAlsoSayItWasNotAnAttempt() {
        var container = container();

        container.onUserFunctionFailure(new PCRetriableException("cannot decode")
                .park("its payload can never be decoded").notAnAttempt());

        assertThat(container.getNumberOfFailedAttempts()).isEqualTo(0);
        assertThat(container.isParked()).isTrue();
    }

    /**
     * A parked record that is handed back again for an ordinary reason - which is what a resume then a failure looks
     * like - stops being parked. The alternative is a record that reads as parked for ever because nobody cleared a
     * field somebody else set.
     */
    @Test
    void anOrdinaryFailureAfterAParkClearsThePark() {
        var container = container();
        container.onUserFunctionFailure(new PCRetriableException("out of attempts").park("it ran out"));

        container.onUserFunctionFailure(new FakeRuntimeException("failed again after a resume"));

        assertThat(container.isParked()).isFalse();
        assertThat(container.getParkedReason()).isNull();
    }

    /**
     * The park sorts last in the retry queue, which is what keeps {@code getLowestRetryTime} answering about a
     * record somebody is actually waiting for.
     */
    @Test
    void aParkedRecordSortsAfterEveryDueRecordInTheRetryQueue() {
        var queue = new RetryQueue();
        var parked = container();
        parked.onUserFunctionFailure(new PCRetriableException("out of attempts").park("it ran out"));
        var soon = containerAt(6, context -> Duration.ofSeconds(1));
        soon.onUserFunctionFailure(new FakeRuntimeException("failed"));

        queue.add(parked);
        queue.add(soon);

        assertThat(queue.first()).isSameInstanceAs(soon);
        assertThat(queue.last()).isSameInstanceAs(parked);
    }

    /**
     * A delay so large that adding it to the failure time leaves {@link java.time.Instant}'s range. The container
     * falls back to the configured default rather than leaving the deadline unset - unset reads as "due now", which
     * would be a hot retry loop against whatever was already failing.
     */
    @Test
    void aCarriedDelayThatCannotBeAppliedFallsBackToTheDefault() {
        var container = container();

        container.onUserFunctionFailure(new PCRetriableException("far future")
                .retryAfter(Duration.ofDays(Long.MAX_VALUE / 100_000)));

        assertThat(container.getDelayUntilRetryDue()).isAtMost(ENGINE_DEFAULT);
        assertThat(container.isParked()).isFalse();
    }

    /**
     * A negative or absent delay is a coding error, and it is refused where it is written rather than degraded at
     * the point it would be applied - the failure a {@code retryDelayProvider} can only answer with a warning.
     */
    @Test
    void aNonsenseCarriedDelayIsRefusedAtTheThrowSite() {
        assertThatThrownBy(() -> new PCRetriableException("x").retryAfter(Duration.ofSeconds(-1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("non-negative");
        assertThatThrownBy(() -> new PCRetriableException("x").retryAfter(null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /**
     * The facts are read the same way a failure is classified for logging: PC's own pass-through wrappers are
     * peeled, and an unrelated exception that merely has one further down its chain carries nothing. Otherwise a
     * user function that caught and rethrew could park a record it never meant to.
     */
    @Test
    void aHandbackUnderAnUnrelatedExceptionIsNotRead() {
        var carried = new PCRetriableException("out of attempts").park("it ran out");
        assertThat(PCRetriableException.handbackIn(carried)).isSameInstanceAs(carried);
        assertThat(PCRetriableException.handbackIn(new IllegalStateException("wrapping", carried))).isNull();

        var container = container();
        container.onUserFunctionFailure(new IllegalStateException("wrapping", carried));
        assertThat(container.isParked()).isFalse();
    }
}
