package bz.stub.parallelconsumer.internal;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;

import static bz.stub.parallelconsumer.internal.AbstractParallelEoSStreamProcessor.longerOf;
import static bz.stub.parallelconsumer.internal.AbstractParallelEoSStreamProcessor.shorterOf;
import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;

/**
 * The control loop's block calculation compares retry delays, and one of the two can be a delay PC did not choose -
 * a {@code retryDelayProvider} returns whatever the user's code returns.
 * <p>
 * <b>The band that broke it.</b> {@code Duration.toMillis()} multiplies seconds by a thousand with
 * {@code Math.multiplyExact}, so it throws above roughly 292 million years - while {@link Instant} accepts a deadline
 * up to a billion years out. A delay between the two is therefore accepted by {@code computeRetryDueAt}'s arithmetic,
 * stored as a perfectly valid deadline, handed to the control loop by {@code getLowestRetryTime()}, and then
 * overflows here: an {@link ArithmeticException} on the control thread, about nothing the operator can connect to the
 * record that caused it. {@code getDelayUntilRetryDue}'s javadoc already acknowledged this hazard, but only for the
 * parked case, where the sentinel is short-circuited before any arithmetic runs.
 * <p>
 * Found by the review of astubbs/parallel-consumer#506, which reached it through
 * {@code PCRetriableException.retryAfter} - now refused at the throw site, where the message can point at
 * {@code park(reason)}. These two comparisons are what close the routes that have no throw site to refuse at.
 * <p>
 * <b>Proved by sabotage:</b> restoring either comparison to its previous form -
 * {@code a.toMillis() < b.toMillis()} - reddens {@link #comparingAGeologicalDelayDoesNotOverflow} with the
 * {@link ArithmeticException} it used to raise, and leaves the ordinary-scale tests here green, which is why those
 * could never have caught it.
 */
class TimeToBlockForDoesNotOverflowTest {

    /**
     * Above what {@code toMillis()} can represent and below what {@link Instant} can hold, which is the whole point:
     * every guard on the way to the control loop accepts it.
     */
    private static final Duration BEYOND_MILLIS_BUT_WITHIN_INSTANT = Duration.ofDays(365L * 500_000_000L);

    private static final Duration A_COMMIT_INTERVAL = Duration.ofSeconds(5);

    @Test
    void comparingAGeologicalDelayDoesNotOverflow() {
        assertWithMessage("a delay PC did not choose must not make the control thread throw on arithmetic")
                .that(longerOf(BEYOND_MILLIS_BUT_WITHIN_INSTANT, A_COMMIT_INTERVAL))
                .isEqualTo(BEYOND_MILLIS_BUT_WITHIN_INSTANT);
        assertWithMessage("and the value the loop actually blocks for stays bounded by the commit interval")
                .that(shorterOf(A_COMMIT_INTERVAL, BEYOND_MILLIS_BUT_WITHIN_INSTANT))
                .isEqualTo(A_COMMIT_INTERVAL);
    }

    /**
     * The premise, asserted rather than assumed: if this delay were representable as milliseconds the test above
     * would prove nothing, because the old comparison would have handled it.
     */
    @Test
    void thePremiseHolds_thatDelayIsNotRepresentableAsMillis() {
        assertThat(BEYOND_MILLIS_BUT_WITHIN_INSTANT).isGreaterThan(Duration.ofMillis(Long.MAX_VALUE));
        Instant reachable = Instant.EPOCH.plus(BEYOND_MILLIS_BUT_WITHIN_INSTANT);
        assertWithMessage("...while still being a deadline Instant can hold, which is why nothing refuses it")
                .that(reachable).isLessThan(Instant.MAX);
    }

    /**
     * The ordinary case, so the change is pinned as order-preserving rather than only overflow-free.
     */
    @Test
    void ordinaryDelaysStillPickTheLongerAndTheShorter() {
        assertThat(longerOf(Duration.ofSeconds(1), Duration.ofSeconds(30))).isEqualTo(Duration.ofSeconds(30));
        assertThat(longerOf(Duration.ofSeconds(30), Duration.ofSeconds(1))).isEqualTo(Duration.ofSeconds(30));
        assertThat(shorterOf(Duration.ofSeconds(1), Duration.ofSeconds(30))).isEqualTo(Duration.ofSeconds(1));
        assertThat(shorterOf(Duration.ofSeconds(30), Duration.ofSeconds(1))).isEqualTo(Duration.ofSeconds(1));
        assertWithMessage("equal delays are equal whichever is named first")
                .that(longerOf(A_COMMIT_INTERVAL, A_COMMIT_INTERVAL)).isEqualTo(A_COMMIT_INTERVAL);
    }
}
