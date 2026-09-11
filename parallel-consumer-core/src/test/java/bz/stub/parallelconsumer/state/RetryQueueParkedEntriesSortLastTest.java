package bz.stub.parallelconsumer.state;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.PCRetriableException;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;

/**
 * The ordering invariant {@code ShardManager.getLowestRetryTime()} stops on: <b>parked entries sort last,
 * contiguously, and nothing in front of one is parked</b> (KTD14).
 *
 * <h2>Why this test exists rather than a comment</h2>
 * That walk used to {@code continue} past each parked entry, which is correct whatever the ordering and costs a
 * full walk of the parked tail on every control-loop pass - and park's steady state is a retry queue holding
 * nothing else. It now {@code break}s, which is only correct while this invariant holds, so the invariant needed
 * somewhere to go red. {@link RetryQueue}'s own javadoc states it and names the three facts it rests on; this is
 * what measures two of them.
 * <p>
 * The third - that no route mutates a container's park state <em>while it is resident in the queue</em>, which is
 * what would leave an entry sorted under a stale key - cannot be pinned by a test: it is the absence of a code
 * path, and a future one would not fail anything here. {@link RetryQueue}'s javadoc says so plainly rather than
 * implying this test covers it.
 *
 * @author Antony Stubbs
 */
class RetryQueueParkedEntriesSortLastTest extends RetryQueueTestBase {

    /**
     * Inserted deliberately out of order, and with the parked pair NOT adjacent in insertion order, so a queue that
     * happened to preserve insertion order would fail rather than pass by luck.
     */
    @Test
    void everyParkedEntryLandsBehindEveryDueEntry() {
        var dueLast = failedAfter(1, Duration.ofMinutes(5));
        var parkedEarly = parked(2);
        var dueSoonest = failedAfter(3, Duration.ofSeconds(1));
        var parkedLate = parked(4);
        var dueMiddle = failedAfter(5, Duration.ofSeconds(10));

        // Insertion order deliberately unrelated to the expected order, and interleaving the two parked entries.
        retryQueue.add(parkedEarly);
        retryQueue.add(dueLast);
        retryQueue.add(parkedLate);
        retryQueue.add(dueSoonest);
        retryQueue.add(dueMiddle);

        assertThat(ascending()).containsExactly(dueSoonest, dueMiddle, dueLast, parkedEarly, parkedLate).inOrder();
    }

    /**
     * The property the {@code break} actually relies on, asserted as a property rather than as one expected
     * ordering: once the ascending walk has seen a parked entry, everything after it is parked too. An ordering
     * assertion alone would keep passing if the comparator changed in a way that merely produced a different - and
     * still legal-looking - arrangement of these particular five records.
     */
    @Test
    void onceTheWalkSeesAParkedEntryEverythingBehindItIsParkedToo() {
        retryQueue.add(parked(2));
        retryQueue.add(failedAfter(1, Duration.ofMinutes(5)));
        retryQueue.add(parked(4));
        retryQueue.add(failedAfter(3, Duration.ofSeconds(1)));
        retryQueue.add(failedAfter(5, Duration.ofSeconds(10)));

        boolean seenParked = false;
        for (WorkContainer<?, ?> entry : ascending()) {
            if (entry.isParked()) {
                seenParked = true;
            } else {
                assertWithMessage("a record still waiting on a clock turned up BEHIND a parked one, so breaking out "
                        + "of getLowestRetryTime() at the first parked entry would now lose it: " + entry)
                        .that(seenParked).isFalse();
            }
        }
        assertWithMessage("fixture: the walk must actually have reached a parked entry").that(seenParked).isTrue();
    }

    /**
     * The discriminator underneath the ordering: a parked container's deadline is the {@link Instant#MAX} sentinel,
     * which is the largest value the queue's comparator can be handed. Without this the ordering above would be a
     * coincidence of the delays chosen rather than a property of a park.
     */
    @Test
    void aParkedContainersDeadlineIsTheLargestValueTheComparatorCanSee() {
        var container = parked(7);

        assertThat(container.isParked()).isTrue();
        assertThat(container.getRetryDueAt()).isEqualTo(Instant.MAX);
    }

    private List<WorkContainer<?, ?>> ascending() {
        List<WorkContainer<?, ?>> entries = new ArrayList<>();
        try (RetryQueue.RetryQueueIterator iterator = retryQueue.iterator()) {
            while (iterator.hasNext()) {
                entries.add(iterator.next());
            }
        }
        return entries;
    }

    private WorkContainer<String, String> parked(long offset) {
        var container = workFor(offset);
        container.onUserFunctionFailure(new PCRetriableException("hopeless").park("it ran out of attempts"));
        return container;
    }

    /**
     * A carried delay rather than a moved clock: it fixes each deadline relative to one failure time without the
     * fixture needing a mutable clock, and it is the same route a user takes.
     */
    private WorkContainer<String, String> failedAfter(long offset, Duration delay) {
        var container = workFor(offset);
        container.onUserFunctionFailure(new PCRetriableException("busy").retryAfter(delay));
        return container;
    }
}
