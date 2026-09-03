package bz.stub.parallelconsumer.state;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.internal.PCModuleTestEnv;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import pl.tlinkowski.unij.api.UniLists;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;

/**
 * {@link RetryQueue#removeAll} contract tests - the one entry point the shard calls on EVERY work request, and
 * the only one with no coverage at all until now (the three pre-existing {@code RetryQueue} tests live in
 * {@link ShardManagerTest} and assert ordering only; see
 * {@code docs/inflight/test-retry-queue-behaviour-untested.md}).
 * <p>
 * <b>What these pin, and what they cannot.</b> {@code removeAll} opens with a fast path that returns
 * {@code false} without taking the write lock. That guard reads the CALLER'S list, never the queue's shared
 * {@code unique} map, and the difference is a real defect the class has already carried: a guard on
 * {@code unique.isEmpty()} is read off-lock, so a stale {@code true} makes the method return {@code false}
 * having removed nothing - leaving a container in the retry queue while it is also in flight. What is
 * single-threaded and testable here is the RETURN CONTRACT either form has to satisfy, and these assert it:
 * an empty or null argument removes nothing and says so, and a non-empty one reports whether the queue
 * actually changed, exactly as {@link java.util.Set#removeAll} does. The staleness itself is a memory-model
 * question no single-threaded test can reach - {@link RetryQueueLincheckTest} is where the concurrent half
 * lives.
 *
 * @author Antony Stubbs
 * @see RetryQueue
 */
class RetryQueueTest {

    private static final String TOPIC = "retry-queue-topic";

    private static final int PARTITION = 0;

    private static final long EPOCH = 0L;

    private final PCModuleTestEnv module = new PCModuleTestEnv();

    private final RetryQueue retryQueue = new RetryQueue();

    private WorkContainer<String, String> workFor(long offset) {
        return new WorkContainer<>(EPOCH, new ConsumerRecord<>(TOPIC, PARTITION, offset, "key-" + offset, "value"), module);
    }

    @Test
    void removeAllOfNullRemovesNothingAndReportsUnmodified() {
        WorkContainer<String, String> present = workFor(0);
        retryQueue.add(present);

        assertThat(retryQueue.removeAll(null)).isFalse();

        assertThat(retryQueue.size()).isEqualTo(1);
        assertThat(retryQueue.contains(present)).isTrue();
    }

    @Test
    void removeAllOfAnEmptyListRemovesNothingAndReportsUnmodified() {
        WorkContainer<String, String> present = workFor(0);
        retryQueue.add(present);

        assertThat(retryQueue.<String, String>removeAll(Collections.emptyList())).isFalse();

        assertThat(retryQueue.size()).isEqualTo(1);
        assertThat(retryQueue.contains(present)).isTrue();
    }

    /**
     * The shard's steady state: it took no previously-failed work, so it hands an empty list to an empty
     * queue. This is the call the fast path exists for, and it must answer the same as the locked path did.
     */
    @Test
    void removeAllOfAnEmptyListAgainstAnEmptyQueueReportsUnmodified() {
        assertThat(retryQueue.isEmpty()).isTrue();

        assertThat(retryQueue.<String, String>removeAll(Collections.emptyList())).isFalse();

        assertThat(retryQueue.isEmpty()).isTrue();
    }

    @Test
    void removeAllOfAbsentContainersReportsUnmodified() {
        WorkContainer<String, String> present = workFor(0);
        retryQueue.add(present);

        assertThat(retryQueue.removeAll(UniLists.of(workFor(1), workFor(2)))).isFalse();

        assertThat(retryQueue.size()).isEqualTo(1);
        assertThat(retryQueue.contains(present)).isTrue();
    }

    @Test
    void removeAllOfAMixOfPresentAndAbsentRemovesThePresentAndReportsModified() {
        WorkContainer<String, String> stays = workFor(0);
        WorkContainer<String, String> goes = workFor(1);
        WorkContainer<String, String> neverAdded = workFor(2);
        retryQueue.add(stays);
        retryQueue.add(goes);

        assertThat(retryQueue.removeAll(UniLists.of(goes, neverAdded))).isTrue();

        assertThat(retryQueue.size()).isEqualTo(1);
        assertThat(retryQueue.contains(stays)).isTrue();
        assertThat(retryQueue.contains(goes)).isFalse();
    }

    /**
     * Uniqueness is by topic/partition/offset, not by container identity - so a DIFFERENT container carrying
     * the same record must remove the queued one. That is not incidental: the shard builds its removal list
     * from the containers it just took, which after a stale replacement are not the instances that were
     * queued.
     */
    @Test
    void removeAllMatchesOnTopicPartitionOffsetRatherThanIdentity() {
        retryQueue.add(workFor(0));

        assertThat(retryQueue.removeAll(UniLists.of(workFor(0)))).isTrue();

        assertThat(retryQueue.isEmpty()).isTrue();
    }

    /**
     * {@link RetryQueue#tryRemove} takes an uncontended lock and removes, keyed by the record's coordinates
     * rather than by container identity - the same uniqueness {@code removeAll} matches on.
     */
    @Test
    void tryRemoveTakesAnUncontendedLockAndRemovesByCoordinates() {
        WorkContainer<String, String> stays = workFor(0);
        WorkContainer<String, String> goes = workFor(1);
        retryQueue.add(stays);
        retryQueue.add(goes);

        assertThat(retryQueue.tryRemove(TOPIC, PARTITION, 1L)).isTrue();

        assertThat(retryQueue.size()).isEqualTo(1);
        assertThat(retryQueue.contains(goes)).isFalse();
        assertThat(retryQueue.contains(stays)).isTrue();
    }

    /**
     * An absent entry is not a refusal. {@code true} means "I got the lock and did the removal"; only the lock
     * can say no, because the only caller decision it feeds is whether the caller may go on to its own paired
     * removal - and for that, "there was nothing here" is as good as "I removed it".
     */
    @Test
    void tryRemoveOfAnAbsentEntryStillReportsThatItRan() {
        retryQueue.add(workFor(0));

        assertThat(retryQueue.tryRemove(TOPIC, PARTITION, 99L)).isTrue();

        assertThat(retryQueue.size()).isEqualTo(1);
    }

    /**
     * The refusal itself, driven the way the controller thread actually causes it: {@link RetryQueue#iterator()}
     * hands out the READ lock and holds it for the whole scan. A writer arriving in that window must be told no
     * rather than parked - and must change nothing, because a caller that was refused goes on to leave its own
     * half of the pair alone.
     * <p>
     * The acquire runs on another thread on purpose: a read lock is not upgradeable, so asking from the thread
     * already holding it would be a deadlock rather than the contention being modelled.
     */
    @Test
    void tryRemoveRefusesWhileAnIteratorHoldsTheReadLockAndChangesNothing() throws InterruptedException {
        WorkContainer<String, String> present = workFor(0);
        retryQueue.add(present);

        var answer = new java.util.concurrent.atomic.AtomicBoolean(true);
        try (RetryQueue.RetryQueueIterator heldElsewhere = retryQueue.iterator()) {
            assertThat(heldElsewhere.hasNext()).isTrue();

            Thread writer = new Thread(() -> answer.set(retryQueue.tryRemove(TOPIC, PARTITION, 0L)), "writer");
            writer.start();
            writer.join(java.util.concurrent.TimeUnit.SECONDS.toMillis(10));

            assertWithMessage("the acquire must have returned rather than parked - a caller that waits here is "
                    + "the defect this method exists to remove")
                    .that(writer.isAlive()).isFalse();
            assertThat(answer.get()).isFalse();
        }

        assertWithMessage("a refusal changes nothing at all")
                .that(retryQueue.contains(present)).isTrue();
        assertThat(retryQueue.size()).isEqualTo(1);
    }

    /**
     * Every container in the argument is removed, not just the first match - a loop that returned early on
     * its first {@code modified = true} would still satisfy the boolean contract while orphaning the rest.
     */
    @Test
    void removeAllRemovesEveryPresentContainerNotOnlyTheFirst() {
        List<WorkContainer<String, String>> all = new ArrayList<>();
        for (long offset = 0; offset < 4; offset++) {
            WorkContainer<String, String> work = workFor(offset);
            all.add(work);
            retryQueue.add(work);
        }

        assertThat(retryQueue.removeAll(all)).isTrue();

        assertThat(retryQueue.isEmpty()).isTrue();
        assertThat(retryQueue.size()).isEqualTo(0);
    }
}
