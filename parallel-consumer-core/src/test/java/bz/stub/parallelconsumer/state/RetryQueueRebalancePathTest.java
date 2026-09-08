package bz.stub.parallelconsumer.state;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.BrokerlessWorkManagerTestBase;
import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.internal.EpochAndRecordsMap;
import bz.stub.parallelconsumer.internal.PCModuleTestEnv;
import bz.stub.parallelconsumer.internal.utils.BlockedThreadAsserter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.junit.jupiter.api.Test;
import pl.tlinkowski.unij.api.UniLists;
import pl.tlinkowski.unij.api.UniMaps;

import java.time.Duration;

import static bz.stub.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.KEY;
import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;

/**
 * The rebalance callbacks run on the broker-poll thread inside {@code consumer.poll()}, so nothing they reach
 * may WAIT - and {@link RetryQueue}'s write lock is something they used to wait for, unboundedly, on every
 * revoke, every lost-partition event and every assignment.
 * <p>
 * <b>The wait is real, not theoretical.</b> {@link RetryQueue#iterator()} hands the READ lock to
 * {@link ShardManager#getLowestRetryTime()} on the controller thread and keeps it for the whole scan, and the
 * lock is constructed fair, so an arriving writer queues behind that scan instead of interleaving with it.
 * The tests here hold that read lock exactly the way the controller thread does - through a live iterator -
 * and then drive the production callback on a thread named {@code broker-poll}.
 * <p>
 * <b>The design these tests pin.</b> The callbacks remove from the SHARDS only, which is a
 * {@link java.util.concurrent.ConcurrentSkipListMap} operation with no lock to decline, and the controller
 * thread collects whatever retry-queue entries that leaves behind - an entry whose container is resident in no
 * shard is garbage, and {@link ShardManager#purgeDepartedRetryEntries()} is what collects it, once per
 * control-loop pass. So the poll thread never touches the retry queue at all, rather than touching it and
 * declining.
 * <p>
 * <b>The superseded design, for the reader who finds it in the history.</b>
 * astubbs/parallel-consumer#431 kept the poll thread on the queue and made it DECLINE - {@code tryRemove},
 * abandoning the paired shard removal on refusal so the pair could never split. It was correct and it is
 * measurably more machinery: a second, non-blocking entry point on {@link RetryQueue}, a second ask after the
 * shard removal with a put-back on refusal, and a retirement bound that varied by ordering mode. The write-up
 * that carries both: {@code docs/solutions/runtime-errors/retry-queue-write-lock-on-the-rebalance-path.md}.
 *
 * @author Antony Stubbs
 * @see ShardManager#purgeDepartedRetryEntries()
 */
@Slf4j
class RetryQueueRebalancePathTest extends BrokerlessWorkManagerTestBase {

    /**
     * How long the poll thread is given to complete a rebalance callback while the retry queue's read lock is
     * held against it. Generous on purpose: this is a liveness deadline, not a performance one, so a slow or
     * loaded machine must not turn a green run red. Before the fix the callback does not complete at all - it
     * is parked on the write lock until the iterator below is closed - so no deadline in this range can be
     * "nearly" met.
     */
    private static final Duration CALLBACK_DEADLINE = Duration.ofSeconds(10);

    /**
     * Registers one record, takes it as work, fails it, and lets the work manager park it for retry - the only
     * state in which a rebalance callback has anything to remove from BOTH the shard and the retry queue.
     */
    private WorkContainer<String, String> aFailedRecordParkedForRetry() {
        WorkContainer<String, String> wc = ModelUtils.registerOneRecordAndTakeIt(wm, tp);
        wc.onUserFunctionFailure(new RuntimeException("deliberate - parks the record for retry"));
        wm.handleFutureResult(wc);

        assertWithMessage("FIXTURE: the failed record must be parked in the retry queue")
                .that(sm.getRetryQueue().contains(wc)).isTrue();
        assertWithMessage("FIXTURE: and still held by its shard - the pair is what the callback has to leave alone")
                .that(sm.getNumberOfRecordsInShards()).isEqualTo(1L);
        return wc;
    }

    /**
     * A revoke on the poll thread must complete whether or not the retry queue's write lock is available,
     * which it can only do by not asking for it.
     * <p>
     * <b>RED on master</b>: {@code ShardManager.removeWorkFromShardFor} calls {@code retryQueue.remove}, the
     * write lock is held against it for the whole of the controller thread's scan, and the callback is still
     * parked when the deadline passes.
     */
    @Test
    void aRevokeDoesNotTouchTheRetryQueueAndSoCannotWaitForItsWriteLock() {
        WorkContainer<String, String> parkedForRetry = aFailedRecordParkedForRetry();

        withTheControllerThreadHoldingTheReadLock(
                () -> wm.onPartitionsRevoked(UniLists.of(tp)),
                revokeReturned -> {
                    assertWithMessage("the rebalance callback runs inside poll() with the whole group waiting on "
                            + "it, so it must not reach the retry queue's write lock at all - the controller "
                            + "thread is holding the read lock for the length of a scan")
                            .that(revokeReturned)
                            .isTrue();

                    assertWithMessage("and it must still have done its own job while contended: the shard "
                            + "removal is a ConcurrentSkipListMap operation with no lock to wait for")
                            .that(sm.getNumberOfRecordsInShards())
                            .isEqualTo(0L);
                    assertWithMessage("the queue entry is deliberately left behind - collecting it is the "
                            + "controller thread's job, and this is the moment that separates the two designs")
                            .that(sm.getRetryQueue().contains(parkedForRetry))
                            .isTrue();
                });
    }

    /**
     * The same contract for the epoch-change stale sweep, which is the second reach into the same write lock
     * from the same thread - and the one the ArchUnit rule could not see until it learned to follow method
     * references.
     * <p>
     * Driven through the real {@code onPartitionsAssigned}: re-assigning an already-assigned partition
     * increments its epoch, which is what makes the resident container stale, and then calls
     * {@link ShardManager#removeStaleContainers()} in the same callback. On master that maps
     * {@code retryQueue::remove} over what it swept.
     */
    @Test
    void theStaleSweepDoesNotTouchTheRetryQueueEither() {
        WorkContainer<String, String> parkedForRetry = aFailedRecordParkedForRetry();

        withTheControllerThreadHoldingTheReadLock(
                () -> wm.onPartitionsAssigned(UniLists.of(tp)),
                assignReturned -> {
                    assertWithMessage("onPartitionsAssigned runs the epoch-change stale sweep, so it is on the "
                            + "same terms as the revoke: it may not wait for the retry queue")
                            .that(assignReturned)
                            .isTrue();

                    assertWithMessage("FIXTURE: the sweep must actually have found something to sweep - a "
                            + "callback that swept nothing would return promptly whatever the lock did, and "
                            + "would be asserting nothing at all")
                            .that(sm.getNumberOfRecordsInShards())
                            .isEqualTo(0L);
                    assertThat(sm.getRetryQueue().contains(parkedForRetry)).isTrue();
                });
    }

    /**
     * The other half of the design, and the reason the callbacks are allowed to leave an entry behind: the
     * controller thread collects it on its next pass.
     * <p>
     * <b>RED on master at the middle assertion</b> - master's sweep removes from the queue itself, so there is
     * nothing left to collect and the claim being made here is not the one master implements. <b>RED with the
     * purge deleted at the last assertion</b>, which is the orphan this whole design has to answer for.
     */
    @Test
    void aDepartedEntryIsCollectedByTheNextControllerPass() {
        WorkContainer<String, String> parkedForRetry = aFailedRecordParkedForRetry();

        wm.onPartitionsRevoked(UniLists.of(tp));

        assertWithMessage("PRECONDITION: the container must have left its shard, or there is nothing departed")
                .that(sm.getNumberOfRecordsInShards())
                .isEqualTo(0L);
        assertWithMessage("the callback leaves the queue alone - so between the callback and the next "
                + "controller pass the entry is garbage, which is exactly the window the invariant bounds")
                .that(sm.getRetryQueue().contains(parkedForRetry))
                .isTrue();

        // one controller-thread work request - what the control loop does every pass
        assertWithMessage("the container is in no shard, so no work comes back; the point is what the pass "
                + "does to the queue on its way past")
                .that(wm.getWorkIfAvailable(10))
                .isEmpty();

        assertWithMessage("a retry-queue entry whose container is resident in no shard is garbage the "
                + "controller collects, and it may exist for at most one control-loop tick")
                .that(sm.getRetryQueue().contains(parkedForRetry))
                .isFalse();
    }

    /**
     * <b>The consequence that makes the purge load-bearing rather than tidy.</b>
     * <p>
     * {@link ShardManager#getNumberOfWorkQueuedInShardsAwaitingSelection()} is
     * {@code readyToRetry + max(0, shardCounters - queueSize)}. A queue-only entry adds one to each of the
     * first and last terms, and those cancel only while {@code shardCounters - queueSize} is positive. Drained
     * - which is exactly when it matters - the second term floors at zero and the {@code readyToRetry}
     * contribution survives alone. That figure is {@code WorkManager#isRecordsAwaitingProcessing()}, which
     * {@code AbstractParallelEoSStreamProcessor#drain()} requires to be false before it transitions to
     * closing, so one entry holds a draining close open to its timeout with nothing in the system.
     * <p>
     * Established by astubbs/parallel-consumer#437, whose
     * {@code RetryQueueRequeueWindowTest#aQueueOnlyOrphanCostsTheDrainFigureAndNotTheLoadGate} owns the
     * measurement of both figures; this arm asserts only that one controller pass clears it.
     * <p>
     * <b>RED on master and with the purge deleted</b>, at the last assertion.
     */
    @Test
    void aQueueOnlyEntryNoLongerHoldsTheDrainOpen() {
        WorkContainer<String, String> parkedForRetry = aFailedRecordParkedForRetry();

        wm.onPartitionsRevoked(UniLists.of(tp));
        // Idempotent, and written this way ON PURPOSE so the fixture is the same whatever the callback did
        // with the queue: after the fix the entry is already there and this returns false; on master the sweep
        // took it, and this puts back exactly the orphan the drain figure is measured against.
        boolean ignoredWasAbsent = sm.getRetryQueue().add(parkedForRetry);

        // the entry only reads as ready-to-retry once its delay elapses; from then on it never un-elapses
        mu.getModule().getMutableClock().add(Duration.ofHours(1));

        assertWithMessage("FIXTURE: nothing is assigned, nothing is in flight and the shards hold nothing, so "
                + "there is genuinely no work waiting")
                .that(sm.getNumberOfRecordsInShards())
                .isEqualTo(0L);
        assertWithMessage("FIXTURE: and the drain figure reads true anyway, which is the defect - without this "
                + "the assertion below would pass on a figure that was never wrong")
                .that(wm.isRecordsAwaitingProcessing())
                .isTrue();

        var ignoredWork = wm.getWorkIfAvailable(10);

        assertWithMessage("drain() transitions to closing only when this is false - one controller pass has to "
                + "be enough, or a draining close hangs to its timeout")
                .that(wm.isRecordsAwaitingProcessing())
                .isFalse();
    }

    /**
     * <b>The bound is one control-loop tick in EVERY ordering mode, and this is the arm that says so.</b>
     * <p>
     * Under {@link ParallelConsumerOptions.ProcessingOrder#KEY} or {@code PARTITION},
     * {@link ProcessingShard#getWorkIfAvailable} breaks out of the shard scan as soon as it takes one
     * container, so anything behind a takeable head is not inspected on that tick. The superseded
     * astubbs/parallel-consumer#431 design retired the abandoned pair from inside that scan, so its bound was
     * "until the head in front leaves the shard" rather than one tick.
     * <p>
     * The purge scans the RETRY QUEUE instead, before the shard scan and independently of it, so the ordered
     * mode's break cannot delay it. Both halves are asserted: the head is taken and the scan does stop there,
     * and the departed entry is collected on that same tick regardless.
     */
    @Test
    void underOrderedProcessingADepartedEntryIsStillCollectedOnTheSameTick() {
        var keyOrdered = new PCModuleTestEnv(ParallelConsumerOptions.<String, String>builder()
                .ordering(KEY)
                .consumer(new MockConsumer<>(OffsetResetStrategy.EARLIEST))
                .build());
        var keyOrderedWm = keyOrdered.workManager();
        var keyOrderedSm = keyOrderedWm.getSm();

        keyOrderedWm.onPartitionsAssigned(UniLists.of(tp));
        // ONE key for both records, so KEY ordering puts them in one shard and the head's break hides the tail
        var head = new ConsumerRecord<>(topic, tp.partition(), 10L, "a-key", "head");
        var tail = new ConsumerRecord<>(topic, tp.partition(), 20L, "a-key", "tail");
        keyOrderedWm.registerWork(new EpochAndRecordsMap<>(
                new ConsumerRecords<>(UniMaps.of(tp, UniLists.of(head, tail))), keyOrderedWm.getPm()));

        var shard = keyOrderedSm.getShard(keyOrderedSm.computeShardKey(head)).orElseThrow(
                () -> new AssertionError("FIXTURE: both records must be in one shard under KEY ordering"));
        assertWithMessage("FIXTURE: and that shard must hold both of them")
                .that(shard.getCountOfWorkTracked()).isEqualTo(2L);

        // model what the rebalance callback now does: remove from the SHARD only, leaving the queue entry
        WorkContainer<String, String> departedTail = shard.removeWorkAtOffset(20L);
        assertWithMessage("FIXTURE: the tail must actually have left the shard")
                .that(departedTail).isNotNull();
        boolean ignoredWasAbsent = keyOrderedSm.getRetryQueue().add(departedTail);

        var taken = keyOrderedSm.getWorkIfAvailable(10);

        assertWithMessage("FIXTURE: ordered processing must have taken the head and stopped there - if the "
                + "scan ran on, this arm is the unordered case in disguise and proves nothing about the break")
                .that(taken).hasSize(1);
        assertThat(taken.get(0).offset()).isEqualTo(10L);

        assertWithMessage("the departed tail's entry is collected on the SAME tick as the head is taken: the "
                + "purge reads the retry queue, not the shards, so the ordered scan's break cannot delay it")
                .that(keyOrderedSm.getRetryQueue().contains(departedTail))
                .isFalse();
    }

    /**
     * The control arm for the purge's predicate: residency, not staleness, not age.
     * <p>
     * A container parked for retry and still resident in its shard is ordinary state, not garbage - the whole
     * engine depends on it surviving every control-loop pass until its delay elapses. This is what goes red if
     * the predicate is inverted or dropped, which the two arms above cannot detect on their own.
     */
    @Test
    void anEntryWhoseContainerIsStillResidentIsNotCollected() {
        WorkContainer<String, String> parkedForRetry = aFailedRecordParkedForRetry();

        assertWithMessage("FIXTURE: still resident, and still in the queue, before the pass")
                .that(sm.getNumberOfRecordsInShards())
                .isEqualTo(1L);

        assertWithMessage("its retry delay has not passed, so it is not takeable - which is precisely the "
                + "state the purge must leave alone")
                .that(wm.getWorkIfAvailable(10))
                .isEmpty();

        assertWithMessage("a resident container's retry-queue entry is not garbage, and a purge that "
                + "collected it would delete work the engine is still waiting to retry")
                .that(sm.getRetryQueue().contains(parkedForRetry))
                .isTrue();
        assertThat(sm.getNumberOfRecordsInShards()).isEqualTo(1L);
    }

    /**
     * What every contended test here is made of: the controller thread holds the retry queue's READ lock
     * through a live iterator, {@code rebalanceCallback} runs on a thread named {@code broker-poll}, and
     * {@code whileStillContended} reads the state while that lock is <em>still held</em> - which is the only
     * window in which a wait on the write lock is observable at all.
     * <p>
     * <b>The join placement is load-bearing and is why this takes a callback rather than returning a
     * handle.</b> The poll thread is joined only after the try-with-resources has released the read lock: on
     * unfixed code it is parked on the write lock until then, so joining any earlier would block for the whole
     * join timeout and then leak the thread into the rest of the suite.
     * <p>
     * Adapted from the harness astubbs/parallel-consumer#431 built for the same lock, against the design that
     * superseded it.
     *
     * @param whileStillContended handed whether the callback returned inside {@link #CALLBACK_DEADLINE}
     */
    private void withTheControllerThreadHoldingTheReadLock(Runnable rebalanceCallback,
                                                           ContendedAssertions whileStillContended) {
        var pollThread = new BlockedThreadAsserter();
        try (RetryQueue.RetryQueueIterator heldByTheControllerThread = sm.getRetryQueue().iterator()) {
            assertWithMessage("FIXTURE: the iterator must actually be holding something, or it is not modelling a scan")
                    .that(heldByTheControllerThread.hasNext()).isTrue();

            whileStillContended.run(pollThread.returnsWithin(rebalanceCallback, "broker-poll", CALLBACK_DEADLINE));
        } finally {
            pollThread.joinQuietly(CALLBACK_DEADLINE);
        }
    }

    /**
     * The assertions a test makes while the read lock is still held against the poll thread.
     */
    @FunctionalInterface
    private interface ContendedAssertions {
        void run(boolean callbackReturned);
    }
}
