package bz.stub.parallelconsumer.state;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.BrokerlessWorkManagerTestBase;
import bz.stub.parallelconsumer.internal.utils.ThreadUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import pl.tlinkowski.unij.api.UniLists;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;

/**
 * The rebalance callbacks run on the broker-poll thread inside {@code consumer.poll()}, so nothing they reach
 * may WAIT - and {@link RetryQueue}'s write lock is something they used to wait for, unboundedly, on every
 * revoke and every lost-partition event.
 * <p>
 * <b>The wait is real, not theoretical.</b> {@link RetryQueue#iterator()} hands the READ lock to
 * {@link ShardManager#getLowestRetryTime()} on the controller thread and keeps it for the whole scan, and the
 * lock is constructed fair, so an arriving writer queues behind that scan instead of interleaving with it.
 * These tests hold that read lock exactly the way the controller thread does - through a live iterator - and
 * then drive the production callback.
 * <p>
 * <b>Two things are asserted, and the second is the one that constrains the fix.</b> Declining is easy; what
 * costs is declining without splitting the pair. A container removed from its shard whose retry-queue entry
 * survives is an orphan no code path can ever remove - work is handed out by scanning shards, so it is never
 * selected, never completed and never swept, while
 * {@link RetryQueue#getQueueSizeAndNumberReadyToBeRetried()} keeps counting it as parked for retry and the
 * broker-poller load gate keeps subtracting it. That consequence is the same one recorded at
 * {@code WorkManager.onFailureResult} and pinned from the other side by
 * {@code ShardPopulationRaceTest.theInlineStaleSweepTakesTheRecordOutOfTheRetryQueueToo}.
 * <p>
 * <b>Both were RED before the fix, and they fail differently</b> - which is what proves the pair of them was
 * needed. {@code declines} timed out: the poll thread was still parked on the write lock when the deadline
 * passed. {@code leavesTheShardAndTheQueueInStep} observed the split state directly, because the unfixed order
 * was shard-first: by the time it blocked, the container had already left the shard and its queue entry had
 * not.
 * <p>
 * The write-up that outlived the in-flight note:
 * {@code docs/solutions/runtime-errors/retry-queue-write-lock-on-the-rebalance-path.md}.
 *
 * @author Antony Stubbs
 * @see RetryQueue#tryRemove(String, int, long)
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
    private static final long CALLBACK_DEADLINE_SECONDS = 10;

    /**
     * Registers one record, takes it as work, fails it, and lets the work manager park it for retry - the only
     * state in which a revoke has anything to remove from BOTH the shard and the retry queue.
     */
    private WorkContainer<String, String> aFailedRecordParkedForRetry() {
        WorkContainer<String, String> wc = ModelUtils.registerOneRecordAndTakeIt(wm, tp);
        wc.onUserFunctionFailure(new RuntimeException("deliberate - parks the record for retry"));
        wm.handleFutureResult(wc);

        assertWithMessage("FIXTURE: the failed record must be parked in the retry queue")
                .that(sm.getRetryQueue().contains(wc)).isTrue();
        assertWithMessage("FIXTURE: and still held by its shard - the pair is what the revoke has to keep in step")
                .that(sm.getNumberOfRecordsInShards()).isEqualTo(1L);
        return wc;
    }

    /**
     * A revoke on the poll thread must complete whether or not it can have the retry queue's write lock.
     */
    @Test
    void revokeDeclinesTheRetryQueueWriteLockRatherThanWaitingForIt() throws InterruptedException {
        var ignoredParkedForRetry = aFailedRecordParkedForRetry(); // the fixture is the point; the container is not needed here

        var revokeReturned = new CountDownLatch(1);
        Thread pollThread = null;
        // the controller thread's own read-lock hold, through the production iterator
        try (RetryQueue.RetryQueueIterator heldByTheControllerThread = sm.getRetryQueue().iterator()) {
            assertWithMessage("FIXTURE: the iterator must actually be holding something, or it is not modelling a scan")
                    .that(heldByTheControllerThread.hasNext()).isTrue();

            pollThread = new Thread(() -> {
                wm.onPartitionsRevoked(UniLists.of(tp));
                revokeReturned.countDown();
            }, "broker-poll");
            pollThread.start();

            assertWithMessage("the rebalance callback runs inside poll() with the whole group waiting on it, so "
                    + "it has to decline the retry queue's write lock rather than wait for the controller "
                    + "thread's scan to finish")
                    .that(revokeReturned.await(CALLBACK_DEADLINE_SECONDS, TimeUnit.SECONDS))
                    .isTrue();
        } finally {
            joinQuietly(pollThread);
        }
    }

    /**
     * Declining must change NOTHING - the shard entry stays too, so no orphan is ever created.
     */
    @Test
    void aDeclinedRevokeLeavesTheShardAndTheRetryQueueInStep() throws InterruptedException {
        WorkContainer<String, String> parkedForRetry = aFailedRecordParkedForRetry();

        var revokeReturned = new CountDownLatch(1);
        Thread pollThread = null;
        try (RetryQueue.RetryQueueIterator heldByTheControllerThread = sm.getRetryQueue().iterator()) {
            assertThat(heldByTheControllerThread.hasNext()).isTrue();

            pollThread = new Thread(() -> {
                wm.onPartitionsRevoked(UniLists.of(tp));
                revokeReturned.countDown();
            }, "broker-poll");
            pollThread.start();

            boolean ignoredCompleted = revokeReturned.await(CALLBACK_DEADLINE_SECONDS, TimeUnit.SECONDS);
            // whether it completed is the OTHER test's assertion; here the deadline is only a way of giving the
            // poll thread every chance to reach the split before the state below is read

            // read while the lock is still contended - the split only exists in this window
            boolean stillQueued = sm.getRetryQueue().contains(parkedForRetry);
            long stillInShards = sm.getNumberOfRecordsInShards();
            log.debug("Under contention: retry queue holds it = {}, shards hold {}", stillQueued, stillInShards);

            assertWithMessage("a retry-queue entry whose container is in no shard is removed by nothing, ever - "
                    + "every removal path reaches the queue through shard contents. So the revoke either does "
                    + "both removals or neither, and under contention it must be neither")
                    .that(stillQueued && stillInShards == 0L)
                    .isFalse();
            assertWithMessage("and 'neither' is what it must be, rather than the queue emptying first")
                    .that(stillQueued).isTrue();
            assertThat(stillInShards).isEqualTo(1L);
        } finally {
            joinQuietly(pollThread);
        }
    }

    /**
     * The same contract for the epoch-change stale sweep, which is the second - and, until now, invisible -
     * reach into the same write lock from the same thread.
     * <p>
     * {@link ShardManager#removeStaleContainers()} is reached from {@code PartitionStateManager}'s
     * {@code onPartitionsAssigned} as well as its {@code onPartitionsRemoved}, and it used to map
     * {@code retryQueue::remove} over the swept containers. A METHOD REFERENCE is not a method call, so
     * {@code ArchitectureTest.rebalanceCallbacksMustNotBlock} could not see it: with every exemption deleted the
     * rule reported six violations and none of them on {@code onPartitionsAssigned}, which reaches the same lock
     * by this route.
     * <p>
     * Driven against a shard built here rather than through {@code wm}, because the sweep has to find a
     * container that is ALREADY stale - and every production route that makes one stale runs the sweep on its
     * way past.
     */
    @Test
    void theStaleSweepDeclinesTheRetryQueueWriteLockRatherThanWaitingForIt() throws InterruptedException {
        var module = mu.getModule();
        var record = new ConsumerRecord<>(topic, tp.partition(), 7L, "a-key", "a-value");
        var retryQueue = new RetryQueue();
        var shard = new ProcessingShard<>(ShardKey.of(record, module.options().getOrdering()),
                module.options(), pm, new RecordPopulation());

        wm.onPartitionsAssigned(UniLists.of(tp));
        var container = new WorkContainer<>(pm.getEpochOfPartition(tp), record, module);
        shard.addWorkContainer(container);
        boolean ignoredWasAbsent = retryQueue.add(container); // parking it is the fixture; presence is asserted below

        // the partition goes away, so what this shard holds is stale - and this shard is not registered with
        // the work manager, so nothing has swept it on the way
        wm.onPartitionsRevoked(UniLists.of(tp));
        assertWithMessage("FIXTURE: the resident must actually be stale now")
                .that(shard.getCountOfWorkTracked()).isEqualTo(1L);
        assertThat(retryQueue.contains(container)).isTrue();

        var sweepReturned = new CountDownLatch(1);
        Thread pollThread = null;
        try (RetryQueue.RetryQueueIterator heldByTheControllerThread = retryQueue.iterator()) {
            assertThat(heldByTheControllerThread.hasNext()).isTrue();

            pollThread = new Thread(() -> {
                var ignoredSwept = shard.removeStaleWorkContainersFromShard(retryQueue);
                sweepReturned.countDown();
            }, "broker-poll");
            pollThread.start();

            assertWithMessage("the epoch-change stale sweep runs inside the rebalance callback too, so it "
                    + "declines the write lock on the same terms")
                    .that(sweepReturned.await(CALLBACK_DEADLINE_SECONDS, TimeUnit.SECONDS))
                    .isTrue();

            assertWithMessage("and it leaves the pair in step - a stale container is a state the engine "
                    + "already tolerates, an orphaned queue entry is not")
                    .that(retryQueue.contains(container) && shard.getCountOfWorkTracked() == 0L)
                    .isFalse();
            assertThat(shard.getCountOfWorkTracked()).isEqualTo(1L);
        } finally {
            joinQuietly(pollThread);
        }
    }

    /**
     * Joins the poll thread after the read lock has been released. On the unfixed code it is still parked on
     * the write lock at that point and only then completes, so this is what stops a red run leaking a thread
     * into the rest of the suite.
     */
    private static void joinQuietly(Thread thread) {
        ThreadUtils.joinQuietly(thread, Duration.ofSeconds(CALLBACK_DEADLINE_SECONDS));
    }
}
