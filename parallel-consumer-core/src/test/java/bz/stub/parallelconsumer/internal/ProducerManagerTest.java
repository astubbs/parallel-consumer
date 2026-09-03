package bz.stub.parallelconsumer.internal;

/*-
 * Copyright (C) 2020-2023 Confluent, Inc.
 * Modifications Copyright (C) 2026 Antony Stubbs and contributors
 */

import com.google.common.truth.Truth;
import bz.stub.parallelconsumer.internal.utils.BlockedThreadAsserter;
import bz.stub.parallelconsumer.internal.utils.LatchTestUtils;
import bz.stub.parallelconsumer.ParallelConsumer;
import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.ParallelEoSStreamProcessor;
import bz.stub.parallelconsumer.PollContextInternal;
import bz.stub.parallelconsumer.ProvesClaim;
import bz.stub.parallelconsumer.Quarantined;
import bz.stub.parallelconsumer.TransactionalClaim;
import bz.stub.parallelconsumer.state.ModelUtils;
import bz.stub.parallelconsumer.state.WorkContainer;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.Mockito;
import pl.tlinkowski.unij.api.UniLists;
import pl.tlinkowski.unij.api.UniMaps;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static bz.stub.parallelconsumer.ManagedTruth.assertThat;
import static bz.stub.parallelconsumer.ManagedTruth.assertWithMessage;
import static bz.stub.parallelconsumer.ParallelConsumerOptions.CommitMode.PERIODIC_TRANSACTIONAL_PRODUCER;
import static bz.stub.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.UNORDERED;
import static bz.stub.parallelconsumer.internal.ProducerWrapper.ProducerState.*;
import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;
import static java.util.Collections.emptyList;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * Covers transaction state systems, and their blocking behaiviour towards sending records and the reverse.
 * <p>
 * <b>Including the produce lock's release, which used to live in its own class.</b> The read lock is acquired once
 * per {@link PollContextInternal}, so exactly one release is owed however many records that context carries - and
 * the acquire/store/release steps are spread across four classes, so a test of any one of them has to explain the
 * whole lifecycle. Explaining it twice, in two classes, is what let the two halves drift; the merge is the cohesive
 * answer until the mechanism gets a single owner
 * ({@code docs/inflight/core-produce-lock-lifecycle-has-no-owner.md}).
 * <p>
 * Both release tests below used to fail <em>silently</em>. The release ran from two places against the one lock and
 * every failure was swallowed: {@link ProducerManager.ProducingLock#unlock()} logs only <em>after</em> the unlock, so
 * a throwing release left no trace, and the worker's {@link java.util.concurrent.Future} that carries the exception
 * is read by nothing in main. Counting acquires against releases in the log therefore reported a clean 1:1 while
 * every second release was blowing up.
 *
 * @author Antony Stubbs
 * @see ProducerManager
 * @see AbstractParallelEoSStreamProcessor#cleanUpContext for the single sanctioned release point
 * @see bz.stub.parallelconsumer.integrationTests.TransactionTimeoutsTest for integration tests checking timeout
 *         behaiviour
 */
@Tag("transactions")
@Tag("confluentinc#355")
@Timeout(60)
@Slf4j
class ProducerManagerTest {

    ParallelConsumerOptions<String, String> opts;

    PCModuleTestEnv module;

    ModelUtils mu;

    ProducerManager<String, String> producerManager;

    /**
     * Default settings
     */
    @BeforeEach
    void setup() {
        setup(ParallelConsumerOptions.<String, String>builder()
                .commitMode(PERIODIC_TRANSACTIONAL_PRODUCER)
                // 10s (was 2s): 2s is too tight on a CI JVM under PIT instrumentation.
                .commitLockAcquisitionTimeout(ofSeconds(10)));
    }

    // This class doesn't extend AbstractParallelEoSStreamProcessorTestBase, so
    // nothing else resets Awaitility between tests. Not closing pc here on purpose:
    // PCModuleTestEnv.withHandDrivenProcessor overrides close() as a no-op so each test manages its own pc
    // lifecycle explicitly (by design, to inspect mid-commit state).
    @AfterEach
    void tearDown() {
        Awaitility.reset();
    }

    private void setup(ParallelConsumerOptions.ParallelConsumerOptionsBuilder<String, String> optionsBuilder) {
        setup(optionsBuilder, true);
    }

    /**
     * @param alwaysTimeToCommit most tests here want a commit attempt on every control-loop pass. The produce-lock
     *                          release tests do NOT - they drive the loop by hand to observe one context's lock, and
     *                          a commit on every pass would be a second thing moving while they measure.
     */
    private void setup(ParallelConsumerOptions.ParallelConsumerOptionsBuilder<String, String> optionsBuilder,
                       boolean alwaysTimeToCommit) {
        opts = optionsBuilder.build();

        module = PCModuleTestEnv.withHandDrivenProcessor(opts, alwaysTimeToCommit);

        mu = new ModelUtils(module);

        producerManager = module.producerManager();
    }



    /**
     * @see ProduceLockHandover#acquireInto - which owns why this must not be released inside the user function
     */
    private void acquireProduceLockInto(PollContextInternal<String, String> context) {
        ProduceLockHandover.acquireInto(producerManager, context);
    }

    /**
     * The guard from {@code docs/plans/2026-08-03-001} §11: the produce lock must still be owned by the context
     * when the user function returns, because that ownership is what defers release to
     * {@link AbstractParallelEoSStreamProcessor#cleanUpContext}. Reintroduce hand-managed
     * unlocking in a user function and this fails deterministically, instead of coming back as a flake that also
     * takes the whole PIT mutation lane down with it.
     */
    private static void assertProduceLockStillOwnedByContext(PollContextInternal<String, String> context) {
        Truth.assertWithMessage("produce lock must still be owned by the context when the user "
                        + "function returns, so release is deferred to cleanUpContext")
                .that(context.getProducingLock().isPresent())
                .isTrue();
    }

    /**
     * Cannot send a record during a tx commit
     */
    @SneakyThrows
    @Test
    @ProvesClaim(TransactionalClaim.PROCESSING_BLOCKED_DURING_COMMIT)
    void sendingGetsLockedInTx() {
        assertThat(producerManager).isNotTransactionCommittingInProgress();

        // should send fine, futures should finish
        var produceReadLock = producerManager.beginProducing(mock(PollContextInternal.class));
        produceOneRecord();

        // acquire work should block
        var blockedCommit = new BlockedThreadAsserter();
        blockedCommit.assertFunctionBlocks(() -> {
            // commit sequence
            try {
                producerManager.preAcquireOffsetsToCommit();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            // releases the commit lock that was acquired
            producerManager.postCommit();
        });

        // pretend to finish producing records, give the lock back
        log.debug("Unlocking produce lock...");
        producerManager.finishProducing(produceReadLock); // triggers commit lock to become acquired as the produce lock is now released

        log.debug("Waiting for commit lock to release...");
        blockedCommit.awaitReturnFully();

        // start actual commit - acquire commit lock
        producerManager.preAcquireOffsetsToCommit();

        //
        assertThat(producerManager).isTransactionCommittingInProgress();

        // try to send more records, which will block as tx in process
        // Thread should be sleeping/blocked and not have returned
        var blockedRecordSenderReturned = new BlockedThreadAsserter();
        blockedRecordSenderReturned.assertFunctionBlocks(() -> {
            log.debug("Starting sending records - will block due to open commit");
            ProducerManager<String, String>.ProducingLock produceLock = null;
            try {
                produceLock = producerManager.beginProducing(mock(PollContextInternal.class));
            } catch (TimeoutException e) {
                throw new RuntimeException(e);
            }
            log.debug("Then after released by finishing tx, complete the producing");
            producerManager.finishProducing(produceLock);
        });


        // pretend to finish tx
        producerManager.postCommit();

        //
        assertThat(producerManager).isNotTransactionCommittingInProgress();

        //
        await("blocked sends should only now complete").until(blockedRecordSenderReturned::functionHasCompleted);
    }

    private List<ParallelConsumer.Tuple<ProducerRecord<String, String>, Future<RecordMetadata>>> produceOneRecord() {
        return producerManager.produceMessages(makeRecord());
    }

    private List<ProducerRecord<String, String>> makeRecord() {
        return mu.createProducerRecords("topic", 1);
    }

    /**
     * Make sure transaction get started lazy - only when a record is sent, not proactively
     */
    @SneakyThrows
    @Test
    void txOnlyStartedUponMessageSend() {
        assertThat(producerManager).isNotTransactionCommittingInProgress();
        assertThat(producerManager).stateIs(INIT);

        assertWithMessage("Transaction is started as not open")
                .that(producerManager)
                .transactionNotOpen();

        {
            var produceLock = producerManager.beginProducing(mock(PollContextInternal.class));

            {
                var notBlockedSends = produceOneRecord();
            }

            assertThat(producerManager).stateIs(BEGIN);
            assertThat(producerManager).transactionOpen();

            {
                var notBlockedSends = produceOneRecord();
            }

            producerManager.finishProducing(produceLock);
        }

        producerManager.preAcquireOffsetsToCommit();

        assertThat(producerManager).isTransactionCommittingInProgress();

        producerManager.commitOffsets(UniMaps.of(), new ConsumerGroupMetadata(""));

        assertThat(producerManager).isTransactionCommittingInProgress();

        producerManager.postCommit();

        assertThat(producerManager).isNotTransactionCommittingInProgress();

        //
        assertWithMessage("A new transaction hasn't been opened")
                .that(producerManager)
                .transactionNotOpen();

        // do another round of producing and check state
        {
            var producingLock = producerManager.beginProducing(mock(PollContextInternal.class));
            assertThat(producerManager).transactionNotOpen();
            produceOneRecord();
            assertThat(producerManager).transactionOpen();
            producerManager.finishProducing(producingLock);
            assertThat(producerManager).transactionOpen();
            producerManager.preAcquireOffsetsToCommit();
            assertThat(producerManager).transactionOpen();
            producerManager.commitOffsets(UniMaps.of(), new ConsumerGroupMetadata(""));
            assertThat(producerManager).transactionNotOpen();
            assertThat(producerManager).stateIs(COMMIT);
        }
    }

    @SneakyThrows
    @Test
    @ProvesClaim(TransactionalClaim.NO_PRODUCE_WITHOUT_ITS_OFFSET)
    void producedRecordsCantBeInTransactionWithoutItsOffsetDirect() {
        // custom settings
        setup(ParallelConsumerOptions.<String, String>builder()
                .commitMode(PERIODIC_TRANSACTIONAL_PRODUCER));

        try (var pc = module.pc()) {
            pc.subscribe(UniLists.of(mu.getTopic()));
            pc.onPartitionsAssigned(mu.getPartitions());
            pc.setState(State.RUNNING);

            // "send" one record
            EpochAndRecordsMap<String, String> freshWork = mu.createFreshWork();
            pc.registerWork(freshWork);

            assertThat(producerManager).getProducerTransactionLock().isNotWriteLocked();


            var offset1Mutex = new CountDownLatch(1);
            var blockedOn1 = new AtomicBoolean(false);
            Function<PollContextInternal<String, String>, List<Object>> userFunc = context -> {
                acquireProduceLockInto(context);
                log.info(context.toString());
                if (context.offset() == 1) {
                    log.debug("Blocking on {}", 1);
                    blockedOn1.set(true);
                    LatchTestUtils.awaitLatch(offset1Mutex);
                }

                // use real user function wrap
                module.producerWrap().send(mock(ProducerRecord.class), (a, b) -> {
                });

                assertProduceLockStillOwnedByContext(context);

                return UniLists.of();
            };


            assertThat(producerManager).getProducerTransactionLock().isNotWriteLocked();


            // won't block because offset 0 goes through
            // distributes first work
            pc.controlLoop(userFunc, o -> {
            });


            // change to TM?
            assertThat(producerManager).getProducerTransactionLock().isNotWriteLocked();

            //
            {
                var msg = "wait for first record to finish";
                log.debug(msg);
                // 20s (was default 10s): tight under PIT's instrumented JVM
                await(msg).atMost(ofSeconds(20))
                        .untilAsserted(() -> assertThat(pc.getWorkMailBox()).hasSize(1));
            }

            // send another record, register the work
            freshWork = mu.createFreshWork();
            pc.registerWork(freshWork);

            // will first try to commit - which will work fine, as there's no produce lock isn't held yet (off 0 goes through fine)
            // then it will get the work, distributes it
            // will then return
            // -- in the worker thread - will trigger the block and hold the produce lock
            pc.controlLoop(userFunc, o -> {
            });

            //
            assertThat(producerManager).getProducerTransactionLock().isNotWriteLocked();

            // blocks, as offset 1 is blocked sending and so cannot acquire commit lock
            var msg = "Ensure expected produce lock is now held by blocked worker thread";
            log.debug(msg);
            await(msg).atMost(ofSeconds(20)).untilTrue(blockedOn1);


            var commitBlocks = new BlockedThreadAsserter();
            // unblock 1 as unblocking function, and make sure that makes us return
            commitBlocks.assertUnblocksAfter(() -> {
                log.debug("Running control loop which should block until offset 1 is released by finishing produce");
                try {
                    pc.controlLoop(userFunc, o -> {
                    });
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }, () -> {
                log.debug("Unblocking offset processing offset1Mutex...");
                offset1Mutex.countDown();
            });

            //
            await().atMost(ofSeconds(20))
                    .untilAsserted(() -> Truth.assertWithMessage("commit should now have unlocked and returned")
                            .that(commitBlocks.functionHasCompleted())
                            .isTrue());


            final int nextExpectedOffset = 2; // as only first of two work completed
            {
                var producer = module.producerWrap();
                Mockito.verify(producer, description("Both offsets are represented in base commit"))
                        .sendOffsetsToTransaction(UniMaps.of(mu.getPartition(), new OffsetAndMetadata(nextExpectedOffset, "")), mu.consumerGroupMeta());

                Mockito.verify(producer, times(2)
                                .description("Should send twice, as it blocks the commit lock until it finishes, so offsets get taken only after"))
                        .send(any(), any());

            }
        }
    }

    /**
     * The offset whose worker is deliberately parked while holding the produce lock, so that the controller's
     * commit attempt overlaps it. Offset 0 is driven through cleanly first - see {@code isDirty} below.
     */
    private static final int OFFSET_HELD_BY_PARKED_WORKER = 1;

    /**
     * C9 - "The system must prevent records from being produced to the brokers whose source consumer record
     * offsets has not been included in this transaction."
     * <p>
     * The existing {@link #producedRecordsCantBeInTransactionWithoutItsOffsetDirect} shows the <em>outcome</em>
     * (both offsets end up in the commit). This one shows the <em>mechanism</em>, by stamping a global sequence at
     * three points and asserting their order:
     * <ol>
     *     <li>the parked worker's {@code send} returns,</li>
     *     <li>its {@link WorkContainer} reaches the controller's mailbox, and</li>
     *     <li>the controller is granted the commit lock.</li>
     * </ol>
     * The claim is that (3) comes after (2), not merely after (1) - "the record was sent" is not the release
     * condition, "the completion is queued for the controller" is. That distinction is the entire content of the
     * flake resolved in {@code docs/plans/2026-08-03-001} §11, and an assertion that only checked (3) after (1)
     * would have passed throughout that bug.
     * <p>
     * <strong>The overlap is constructed, not hoped for.</strong> Two latches, as in
     * {@code TransactionTimeoutsTest#commitTimeout}: one says a worker is holding the produce lock, one says the
     * controller has entered commit-lock acquisition. The second is gated on the first so the happy-path commits
     * cannot open it, and it is asserted to have reached zero - a missed overlap then names itself instead of
     * presenting as a generic timeout.
     * <p>
     * <strong>Non-vacuity (KTD10).</strong> {@code maybeAcquireCommitLock} is gated on {@code wm.isDirty()}, whose
     * only setter is {@code PartitionState#onSuccess}. If no record has succeeded, no commit is attempted at all
     * and every blocking assertion below passes because nothing happened. Offset 0 is therefore driven all the way
     * to success first, and the dirty flag is asserted before the overlap is created.
     */
    @SneakyThrows
    @Test
    @ProvesClaim(TransactionalClaim.NO_PRODUCE_WITHOUT_ITS_OFFSET)
    void commitLockIsGrantedOnlyAfterTheProducedWorkReachesTheMailbox() {
        var sequence = new AtomicInteger();
        var sendReturnedAt = new AtomicInteger();
        var workReachedMailboxAt = new AtomicInteger();
        var commitLockGrantedAt = new AtomicInteger();

        // -1 means sendOffsetsToTransaction was never reached at all, which is a different failure from
        // "reached it while a produce lock was still held" - see the assertions.
        var produceLockHoldersAtSendOffsets = new AtomicInteger(-1);
        var commitLockOwnedAtSendOffsets = new AtomicBoolean();

        var workerHoldsProduceLock = new CountDownLatch(1);
        var controllerEnteredCommitLockAcquisition = new CountDownLatch(1);
        var releaseParkedWorker = new CountDownLatch(1);

        opts = ParallelConsumerOptions.<String, String>builder()
                .commitMode(PERIODIC_TRANSACTIONAL_PRODUCER)
                // 30s: the controller is parked on the commit lock by design here, for as long as this test
                // takes to prove it is parked
                .commitLockAcquisitionTimeout(ofSeconds(30))
                .build();

        module = new PCModuleTestEnv(opts) {

            private ProducerManager<String, String> observedProducerManager;

            /**
             * KTD9 - the instance MUST be cached. {@link PCModule#producerManager()} memoises, and a fresh
             * {@link ProducerManager} per call would carry a fresh {@code producerTransactionLock}: the produce
             * lock taken by the worker and the commit lock taken by the controller would stop being two sides of
             * one lock, and this test would prove nothing.
             */
            @Override
            protected ProducerManager<String, String> producerManager() {
                if (observedProducerManager == null) {
                    observedProducerManager = new ProducerManager<>(producerWrap(), consumerManager(), workManager(), options()) {
                        @Override
                        protected void preAcquireOffsetsToCommit() throws TimeoutException, InterruptedException {
                            // Gate: the happy-path commits of offset 0 reach here too, and must not be mistaken
                            // for the attempt under test.
                            boolean attemptUnderTest = workerHoldsProduceLock.getCount() == 0;
                            if (attemptUnderTest) {
                                controllerEnteredCommitLockAcquisition.countDown();
                            }
                            super.preAcquireOffsetsToCommit();
                            if (attemptUnderTest) {
                                // compareAndSet: retrieveOffsetsAndCommit calls this a second time within the
                                // same commit, which returns immediately as the lock is already ours. Only the
                                // first, blocking, acquisition is the moment being timed.
                                commitLockGrantedAt.compareAndSet(0, sequence.incrementAndGet());
                            }
                        }
                    };
                }
                return observedProducerManager;
            }

            @Override
            protected AbstractParallelEoSStreamProcessor<String, String> pc() {
                if (parallelEoSStreamProcessor == null) {
                    parallelEoSStreamProcessor = new ParallelEoSStreamProcessor<String, String>(options(), this) {
                        @Override
                        protected boolean isTimeToCommitNow() {
                            return true;
                        }

                        @Override
                        public void close() {
                            // see producedRecordsCantBeInTransactionWithoutItsOffsetDirect - closing would NPE
                            // on the mocked Producer's absent transaction manager
                        }

                        @Override
                        protected void addToMailbox(PollContextInternal<String, String> pollContext, WorkContainer<String, String> wc) {
                            // Stamped BEFORE super, deliberately: super only enqueues the work - the produce lock
                            // is released later, in runUserFunction's finally, by
                            // AbstractParallelEoSStreamProcessor#cleanUpContext. Stamping before the enqueue is
                            // therefore the conservative choice: the produce lock is provably still held at this
                            // instant, so the stamp cannot be later than the moment the work became visible to the
                            // controller, and the ordering assertion cannot pass on a technicality of when the
                            // stamp was taken.
                            if (wc.offset() == OFFSET_HELD_BY_PARKED_WORKER) {
                                workReachedMailboxAt.compareAndSet(0, sequence.incrementAndGet());
                            }
                            super.addToMailbox(pollContext, wc);
                        }
                    };
                }
                return parallelEoSStreamProcessor;
            }
        };
        mu = new ModelUtils(module);
        producerManager = module.producerManager();

        // Snapshot the lock state at the exact moment the offsets are handed to the transaction. Asserted after
        // the run rather than here, so a failure surfaces as a test failure rather than as an exception thrown
        // inside PC's control thread.
        Mockito.doAnswer(invocation -> {
            var lock = producerManager.getProducerTransactionLock();
            produceLockHoldersAtSendOffsets.set(lock.getReadLockCount());
            commitLockOwnedAtSendOffsets.set(lock.isWriteLockedByCurrentThread());
            return invocation.callRealMethod();
        }).when(module.producerWrap()).sendOffsetsToTransaction(anyMap(), any(ConsumerGroupMetadata.class));

        try (var pc = module.pc()) {
            pc.subscribe(UniLists.of(mu.getTopic()));
            pc.onPartitionsAssigned(mu.getPartitions());
            pc.setState(State.RUNNING);

            Function<PollContextInternal<String, String>, List<Object>> userFunc = context -> {
                acquireProduceLockInto(context);

                boolean parkedWorker = context.offset() == OFFSET_HELD_BY_PARKED_WORKER;
                if (parkedWorker) {
                    workerHoldsProduceLock.countDown();
                    // Held until the test has established that the controller is blocked on the commit lock.
                    LatchTestUtils.awaitLatch(releaseParkedWorker, 60);
                }

                producerManager.produceMessages(makeRecord());

                if (parkedWorker) {
                    sendReturnedAt.compareAndSet(0, sequence.incrementAndGet());
                }

                assertProduceLockStillOwnedByContext(context);

                return UniLists.of();
            };

            // offset 0 - clean pass, so that a success exists to make the state dirty (KTD10)
            pc.registerWork(mu.createFreshWork());
            pc.controlLoop(userFunc, ignore -> {
            });

            await("offset 0's completion reaches the mailbox")
                    .atMost(ofSeconds(20))
                    .untilAsserted(() -> assertThat(pc.getWorkMailBox()).hasSize(1));

            // offset 1 - the one that will park while holding the produce lock
            pc.registerWork(mu.createFreshWork());
            // drains offset 0 (marking the partition dirty) and distributes offset 1
            pc.controlLoop(userFunc, ignore -> {
            });

            Truth.assertWithMessage("offset 0's success must have marked the state dirty, or maybeAcquireCommitLock "
                    + "attempts no commit at all and every assertion below is vacuous (KTD10)")
                    .that(pc.getWm().isDirty())
                    .isTrue();

            LatchTestUtils.awaitLatch(workerHoldsProduceLock, 30);
            assertThat(producerManager).hasProduceLockHoldCount(1);
            assertThat(producerManager).commitLockNotHeld();

            // The commit attempt must not get in while the parked worker holds the produce lock.
            var commitAttempt = new BlockedThreadAsserter();
            commitAttempt.assertFunctionBlocks(() -> {
                try {
                    pc.controlLoop(userFunc, ignore -> {
                    });
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }, ofSeconds(1));

            Truth.assertWithMessage("the controller must have entered commit-lock acquisition while the worker held "
                    + "the produce lock - without that overlap the block above is just an idle control loop")
                    .that(controllerEnteredCommitLockAcquisition.getCount())
                    .isEqualTo(0);
            Truth.assertWithMessage("and it must not have been granted the lock yet")
                    .that(commitLockGrantedAt.get())
                    .isEqualTo(0);
            assertThat(producerManager).hasProduceLockHoldCount(1);

            releaseParkedWorker.countDown();
            commitAttempt.awaitReturnFully();

            //
            Truth.assertWithMessage("the parked worker's send must have returned")
                    .that(sendReturnedAt.get())
                    .isGreaterThan(0);
            Truth.assertWithMessage("the work reaches the controller's mailbox only after its record was sent")
                    .that(workReachedMailboxAt.get())
                    .isGreaterThan(sendReturnedAt.get());
            Truth.assertWithMessage("the commit lock is granted only AFTER the work reached the controller's mailbox - "
                    + "not merely after the record's send returned, which is the window that let a commit "
                    + "exclude an offset whose record was already produced")
                    .that(commitLockGrantedAt.get())
                    .isGreaterThan(workReachedMailboxAt.get());

            //
            Truth.assertWithMessage("no offsets were sent to the transaction at all - there was no commit to observe, "
                    + "so the lock-state assertions below would be vacuous")
                    .that(produceLockHoldersAtSendOffsets.get())
                    .isNotEqualTo(-1);
            Truth.assertWithMessage("no produce lock may still be held when the offset map is handed to the "
                    + "transaction - a held one means a record could still be produced into it")
                    .that(produceLockHoldersAtSendOffsets.get())
                    .isEqualTo(0);
            Truth.assertWithMessage("and the committing thread must own the commit lock while it does so")
                    .that(commitLockOwnedAtSendOffsets.get())
                    .isTrue();

            final int nextExpectedOffset = 2; // both records completed, so the commit moves past both
            verify(module.producerWrap(), description("both offsets are in the transaction - the second only "
                    + "because the commit waited for its produce lock"))
                    .sendOffsetsToTransaction(UniMaps.of(mu.getPartition(), new OffsetAndMetadata(nextExpectedOffset, "")),
                            mu.consumerGroupMeta());
        }
    }

    /**
     * C10 - "This periodically slows down record production during this phase, by the time needed to commit the
     * transaction."
     * <p>
     * The commit lock is taken and held on a dedicated thread, because
     * {@link java.util.concurrent.locks.ReentrantReadWriteLock} only lets the acquiring thread release it -
     * {@code releaseCommitLock} throws otherwise. The unblocking function therefore only tells that thread to let
     * go. {@link BlockedThreadAsserter#assertUnblocksAfter} then asserts both halves of the claim in one run:
     * production was still parked when the commit released, and returned only after it.
     * <p>
     * That is a <em>causal</em> assertion, not a duration one, since astubbs#265 rewrote the helper - and for this
     * claim that is the stronger of the two. C10 is about production being blocked <em>by</em> the commit; a
     * wall-clock minimum would also pass for production that was merely slow for unrelated reasons. The return
     * budget is left at the helper's 20s default deliberately: it is sized for PIT's instrumented JVM, where a
     * tighter one flakes.
     * <p>
     * Non-vacuity: the commit lock is asserted to actually be held before the produce attempt starts, and the
     * acquired {@link ProducerManager.ProducingLock} is asserted non-null afterwards - {@code assertUnblocksAfter}
     * logs and swallows exceptions from the blocked function, so without that a produce-lock timeout would read as
     * a pass.
     */
    @SneakyThrows
    @Test
    @ProvesClaim(TransactionalClaim.PROCESSING_BLOCKED_DURING_COMMIT)
    void producingIsBlockedForTheDurationOfTheCommitAndResumesOnRelease() {
        var commitLockHeld = new CountDownLatch(1);
        var releaseCommitLock = new CountDownLatch(1);
        var committerFailure = new AtomicReference<Throwable>();

        var committer = new Thread(() -> {
            try {
                producerManager.preAcquireOffsetsToCommit();
                commitLockHeld.countDown();
                LatchTestUtils.awaitLatch(releaseCommitLock, 60);
                producerManager.postCommit();
            } catch (Throwable t) {
                committerFailure.set(t);
                commitLockHeld.countDown();
            }
        }, "test-committer");
        committer.start();

        LatchTestUtils.awaitLatch(commitLockHeld, 30);
        Truth.assertWithMessage("the commit lock must genuinely be held, or producing has nothing to block on")
                .that(committerFailure.get())
                .isNull();
        assertThat(producerManager).isTransactionCommittingInProgress();
        assertThat(producerManager).hasNoProduceLockHolders();

        // Acquired AND released inside the blocked function, on the one thread, because a read lock's hold count
        // is per-thread: ReadLock#unlock decrements the calling thread's count and throws
        // IllegalMonitorStateException at zero. astubbs#265 moved the blocked function off the calling thread and
        // onto its own, so releasing from the test thread here would fail on a lock the test thread never held -
        // which it did, as "Need to call #beginProducing first" out of ensureProduceStarted.
        var acquired = new AtomicReference<ProducerManager<String, String>.ProducingLock>();
        var released = new AtomicBoolean();
        var producing = new BlockedThreadAsserter();
        producing.assertUnblocksAfter(
                () -> {
                    try {
                        var lock = producerManager.beginProducing(mock(PollContextInternal.class));
                        acquired.set(lock);
                        producerManager.finishProducing(lock);
                        released.set(true);
                    } catch (TimeoutException e) {
                        throw new RuntimeException(e);
                    }
                },
                releaseCommitLock::countDown);

        Truth.assertWithMessage("producing must have resumed by acquiring the produce lock once the commit released "
                + "it - a swallowed timeout would otherwise look identical to a pass")
                .that(acquired.get())
                .isNotNull();
        Truth.assertWithMessage("and it must have given the produce lock back - finishProducing runs its "
                + "ensureProduceStarted check, so reaching here proves the lock was genuinely held by the "
                + "thread that took it")
                .that(released.get())
                .isTrue();
        assertThat(producerManager).hasNoProduceLockHolders();

        committer.join(ofSeconds(20).toMillis());
        Truth.assertWithMessage("the committing thread must have finished cleanly")
                .that(committerFailure.get())
                .isNull();
        assertThat(producerManager).commitLockNotHeld();
        assertThat(producerManager).isNotTransactionCommittingInProgress();
    }

    /**
     * {@code abortTransaction} throws on a fenced or poisoned producer - the very state a close after a fatal
     * producer error is in. {@code close} must still close the Producer, or it leaks its IO thread, sockets and
     * buffers while {@code doClose}'s finally marks the instance CLOSED.
     */
    @Test
    void closeStillClosesTheProducerWhenTheAbortThrows() {
        var producerWrap = module.producerWrap();
        doReturn(false).when(producerWrap).isTransactionReady(); // a transaction is open, so close takes the abort path
        doThrow(new ProducerFencedException("fenced")).when(producerWrap).abortTransaction();
        var closeTimeout = ofSeconds(1);

        producerManager.close(closeTimeout); // must not throw

        verify(producerWrap).abortTransaction();
        verify(producerWrap, description("the Producer must still be closed when the abort throws"))
                .close(closeTimeout);
        assertThat(producerManager).stateIs(CLOSE);
        assertThat(producerManager).commitLockNotHeld();
    }

    /**
     * When close cannot take the commit lock it deliberately aborts anyway - and must not then release a lock it
     * never took: {@code releaseCommitLock} throws {@code IllegalStateException("Not held be me")}, which escaped
     * past {@code closeProducer} and leaked the Producer in exactly the same way as the abort above.
     */
    @SneakyThrows
    @Test
    void closeStillClosesTheProducerWhenTheCommitLockCannotBeAcquired() {
        setup(ParallelConsumerOptions.<String, String>builder()
                .commitMode(PERIODIC_TRANSACTIONAL_PRODUCER)
                // The produce lock is held for the whole of close() below, so tryLock can never succeed early: this
                // is waited out in FULL on every passing run, not a failure-side ceiling. Kept short for that
                // reason, and it cannot flake short - the read lock is provably held before close() is called, so
                // the write lock is unavailable however fast or slow the machine is.
                .commitLockAcquisitionTimeout(ofMillis(50)));
        var producerWrap = module.producerWrap();
        doReturn(false).when(producerWrap).isTransactionReady();

        // hold the produce (read) lock on another thread so the commit (write) lock cannot be taken
        var produceLockHeld = new CountDownLatch(1);
        var releaseProduceLock = new CountDownLatch(1);
        var producing = new Thread(() -> {
            try {
                var lock = producerManager.beginProducing(mock(PollContextInternal.class));
                produceLockHeld.countDown();
                LatchTestUtils.awaitLatch(releaseProduceLock);
                producerManager.finishProducing(lock);
            } catch (TimeoutException e) {
                throw new RuntimeException(e);
            }
        }, "produce-lock-holder");
        producing.start();
        try {
            LatchTestUtils.awaitLatch(produceLockHeld);
            var closeTimeout = ofSeconds(1);

            producerManager.close(closeTimeout); // must not throw

            verify(producerWrap, description("the Producer must still be closed when the commit lock timed out"))
                    .close(closeTimeout);
            assertThat(producerManager).stateIs(CLOSE);
        } finally {
            releaseProduceLock.countDown();
            producing.join(ofSeconds(10).toMillis());
            // Asserted, not just joined: a join that times out returns normally, so a hanging
            // finishProducing would leak the thread and leave this test passing on everything else.
            Truth.assertWithMessage("the produce-lock holder must have given the read lock back and finished")
                    .that(producing.isAlive())
                    .isFalse();
        }
    }

    @Test
    void testOptions() {
        assertThrows(IllegalArgumentException.class, () ->
                ParallelConsumerOptions.builder()
                        .consumer(mock(Consumer.class))
                        .commitMode(PERIODIC_TRANSACTIONAL_PRODUCER)
                        .build()
                        .validate());


        assertThrows(IllegalArgumentException.class, () ->
                ParallelConsumerOptions.builder()
                        .consumer(mock(Consumer.class))
                        .allowEagerProcessingDuringTransactionCommit(true)
                        .build()
                        .validate());
    }

    /**
     * A single record's produce lock was released twice - once from the mailbox hook and again from
     * {@link AbstractParallelEoSStreamProcessor#runUserFunction}'s {@code finally} - and the second
     * {@link java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock#unlock()} threw
     * {@link IllegalMonitorStateException} on a thread holding zero read locks. That happened on every transactional
     * produce, unnoticed, because nothing reads the worker's future.
     */
    @SneakyThrows
    @Test
    void produceLockIsReleasedExactlyOnce() {
        setupForHandDrivenRelease(1);

        try (var pc = module.pc()) {
            startWork(pc, 1);

            pc.controlLoop(lockAcquiringUserFunction(), o -> {
            });

            var results = awaitWorkResults(pc, 1);

            // released twice -> the worker future carries IllegalMonitorStateException
            Truth.assertWithMessage("the worker must not fail: the produce lock is released exactly once per context")
                    .that(results.get(0).getFuture().get(20, TimeUnit.SECONDS))
                    .isNotNull();

            // released zero times -> the read lock is still held and no transaction could ever commit
            assertWithMessage("the produce lock must actually be given back, or commits would block forever")
                    .that(producerManager)
                    .hasNoProduceLockHolders();

            Truth.assertWithMessage("record processed successfully")
                    .that(results.get(0).isUserFunctionSucceeded())
                    .isTrue();
        }
    }

    /**
     * The batch case was a live defect, not just silent noise. One lock is acquired for the whole context, but the
     * release used to run per record: the second record found zero read holds, so
     * {@code ProducerManager#ensureProduceStarted} threw {@code "Need to call #beginProducing first"}, which landed in
     * {@link AbstractParallelEoSStreamProcessor#runUserFunction}'s failure handler and marked a record the user
     * function had just processed successfully as FAILED - on every batch.
     * <p>
     * Whether that marking goes on to cause a redelivery is a race with the controller draining the mailbox, so it is
     * covered end to end and at volume by {@code TransactionalBatchProduceTest}. This test asserts the state that race
     * reads from, which is deterministic.
     */
    @SneakyThrows
    @Test
    void wholeBatchSucceedsWhenProducing() {
        setupForHandDrivenRelease(2);

        try (var pc = module.pc()) {
            startWork(pc, 2);

            pc.controlLoop(lockAcquiringUserFunction(), o -> {
            });

            var results = awaitWorkResults(pc, 2);

            Truth.assertWithMessage("both records of the batch returned a result")
                    .that(results)
                    .hasSize(2);

            for (var wc : results) {
                Truth.assertWithMessage("offset %s: the user function succeeded, so it must not be recorded as failed",
                                wc.offset())
                        .that(wc.getNumberOfFailedAttempts())
                        .isEqualTo(0);
                Truth.assertWithMessage("offset %s: the user function succeeded", wc.offset())
                        .that(wc.isUserFunctionSucceeded())
                        .isTrue();
            }

            assertWithMessage("the produce lock must actually be given back, or commits would block forever")
                    .that(producerManager)
                    .hasNoProduceLockHolders();
        }
    }

    /**
     * The catch in {@link AbstractParallelEoSStreamProcessor#cleanUpContext} is not defensive decoration. It runs in
     * {@code runUserFunction}'s {@code finally}, and <b>an exception thrown from a finally REPLACES the one
     * propagating out of the catch above it</b> - plain try/finally does not attach it as suppressed the way
     * try-with-resources would. A throwing release would therefore destroy the user function's real failure and
     * report a produce-lock error in its place, which is strictly worse: by then the lock is unrecoverable either
     * way, because {@code takeProducingLock} has already claimed it out of the context.
     * <p>
     * The release is made to fail with main's own mechanism rather than a mock: the context is handed a real lock
     * and the read hold is then dropped behind its back, so {@code cleanUpContext} still finds a lock to return and
     * {@code finishProducing}'s {@code ensureProduceStarted} check throws on zero holds. One term changed.
     * <p>
     * Found missing by codecov, which reported the catch block as the only uncovered lines this PR adds - a guard
     * whose whole contract is "does not rethrow", with nothing proving it.
     */
    @SneakyThrows
    @Test
    void aFailedProduceLockReleaseMustNotReplaceTheUserFunctionsFailure() {
        setupForHandDrivenRelease(1);

        try (var pc = module.pc()) {
            startWork(pc, 1);

            var usersFailure = new RuntimeException("the user function's own failure");

            pc.controlLoop(context -> {
                ProduceLockHandover.acquireInto(producerManager, context);
                // drop the hold while leaving the lock in the context, so the release is attempted and fails
                context.getProducingLock().get().unlock();
                throw usersFailure;
            }, o -> {
            });

            var results = awaitWorkResults(pc, 1);

            Truth.assertWithMessage("the record must be recorded as failed")
                    .that(results.get(0).isUserFunctionSucceeded())
                    .isFalse();

            // THE ASSERTION THAT CARRIES IT, and it has to be the FUTURE rather than the recorded failure.
            // runUserFunction marks the container failed inside its catch - onUserFunctionFailure runs BEFORE the
            // finally - so lastFailureReason already holds the user's exception whether or not cleanUpContext
            // rethrows. An earlier version of this test asserted that, passed, and passed just as happily with the
            // catch replaced by a rethrow: vacuous. What the catch actually protects is the exception `throw e`
            // sends out of runUserFunction, which is what lands in the worker's Future.
            var thrown = assertThrows(java.util.concurrent.ExecutionException.class,
                    () -> results.get(0).getFuture().get(20, TimeUnit.SECONDS));
            Truth.assertWithMessage("THE POINT: the worker's future must carry the USER's failure. A throw out of "
                            + "cleanUpContext's finally replaces the exception on its way out, so the real cause "
                            + "would be gone and a produce-lock error reported in its place")
                    .that(thrown.getCause())
                    .isSameInstanceAs(usersFailure);
        }
    }

    /**
     * The release tests drive the control loop by hand, so they want a module that does not commit on every pass.
     *
     * @param batchSize 1 for the single-record release, 2 for the per-record-release defect
     */
    private void setupForHandDrivenRelease(int batchSize) {
        setup(ParallelConsumerOptions.<String, String>builder()
                .commitMode(PERIODIC_TRANSACTIONAL_PRODUCER)
                // ModelUtils gives every record the same key, so KEY ordering would admit only one at a time
                .ordering(UNORDERED)
                .batchSize(batchSize)
                // 10s: 2s is too tight on a CI JVM under PIT instrumentation
                .commitLockAcquisitionTimeout(ofSeconds(10)), false);
    }

    /**
     * Hands the produce lock to the real context and leaves it there, exactly as {@link ParallelEoSStreamProcessor}'s
     * produce wrapper does - releasing it is the framework's job, not the user function's.
     */
    private Function<PollContextInternal<String, String>, List<Object>> lockAcquiringUserFunction() {
        return context -> {
            ProduceLockHandover.acquireInto(producerManager, context);
            module.producerWrap().send(mock(ProducerRecord.class), (a, b) -> {
            });
            return UniLists.of();
        };
    }

    private void startWork(AbstractParallelEoSStreamProcessor<String, String> pc, int recordCount) {
        pc.subscribe(UniLists.of(mu.getTopic()));
        pc.onPartitionsAssigned(mu.getPartitions());
        pc.setState(State.RUNNING);

        for (int i = 0; i < recordCount; i++) {
            pc.registerWork(mu.createFreshWork());
        }
    }

    /**
     * Reads the work results back off the controller's inbound queue, which is where a completed record lands.
     */
    private List<WorkContainer<String, String>> awaitWorkResults(AbstractParallelEoSStreamProcessor<String, String> pc,
                                                                 int expected) {
        var seen = new ArrayList<WorkContainer<String, String>>();
        await("work results reach the controller's inbound queue")
                .atMost(ofSeconds(20))
                .untilAsserted(() -> {
                    for (var msg : pc.getWorkMailBox()) {
                        var wc = msg.getWorkContainer();
                        if (wc != null && !seen.contains(wc)) {
                            seen.add(wc);
                        }
                    }
                    Truth.assertThat(seen).hasSize(expected);
                });
        return seen;
    }

    /**
     * The offset whose success is produced into the open transaction and then deliberately left sitting in the
     * controller's mailbox, undrained, at the moment the revoke-time commit runs.
     */
    private static final int OFFSET_PRODUCED_BUT_UNDRAINED = 1;

    /**
     * C9, on the <em>revoke</em> path rather than the control loop's.
     * <p>
     * <b>Why this is not already covered by {@link #commitLockIsGrantedOnlyAfterTheProducedWorkReachesTheMailbox}.</b>
     * That test proves the first half of the contract - the commit lock is granted only after every produced
     * record's success has reached the mailbox. The second half is that somebody then <em>drains</em> that mailbox
     * before collecting offsets, because {@link bz.stub.parallelconsumer.state.PartitionState#onSuccess} - the only
     * caller that marks a partition dirty on a success - is reachable from
     * {@link AbstractParallelEoSStreamProcessor#processWorkCompleteMailBox} and nowhere else in main. The control
     * loop does both halves, in that order. The revoke path does the first and not the second.
     * <p>
     * <b>The lock discipline is what makes this reachable, not what prevents it.</b> The intuition that the produce
     * lock might leave the mailbox empty of produced work whenever a commit can begin is backwards:
     * {@link AbstractParallelEoSStreamProcessor#cleanUpContext} is the single release point and runs strictly after
     * the batch is mailboxed, so a returned produce lock is a <em>guarantee</em> that the work is already queued.
     * A commit granted the write lock therefore always has that work in front of it - drained by the control loop,
     * ignored by the revoke path.
     *
     * <b>Deliberately NOT annotated {@link ProvesClaim}, though it is the test that refuted
     * {@link TransactionalClaim#NO_PRODUCE_WITHOUT_ITS_OFFSET}.</b>
     * {@code TransactionalClaimCoverageTest#claimProofsMustLiveWhereATestRunnerWillFindThem} rejects a
     * {@code @ProvesClaim} method the gating lanes exclude, and it is right to: a claim whose only proof is
     * quarantined is covered on paper only. C9 keeps its enforced coverage from the two control-loop proofs that
     * really run, and this test is cited from the claim's own recorded reason and from its tracking note instead.
     *
     * @see #aRevokeTimeCommitIncludesThatOffsetWhenTheMailboxIsDrainedFirst for the control arm - one term changed
     */
    @SneakyThrows
    @Test
    @Quarantined(
            reason = "Deterministic, 5/5 - not a flake. The revoke-path commit "
                    + "(AbstractParallelEoSStreamProcessor#tryCommitOffsetsOnRevoke) collects offsets without "
                    + "first draining the controller's work mailbox, and PartitionState#onSuccess - the only "
                    + "thing that marks a partition dirty on a success - is reachable from "
                    + "processWorkCompleteMailBox and nowhere else in main. So a revoke-time commit publishes a "
                    + "transaction containing a record whose source offset it omits. Observed: the commit sends "
                    + "offset 1 where 2 is required. The sibling control arm "
                    + "#aRevokeTimeCommitIncludesThatOffsetWhenTheMailboxIsDrainedFirst passes with the drain "
                    + "inserted and nothing else changed, so the drain is the responsible term. Master-state, "
                    + "and older than the branch that found it. NOT fixed here on purpose: the revoke callback "
                    + "runs on the broker-poll thread and the mailbox drain mutates control-thread-confined "
                    + "WorkManager state, so the obvious one-line fix is the same cross-thread mutation that "
                    + "corrupted the out-for-processing counter in astubbs#29. The fix is a thread-ownership "
                    + "decision, not a line.",
            tracking = "docs/inflight/core-revoke-commit-skips-the-work-mailbox-drain.md")
    void aRevokeTimeCommitIncludesTheOffsetOfEveryRecordItAlreadyProduced() {
        var committed = offsetCommittedByARevoke(false);

        Truth.assertWithMessage("the revoke-time commit sent no offsets to the transaction at all, so there was no "
                        + "commit to observe and the assertion below would be vacuous")
                .that(committed.isPresent())
                .isTrue();
        Truth.assertWithMessage("offset " + OFFSET_PRODUCED_BUT_UNDRAINED + "'s record was produced into the very "
                        + "transaction this commit closed, so its source offset has to be committed by that same "
                        + "transaction. Committing the output without the input is exactly-once degrading to "
                        + "at-least-once: the next owner of the partition reprocesses that input and produces the "
                        + "output a second time")
                .that(committed.get().offset())
                .isEqualTo(OFFSET_PRODUCED_BUT_UNDRAINED + 1);
    }

    /**
     * The control arm for {@link #aRevokeTimeCommitIncludesTheOffsetOfEveryRecordItAlreadyProduced}.
     * <p>
     * Identical state, identical revoke, one term moved: the mailbox is drained immediately before the revoke
     * rather than not at all. Same magnitude, different position - so a difference in outcome is attributable to
     * the drain and not to added latency or to the revoke doing something else unusual.
     */
    @SneakyThrows
    @Test
    void aRevokeTimeCommitIncludesThatOffsetWhenTheMailboxIsDrainedFirst() {
        var committed = offsetCommittedByARevoke(true);

        Truth.assertWithMessage("the revoke-time commit sent no offsets to the transaction at all, so the control "
                        + "arm proved nothing")
                .that(committed.isPresent())
                .isTrue();
        Truth.assertWithMessage("with the mailbox drained first, the revoke-time commit must cover both offsets - "
                        + "if this arm does not hold, the drain is not the term the other arm turns on")
                .that(committed.get().offset())
                .isEqualTo(OFFSET_PRODUCED_BUT_UNDRAINED + 1);
    }

    /**
     * Builds the one state both arms measure, and returns the offset the revoke-time commit actually sent for our
     * partition.
     * <p>
     * At the moment {@code onPartitionsRevoked} is called: offset 0 has been produced <em>and</em> drained, so the
     * partition is dirty and there is a commit to make at all (without it {@code collectCommitDataForDirtyPartitions}
     * returns nothing, no offsets are sent, and every assertion downstream is vacuous); offset 1 has been produced
     * into the same still-open transaction with its success queued but not drained; and no produce lock is held, so
     * the commit's write-lock acquisition cannot be what excludes it.
     *
     * @param drainTheMailboxFirst the single term under test
     * @return the {@link OffsetAndMetadata} sent for {@code mu.getPartition()}, or empty if no offsets were sent
     */
    @SneakyThrows
    private Optional<OffsetAndMetadata> offsetCommittedByARevoke(boolean drainTheMailboxFirst) {
        // alwaysTimeToCommit=false: this test decides when a commit happens. A commit on every control-loop pass
        // would drain and commit the very mailbox entry the revoke path is supposed to be measured against.
        setup(ParallelConsumerOptions.<String, String>builder()
                .commitMode(PERIODIC_TRANSACTIONAL_PRODUCER), false);

        var offsetsSentToTransaction = new AtomicReference<Map<TopicPartition, OffsetAndMetadata>>();
        Mockito.doAnswer(invocation -> {
            offsetsSentToTransaction.set(invocation.getArgument(0));
            return invocation.callRealMethod();
        }).when(module.producerWrap()).sendOffsetsToTransaction(anyMap(), any(ConsumerGroupMetadata.class));

        try (var pc = module.pc()) {
            pc.subscribe(UniLists.of(mu.getTopic()));
            pc.onPartitionsAssigned(mu.getPartitions());
            pc.setState(State.RUNNING);

            Function<PollContextInternal<String, String>, List<Object>> userFunc = context -> {
                acquireProduceLockInto(context);
                producerManager.produceMessages(makeRecord());
                assertProduceLockStillOwnedByContext(context);
                return UniLists.of();
            };

            // offset 0 - distributed here, drained on the next pass, which is what makes the partition dirty
            pc.registerWork(mu.createFreshWork());
            pc.controlLoop(userFunc, ignore -> {
            });
            await("offset 0's completion reaches the mailbox")
                    .atMost(ofSeconds(20))
                    .untilAsserted(() -> assertThat(pc.getWorkMailBox()).hasSize(1));

            // this pass drains offset 0 (marking the partition dirty) and distributes offset 1
            pc.registerWork(mu.createFreshWork());
            pc.controlLoop(userFunc, ignore -> {
            });
            await("offset " + OFFSET_PRODUCED_BUT_UNDRAINED + "'s completion reaches the mailbox, where it stays")
                    .atMost(ofSeconds(20))
                    .untilAsserted(() -> assertThat(pc.getWorkMailBox()).hasSize(1));

            Truth.assertWithMessage("offset 0's success must have marked the state dirty, or the revoke attempts no "
                            + "commit at all and both arms are vacuous")
                    .that(pc.getWm().isDirty())
                    .isTrue();
            Truth.assertWithMessage("no produce lock may still be held - otherwise the revoke's write-lock "
                            + "acquisition, not the missing drain, is what the arms would be measuring")
                    .that(producerManager.getProducerTransactionLock().getReadLockCount())
                    .isEqualTo(0);

            if (drainTheMailboxFirst) {
                pc.processWorkCompleteMailBox(Duration.ZERO);
            }

            pc.onPartitionsRevoked(mu.getPartitions());
        }

        var sent = offsetsSentToTransaction.get();
        return sent == null ? Optional.empty() : Optional.ofNullable(sent.get(mu.getPartition()));
    }
}
