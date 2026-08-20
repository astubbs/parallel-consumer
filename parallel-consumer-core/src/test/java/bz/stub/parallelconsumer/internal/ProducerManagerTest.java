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
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static bz.stub.parallelconsumer.ManagedTruth.assertThat;
import static bz.stub.parallelconsumer.ManagedTruth.assertWithMessage;
import static bz.stub.parallelconsumer.ParallelConsumerOptions.CommitMode.PERIODIC_TRANSACTIONAL_PRODUCER;
import static bz.stub.parallelconsumer.internal.ProducerWrapper.ProducerState.*;
import static java.time.Duration.ofSeconds;
import static java.util.Collections.emptyList;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * Covers transaction state systems, and their blocking behaiviour towards sending records and the reverse.
 *
 * @author Antony Stubbs
 * @see ProducerManager
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
    // buildModule() overrides close() as a no-op so each test manages its own pc
    // lifecycle explicitly (by design, to inspect mid-commit state).
    @AfterEach
    void tearDown() {
        Awaitility.reset();
    }

    private void setup(ParallelConsumerOptions.ParallelConsumerOptionsBuilder<String, String> optionsBuilder) {
        opts = optionsBuilder.build();

        module = buildModule(opts);

        mu = new ModelUtils(module);

        producerManager = module.producerManager();
    }

    private PCModuleTestEnv buildModule(ParallelConsumerOptions<String, String> opts) {
        return new PCModuleTestEnv(opts) {
            @Override
            protected AbstractParallelEoSStreamProcessor<String, String> pc() {
                if (parallelEoSStreamProcessor == null) {
                    AbstractParallelEoSStreamProcessor<String, String> raw = super.pc();
                    parallelEoSStreamProcessor = spy(raw);

                    parallelEoSStreamProcessor = new ParallelEoSStreamProcessor<>(options(), this) {
                        @Override
                        protected boolean isTimeToCommitNow() {
                            return true;
                        }

                        @Override
                        public void close() {
                        }
                    };
                }
                return parallelEoSStreamProcessor;
            }
        };
    }


    /**
     * Acquires the produce lock against the <em>real</em> context and hands it to that context, exactly as
     * {@link bz.stub.parallelconsumer.ParallelEoSStreamProcessor#pollAndProduce} does.
     * <p>
     * This is load-bearing, not tidying: the lock must not be released until the work has reached the
     * controller's inbound queue - see {@link AbstractParallelEoSStreamProcessor#cleanUpContext},
     * which states the invariant and is the sanctioned release point. Releasing it inside the user function opens
     * a window in which the controller can take the commit lock, drain a mailbox that does not yet contain this
     * work, and commit an offset one behind - the ~1-in-6 flake written up in
     * {@code docs/plans/2026-08-03-001-investigate-transactional-commit-flake.md} §11.
     */
    private void acquireProduceLockInto(PollContextInternal<String, String> context) {
        try {
            context.setProducingLock(Optional.of(producerManager.beginProducing(context)));
        } catch (TimeoutException e) {
            throw new RuntimeException(e);
        }
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

}