package bz.stub.parallelconsumer.internal;

/*-
 * Copyright (C) 2020-2023 Confluent, Inc.
 * Modifications Copyright (C) 2026 Antony Stubbs and contributors
 */

import com.google.common.truth.Truth;
import bz.stub.parallelconsumer.CommitFailureContext;
import bz.stub.parallelconsumer.internal.utils.BlockedThreadAsserter;
import bz.stub.parallelconsumer.internal.utils.LatchTestUtils;
import bz.stub.parallelconsumer.ParallelConsumer;
import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.ParallelEoSStreamProcessor;
import bz.stub.parallelconsumer.PollContextInternal;
import bz.stub.parallelconsumer.Quarantined;
import bz.stub.parallelconsumer.state.ModelUtils;
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
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import static bz.stub.parallelconsumer.ManagedTruth.assertThat;
import static bz.stub.parallelconsumer.ManagedTruth.assertWithMessage;
import static bz.stub.parallelconsumer.ParallelConsumerOptions.CommitMode.PERIODIC_TRANSACTIONAL_PRODUCER;
import static bz.stub.parallelconsumer.internal.utils.ThreadUtils.sleepOrFail;
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
     * Cannot send a record during a tx commit
     */
    @SneakyThrows
    @Test
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
    @Quarantined(
            reason = "Timing-precision failure in the shared BlockedThreadAsserter#assertUnblocksAfter helper, "
                    + "not in this test's own assertions. The unblocker is scheduled BEFORE the elapsed clock "
                    + "starts, so the measured window begins later than the delay it is compared against and is "
                    + "systematically SHORT by however long arming the scheduler takes. `isAtLeast(unblocksAfter)` "
                    + "then fails by a millisecond or two whenever the machine is busy enough to widen that gap. "
                    + "Seen as `getElapsed() expected to be at least PT20S but was PT19.998S` - 2ms short on a 20s "
                    + "bound - on a PR whose diff contained no Java at all.",
            tracking = "docs/inflight/test-untracked-ci-flakes.md",
            fixedBy = "astubbs#262",
            flapping = true)
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
                // Acquire against the REAL context and hand the lock to it, exactly as
                // ParallelEoSStreamProcessor#pollAndProduce does. This is load-bearing, not tidying: the
                // lock must not be released until the work has reached the controller's inbound queue -
                // see WorkContainer#onPostAddToMailBox, which states the invariant and is the sanctioned
                // release point. Releasing it here, inside the user function, opens a window in which the
                // controller can take the commit lock, drain a mailbox that does not yet contain this
                // work, and commit an offset one behind - which is what made this test flaky (~1 in 6).
                try {
                    context.setProducingLock(Optional.of(producerManager.beginProducing(context)));
                } catch (TimeoutException e) {
                    throw new RuntimeException(e);
                }
                log.info(context.toString());
                if (context.offset() == 1) {
                    log.debug("Blocking on {}", 1);
                    blockedOn1.set(true);
                    LatchTestUtils.awaitLatch(offset1Mutex);
                }

                // use real user function wrap
                module.producerWrap().send(mock(ProducerRecord.class), (a, b) -> {
                });

                // Guard against this test regressing to hand-managing the lock. The lock must still be
                // owned by the context when the user function returns - that ownership is what defers
                // release to WorkContainer#onPostAddToMailBox. Reintroduce a manual unlock here and this
                // fails deterministically, instead of coming back as a ~1-in-6 flake that also takes the
                // whole PIT mutation lane down with it.
                Truth.assertWithMessage("produce lock must still be owned by the context when the user "
                                + "function returns, so release is deferred to onPostAddToMailBox")
                        .that(context.getProducingLock().isPresent())
                        .isTrue();

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
     * The commit-failure seam's rebalance lane, transactional half (astubbs#317): a transactional commit whose
     * {@code offsetCommitTimeout} budget exhausts DURING partition revocation is a DEFERRAL - the callback carries
     * on to truncation, the {@link bz.stub.parallelconsumer.CommitFailureHandler} is NOT consulted (there is no
     * waiter inside a rebalance callback to act on a decision), and the instance records no failure. The dirty
     * offsets are the new assignee's to resolve by reprocessing; the producer's in-flight transaction is
     * complete-else-abort recovered at the head of the next commit cycle, exactly as after a scheduled-lane
     * exhaustion ({@link ProducerManagerCommitBudgetTest} pins that recovery).
     * <p>
     * Red before the seam's revocation-time catch: the budget exhaustion escaped
     * {@code onPartitionsRevoked} wrapped as "onPartitionsRevoked event error", failing the whole rebalance.
     * <p>
     * The work is driven straight into the {@link bz.stub.parallelconsumer.state.WorkManager} rather than through
     * {@code controlLoop} - this harness's pc commits on every loop pass ({@code isTimeToCommitNow} is pinned
     * true), so a loop-driven variant would commit the offsets before the revocation ever saw them dirty.
     */
    @Test
    void revocationTimeBudgetExhaustionDefersInsteadOfConsultingTheHandlerOrFailingTheRebalance() throws Exception {
        var consultedContexts = new ConcurrentLinkedQueue<CommitFailureContext>();
        setup(ParallelConsumerOptions.<String, String>builder()
                .commitMode(PERIODIC_TRANSACTIONAL_PRODUCER)
                .commitLockAcquisitionTimeout(ofSeconds(10))
                .offsetCommitTimeout(Duration.ofMillis(500))
                // SHUT_DOWN so a wrongly-consulted handler is loud (a closed pc), not just a non-empty queue
                .commitFailureHandler(context -> {
                    consultedContexts.add(context);
                    return bz.stub.parallelconsumer.CommitFailureHandler.CommitFailureDecision.SHUT_DOWN;
                }));

        try (var pc = module.pc()) {
            pc.subscribe(UniLists.of(mu.getTopic()));
            pc.onPartitionsAssigned(mu.getPartitions());
            pc.setState(State.RUNNING);

            // one record's work, completed successfully, straight into the work manager: dirty offsets waiting
            var wm = module.workManager();
            wm.registerWork(mu.createFreshWork());
            var work = wm.getWorkIfAvailable();
            Truth.assertThat(work).hasSize(1);
            for (var workContainer : work) {
                workContainer.onUserFunctionSuccess();
                wm.handleFutureResult(workContainer);
            }
            Truth.assertThat(wm.isDirty()).isTrue();

            // the transactional commit fails retriably for longer than the whole budget
            doReturn(true).when(module.producerWrap()).isTransactionCompleting();
            doAnswer(invocation -> {
                sleepOrFail(Duration.ofMillis(100), "Interrupted while mocking a slow transactional commit");
                throw new org.apache.kafka.common.errors.TimeoutException("Broker unreachable (mocking)");
            }).when(module.producerWrap()).commitTransaction();

            // the revocation-time commit exhausts its budget - and the callback must complete anyway
            pc.onPartitionsRevoked(mu.getPartitions());

            Truth.assertWithMessage("a revocation-time exhaustion has no waiter, so the handler must not be consulted")
                    .that(consultedContexts).isEmpty();
            Truth.assertThat(pc.getFailureCause()).isNull();
            // the budget loop genuinely ran and exhausted, rather than the commit never being attempted
            verify(module.producerWrap(), atLeast(2)).commitTransaction();
        }
    }

}