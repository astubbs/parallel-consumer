package io.confluent.parallelconsumer.internal;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import com.google.common.truth.Truth;
import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.ParallelEoSStreamProcessor;
import io.confluent.parallelconsumer.PollContextInternal;
import io.confluent.parallelconsumer.state.ModelUtils;
import io.confluent.parallelconsumer.state.WorkContainer;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import pl.tlinkowski.unij.api.UniLists;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

import static io.confluent.parallelconsumer.ParallelConsumerOptions.CommitMode.PERIODIC_TRANSACTIONAL_PRODUCER;
import static io.confluent.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.UNORDERED;
import static java.time.Duration.ofSeconds;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.mock;

/**
 * The produce read lock is acquired once per {@link PollContextInternal}, so exactly one release is owed - no matter
 * how many records that context carries.
 * <p>
 * Both of these used to fail silently. The release ran from two places against the one lock, and every failure was
 * swallowed: {@link ProducerManager.ProducingLock#unlock()} logs only <em>after</em> the unlock, so a throwing release
 * left no trace in the log, and the worker's {@link java.util.concurrent.Future} that carries the exception is read by
 * nothing in main. Counting acquires against releases in the log therefore reported a clean 1:1 while every second
 * release was blowing up.
 *
 * @author Antony Stubbs
 * @see AbstractParallelEoSStreamProcessor#cleanUpContext
 */
@Tag("transactions")
@Timeout(60)
@Slf4j
class ProduceLockReleaseTest {

    PCModuleTestEnv module;

    ModelUtils mu;

    ProducerManager<String, String> producerManager;

    @AfterEach
    void tearDown() {
        Awaitility.reset();
    }

    private void setup(int batchSize) {
        var opts = ParallelConsumerOptions.<String, String>builder()
                .commitMode(PERIODIC_TRANSACTIONAL_PRODUCER)
                // ModelUtils gives every record the same key, so KEY ordering would admit only one at a time
                .ordering(UNORDERED)
                .batchSize(batchSize)
                // 10s: 2s is too tight on a CI JVM under PIT instrumentation
                .commitLockAcquisitionTimeout(ofSeconds(10))
                .build();

        module = new PCModuleTestEnv(opts) {
            @Override
            protected AbstractParallelEoSStreamProcessor<String, String> pc() {
                if (parallelEoSStreamProcessor == null) {
                    parallelEoSStreamProcessor = new ParallelEoSStreamProcessor<>(options(), this) {
                        @Override
                        public void close() {
                            // this test drives the control loop by hand, so it owns the pc lifecycle
                        }
                    };
                }
                return parallelEoSStreamProcessor;
            }
        };
        mu = new ModelUtils(module);
        producerManager = module.producerManager();
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
        setup(1);

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
            Truth.assertWithMessage("the produce lock must actually be given back, or commits would block forever")
                    .that(producerManager.getProducerTransactionLock().getReadLockCount())
                    .isEqualTo(0);

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
        setup(2);

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

            Truth.assertWithMessage("the produce lock must actually be given back, or commits would block forever")
                    .that(producerManager.getProducerTransactionLock().getReadLockCount())
                    .isEqualTo(0);
        }
    }

    /**
     * Hands the produce lock to the real context and leaves it there, exactly as {@link ParallelEoSStreamProcessor}'s
     * produce wrapper does - releasing it is the framework's job, not the user function's.
     */
    private Function<PollContextInternal<String, String>, List<Object>> lockAcquiringUserFunction() {
        return context -> {
            try {
                context.setProducingLock(Optional.of(producerManager.beginProducing(context)));
            } catch (TimeoutException e) {
                throw new RuntimeException(e);
            }
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
}
