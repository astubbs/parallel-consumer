package io.confluent.parallelconsumer.internal;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import com.google.common.truth.Truth;
import io.confluent.csid.utils.LatchTestUtils;
import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.ParallelEoSStreamProcessor;
import io.confluent.parallelconsumer.PollContextInternal;
import io.confluent.parallelconsumer.ProvesClaim;
import io.confluent.parallelconsumer.TransactionalClaim;
import io.confluent.parallelconsumer.state.ModelUtils;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.Mockito;
import pl.tlinkowski.unij.api.UniLists;
import pl.tlinkowski.unij.api.UniMaps;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

import static io.confluent.parallelconsumer.ManagedTruth.assertThat;
import static io.confluent.parallelconsumer.ParallelConsumerOptions.CommitMode.PERIODIC_CONSUMER_ASYNCHRONOUS;
import static io.confluent.parallelconsumer.ParallelConsumerOptions.CommitMode.PERIODIC_TRANSACTIONAL_PRODUCER;
import static io.confluent.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.UNORDERED;
import static io.confluent.parallelconsumer.internal.ProducerWrapper.ProducerState.COMMIT;
import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.description;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Proves the two {@link ParallelConsumerOptions.CommitMode#PERIODIC_TRANSACTIONAL_PRODUCER} claims that are
 * about <em>batching</em> rather than about visibility - and so can be settled in the unit lane, without a
 * broker (KTD8).
 * <ul>
 *     <li>{@link TransactionalClaim#BULK_SHARED_TRANSACTION} - records produced by different worker threads
 *     inside one commit interval all land in one transaction, and the next interval opens a new one.</li>
 *     <li>{@link TransactionalClaim#COMMIT_INTERVAL_AUTO_REDUCED} - choosing transactional mode silently
 *     reduces the commit interval default from 5s to 100ms, but only when the user did not set one.</li>
 * </ul>
 * <p>
 * <strong>Why the control loop is driven by hand.</strong> Like {@link ProducerManagerTest}, this test overrides
 * {@link AbstractParallelEoSStreamProcessor#isTimeToCommitNow()} to always return true and makes
 * {@link ParallelEoSStreamProcessor#close()} a no-op, then calls
 * {@link AbstractParallelEoSStreamProcessor#controlLoop} one iteration at a time. A commit interval is therefore
 * expressed as "the iterations between two commits", which is what makes "these four records were in the same
 * interval" an assertable fact rather than a timing hope.
 * <p>
 * <strong>Trap - do not close the {@link ProducerManager} here.</strong> {@link PCModuleTestEnv} injects a
 * {@code Mockito.mock(Producer.class)}, not a {@link org.apache.kafka.clients.producer.MockProducer}, so
 * {@link ProducerWrapper}'s reflective transaction-manager handles are never initialised and
 * {@code isTransactionReady()} throws {@link NullPointerException}. The overridden no-op {@code close()} is what
 * keeps the try-with-resources below safe.
 *
 * @author Antony Stubbs
 * @see ProducerManagerTest
 * @see TransactionalClaim
 */
@Tag("transactions")
@Timeout(120)
@Slf4j
class TransactionalBulkCommitTest {

    private static final String INPUT_TOPIC = "input-topic";

    private static final String OUTPUT_TOPIC = "output-topic";

    /**
     * Four records, spread over two partitions, so the offsets committed in the single transaction cannot be
     * satisfied by one partition's high-water mark alone - the {@code sendOffsetsToTransaction} map has to carry
     * both partitions.
     */
    private static final int RECORDS_IN_FIRST_INTERVAL = 4;

    private static final TopicPartition PARTITION_ZERO = new TopicPartition(INPUT_TOPIC, 0);

    private static final TopicPartition PARTITION_ONE = new TopicPartition(INPUT_TOPIC, 1);

    PCModuleTestEnv module;

    ModelUtils mu;

    ProducerManager<String, String> producerManager;

    /**
     * Nothing closes {@code pc} on purpose - see the class javadoc. Awaitility is reset because this class, like
     * {@link ProducerManagerTest}, does not extend a base class that would do it.
     */
    @AfterEach
    void tearDown() {
        Awaitility.reset();
    }

    /**
     * Mirrors {@link ProducerManagerTest}'s module builder. The {@link ProducerManager} is fetched once and
     * reused (KTD9): {@link PCModule#producerManager()} memoises, and a second instance would carry a second
     * {@code producerTransactionLock}, at which point the produce lock taken by the worker threads and the commit
     * lock taken by the control loop would no longer be two sides of the same lock.
     */
    private void setup(ParallelConsumerOptions.ParallelConsumerOptionsBuilder<String, String> optionsBuilder) {
        module = new PCModuleTestEnv(optionsBuilder.build()) {
            @Override
            protected AbstractParallelEoSStreamProcessor<String, String> pc() {
                if (parallelEoSStreamProcessor == null) {
                    parallelEoSStreamProcessor = new ParallelEoSStreamProcessor<>(options(), this) {
                        @Override
                        protected boolean isTimeToCommitNow() {
                            return true;
                        }

                        @Override
                        public void close() {
                            // see class javadoc - closing would NPE on the mocked Producer's absent tx manager
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
     * C1 - "Messages sent in parallel by different workers get added to the same transaction block".
     * <p>
     * Four records are held on a latch until all four worker threads are simultaneously inside the user function
     * holding the produce lock, which is what makes "in parallel by different workers" a fact of the run rather
     * than an assumption. They are then released to produce. One {@code beginTransaction} and one
     * {@code sendOffsetsToTransaction} carrying every partition's offsets prove they shared one transaction; the
     * second round proves the following interval opens a fresh one instead of reusing the committed producer
     * state.
     * <p>
     * The commit does not happen where you might expect. {@code maybeAcquireCommitLock} is gated on
     * {@code wm.isDirty()}, and the only thing that sets dirty is a record <em>succeeding</em> in
     * {@code PartitionState#onSuccess}, which happens while the mailbox is drained. So the iteration that drains
     * the four results cannot also commit them - the commit lands on the iteration after (KTD10). Getting this
     * wrong does not fail loudly; it silently commits nothing and leaves the verifications below vacuous, which
     * is why the offset expectations are exact rather than "at least".
     */
    @SneakyThrows
    @Test
    @ProvesClaim(TransactionalClaim.BULK_SHARED_TRANSACTION)
    void recordsFromDifferentWorkersInOneIntervalShareOneTransaction() {
        setup(ParallelConsumerOptions.<String, String>builder()
                .commitMode(PERIODIC_TRANSACTIONAL_PRODUCER)
                // so all four records can be in flight at once - the claim is about parallel workers
                .ordering(UNORDERED)
                // 30s: the control loop holds the commit lock while four workers are parked on a latch
                .commitLockAcquisitionTimeout(ofSeconds(30)));

        try (var pc = module.pc()) {
            pc.subscribe(UniLists.of(INPUT_TOPIC));
            pc.onPartitionsAssigned(UniLists.of(PARTITION_ZERO, PARTITION_ONE));
            pc.setState(State.RUNNING);

            var allWorkersStarted = new CountDownLatch(RECORDS_IN_FIRST_INTERVAL);
            var releaseWorkers = new CountDownLatch(1);
            Set<String> workerThreadNames = ConcurrentHashMap.newKeySet();

            Function<PollContextInternal<String, String>, List<Object>> userFunc = context -> {
                // Acquire against the real context and hand the lock to it, exactly as
                // ParallelEoSStreamProcessor#processAndProduceResults does. Release is then deferred to
                // AbstractParallelEoSStreamProcessor#cleanUpContext, so the offset cannot be committed before its produced
                // record is inside the transaction.
                try {
                    context.setProducingLock(Optional.of(producerManager.beginProducing(context)));
                } catch (TimeoutException e) {
                    throw new RuntimeException(e);
                }

                workerThreadNames.add(Thread.currentThread().getName());
                allWorkersStarted.countDown();
                // Park until every worker of this interval is here, so the sends below are genuinely
                // concurrent. Already open on the second interval's single record, which passes straight
                // through.
                LatchTestUtils.awaitLatch(releaseWorkers, 60);

                producerManager.produceMessages(UniLists.of(
                        new ProducerRecord<>(OUTPUT_TOPIC, "out-key-" + context.offset(), "out-value")));

                return UniLists.of();
            };

            // ---- interval one -------------------------------------------------------------------------

            pc.registerWork(workOf(
                    consumerRecord(PARTITION_ZERO, 0), consumerRecord(PARTITION_ZERO, 1),
                    consumerRecord(PARTITION_ONE, 0), consumerRecord(PARTITION_ONE, 1)));

            // ingests the polled records and distributes all four to the worker pool
            pc.controlLoop(userFunc, ignore -> {
            });

            LatchTestUtils.awaitLatch(allWorkersStarted, 60);
            Truth.assertWithMessage("all four records must be in flight simultaneously, on distinct worker "
                            + "threads - otherwise 'in parallel by different workers' is not what was tested")
                    .that(workerThreadNames)
                    .hasSize(RECORDS_IN_FIRST_INTERVAL);
            Truth.assertWithMessage("the workers hold the produce lock, so no commit can have slipped in")
                    .that(producerManager.isTransactionCommittingInProgress())
                    .isFalse();

            releaseWorkers.countDown();

            await("all four results reach the controller's mailbox")
                    .atMost(ofSeconds(60))
                    .untilAsserted(() -> assertThat(pc.getWorkMailBox()).hasSize(RECORDS_IN_FIRST_INTERVAL));

            // drains the mailbox, marking the partitions dirty - cannot commit in the same iteration (KTD10)
            pc.controlLoop(userFunc, ignore -> {
            });
            Truth.assertWithMessage("the four successes must have marked state dirty, or the next iteration "
                            + "commits nothing and every verification below is vacuous")
                    .that(pc.getWm().isDirty())
                    .isTrue();

            // commits
            pc.controlLoop(userFunc, ignore -> {
            });

            var producer = module.producerWrap();
            Map<TopicPartition, OffsetAndMetadata> bothPartitions = UniMaps.of(
                    PARTITION_ZERO, new OffsetAndMetadata(2, ""),
                    PARTITION_ONE, new OffsetAndMetadata(2, ""));

            verify(producer, times(RECORDS_IN_FIRST_INTERVAL)
                    .description("each worker produced its own record"))
                    .send(any(), any());
            verify(producer, times(1)
                    .description("all four workers shared ONE transaction block, not one each"))
                    .beginTransaction();
            verify(producer, times(1)
                    .description("one sendOffsetsToTransaction carrying every source offset of the interval"))
                    .sendOffsetsToTransaction(bothPartitions, mu.consumerGroupMeta());
            verify(producer, times(1)
                    .description("the shared transaction is committed once"))
                    .commitTransaction();

            // ---- interval two -------------------------------------------------------------------------

            assertThat(producerManager).stateIs(COMMIT);
            assertThat(producerManager).transactionNotOpen();
            Truth.assertWithMessage("dirty state must have been cleared by the commit, otherwise the next "
                            + "iteration would re-commit interval one and this arm would prove nothing")
                    .that(pc.getWm().isDirty())
                    .isFalse();

            pc.registerWork(workOf(consumerRecord(PARTITION_ZERO, 2)));

            pc.controlLoop(userFunc, ignore -> {
            });

            await("the second interval's result reaches the mailbox")
                    .atMost(ofSeconds(60))
                    .untilAsserted(() -> assertThat(pc.getWorkMailBox()).hasSize(1));

            pc.controlLoop(userFunc, ignore -> {
            });
            pc.controlLoop(userFunc, ignore -> {
            });

            verify(producer, times(2)
                    .description("the second interval opened a NEW transaction rather than reusing the "
                            + "committed one"))
                    .beginTransaction();
            verify(producer, times(2)
                    .description("and committed it separately"))
                    .commitTransaction();
            verify(producer, times(1)
                    .description("the second transaction carries only the second interval's offset - the "
                            + "first interval's offsets were already committed"))
                    .sendOffsetsToTransaction(UniMaps.of(PARTITION_ZERO, new OffsetAndMetadata(3, "")),
                            mu.consumerGroupMeta());
            verify(producer, times(RECORDS_IN_FIRST_INTERVAL + 1)
                    .description("five records produced in total across the two intervals"))
                    .send(any(), any());
        }
    }

    /**
     * C5, first direction - "gets automatically reduced from the default of 5 seconds to 100ms".
     * <p>
     * Asserted against the literal 100ms rather than
     * {@link ParallelConsumerOptions#DEFAULT_COMMIT_INTERVAL_FOR_TRANSACTIONS}, because the documented sentence
     * names the number: comparing the resolved value to the constant would still pass if the constant were
     * changed and the javadoc left behind.
     * <p>
     * {@link ParallelConsumerOptions#validate()} is the resolution point - it is what
     * {@link AbstractParallelEoSStreamProcessor}'s constructor calls - so the assertion is on the options object
     * after validation, not on what the builder was handed.
     */
    @Test
    @ProvesClaim(TransactionalClaim.COMMIT_INTERVAL_AUTO_REDUCED)
    void transactionalModeWithNoExplicitCommitIntervalResolvesTo100ms() {
        var options = ParallelConsumerOptions.<String, String>builder()
                .consumer(Mockito.mock(Consumer.class))
                .producer(Mockito.mock(Producer.class))
                .commitMode(PERIODIC_TRANSACTIONAL_PRODUCER)
                .build();

        Truth.assertWithMessage("before resolution the builder default is still Kafka's 5s - so the assertion "
                        + "below is about what validate() changed, not about the builder input")
                .that(options.getCommitInterval())
                .isEqualTo(ofSeconds(5));

        options.validate();

        Truth.assertWithMessage("transactional mode with an unset commit interval must resolve to 100ms")
                .that(options.getCommitInterval())
                .isEqualTo(ofMillis(100));
    }

    /**
     * C5, second direction - the reduction must not overwrite a user's explicit choice.
     */
    @Test
    @ProvesClaim(TransactionalClaim.COMMIT_INTERVAL_AUTO_REDUCED)
    void transactionalModeLeavesAnExplicitCommitIntervalAlone() {
        var options = ParallelConsumerOptions.<String, String>builder()
                .consumer(Mockito.mock(Consumer.class))
                .producer(Mockito.mock(Producer.class))
                .commitMode(PERIODIC_TRANSACTIONAL_PRODUCER)
                .commitInterval(ofSeconds(2))
                .build();

        options.validate();

        Truth.assertWithMessage("an explicitly configured commit interval must survive transactional mode's "
                        + "auto-reduction")
                .that(options.getCommitInterval())
                .isEqualTo(ofSeconds(2));
    }

    /**
     * C5, control - the reduction is scoped to transactional mode. Without this, a global default change to
     * 100ms would satisfy both arms above.
     */
    @Test
    @ProvesClaim(TransactionalClaim.COMMIT_INTERVAL_AUTO_REDUCED)
    void nonTransactionalModeKeepsTheFiveSecondDefault() {
        var options = ParallelConsumerOptions.<String, String>builder()
                .consumer(Mockito.mock(Consumer.class))
                .commitMode(PERIODIC_CONSUMER_ASYNCHRONOUS)
                .build();

        options.validate();

        Truth.assertWithMessage("only transactional mode reduces the interval - Kafka's 5s default stands "
                        + "everywhere else")
                .that(options.getCommitInterval())
                .isEqualTo(ofSeconds(5));
    }

    private ConsumerRecord<String, String> consumerRecord(TopicPartition tp, long offset) {
        return new ConsumerRecord<>(tp.topic(), tp.partition(), offset,
                "key-" + tp.partition() + "-" + offset, "value-" + offset);
    }

    /**
     * Builds the polled-records message the controller ingests. Records are grouped by their own partition, so
     * one call can span both partitions of an interval.
     */
    @SafeVarargs
    private final EpochAndRecordsMap<String, String> workOf(ConsumerRecord<String, String>... records) {
        Map<TopicPartition, List<ConsumerRecord<String, String>>> byPartition = new LinkedHashMap<>();
        for (ConsumerRecord<String, String> record : records) {
            byPartition.computeIfAbsent(new TopicPartition(record.topic(), record.partition()),
                    ignore -> new ArrayList<>()).add(record);
        }
        return new EpochAndRecordsMap<>(new ConsumerRecords<>(byPartition), module.workManager().getPm());
    }

}
