package bz.stub.parallelconsumer.internal;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.ParallelConsumerOptions.CommitMode;
import bz.stub.parallelconsumer.ParallelEoSStreamProcessor;
import bz.stub.parallelconsumer.ProducerFactory;
import bz.stub.parallelconsumer.internal.utils.LongPollingMockConsumer;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.LoggerFactory;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InvalidPidMappingException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TransactionalIdAuthorizationException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.ArgumentCaptor;
import pl.tlinkowski.unij.api.UniLists;
import pl.tlinkowski.unij.api.UniMaps;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

/**
 * Recovery end to end, on the PC-built path, against producers a factory hands out: what happens after the broker
 * reports the producer invalid (R10-R15, R18, KTD4-KTD7). Every producer is a spied {@link MockProducer}, so a commit
 * or a send can be made to fail the way kafka-clients fails it, and the replacement is a fresh one the factory
 * returns. In this package so the processor's protected accessors and the manager's package-private pacing hooks are
 * reachable.
 */
@Slf4j
@Timeout(90)
class ProducerRecoveryTest {

    private static final String TOPIC = "in";
    private static final TopicPartition TP = new TopicPartition(TOPIC, 0);
    private static final String GROUP = "recovery-group";

    /** Every producer the factory built, in order; the last is the one PC is using. */
    private final List<MockProducer<String, String>> producers = new CopyOnWriteArrayList<>();
    private final List<Instant> factoryCallTimes = new CopyOnWriteArrayList<>();
    /** How many times the user function saw each offset. */
    private final Map<Long, AtomicInteger> seen = new ConcurrentHashMap<>();
    /** Applied to each producer as the factory builds it, keyed by build index (0 = the initial producer). */
    private final Map<Integer, Consumer<MockProducer<String, String>>> onBuild = new ConcurrentHashMap<>();
    private volatile Optional<CountDownLatch> holdFactoryUntil = Optional.empty();

    private LongPollingMockConsumer<String, String> consumer;
    private volatile ConsumerGroupMetadata groupMetadata = new ConsumerGroupMetadata(GROUP, 1, "member-1", Optional.empty());
    /** How many times PC's poll thread has read a generation other than the first - its cache refresh. */
    private final AtomicInteger refreshedMetadataReads = new AtomicInteger();
    private ParallelEoSStreamProcessor<String, String> pc;
    private volatile String derivedTransactionalId;
    private final SimpleMeterRegistry registry = new SimpleMeterRegistry();

    @AfterEach
    void tearDown() {
        holdFactoryUntil.ifPresent(CountDownLatch::countDown);
        if (pc != null && !pc.isClosedOrFailed()) {
            pc.closeDontDrainFirst(Duration.ofSeconds(10));
        }
    }

    private ProducerFactory<String, String> factory() {
        return config -> {
            factoryCallTimes.add(Instant.now());
            holdFactoryUntil.ifPresent(latch -> {
                try {
                    latch.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
            derivedTransactionalId = (String) config.get(ProducerConfig.TRANSACTIONAL_ID_CONFIG);
            MockProducer<String, String> producer = spy(new MockProducer<>(true, new StringSerializer(), new StringSerializer()));
            int index = producers.size();
            producers.add(producer);
            Consumer<MockProducer<String, String>> hook = onBuild.get(index);
            if (hook != null) {
                hook.accept(producer);
            }
            return producer;
        };
    }

    private ParallelConsumerOptions.ParallelConsumerOptionsBuilder<String, String> optionsBuilder() {
        consumer = spy(new LongPollingMockConsumer<>(OffsetResetStrategy.EARLIEST));
        doAnswer(ignored -> {
            ConsumerGroupMetadata current = groupMetadata;
            if (current.generationId() != 1) {
                refreshedMetadataReads.incrementAndGet();
            }
            return current;
        }).when(consumer).groupMetadata();
        return ParallelConsumerOptions.<String, String>builder()
                .consumer(consumer)
                .meterRegistry(registry)
                .producerConfig(UniMaps.of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "mock:9092"))
                .producerFactory(factory())
                .commitMode(CommitMode.PERIODIC_TRANSACTIONAL_PRODUCER)
                .commitInterval(Duration.ofMillis(100))
                .commitLockAcquisitionTimeout(Duration.ofSeconds(5))
                .produceLockAcquisitionTimeout(Duration.ofSeconds(1))
                .defaultMessageRetryDelay(Duration.ofMillis(100))
                .maxConcurrency(2);
    }

    private void start(ParallelConsumerOptions<String, String> options) {
        pc = new ParallelEoSStreamProcessor<>(options);
        pc.subscribe(UniLists.of(TOPIC));
        consumer.subscribeWithRebalanceAndAssignment(UniLists.of(TOPIC), 1);
        pc.pollAndProduceMany(context -> {
            seen.computeIfAbsent(context.offset(), ignored -> new AtomicInteger()).incrementAndGet();
            return UniLists.of(new ProducerRecord<>("out", context.key(), context.value()));
        });
    }

    private ProducerManager<String, String> producerManager() {
        return pc.getProducerManager().get();
    }

    private void addRecords(long fromOffset, long toOffsetInclusive) {
        for (long offset = fromOffset; offset <= toOffsetInclusive; offset++) {
            consumer.addRecord(new ConsumerRecord<>(TOPIC, 0, offset, "k" + offset, "v" + offset));
        }
    }

    /** Fences the producer at its first sendOffsetsToTransaction, the way a rebalance race under KIP-447 does. */
    private void fenceAtFirstCommit(MockProducer<String, String> producer) {
        doAnswer(invocation -> {
            producer.fenceProducer();
            // the generation moves on with the rebalance that fenced this producer
            groupMetadata = new ConsumerGroupMetadata(GROUP, groupMetadata.generationId() + 1, "member-1", Optional.empty());
            throw new ProducerFencedException("fenced at commit");
        }).when(producer).sendOffsetsToTransaction(anyMap(), any(ConsumerGroupMetadata.class));
    }

    /** Every send's future fails with the condition - the shape FutureRecordMetadata.valueOrError produces. */
    private static void failEverySendFromTheFuture(MockProducer<String, String> producer, RuntimeException condition) {
        doAnswer(invocation -> {
            var future = new CompletableFuture<RecordMetadata>();
            future.completeExceptionally(condition);
            return future;
        }).when(producer).send(any(ProducerRecord.class), any(Callback.class));
    }

    private static long highestCommittedOffset(MockProducer<String, String> producer) {
        return producer.consumerGroupOffsetsHistory().stream()
                .map(byGroup -> byGroup.get(GROUP))
                .filter(Objects::nonNull)
                .map(byTp -> byTp.get(TP))
                .filter(Objects::nonNull)
                .mapToLong(OffsetAndMetadata::offset)
                .max()
                .orElse(-1);
    }

    private void awaitCommittedThrough(MockProducer<String, String> producer, long nextOffset) {
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(highestCommittedOffset(producer)).isAtLeast(nextOffset));
    }

    private void awaitProducers(int count) {
        await().atMost(Duration.ofSeconds(30)).until(() -> producers.size() >= count);
    }

    /**
     * Covers AE1 (R8, R10, R11, R13, R14): fenced during offset commit - the instance keeps running, the aborted
     * transaction's offsets never reach the first producer, every record in it runs again, the replacement commits
     * them, and the commit that lands them carries the generation the consumer is at now, not the one the fence
     * happened under.
     */
    @Test
    void fencedDuringCommitRecoversReplaysTheAbortedWorkAndCommitsUnderTheLiveGeneration() {
        onBuild.put(0, this::fenceAtFirstCommit);
        start(optionsBuilder().build());
        addRecords(0, 4);

        awaitProducers(2);
        var replacement = producers.get(1);
        awaitCommittedThrough(replacement, 5);

        assertThat(pc.isClosedOrFailed()).isFalse();
        assertWithMessage("the fenced producer committed nothing").that(producers.get(0).consumerGroupOffsetsHistory()).isEmpty();
        @SuppressWarnings("unchecked")
        ArgumentCaptor<Map<TopicPartition, OffsetAndMetadata>> abortedOffsets = ArgumentCaptor.forClass(Map.class);
        verify(producers.get(0)).sendOffsetsToTransaction(abortedOffsets.capture(), any(ConsumerGroupMetadata.class));
        long frontierOfAbortedTransaction = abortedOffsets.getValue().get(TP).offset();
        assertWithMessage("fixture: the aborted transaction carried at least one completed record").that(frontierOfAbortedTransaction).isAtLeast(1);
        for (long offset = 0; offset < frontierOfAbortedTransaction; offset++) {
            assertWithMessage("offset %s was in the aborted transaction, so it is processed again", offset)
                    .that(seen.get(offset).get()).isAtLeast(2);
        }
        // R14: metadata is read at commit time from the cache the poll thread refreshes, so once a poll has seen
        // the new generation the next commit carries it. (A MockProducer accepts a stale generation; a broker would
        // answer CommitFailedException and recovery would run again until a poll had refreshed the cache.)
        await().atMost(Duration.ofSeconds(30)).until(() -> refreshedMetadataReads.get() >= 1);
        addRecords(5, 6);
        awaitCommittedThrough(replacement, 7);
        ArgumentCaptor<ConsumerGroupMetadata> metadataUsed = ArgumentCaptor.forClass(ConsumerGroupMetadata.class);
        verify(replacement, atLeastOnce()).sendOffsetsToTransaction(anyMap(), metadataUsed.capture());
        List<ConsumerGroupMetadata> all = metadataUsed.getAllValues();
        assertWithMessage("the commit made after the refresh carries the live generation")
                .that(all.get(all.size() - 1).generationId()).isEqualTo(2);
        assertThat(derivedTransactionalId).startsWith(TransactionalIdDerivation.prefixFor(GROUP));
        assertThat(producerManager().isProducerAvailable()).isTrue();
    }

    /**
     * Covers AE2 / R9 / R18: the field report's shape - InvalidPidMappingException from the send future - on the
     * PC-built path builds a replacement, the record runs again, and its offset is committed. The instance staying
     * alive alone would pass on the old code, where the outcome was the spin.
     */
    @Test
    void invalidPidMappingFromTheSendFutureIsRecoveredAndTheRecordIsCommittedByTheReplacement() {
        onBuild.put(0, producer -> failEverySendFromTheFuture(producer, new InvalidPidMappingException("producer id expired")));
        start(optionsBuilder().build());
        addRecords(0, 2);

        awaitProducers(2);
        awaitCommittedThrough(producers.get(1), 3);

        assertThat(pc.isClosedOrFailed()).isFalse();
        assertThat(producers.get(0).consumerGroupOffsetsHistory()).isEmpty();
        assertWithMessage("the record whose send failed ran again").that(seen.get(0L).get()).isAtLeast(2);
        // the others were either failed and retried too, or parked at the produce lock until the replacement existed
        // and produced once - both are correct, and which one depends on dispatch timing
        for (long offset = 0; offset <= 2; offset++) {
            assertThat(seen.get(offset).get()).isAtLeast(1);
        }
    }

    /**
     * Covers AE3 / R12 and AE10 / R22-R24: three fences in a row, three recoveries, no terminal state reached by
     * counting; each recovery is logged with its condition and outcome, the consecutive ones at ERROR, and the
     * counter tells them apart from ordinary commit failures.
     */
    @Test
    void recoveryRepeatsAsOftenAsTheConditionRecursAndTheConsecutiveOnesAreLoggedLouder() {
        onBuild.put(0, this::fenceAtFirstCommit);
        onBuild.put(1, this::fenceAtFirstCommit);
        onBuild.put(2, this::fenceAtFirstCommit);
        var logger = (Logger) LoggerFactory.getLogger(ProducerManager.class);
        var appender = new ListAppender<ILoggingEvent>();
        appender.start();
        logger.addAppender(appender);
        try {
            start(optionsBuilder().build());
            producerManager().recoveryBackoffInitial = Duration.ofMillis(50); // the pacing is asserted elsewhere
            addRecords(0, 4);

            awaitProducers(4);
            awaitCommittedThrough(producers.get(3), 5);
        } finally {
            logger.detachAppender(appender);
        }

        assertThat(pc.isClosedOrFailed()).isFalse();
        assertThat(producers).hasSize(4);
        for (int i = 0; i < 3; i++) {
            assertThat(producers.get(i).consumerGroupOffsetsHistory()).isEmpty();
        }
        // this instance's recovery lines, told apart from a concurrently running test's by the id they name
        List<ILoggingEvent> recoveries = appender.list.stream()
                .filter(event -> event.getFormattedMessage().startsWith("Producer recovery replaced"))
                .filter(event -> event.getFormattedMessage().contains(derivedTransactionalId))
                .collect(java.util.stream.Collectors.toList());
        assertThat(recoveries).hasSize(3);
        assertThat(recoveries.get(0).getLevel()).isEqualTo(Level.WARN);
        assertThat(recoveries.get(1).getLevel()).isEqualTo(Level.ERROR);
        assertThat(recoveries.get(2).getLevel()).isEqualTo(Level.ERROR);
        for (ILoggingEvent event : recoveries) {
            assertThat(event.getFormattedMessage()).contains("ProducerFencedException");
        }
        assertThat(recoveries.get(2).getFormattedMessage()).contains("3 consecutive recoveries");
        assertThat(registry.get("pc.producer.recoveries").tag("condition", "ProducerFencedException").counter().count()).isEqualTo(3.0);
        assertWithMessage("a successful commit resets the consecutive count")
                .that(registry.get("pc.producer.consecutive.recoveries").gauge().value()).isEqualTo(0.0);
    }

    /**
     * Covers AE9 (retriable half) and the wake cadence KTD7 pins: a replacement that cannot reach the coordinator is
     * retried on a later pass, spaced by the backoff and not inline - and the control thread wakes for it even though
     * the commit interval, the only other thing that would wake it, is far longer.
     */
    @Test
    void aReplacementThatCannotInitialiseIsRetriedWithBackoffOnLaterPasses() {
        onBuild.put(0, this::fenceAtFirstCommit);
        Consumer<MockProducer<String, String>> coordinatorUnreachable = producer -> producer.initTransactionException = new TimeoutException("coordinator unreachable");
        onBuild.put(1, coordinatorUnreachable);
        onBuild.put(2, coordinatorUnreachable);
        start(optionsBuilder().commitInterval(Duration.ofSeconds(5)).build());
        producerManager().recoveryBackoffInitial = Duration.ofMillis(300);
        addRecords(0, 2);

        awaitProducers(4);
        awaitCommittedThrough(producers.get(3), 3);

        assertThat(pc.isClosedOrFailed()).isFalse();
        Duration firstRetryAfter = Duration.between(factoryCallTimes.get(1), factoryCallTimes.get(2));
        Duration secondRetryAfter = Duration.between(factoryCallTimes.get(2), factoryCallTimes.get(3));
        assertWithMessage("the first retry waits at least one backoff, not inline").that(firstRetryAfter).isAtLeast(Duration.ofMillis(300));
        assertWithMessage("the second retry waits at least the doubled backoff").that(secondRetryAfter).isAtLeast(Duration.ofMillis(600));
        assertWithMessage("the control thread wakes for the attempt rather than sleeping out the 5 s commit interval")
                .that(secondRetryAfter).isLessThan(Duration.ofSeconds(3));
    }

    /**
     * Covers AE9 (terminal half) and R15: an unauthorised transactional id ends the instance, naming the id, and
     * the failure carries no raw cause message.
     */
    @Test
    void anUnauthorisedTransactionalIdIsTerminalAndNamesTheId() {
        onBuild.put(0, this::fenceAtFirstCommit);
        onBuild.put(1, producer -> producer.initTransactionException = new TransactionalIdAuthorizationException("denied for user-svc with secret=hunter2"));
        start(optionsBuilder().build());
        addRecords(0, 2);

        await().atMost(Duration.ofSeconds(30)).until(() -> pc.isClosedOrFailed());

        assertThat(producers).hasSize(2);
        Exception cause = pc.getFailureCause();
        assertThat(cause).isInstanceOf(ProducerInvalidatedException.class);
        assertThat(cause).hasMessageThat().contains(derivedTransactionalId);
        assertThat(cause).hasMessageThat().contains(TransactionalIdAuthorizationException.class.getName());
        assertWithMessage("the raw cause message may carry configuration values, so it is not carried")
                .that(cause).hasMessageThat().doesNotContain("hunter2");
        assertThat(cause.getCause()).hasMessageThat().doesNotContain("hunter2");
    }

    /**
     * Covers R15: a close during an outage does not wait out the shutdown timeout on records that can never be
     * produced - the parked workers are released as soon as the processor leaves RUNNING.
     */
    @Test
    void closingDuringAnOutageReleasesTheParkedWorkersPromptly() {
        onBuild.put(0, this::fenceAtFirstCommit);
        // the replacement cannot initialise, and the backoff keeps the outage open for the rest of the test
        onBuild.put(1, producer -> producer.initTransactionException = new TimeoutException("coordinator unreachable"));
        start(optionsBuilder().shutdownTimeout(Duration.ofSeconds(30)).build());
        producerManager().recoveryBackoffInitial = Duration.ofMinutes(5);
        addRecords(0, 9);

        await().atMost(Duration.ofSeconds(30)).until(() -> producers.size() >= 2 && producerManager().isReplacing()
                && !producerManager().isRecoveryAttemptDue(Instant.now()));
        Instant closeStarted = Instant.now();
        pc.closeDontDrainFirst(Duration.ofSeconds(30));
        Duration closeTook = Duration.between(closeStarted, Instant.now());

        assertThat(pc.isClosedOrFailed()).isTrue();
        assertWithMessage("close did not wait out the 30 s shutdown timeout").that(closeTook).isLessThan(Duration.ofSeconds(20));
    }
}
