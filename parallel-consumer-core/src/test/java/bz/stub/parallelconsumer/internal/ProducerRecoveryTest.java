package bz.stub.parallelconsumer.internal;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.ParallelConsumerOptions.CommitMode;
import bz.stub.parallelconsumer.ParallelEoSStreamProcessor;
import bz.stub.parallelconsumer.PollContext;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertThrows;
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
    /** The keys of every record whose send the discarded producer failed - the records recovery must run again. */
    private final java.util.Set<String> keysWhoseSendFailed = ConcurrentHashMap.newKeySet();
    /** The failed-attempt count each delivery of each offset reported through its RecordContext, in order. */
    private final Map<Long, List<Integer>> failedAttemptsSeen = new ConcurrentHashMap<>();
    /** Applied to each producer as the factory builds it, keyed by build index (0 = the initial producer). */
    private final Map<Integer, Consumer<MockProducer<String, String>>> onBuild = new ConcurrentHashMap<>();
    private volatile Optional<CountDownLatch> holdFactoryUntil = Optional.empty();
    /** Run before the factory builds, keyed by factory call index (0 = the initial producer); throws to fail the build. */
    private final Map<Integer, Runnable> beforeBuild = new ConcurrentHashMap<>();
    /** Runs inside the user function before it returns its record - a hook for holding a worker where it stands. */
    private volatile Consumer<PollContext<String, String>> insideUserFunction = ignored -> {
    };

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
            Runnable before = beforeBuild.get(factoryCallTimes.size() - 1);
            if (before != null) {
                before.run();
            }
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
            failedAttemptsSeen.computeIfAbsent(context.offset(), ignored -> new CopyOnWriteArrayList<>())
                    .add(context.getSingleRecord().getNumberOfFailedAttempts());
            insideUserFunction.accept(context);
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

    /** Fences the producer at its second sendOffsetsToTransaction; the first commit lands as normal. */
    private void fenceAtSecondCommit(MockProducer<String, String> producer) {
        var commits = new AtomicInteger();
        doAnswer(invocation -> {
            if (commits.incrementAndGet() == 1) {
                return invocation.callRealMethod();
            }
            producer.fenceProducer();
            throw new ProducerFencedException("fenced at the second commit");
        }).when(producer).sendOffsetsToTransaction(anyMap(), any(ConsumerGroupMetadata.class));
    }

    /**
     * The exactly-once invariant a replay defect breaks: every offset the replacement committed below its frontier
     * has output in the replacement's own history. An offset committed by the replacement whose only output went
     * into the discarded producer's aborted transaction was committed for output the broker never saw.
     */
    private void assertEveryOffsetTheReplacementCommittedWasProducedByIt(MockProducer<String, String> replacement) {
        long frontier = highestCommittedOffset(replacement);
        assertWithMessage("fixture: the replacement committed something").that(frontier).isAtLeast(1);
        List<String> keysProducedByReplacement = replacement.history().stream().map(ProducerRecord::key).collect(java.util.stream.Collectors.toList());
        for (long offset = 0; offset < frontier; offset++) {
            assertWithMessage("offset %s was committed by the replacement, so its output must have been produced by the replacement, not only by the discarded producer", offset)
                    .that(keysProducedByReplacement).contains("k" + offset);
        }
    }

    /** Every send's future fails with the condition - the shape FutureRecordMetadata.valueOrError produces. */
    private void failEverySendFromTheFuture(MockProducer<String, String> producer, RuntimeException condition) {
        doAnswer(invocation -> {
            keysWhoseSendFailed.add(invocation.<ProducerRecord<String, String>>getArgument(0).key());
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
        // Which records reached the discarded producer depends on dispatch timing: with two workers, the first
        // failed send records the condition and a worker not yet at the produce lock parks there until the
        // replacement exists, then produces once. So the records that must run again are exactly the ones whose send
        // failed - at least one, or the fixture proved nothing - and every record ran at least once.
        assertWithMessage("fixture: at least one send failed on the discarded producer").that(keysWhoseSendFailed).isNotEmpty();
        for (String key : keysWhoseSendFailed) {
            long offset = Long.parseLong(key.substring(1));
            assertWithMessage("record %s, whose send failed, ran again", key).that(seen.get(offset).get()).isAtLeast(2);
        }
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
        var logger = (Logger) LoggerFactory.getLogger(ProducerRecovery.class);
        var appender = new ListAppender<ILoggingEvent>();
        appender.start();
        logger.addAppender(appender);
        try {
            start(optionsBuilder().build());
            producerManager().recovery().recoveryBackoffInitial = Duration.ofMillis(50); // the pacing is asserted elsewhere
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
        producerManager().recovery().recoveryBackoffInitial = Duration.ofMillis(300);
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
        producerManager().recovery().recoveryBackoffInitial = Duration.ofMinutes(5);
        addRecords(0, 9);

        await().atMost(Duration.ofSeconds(30)).until(() -> producers.size() >= 2 && producerManager().isReplacing()
                && !producerManager().recovery().isRecoveryAttemptDue(Instant.now()));
        Instant closeStarted = Instant.now();
        pc.closeDontDrainFirst(Duration.ofSeconds(30));
        Duration closeTook = Duration.between(closeStarted, Instant.now());

        assertThat(pc.isClosedOrFailed()).isTrue();
        assertWithMessage("close did not wait out the 30 s shutdown timeout").that(closeTook).isLessThan(Duration.ofSeconds(20));
    }

    /**
     * The control thread's own wake-up is not a stop signal. {@code notifySomethingToDo} interrupts the control thread
     * whenever the producer write lock is not HELD - and recovery does not hold it while WAITING for it, which it
     * does for as long as a worker holds the produce lock through its user function. The rebalance that fenced the
     * producer ends with {@code onPartitionsAssigned}, which notifies; before this was caught, that interrupt escaped
     * {@code maybeRecoverProducer} as an {@link InterruptedException} and the supervisor closed the instance with no
     * failure cause. One record only, so no commit is attempted: the commit path's own lock wait is a separate,
     * pre-existing exposure to the same interrupt and would take the interrupt first.
     */
    @Test
    void aWakeUpInterruptWhileRecoveryWaitsForTheWriteLockDoesNotCloseTheInstance() throws Exception {
        var workerHoldsTheProduceLock = new CountDownLatch(1);
        var releaseTheWorker = new CountDownLatch(1);
        var firstDelivery = new AtomicBoolean(true);
        insideUserFunction = context -> {
            if (firstDelivery.compareAndSet(true, false)) {
                workerHoldsTheProduceLock.countDown();
                try {
                    releaseTheWorker.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        };
        start(optionsBuilder().build());
        addRecords(0, 0);
        assertThat(workerHoldsTheProduceLock.await(30, TimeUnit.SECONDS)).isTrue();
        assertWithMessage("fixture: strict mode, so the worker took the produce lock before its user function ran")
                .that(producerManager().getProducerTransactionLock().getReadLockCount()).isAtLeast(1);

        producerManager().recovery().recordInvalidation(new ProducerFencedException("fenced by a rebalance"));
        await("the control thread to be waiting for the write lock").atMost(Duration.ofSeconds(30))
                .until(() -> producerManager().getProducerTransactionLock().hasQueuedThreads());
        pc.notifySomethingToDo(); // what onPartitionsAssigned does at the end of the rebalance
        Thread.sleep(500); // let the interrupt land while the wait is open, before the worker moves on
        releaseTheWorker.countDown();

        awaitProducers(2);
        awaitCommittedThrough(producers.get(1), 1);
        assertThat(pc.isClosedOrFailed()).isFalse();
        assertThat(pc.getFailureCause()).isNull();
        assertWithMessage("the record produced into the aborted transaction ran again").that(seen.get(0L).get()).isAtLeast(2);
    }

    /**
     * The replay is owed until it completes. A successful-work listener is user code that runs inside the drain;
     * one that throws there used to leave the pass's catch saying "attempted again on a later pass" while the next
     * pass, finding no condition pending, built the replacement straight away - and its first commit trimmed the
     * intact ledger for output the broker had discarded. Now the replay stays owed, the next pass re-enters the lock
     * and runs it first, and no offset the replacement commits lacks output the replacement produced.
     */
    @Test
    void aListenerThrowingDuringTheRecoveryDrainLeavesTheReplayOwedUntilALaterPassCompletesIt() throws Exception {
        var workerHoldsTheProduceLock = new CountDownLatch(1);
        var releaseTheWorker = new CountDownLatch(1);
        var firstDelivery = new AtomicBoolean(true);
        insideUserFunction = context -> {
            if (firstDelivery.compareAndSet(true, false)) {
                workerHoldsTheProduceLock.countDown();
                try {
                    releaseTheWorker.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        };
        start(optionsBuilder().build());
        producerManager().recovery().recoveryBackoffInitial = Duration.ofMillis(50);
        AbstractParallelEoSStreamProcessor<String, String> engine = pc;
        var listenerThrewDuringTheDrain = new AtomicBoolean(false);
        engine.wm.addSuccessfulWorkListener(wc -> {
            // only inside the recovery drain, and only once: the ordinary control-loop drain would end the instance
            if (producerManager().recovery().isReplayOwed() && listenerThrewDuringTheDrain.compareAndSet(false, true)) {
                throw new RuntimeException("listener threw during the recovery drain");
            }
        });
        addRecords(0, 0);
        assertThat(workerHoldsTheProduceLock.await(30, TimeUnit.SECONDS)).isTrue();

        // the fence lands while the worker holds the produce lock; its result is mailboxed after, so the recovery
        // drain is what lands it - and that is where the listener throws
        producerManager().recovery().recordInvalidation(new ProducerFencedException("fenced by a rebalance"));
        await().atMost(Duration.ofSeconds(30)).until(() -> producerManager().getProducerTransactionLock().hasQueuedThreads());
        releaseTheWorker.countDown();

        await("the listener to have thrown inside the recovery drain").atMost(Duration.ofSeconds(30)).until(listenerThrewDuringTheDrain::get);
        awaitProducers(2);
        awaitCommittedThrough(producers.get(1), 1);
        assertThat(pc.isClosedOrFailed()).isFalse();
        assertEveryOffsetTheReplacementCommittedWasProducedByIt(producers.get(1));
        assertWithMessage("the record whose output the aborted transaction discarded ran again").that(seen.get(0L).get()).isAtLeast(2);
    }

    /**
     * Ordering across a replay (KEY ordering, R13): the second record of a key is dispatched once the first
     * completes, and can be on its way to the produce lock when the replay puts the first back. It must not produce
     * ahead of it into the replacement's transaction - it re-queues, and ordered selection takes the restored, lower
     * offset first. Eager processing, so the record is dispatched and held in its user function before the fence
     * with no lock held; the fence lands on the second commit, after the warm-up record's first, immediate one.
     */
    @Test
    void aRecordDispatchedBeforeTheReplayProducesAfterTheRestoredEarlierRecordOfItsKey() throws Exception {
        onBuild.put(0, this::fenceAtSecondCommit);
        var laterRecordDispatched = new CountDownLatch(1);
        var releaseTheLaterRecord = new CountDownLatch(1);
        var firstDeliveryOfOffsetTwo = new AtomicBoolean(true);
        insideUserFunction = context -> {
            if (context.offset() == 2 && firstDeliveryOfOffsetTwo.compareAndSet(true, false)) {
                laterRecordDispatched.countDown();
                try {
                    releaseTheLaterRecord.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        };
        start(optionsBuilder()
                .ordering(ParallelConsumerOptions.ProcessingOrder.KEY)
                .allowEagerProcessingDuringTransactionCommit(true)
                .commitInterval(Duration.ofSeconds(2))
                .build());
        consumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 0, "warm-up", "v0"));
        awaitCommittedThrough(producers.get(0), 1); // the first commit is immediate; the second is two seconds out
        consumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 1, "same-key", "v1"));
        consumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 2, "same-key", "v2"));

        assertThat(laterRecordDispatched.await(30, TimeUnit.SECONDS)).isTrue();
        assertWithMessage("fixture: offset 2 was dispatched before the fence, so it is in flight across the replay")
                .that(registry.find("pc.producer.recoveries").counters()).isEmpty();
        assertThat(producerManager().isProducerAvailable()).isTrue();
        // the second commit fences; recovery replays offset 1 and publishes the replacement, all while 2 is held
        awaitProducers(2);
        await().atMost(Duration.ofSeconds(30)).until(() -> producerManager().isProducerAvailable());
        releaseTheLaterRecord.countDown();

        awaitCommittedThrough(producers.get(1), 3);
        List<String> sameKeyValuesInReplacementOrder = producers.get(1).history().stream()
                .filter(record -> record.key().equals("same-key"))
                .map(ProducerRecord::value)
                .collect(java.util.stream.Collectors.toList());
        assertWithMessage("the restored earlier offset is produced before the later one that waited out the replay")
                .that(sameKeyValuesInReplacementOrder).containsExactly("v1", "v2").inOrder();
        assertWithMessage("offset 2 ran again after being re-queued").that(seen.get(2L).get()).isEqualTo(2);
        assertWithMessage("being re-queued for the recovery is not a failed attempt of the record")
                .that(failedAttemptsSeen.get(2L)).containsExactly(0, 0).inOrder();
    }

    /**
     * Recovery is silent at the record level: a record whose produce failed because the producer was being
     * replaced is re-queued with no attempt counted against it - the count a retry-delay function or a
     * dead-letter policy reads through RecordContext - and nothing counted on the failed-records meter.
     */
    @Test
    void aRecordDeferredForRecoveryCarriesNoFailedAttemptAndCountsOnNoFailureMeter() {
        onBuild.put(0, producer -> failEverySendFromTheFuture(producer, new InvalidPidMappingException("producer id expired")));
        start(optionsBuilder().build());
        addRecords(0, 0);

        awaitProducers(2);
        awaitCommittedThrough(producers.get(1), 1);

        assertWithMessage("the second delivery saw no failed attempt from the first").that(failedAttemptsSeen.get(0L)).containsExactly(0, 0).inOrder();
        double failedRecords = registry.find("pc.failed.records").counters().stream().mapToDouble(c -> c.count()).sum();
        assertWithMessage("the failed-records meter did not count the deferral").that(failedRecords).isEqualTo(0.0);
    }

    /**
     * A factory is user code. An {@link Error} from it - a serializer whose static initialiser fails, say - is
     * deterministic, so it is terminal rather than retried: the instance closes naming the type, and a worker parked
     * on the produce lock is released by the close rather than waiting on a replacement that will never come.
     */
    @Test
    void anErrorFromTheFactoryIsTerminalAndClosesTheInstanceNamingIt() {
        onBuild.put(0, this::fenceAtFirstCommit);
        beforeBuild.put(1, () -> {
            throw new NoClassDefFoundError("com/example/Serializer");
        });
        start(optionsBuilder().build());
        addRecords(0, 2);

        await().atMost(Duration.ofSeconds(30)).until(() -> pc.isClosedOrFailed());

        assertThat(factoryCallTimes).hasSize(2);
        Exception cause = pc.getFailureCause();
        assertThat(cause).isInstanceOf(ProducerInvalidatedException.class);
        assertThat(cause).hasMessageThat().contains(NoClassDefFoundError.class.getName());
        assertThat(cause).hasMessageThat().contains(derivedTransactionalId);
        assertWithMessage("closed, not merely failed: the parked workers were released and the shutdown completed")
                .that(producerManager().isProducerAvailable()).isFalse();
    }

    /**
     * The pass declares itself confined to the control thread (RacerD reads the annotation) and asserts it, so a
     * caller on any other thread fails loudly instead of racing the control loop on the ledger and the locks. An
     * unstarted instance has no control thread and nothing to race, which is what lets the closing-state test below
     * drive the gate directly.
     */
    @Test
    void theRecoveryPassRefusesAnyThreadButTheControlThreadOnceOneExists() {
        start(optionsBuilder().build());
        addRecords(0, 0);
        awaitCommittedThrough(producers.get(0), 1);

        AbstractParallelEoSStreamProcessor<String, String> engine = pc;

        var thrown = assertThrows(IllegalStateException.class, engine::maybeRecoverProducer);

        assertThat(thrown).hasMessageThat().contains("confined to the control thread");
        assertThat(thrown).hasMessageThat().contains(Thread.currentThread().getName());
    }

    /**
     * Recovery is skipped once the instance is closing: a close during an outage must not wait on a rebuild that
     * blocks up to {@code max.block.ms} for a producer nobody will use. Driven directly, because the window between
     * CLOSING and the manager's own close is the control thread's alone. An unstarted instance closes to CLOSED
     * without touching the manager, which is what leaves the condition recordable.
     */
    @Test
    void noReplacementIsAttemptedOnceTheInstanceIsClosing() {
        pc = new ParallelEoSStreamProcessor<>(optionsBuilder().build());
        pc.close();
        assertThat(pc.isClosedOrFailed()).isTrue();
        producerManager().recovery().recordInvalidation(new ProducerFencedException("fenced during close"));
        assertWithMessage("fixture: the attempt is due, so only the state gate can stop it")
                .that(producerManager().recovery().isRecoveryAttemptDue(Instant.now())).isTrue();

        // through the declaring type: a package-private member is not inherited across packages
        AbstractParallelEoSStreamProcessor<String, String> engine = pc;
        engine.maybeRecoverProducer();

        assertWithMessage("no replacement was built").that(factoryCallTimes).hasSize(1);
        assertThat(producers).hasSize(1);
    }

    /**
     * While a replacement is deferred, the only thing that wakes the control thread is its own deadline; the sleep
     * it chose used to skip that cap on the path taken when a failed record sits in the retry queue, so a long
     * retry delay and commit interval slept straight through the attempt. Both are set long here; a record is left
     * waiting to be retried; the first replacement build is made to fail so the second is scheduled by backoff; the
     * second must arrive on the backoff's timescale, not the retry delay's.
     */
    @Test
    void aDeferredReplacementIsRetriedOnItsOwnScheduleEvenWhileARecordWaitsInTheRetryQueue() {
        insideUserFunction = context -> {
            if (context.offset() == 1 && seen.get(1L).get() == 1) {
                throw new RuntimeException("fail offset 1 once, so it waits in the retry queue");
            }
        };
        beforeBuild.put(1, () -> {
            throw new RuntimeException("first replacement build fails, so the second is scheduled by backoff");
        });
        start(optionsBuilder()
                .commitInterval(Duration.ofSeconds(30))
                .defaultMessageRetryDelay(Duration.ofSeconds(30))
                .build());
        producerManager().recovery().recoveryBackoffInitial = Duration.ofMillis(200);
        producerManager().recovery().recoveryBackoffMax = Duration.ofMillis(200);
        fenceAtFirstCommit(producers.get(0));
        addRecords(0, 1);

        await().atMost(Duration.ofSeconds(8)).untilAsserted(() ->
                assertWithMessage("the replacement was built on the backoff's timescale, not after the 30 s retry delay")
                        .that(producers.size()).isAtLeast(2));
    }
}
