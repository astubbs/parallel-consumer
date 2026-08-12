package io.confluent.parallelconsumer.integrationTests.state;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 * Modifications Copyright (C) 2026 Antony Stubbs and contributors
 */

import com.google.common.truth.StringSubject;
import io.confluent.csid.utils.JavaUtils;
import io.confluent.csid.utils.ThreadUtils;
import io.confluent.parallelconsumer.FakeRuntimeException;
import io.confluent.parallelconsumer.ManagedTruth;
import io.confluent.parallelconsumer.ParallelEoSStreamProcessor;
import io.confluent.parallelconsumer.PollContext;
import io.confluent.parallelconsumer.integrationTests.BrokerIntegrationTest;
import io.confluent.parallelconsumer.integrationTests.utils.KafkaClientUtils;
import io.confluent.parallelconsumer.integrationTests.utils.KafkaClientUtils.GroupOption;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.awaitility.Awaitility;
import org.awaitility.core.TerminalFailureException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.testcontainers.kafka.KafkaContainer;
import pl.tlinkowski.unij.api.UniMaps;
import pl.tlinkowski.unij.api.UniSets;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static io.confluent.csid.utils.JavaUtils.getLast;
import static io.confluent.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.PARTITION;
import static io.confluent.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.UNORDERED;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.kafka.clients.consumer.OffsetResetStrategy.NONE;
import static pl.tlinkowski.unij.api.UniLists.of;

/**
 * Integration test versions of {@link io.confluent.parallelconsumer.state.PartitionStateCommittedOffsetTest}, where
 * committed offset gets moved around or deleted, or random offsets are removed.
 *
 * @author Antony Stubbs
 * @see io.confluent.parallelconsumer.state.PartitionStateCommittedOffsetTest
 */
@Slf4j
class PartitionStateCommittedOffsetIT extends BrokerIntegrationTest<String, String> {

    public static final OffsetResetStrategy DEFAULT_OFFSET_RESET_POLICY = OffsetResetStrategy.EARLIEST;

    TopicPartition tp;

    int TO_PRODUCE = 200;

    private OffsetResetStrategy offsetResetStrategy = DEFAULT_OFFSET_RESET_POLICY;

    // messy but as result of test is thrown as an exception, is a bit of a pain. But - only used in one test - forgive me
    private ParallelEoSStreamProcessor<String, String> activePc;

    @BeforeEach
    void setup() {
        setupTopic();
        tp = new TopicPartition(getTopic(), 0);
    }

    /**
     * Test for offset gaps in partition data (i.e. compacted topics)
     */
    @Test
    void compactedTopic() {
        try (KafkaContainer compactingBroker = setupCompactingKafkaBroker();) {

            var TO_PRODUCE = this.TO_PRODUCE / 10; // local override, produce less

            List<String> keys = produceMessages(TO_PRODUCE);

            final int UNTIL_OFFSET = TO_PRODUCE / 2;
            var processedOnFirstRun = runPcUntilOffset(UNTIL_OFFSET, TO_PRODUCE, UniSets.of(TO_PRODUCE - 3L));
            assertWithMessage("Last processed should be at least half of the total sent, so that there is incomplete data to track")
                    .that(getLast(processedOnFirstRun).get().offset())
                    .isGreaterThan(TO_PRODUCE / 2);

            //
            ArrayList<String> compactionKeysRaw = sendRandomCompactionRecords(keys, TO_PRODUCE);
            Set<String> compactedKeys = new HashSet<>(compactionKeysRaw);

            var processedOnFirstRunWithTombstoneTargetsRemoved = processedOnFirstRun.stream()
                    .filter(context -> !compactedKeys.contains(context.key()))
                    .map(PollContext::key)
                    .collect(Collectors.toList());

            var firstRunPartitioned = processedOnFirstRun.stream().collect(Collectors.partitioningBy(context -> compactedKeys.contains(context.key())));
            var saved = firstRunPartitioned.get(Boolean.FALSE);
            var compacted = firstRunPartitioned.get(Boolean.TRUE);
            log.debug("kept offsets: {}", saved.stream().mapToLong(PollContext::offset).boxed().collect(Collectors.toList()));
            log.debug("kept keys: {}", saved.stream().map(PollContext::key).collect(Collectors.toList()));
            log.debug("compacted offsets: {}", compacted.stream().map(PollContext::key).collect(Collectors.toList()));
            log.debug("compacted keys: {}", compacted.stream().mapToLong(PollContext::offset).boxed().collect(Collectors.toList()));


            var tombstoneTargetOffsetsFromFirstRun = compacted.stream()
                    .filter(context -> compactedKeys.contains(context.key()))
                    .map(PollContext::offset)
                    .collect(Collectors.toList());

            var tombStonedOffsetsFromKey = compactedKeys.stream()
                    .map(PartitionStateCommittedOffsetIT::getOffsetFromKey).collect(Collectors.toList());
            log.debug("First run produced, with compaction targets removed: {}", processedOnFirstRunWithTombstoneTargetsRemoved);

            //
            triggerCompactionProcessing();

            // The offsets of the tombstone targets should not be read in second run
            final int expectedOffsetProcessedToSecondRun = TO_PRODUCE + compactedKeys.size();
            var processedOnSecondRun = runPcUntilOffset(expectedOffsetProcessedToSecondRun, GroupOption.REUSE_GROUP).stream()
                    .filter(recordContexts -> !recordContexts.key().contains("compaction-trigger"))
                    .collect(Collectors.toList());

            //
            List<Long> offsetsFromSecond = processedOnSecondRun.stream()
                    .map(PollContext::offset)
                    .collect(Collectors.toList());

            assertWithMessage("Finish reading rest of records from %s to %s",
                    UNTIL_OFFSET, TO_PRODUCE)
                    .that(processedOnSecondRun.size()).isGreaterThan(TO_PRODUCE - UNTIL_OFFSET);

            assertWithMessage("Off the offsets read on the second run, offsets that were compacted (below the initial produce target) should now be removed, as they were replaced with newer ones.")
                    .that(offsetsFromSecond)
                    .containsNoneIn(tombstoneTargetOffsetsFromFirstRun);

        }
    }

    /**
     * Set up our extra special compacting broker
     */
    @NonNull
    private KafkaContainer setupCompactingKafkaBroker() {
        KafkaContainer compactingBroker = null;
        {
            // set up new broker
            compactingBroker = BrokerIntegrationTest.createKafkaContainer("40000");
            compactingBroker.start();

            setup();
        }

        setupCompactedEnvironment();

        return compactingBroker;
    }

    private List<PollContext<String, String>> runPcUntilOffset(int offset) {
        return runPcUntilOffset(DEFAULT_OFFSET_RESET_POLICY, offset);
    }

    private List<PollContext<String, String>> runPcUntilOffset(OffsetResetStrategy offsetResetPolicy, int offset) {
        return runPcUntilOffset(offsetResetPolicy, offset, offset, UniSets.of(), GroupOption.NEW_GROUP);
    }

    private List<PollContext<String, String>> runPcUntilOffset(int offset, GroupOption reuseGroup) {
        return runPcUntilOffset(DEFAULT_OFFSET_RESET_POLICY, Long.MAX_VALUE, offset, UniSets.of(), reuseGroup);
    }

    private static long getOffsetFromKey(String key) {
        return Long.parseLong(key.substring(key.indexOf("-") + 1));
    }

    @SneakyThrows
    private void setupCompactedEnvironment() {
        log.debug("Setting up aggressive compaction...");
        ConfigResource topicConfig = new ConfigResource(ConfigResource.Type.TOPIC, getTopic());

        Collection<AlterConfigOp> alterConfigOps = new ArrayList<>();

        alterConfigOps.add(new AlterConfigOp(new ConfigEntry(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT), AlterConfigOp.OpType.SET));
        alterConfigOps.add(new AlterConfigOp(new ConfigEntry(TopicConfig.MAX_COMPACTION_LAG_MS_CONFIG, "1"), AlterConfigOp.OpType.SET));
        alterConfigOps.add(new AlterConfigOp(new ConfigEntry(TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG, "0"), AlterConfigOp.OpType.SET));

        var configs = UniMaps.of(topicConfig, alterConfigOps);
        KafkaFuture<Void> all = getKcu().getAdmin().incrementalAlterConfigs(configs).all();
        all.get(5, SECONDS);

        log.debug("Compaction setup complete");
    }

    @SneakyThrows
    private List<String> triggerCompactionProcessing() {
        // send a lot of messages to fill up segments
        List<String> keys = produceMessages(TO_PRODUCE * 2, "log-compaction-trigger-");
        // or wait?
        final int pauseSeconds = 20;
        log.info("Pausing for {} seconds to allow for compaction", pauseSeconds);
        ThreadUtils.sleepSecondsLog(pauseSeconds);

        return keys;
    }

    @SneakyThrows
    private ArrayList<String> sendRandomCompactionRecords(List<String> keys, int howMany) {
        var tombstoneKeys = new ArrayList<String>();
        // fix randomness
        List<Future<RecordMetadata>> futures = JavaUtils.getRandom(keys, howMany).stream()
                .map((String key) -> {
                    tombstoneKeys.add(key);
                    var tombstone = new ProducerRecord<>(getTopic(), key, "compactor");
                    return getKcu().getProducer()
                            .send(tombstone);
                })
                .collect(Collectors.toList());
        List<Long> tombstoneOffsets = new ArrayList<>();
        for (Future<RecordMetadata> future : futures) {
            RecordMetadata recordMetadata = future.get(5, SECONDS);
            tombstoneOffsets.add(recordMetadata.offset());
        }

        tombstoneKeys.sort(Comparator.comparingLong(PartitionStateCommittedOffsetIT::getOffsetFromKey));

        log.debug("Keys to tombstone: {}\n" +
                        "Offsets of the generated tombstone: {}",
                tombstoneKeys,
                tombstoneOffsets);
        return tombstoneKeys;
    }

    /**
     * CG offset has been changed to a lower offset (partition rewind / replay) (metdata lost?)
     */
    @Test
    void committedOffsetLower() {
        produceMessages(TO_PRODUCE);

        runPcUntilOffset(50);

        final int moveToOffset = 25;

        moveCommittedOffset(getKcu().getGroupId(), moveToOffset);

        runPcCheckStartIs(moveToOffset, TO_PRODUCE);
    }

    /**
     * Ensure that the PC starts at the correct offset
     *
     * @param targetStartOffset the offset to check that PC starts at
     * @param checkUpTo         the offset to run the PC until, while checking for the start offset
     */
    @SneakyThrows
    private void runPcCheckStartIs(long targetStartOffset, long checkUpTo, GroupOption groupOption) {
        var tempPc = super.getKcu().buildPc(PARTITION, groupOption);
        tempPc.subscribe(of(getTopic()));

        AtomicLong lowest = new AtomicLong(Long.MAX_VALUE);
        AtomicLong highest = new AtomicLong(Long.MIN_VALUE);

        AtomicLong bumpersSent = new AtomicLong();

        tempPc.poll(recordContexts -> {
            log.error("Consumed: {} Bumpers sent {}", recordContexts.offset(), bumpersSent);
            long thisOffset = recordContexts.offset();
            if (thisOffset < lowest.get()) {
                log.error("Found lowest offset {}", thisOffset);
                lowest.set(thisOffset);
            } else if (thisOffset > highest.get()) {
                highest.set(thisOffset);
            }
        });

        //
        if (offsetResetStrategy.equals(NONE)) {
            Awaitility.await().untilAsserted(() -> assertThat(tempPc.isClosedOrFailed()).isFalse()); // started
            Awaitility.await().untilAsserted(() -> assertThat(tempPc.isClosedOrFailed()).isTrue()); // crashed
            var throwable = tempPc.getFailureCause();
            StringSubject causeMessage = assertThat(ExceptionUtils.getRootCauseMessage(throwable));
            causeMessage.contains("NoOffsetForPartitionException");
            causeMessage.contains("Undefined offset with no reset policy");

            getKcu().close();
        } else {
            // nudge-inside-the-await shared primitive - see BrokerIntegrationTest#awaitWithTopicNudge
            awaitWithTopicNudge(tempPc, Duration.ofSeconds(5), Duration.ofSeconds(30), bumpersSent, () -> {
                final long endOffset = getKcu().getAdmin().listOffsets(UniMaps.of(tp, OffsetSpec.earliest())).partitionResult(tp).get().offset();
                final long startOffset = getKcu().getAdmin().listOffsets(UniMaps.of(tp, OffsetSpec.latest())).partitionResult(tp).get().offset();
                log.error("start await loop: {}, end: {}, bumpersSent: {}", startOffset, endOffset, bumpersSent);

                //
                assertWithMessage("Highest seen offset to read up to")
                        .that(highest.get())
                        .isAtLeast(checkUpTo - 1);
            });

            log.warn("Offset started at should equal the target {}, lowest {}, sent {}, diff is {})", targetStartOffset, lowest, bumpersSent, lowest.get() - targetStartOffset);

            assertWithMessage("Offset started at should equal the target (sent %s , diff is %s)",
                    bumpersSent,
                    lowest.get() - targetStartOffset
            )
                    .that(lowest.get())
                    .isEqualTo(targetStartOffset);

            //
            tempPc.close();
        }

    }

    @SneakyThrows
    private void moveCommittedOffset(String groupId, long offset) {
        log.debug("Moving offset of {} to {}", groupId, offset);
        var data = UniMaps.of(tp, new OffsetAndMetadata(offset));
        var result = getKcu().getAdmin().alterConsumerGroupOffsets(groupId, data);
        result.all().get(5, SECONDS);
        log.debug("Moved offset to {}", offset);
    }

    private List<PollContext<String, String>> runPcUntilOffset(long succeedUpToOffset, long expectedProcessToOffset, Set<Long> exceptionsToSucceed) {
        return runPcUntilOffset(DEFAULT_OFFSET_RESET_POLICY, succeedUpToOffset, expectedProcessToOffset, exceptionsToSucceed, GroupOption.NEW_GROUP);
    }

    @SneakyThrows
    private List<PollContext<String, String>> runPcUntilOffset(OffsetResetStrategy offsetResetPolicy,
                                                               long succeedUpToOffset,
                                                               long expectedProcessToOffset,
                                                               Set<Long> exceptionsToSucceed,
                                                               GroupOption newGroup) {
        log.debug("Running PC until at least offset {}", succeedUpToOffset);
        super.getKcu().setOffsetResetPolicy(offsetResetPolicy);
        var tempPc = super.getKcu().buildPc(UNORDERED, newGroup);
        activePc = tempPc;
        try { // can't use auto closeable because close is complicated as it's expected to crash and close rethrows error

            SortedSet<PollContext<String, String>> seenOffsets = Collections.synchronizedSortedSet(new TreeSet<>(Comparator.comparingLong(PollContext::offset)));
            SortedSet<PollContext<String, String>> succeededOffsets = Collections.synchronizedSortedSet(new TreeSet<>(Comparator.comparingLong(PollContext::offset)));

            tempPc.subscribe(of(getTopic()));

            tempPc.poll(pollContext -> {
                seenOffsets.add(pollContext);
                long thisOffset = pollContext.offset();
                if (exceptionsToSucceed.contains(thisOffset)) {
                    log.debug("Exceptional offset {} succeeded", thisOffset);
                } else if (thisOffset >= succeedUpToOffset) {
                    log.debug("Failing on {}", thisOffset);
                    throw new FakeRuntimeException("Failing on " + thisOffset);
                } else {
                    log.debug("Succeeded {}: {}", thisOffset, pollContext.getSingleRecord());
                    succeededOffsets.add(pollContext);
                }
            });

            // give first poll a chance to run
            ThreadUtils.sleepSecondsLog(1);

            // Nudge records are produced INSIDE the await, not just once up front: with
            // auto.offset.reset=latest and a contention-slowed bootstrap, a single pre-await record can be
            // leapfrogged by the offset reset resolving after it - leaving the consumer positioned past
            // every record that will ever exist, making this await unwinnable at ANY timeout. That race
            // was this test's long-standing CI "flake" ([1]=latest only). See
            // BrokerIntegrationTest#awaitWithTopicNudge and the linked solution doc.
            log.info("first-poll await: pcId={} topic={} groupId={}",
                    tempPc.getMyId().orElse("?"), getTopic(), getKcu().getGroupId());
            var nudgesSent = new AtomicLong();
            awaitWithTopicNudge(tempPc, Duration.ofSeconds(1), Duration.ofSeconds(10), nudgesSent, () -> {
                assertThat(seenOffsets).isNotEmpty();
                assertThat(seenOffsets.last().offset()).isGreaterThan(expectedProcessToOffset - 2);
            });

            if (!succeededOffsets.isEmpty()) {
                log.debug("Succeeded up to: {}", succeededOffsets.last().offset());
            }
            log.debug("Consumed up to {}", seenOffsets.last().offset());

            var sorted = new ArrayList<>(seenOffsets);
            Collections.sort(sorted, Comparator.comparingLong(PollContext::offset));
            return sorted;
        } finally {
            try {
                if (!tempPc.isClosedOrFailed()) {
                    // some test branches, pc will crash and close in the test already
                    tempPc.close();
                }
            } catch (Exception e) {
                log.debug("Cause will get rethrown close on the NONE parameter branch", e);
            }
        }
    }

    /**
     * CG offset has been changed to something higher than expected (offset skip) (metdata lost?)
     */
    @Test
    void committedOffsetHigher() {
        final int quantity = 100;
        produceMessages(quantity);

        runPcUntilOffset(50);

        final int moveToOffset = 75;

        // resolve groupId mess
        moveCommittedOffset(getKcu().getGroupId(), moveToOffset);

        runPcCheckStartIs(moveToOffset, quantity);
    }

    private void runPcCheckStartIs(int targetStartOffset, int checkUpTo) {
        runPcCheckStartIs(targetStartOffset, checkUpTo, GroupOption.REUSE_GROUP);
    }

    /**
     * CG offset has disappeared - committed offset hasn't been changed, but broker gives us a bootstrap poll result
     * with a higher offset than expected. Could be caused by retention period, or compaction.
     *
     * @see #noOffsetPolicyOnStartup
     */
    @SneakyThrows
    @EnumSource(value = OffsetResetStrategy.class)
    @ParameterizedTest
    void committedOffsetRemoved(OffsetResetStrategy offsetResetPolicy) {
        this.offsetResetStrategy = offsetResetPolicy;
        try (
                KafkaContainer compactingKafkaBroker = setupCompactingKafkaBroker();
                KafkaClientUtils clientUtils = new KafkaClientUtils(compactingKafkaBroker);
        ) {
            log.debug("Compacting broker started {}", compactingKafkaBroker.getBootstrapServers());

            clientUtils.setOffsetResetPolicy(offsetResetPolicy);
            clientUtils.open();

            if (offsetResetPolicy.equals(NONE)) {
                // no reset policy, so we set initial offset to zero, to avoid crash on startup - see startup test
                var consumer = getKcu().getConsumer();
                consumer.subscribe(of(getTopic()));
                consumer.poll(Duration.ofSeconds(1));
                // commit offset 0 to partition
                consumer.commitSync(UniMaps.of(tp, new OffsetAndMetadata(0)));
                consumer.close();
            }

            var producedCount = produceMessages(TO_PRODUCE).size();

            final int END_OFFSET = 50;
            var groupId = clientUtils.getGroupId();
            runPcUntilOffset(offsetResetPolicy, END_OFFSET, END_OFFSET, UniSets.of(), GroupOption.REUSE_GROUP);

            // REGRESSION GUARD - deliberately makes this test exercise the case that used to break it.
            // awaitWithTopicNudge above produces 1..N nudge records depending on load, and on an idle
            // machine N is reliably 1, which is exactly why "run sends one" arithmetic passed locally
            // for months and failed on a contended CI runner. Producing one extra record here forces
            // N >= 2 on EVERY run, so any future assertion that assumes a fixed record count fails
            // immediately and everywhere, instead of intermittently and only under load.
            getKcu().produceMessages(getTopic(), 1, "nudge-count-regression-guard-");

            // How many records exist is a question the BROKER can answer, so ask it. This used to be
            // "+ 1 // run sends one", true when runPcUntilOffset produced a single record up front. It
            // now nudges from INSIDE the await, one record per poll iteration, so the real count is
            // 1..10 and load-dependent. Under LATEST, producedCount IS the expected reset offset, so a
            // wrong count also moved the goalposts for the assertion at the end of the test.
            producedCount = (int) currentEndOffset();

            //
            final String compactedKey = "key-50";

            // before compaction
            checkHowManyRecordsWithKeyPresent(compactedKey, 1);

            final int triggerRecordsCount = causeCommittedOffsetToBeRemoved(END_OFFSET);

            // after compaction
            checkHowManyRecordsWithKeyPresent(compactedKey, 1);

            producedCount = producedCount + triggerRecordsCount;

            final int EXPECTED_RESET_OFFSET = switch (offsetResetPolicy) {
                case EARLIEST -> 0;
                case LATEST -> producedCount;
                case NONE -> -1; // will crash / fail fast
            };

            clientUtils.setGroupId(groupId);
            runPcCheckStartIs(EXPECTED_RESET_OFFSET, producedCount);
        }
    }

    /**
     * Scans the whole partition and counts the records carrying {@code keyToSearchFor}.
     *
     * <p>The search bound is read from the BROKER ({@code endOffsets}) rather than derived from how many
     * records we think we produced. It used to be a caller-supplied {@code TO_PRODUCE + 2}, which assumed
     * the partition held exactly the seeded records plus the two compaction records - an assumption
     * {@code awaitWithTopicNudge} breaks, because it produces one nudge record per poll iteration (1..10
     * of them, not one). Each extra nudge pushed the compaction records past the window, so the scan
     * stopped before reaching them and reported them missing. That was this test's CI flake: the records
     * were always present, the reader just stopped early.
     */
    private void checkHowManyRecordsWithKeyPresent(String keyToSearchFor, int expectedQuantityToFind) {
        try (KafkaConsumer<String, String> newConsumer = getKcu().createNewConsumer(GroupOption.NEW_GROUP);) {
            newConsumer.assign(of(tp));
            long searchUpToOffset = newConsumer.endOffsets(of(tp)).get(tp);
            log.debug("Looking for {} records with key {} up to the partition end offset {}",
                    expectedQuantityToFind, keyToSearchFor, searchUpToOffset);
            newConsumer.seekToBeginning(UniSets.of(tp));
            long positionAfter = newConsumer.position(tp); // trigger eager seek
            assertThat(positionAfter).isEqualTo(0);
            final List<ConsumerRecord<String, String>> records = new ArrayList<>();
            long highest = -1;
            while (highest < searchUpToOffset - 1) {
                ConsumerRecords<String, String> poll = newConsumer.poll(Duration.ofSeconds(1));
                records.addAll(poll.records(tp));
                var lastOpt = getLast(records);
                if (lastOpt.isPresent()) {
                    highest = lastOpt.get().offset();
                }
            }

            var collect = records.stream().filter(value -> value.key().equals(keyToSearchFor)).collect(Collectors.toList());
            ManagedTruth.assertThat(collect).hasSize(expectedQuantityToFind);
        }
    }

    /** The partition's end offset - how many records actually exist, not how many we think we sent. */
    private long currentEndOffset() {
        try (KafkaConsumer<String, String> probe = getKcu().createNewConsumer(GroupOption.NEW_GROUP)) {
            probe.assign(of(tp));
            return probe.endOffsets(of(tp)).get(tp);
        }
    }

    @SneakyThrows
    private int causeCommittedOffsetToBeRemoved(long offset) {
        sendCompactionKeyForOffset(offset);
        sendCompactionKeyForOffset(offset + 1);

        checkHowManyRecordsWithKeyPresent("key-" + offset, 2);

        List<String> strings = triggerCompactionProcessing();

        return 2 + strings.size();
    }

    private void sendCompactionKeyForOffset(long offset) throws InterruptedException, ExecutionException, TimeoutException {
        String key = "key-" + offset;
        ProducerRecord<String, String> compactingRecord = new ProducerRecord<>(getTopic(), 0, key, "compactor");
        getKcu().getProducer()
                .send(compactingRecord)
                .get(1, SECONDS);
    }

    /**
     * When there's no offset reset policy and there are no offsets for the consumer group, the pc should fail fast,
     * passing up the exception
     */
    @Test
    void noOffsetPolicyOnStartup() {
        this.offsetResetStrategy = NONE;
        try (
                KafkaClientUtils clientUtils = new KafkaClientUtils(kafkaContainer);
        ) {
            clientUtils.setOffsetResetPolicy(offsetResetStrategy);
            clientUtils.open();

            var producedCount = produceMessages(TO_PRODUCE).size();

            try {
                runPcUntilOffset(offsetResetStrategy, producedCount, producedCount, UniSets.of(), GroupOption.REUSE_GROUP);
            } catch (TerminalFailureException e) {
                var failureCause = activePc.getFailureCause();
                var rootCauseMessage = ExceptionUtils.getRootCauseMessage(failureCause);
                var message = assertThat(rootCauseMessage);
                message.contains("NoOffsetForPartitionException");
                message.contains("Undefined offset");
                message.contains("no reset policy");
            }
        }
    }

}