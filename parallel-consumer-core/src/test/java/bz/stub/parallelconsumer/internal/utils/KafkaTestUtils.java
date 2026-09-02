package bz.stub.parallelconsumer.internal.utils;

/*-
 * Copyright (C) 2020-2023 Confluent, Inc.
 * Modifications Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.AbstractParallelEoSStreamProcessorTestBase;
import bz.stub.parallelconsumer.PollContext;
import bz.stub.parallelconsumer.offsets.OffsetMapCodecManager;
import bz.stub.parallelconsumer.state.WorkContainer;
import bz.stub.parallelconsumer.state.WorkManager;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.TopicPartition;
import org.assertj.core.api.Assertions;
import org.assertj.core.util.Lists;

import java.util.*;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static java.lang.Math.random;
import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
@RequiredArgsConstructor
public class KafkaTestUtils {

    /**
     * Partition key used when a commit history is known to cover one partition, so the per-partition collapse
     * degenerates to a plain sequential one.
     *
     * @see #collapseRepeatedCommits(List)
     */
    private static final Object SINGLE_PARTITION = new Object();

    private final String INPUT_TOPIC;

    private final String CONSUMER_GROUP_ID;

    @Getter
    private final LongPollingMockConsumer consumerSpy;

    int offset = 0;

    // todo not used anymore - delete?
    public void assignConsumerToTopic(final MockConsumer mc) {
        TopicPartition tp1 = new TopicPartition(INPUT_TOPIC, 1);
        TopicPartition tp0 = new TopicPartition(INPUT_TOPIC, 0);
        mc.assign(Lists.list(tp0, tp1));

        HashMap<TopicPartition, Long> beginningOffsets = new HashMap<>();
        beginningOffsets.put(tp0, 0L);
        beginningOffsets.put(tp1, 0L);
        mc.updateBeginningOffsets(beginningOffsets);
    }

    /**
     * It's a race to see if the genesis offset gets committed or not. So lets remove it if it exists, and all tests can
     * assume it doesn't.
     */
    public static List<Integer> trimAllGenesisOffset(final List<Integer> collect) {
        return collect.stream().filter(x -> x != 0).collect(Collectors.toList());
    }

    /**
     * Collapses a repeat commit of the same base offset, for a commit history that covers a single partition.
     * <p>
     * PC commits {@link bz.stub.parallelconsumer.state.PartitionState#getOffsetToCommit()} - the highest
     * <em>sequentially</em> succeeded offset plus one. When a record completes that cannot advance that (say
     * an independent key's record finishing while a lower offset is still in flight), the base offset is
     * committed again with updated incomplete-offset encoding. The repeat carries new information and is
     * correct behaviour, but how many such commits occur depends on where the wall-clock commit ticks fall,
     * so it is not something a test can assert on.
     * <p>
     * Only a repeat of the <em>immediately preceding</em> commit is collapsed. A committed offset that goes
     * backwards - {@code [1, 2, 1]} - survives and still fails the assertion, because that would be a real
     * defect.
     *
     * @see #collapseRepeatedCommits(List, List) for a history spanning more than one partition
     */
    public static List<Integer> collapseRepeatedCommits(final List<Integer> offsets) {
        return collapseRepeatedCommits(Collections.nCopies(offsets.size(), SINGLE_PARTITION), offsets);
    }

    /**
     * Collapses a repeat commit of the same base offset <em>per partition</em>, preserving the order in which
     * the commits were emitted.
     * <p>
     * The partition has to be part of the comparison. A commit history is flattened across partitions, so two
     * partitions committing the same offset in one round land next to each other; collapsing on the offset
     * alone would merge them into one entry and hide a partition that failed to commit at all. Keying on the
     * partition also keeps the genesis-offset race ({@link #trimAllGenesisOffset(List)}) benign: a partition
     * still sitting at offset 0 while another advances interleaves as {@code [x, 0, x, 0]}, which collapses to
     * {@code [x, 0]} rather than being read as an offset going backwards.
     *
     * @param partitions the partition each offset was committed for, in the same order as {@code offsets}
     */
    public static <PARTITION> List<Integer> collapseRepeatedCommits(final List<PARTITION> partitions, final List<Integer> offsets) {
        Map<PARTITION, Integer> lastCommitPerPartition = new HashMap<>();
        List<Integer> collapsed = new ArrayList<>();
        for (int i = 0; i < offsets.size(); i++) {
            Integer offset = offsets.get(i);
            Integer previous = lastCommitPerPartition.put(partitions.get(i), offset);
            boolean repeatsThePreviousCommitForThisPartition = offset.equals(previous);
            if (!repeatsThePreviousCommitForThisPartition) {
                collapsed.add(offset);
            }
        }
        return collapsed;
    }

    public ConsumerRecord<String, String> makeRecord(String key, String value) {
        return makeRecord(0, key, value);
    }

    public ConsumerRecord<String, String> makeRecord(int part, String key, String value) {
        ConsumerRecord<String, String> stringStringConsumerRecord = new ConsumerRecord<>(INPUT_TOPIC, part, offset, (key), (value));
        offset++;
        return stringStringConsumerRecord;
    }

    public void assertCommits(MockProducer mp, List<Integer> integers) {
        assertCommits(mp, integers, Optional.empty());
    }

    /**
     * Asserts the sequence of offsets committed, ignoring repeated commits of the same base offset
     * ({@link OffsetMapCodecManager}).
     * <p>
     * Like {@link AbstractParallelEoSStreamProcessorTestBase#assertCommits(List, Optional)} but for a
     * {@link MockProducer}. Both tolerate a repeat commit of the same base offset. They differ on order: the
     * other method's consumer-side branch is order-insensitive, because it flattens each commit round's
     * per-partition map and so cannot rely on a global ordering, and it therefore does not detect a committed
     * offset going backwards either. Here the history is a single ordered stream, so order is asserted and a
     * backwards offset does fail.
     * <p>
     * The collapse runs before the genesis trim, and is keyed on the partition. Trimming first would turn
     * {@code [1, 0, 1]} into {@code [1, 1]} and then into {@code [1]}, silently accepting an offset that went
     * backwards through genesis.
     *
     * @see AbstractParallelEoSStreamProcessorTestBase#assertCommits(List, Optional)
     * @see #collapseRepeatedCommits(List, List)
     * @see OffsetMapCodecManager
     */
    public void assertCommits(MockProducer mp, List<Integer> expectedOffsets, Optional<String> description) {
        log.debug("Asserting commits of {}", expectedOffsets);
        List<Integer> offsets = getProducerCommitsFlattenedCollapsed(mp);

        if (!expectedOffsets.contains(0)) {
            offsets = KafkaTestUtils.trimAllGenesisOffset(offsets);
        }

        assertThat(offsets)
                .describedAs(description.orElse("Which offsets are committed and in the expected order"))
                .containsExactlyElementsOf(expectedOffsets);
    }

    public List<Integer> getProducerCommitsFlattened(MockProducer mp) {
        return getProducerCommitsMeta(mp).stream().map(x -> (int) x.offset()).collect(Collectors.toList());
    }

    /**
     * The producer's commit history, flattened in emission order, with a repeat commit of the same base offset
     * collapsed per partition.
     *
     * @see #collapseRepeatedCommits(List, List)
     */
    public List<Integer> getProducerCommitsFlattenedCollapsed(MockProducer mp) {
        List<Map<String, Map<TopicPartition, OffsetAndMetadata>>> history = mp.consumerGroupOffsetsHistory();

        List<TopicPartition> partitions = new ArrayList<>();
        List<Integer> offsets = new ArrayList<>();
        for (var round : history) {
            for (var partitionOffsets : round.get(CONSUMER_GROUP_ID).entrySet()) {
                partitions.add(partitionOffsets.getKey());
                offsets.add((int) partitionOffsets.getValue().offset());
            }
        }
        return collapseRepeatedCommits(partitions, offsets);
    }

    public List<OffsetAndMetadata> getProducerCommitsMeta(MockProducer mp) {
        List<Map<String, Map<TopicPartition, OffsetAndMetadata>>> history = mp.consumerGroupOffsetsHistory();

        List<OffsetAndMetadata> set = history.stream().flatMap(histories -> {
            // get all partition offsets and flatten
            ArrayList<OffsetAndMetadata> results = new ArrayList<>();
            var group = histories.get(CONSUMER_GROUP_ID);
            for (var partitionOffsets : group.entrySet()) {
                OffsetAndMetadata commit = partitionOffsets.getValue();
                results.add(commit);
            }
            return results.stream();
        }).collect(Collectors.toList()); // set - ignore repeated commits ({@link OffsetMap})
        return set;
    }

    public void assertCommitLists(MockProducer mp, List<List<Integer>> expectedPartitionOffsets, Optional<String> description) {
        assertCommitLists(mp.consumerGroupOffsetsHistory(), expectedPartitionOffsets, description);
    }

    /**
     * Collects into a set - ignore repeated commits ({@link OffsetMapCodecManager}).
     * <p>
     * Ignores duplicates.
     *
     * @see OffsetMapCodecManager
     */
    public void assertCommitLists(List<Map<String, Map<TopicPartition, OffsetAndMetadata>>> history,
                                  List<List<Integer>> expectedPartitionOffsets,
                                  Optional<String> description) {
        log.info("Asserting commits of {}", expectedPartitionOffsets);

        AtomicReference<String> topicName = new AtomicReference<>("");
        var partitionToCommittedOffsets = new HashMap<TopicPartition, Set<Integer>>(); // set - ignore repeated commits ({@link OffsetMap})
        new ArrayList<>(history).stream().forEachOrdered(histories -> {
            // get all partition offsets and flatten
            var partitionCommits = histories.get(CONSUMER_GROUP_ID);
            Set<Map.Entry<TopicPartition, OffsetAndMetadata>> entries = partitionCommits.entrySet();
            ArrayList<Map.Entry<TopicPartition, OffsetAndMetadata>> wrapped = new ArrayList<>(entries);
            for (var singlePartitionCommit : wrapped) {
                TopicPartition key = singlePartitionCommit.getKey();
                topicName.set(key.topic());
                OffsetAndMetadata commit = singlePartitionCommit.getValue();
                int offset = (int) commit.offset();
                partitionToCommittedOffsets.computeIfAbsent(key, x -> new HashSet<>());
                // ignore genesis commits
                if (offset != 0)
                    partitionToCommittedOffsets.get(key).add(offset);
            }
        });

        // compute the matching expected map
        var expectedMap = new HashMap<TopicPartition, Set<Integer>>();
        for (int i = 0; i < expectedPartitionOffsets.size(); i++) {
            List<Integer> offsets = expectedPartitionOffsets.get(i);
            var tp = new TopicPartition(topicName.get(), i);
            expectedMap.put(tp, new HashSet<>(offsets));
        }

        assertThat(partitionToCommittedOffsets)
                .describedAs(description.orElse("Which offsets are committed and in the expected order"))
                .containsExactlyEntriesOf(expectedMap);
    }

    public static void assertLastCommitIs(final LongPollingMockConsumer<String, String> mockConsumer, final int expected) {
        List<Map<TopicPartition, OffsetAndMetadata>> commits = mockConsumer.getCommitHistoryInt();
        Assertions.assertThat(commits).isNotEmpty();
        long offset = (int) commits.get(commits.size() - 1).values().iterator().next().offset();
        Assertions.assertThat(offset).isEqualTo(expected);
    }
    public List<ConsumerRecord<String, String>> generateRecords( int quantity) {
        return generateRecords(0, quantity);
    }

    public List<ConsumerRecord<String, String>> generateRecords(int partition, int quantity) {
        HashMap<Integer, List<ConsumerRecord<String, String>>> integerListHashMap = generateRecords(partition, defaultKeys, quantity);
        Collection<List<ConsumerRecord<String, String>>> values = integerListHashMap.values();
        return flatten(values);
    }

    /**
     * Randomly create records for randomly selected keys until requested quantity is reached.
     */
    public HashMap<Integer, List<ConsumerRecord<String, String>>> generateRecords(int partition, List<Integer> keys, int quantity) {
        var keyRecords = new HashMap<Integer, List<ConsumerRecord<String, String>>>(quantity);
//        List<Integer> keyWork = UniLists.copyOf(keys);

        int globalCount = 0;
        while (globalCount < quantity) {
            Integer key = getRandomKey(keys);
            int recsCountForThisKey = (int) (Math.random() * quantity);
            String keyString = key.toString();
            final List<ConsumerRecord<String, String>> keyList = keyRecords.computeIfAbsent(key, (ignore) -> new ArrayList<>());
            int keyCount = keyList.size();
            String value = keyCount + "," + globalCount;
            ConsumerRecord<String, String> rec = makeRecord(partition, keyString, value);
//            var consumerRecords = generateRecordsForKey(key, recsCountForThisKey);
            keyList.add(rec);
//            keyRecords.put(key, consumerRecords);
            globalCount++;
        }
        return keyRecords;
    }
    public HashMap<Integer, List<ConsumerRecord<String, String>>> generateRecords(List<Integer> keys, int quantity) {
        return generateRecords(0, keys, quantity);
    }

        private Integer removeRandomKey(List<Integer> keyWork) {
        int i = (int) (random() * keyWork.size());
        Integer remove = keyWork.remove(i);
        return remove;
    }

    public ArrayList<ConsumerRecord<String, String>> generateRecordsForKey(Integer key, int quantity) {
        var records = new ArrayList<ConsumerRecord<String, String>>(quantity);
        for (long i : Range.range(quantity)) {
            var rec = makeRecord(key.toString(), i + "");
            records.add(rec);
        }
        return records;
    }

    public <KEY, VALUE> List<ConsumerRecord<KEY, VALUE>> flatten(Collection<List<ConsumerRecord<KEY, VALUE>>> listlist) {
        SortedSet<ConsumerRecord<KEY, VALUE>> all = new TreeSet<>(Comparator.comparing(ConsumerRecord::offset));
        for (Collection<ConsumerRecord<KEY, VALUE>> value : listlist) {
            all.addAll(value);
        }
        return new ArrayList<>(all);
    }

    @Setter
    @Getter
    private List<Integer> defaultKeys = Range.listOfIntegers(100);

    private Integer getRandomDefaultKey() {
        int i = (int) (random() * defaultKeys.size());
        return defaultKeys.get(i);
    }

    private Integer getRandomKey(List<Integer> keyList) {
        int i = (int) (random() * keyList.size());
        return keyList.get(i);
    }

    private ConsumerRecord<String, String> addRecord(String k, String v) {
        ConsumerRecord<String, String> record = makeRecord(k, v);
        consumerSpy.addRecord(record);
        return record;
    }

    public void send(MockConsumer<String, String> consumerSpy, HashMap<?, List<ConsumerRecord<String, String>>> records) {
        List<ConsumerRecord<String, String>> collect = records.entrySet().stream()
                .flatMap(x -> x.getValue().stream())
                .collect(Collectors.toList());
        send(consumerSpy, collect);
    }

    public void send(MockConsumer<String, String> consumerSpy, List<ConsumerRecord<String, String>> records) {
        log.debug("Sending {} more messages to the consumer stub", records.size());
        // send records in `correct` offset order as declared by the input data, regardless of the order of the input list
        List<ConsumerRecord<String, String>> sorted = new ArrayList<>(records);
        sorted.sort(Comparator.comparingLong(ConsumerRecord::offset));
        for (ConsumerRecord<String, String> record : sorted) {
            consumerSpy.addRecord(record);
        }
    }


    public static void completeWork(final WorkManager<String, String> wmm, List<WorkContainer<String, String>> work, long offset) {
        WorkContainer<String, String> workMatchingProvidedOffset = work.stream()
                .filter(x ->
                        x.getCr().offset() == offset
                )
                .findFirst().get();
        KafkaTestUtils.completeWork(wmm, workMatchingProvidedOffset);
    }

    public static void completeWork(final WorkManager<String, String> wmm, final WorkContainer<String, String> wc) {
        FutureTask future = new FutureTask<>(() -> true);
        future.run();
        assertThat(future).isDone();
        wc.setFuture(future);
        wc.onUserFunctionSuccess();
        wmm.onSuccessResult(wc);
        assertThat(wc.isUserFunctionComplete()).isTrue();
    }

    public List<ConsumerRecord<String, String>> sendRecords(final int i) {
        var consumerRecords = generateRecords(i);
        send(consumerSpy, consumerRecords);
        return consumerRecords;
    }

    /**
     * Checks that the ordering of the results is the same as the ordering of the input records.
     * <p>
     * <b>Only valid where a record cannot be REDELIVERED</b> - one instance, a {@code MockConsumer}, no
     * rebalance, drained before checking. It asserts a gapless {@code +1} value sequence of exactly the
     * produced size over the whole run, so the first at-least-once duplicate or post-revoke redelivery
     * fails it, correctly-behaving consumer or not. Under churn the equivalent check has to be scoped to
     * an ordering window, which is a different assertion with a different failure mode: see
     * {@code KeyOrderLedger} in {@code src/test-integration/.../integrationTests/chaostests}, and grep
     * {@code LEDGER_KEY_ORDER}. Do not reach for this one from a rebalance test.
     */
    // todo move to specific assertion utils class, along with other legacy assertion utils?
    public static <T> void checkExactOrdering(Map<String, Queue<PollContext<String, String>>> results,
                                              HashMap<Integer, List<T>> originalRecords) {
        originalRecords.entrySet().forEach(entry -> {
            var originalRecordList = entry.getValue();
            var originalKey = entry.getKey();
            var sequence = results.get(originalKey.toString());
            assertThat(sequence).hasSameSizeAs(originalRecordList);
            assertThat(sequence.size()).describedAs("Sanity: is same size as original list").isEqualTo(originalRecordList.size());
            log.debug("Key {} has same size of record as original - {}", originalKey, sequence.size());
            // check the integer sequence of PollContext value is linear and is without gaps
            var last = sequence.poll();
            PollContext<String, String> next = null;
            while (!sequence.isEmpty()) {
                next = sequence.poll();
                var thisValue = Integer.parseInt(org.apache.commons.lang3.StringUtils.substringBefore(next.value(), ","));
                var lastValuePlusOne = Integer.parseInt(StringUtils.substringBefore(last.value(), ",")) + 1;
                assertThat(thisValue).isEqualTo(lastValuePlusOne);
                last = next;
            }
            log.debug("Key {} a an exactly sequential series of values, ending in {} (starts at zero)", originalKey, next.value());

        });
    }
}
