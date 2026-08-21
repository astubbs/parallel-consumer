package bz.stub.parallelconsumer.state;

/*-
 * Copyright (C) 2020-2024 Confluent, Inc.
 * Modifications Copyright (C) 2026 Antony Stubbs and contributors
 */

import com.google.common.truth.Truth;
import bz.stub.parallelconsumer.internal.utils.KafkaTestUtils;
import bz.stub.parallelconsumer.internal.utils.LongPollingMockConsumer;
import bz.stub.parallelconsumer.internal.utils.Range;
import bz.stub.parallelconsumer.FakeRuntimeException;
import bz.stub.parallelconsumer.ManagedTruth;
import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.internal.EpochAndRecordsMap;
import bz.stub.parallelconsumer.internal.PCModule;
import bz.stub.parallelconsumer.internal.PCModuleTestEnv;
import bz.stub.parallelconsumer.truth.CommitHistorySubject;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.assertj.core.api.AbstractListAssert;
import org.assertj.core.api.ObjectAssert;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.threeten.extra.MutableClock;
import pl.tlinkowski.unij.api.UniLists;
import pl.tlinkowski.unij.api.UniMaps;

import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

import static com.google.common.truth.Truth.assertWithMessage;
import static bz.stub.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.*;
import static java.time.Duration.ofSeconds;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static pl.tlinkowski.unij.api.UniLists.of;

/**
 * Needs to run in {@link ExecutionMode#SAME_THREAD} because it manipulates the static state in
 * {@link WorkContainer#setStaticModule(PCModule)}.
 *
 * @see WorkManager
 */
@Execution(ExecutionMode.SAME_THREAD)
@Slf4j
public class WorkManagerTest {

    public static final String INPUT_TOPIC = "input";
    public static final String OUTPUT_TOPIC = "output";

    WorkManager<String, String> wm;

    int offset;

    PCModuleTestEnv module;

    @BeforeEach
    public void setup() {
        var options = ParallelConsumerOptions.builder().build();
        setupWorkManager(options);
    }

    private MutableClock getClock() {
        return module.getMutableClock();
    }

    protected List<WorkContainer<String, String>> successfulWork = new ArrayList<>();

    private void setupWorkManager(ParallelConsumerOptions options) {
        offset = 0;

        var mockConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        var optsOverride = options.toBuilder().consumer(mockConsumer).build();

        module = new PCModuleTestEnv(optsOverride);

        wm = module.workManager();
        wm.getSuccessfulWorkListeners().add((work) -> {
            log.debug("Heard some successful work: {}", work);
            successfulWork.add(work);
        });

        module.setWorkManager(wm);
    }

    private void assignPartition(final int partition) {
        wm.onPartitionsAssigned(UniLists.of(topicPartitionOf(partition)));
    }

    @NotNull
    private TopicPartition topicPartitionOf(int partition) {
        return new TopicPartition(INPUT_TOPIC, partition);
    }

    private void registerSomeWork() {
        registerSomeWork(0);
    }

    /**
     * Adds 3 units of work
     */
    private void registerSomeWork(int partition) {
        assignPartition(partition);

        String key = "key-0";

        var rec0 = makeRec("0", key, partition);
        var rec1 = makeRec("1", key, partition);
        var rec2 = makeRec("2", key, partition);
        Map<TopicPartition, List<ConsumerRecord<String, String>>> m = new HashMap<>();
        m.put(topicPartitionOf(partition), of(rec0, rec1, rec2));
        var recs = new ConsumerRecords<>(m);
        wm.registerWork(new EpochAndRecordsMap(recs, wm.getPm()));
    }

    private ConsumerRecord<String, String> makeRec(String value, String key, int partition) {
        ConsumerRecord<String, String> stringStringConsumerRecord = new ConsumerRecord<>(INPUT_TOPIC, partition, offset, key, value);
        offset++;
        return stringStringConsumerRecord;
    }


    @ParameterizedTest
    @EnumSource
    void basic(ParallelConsumerOptions.ProcessingOrder order) {
        setupWorkManager(ParallelConsumerOptions.builder()
                .ordering(order)
                .build());
        registerSomeWork();

        //
        var gottenWork = wm.getWorkIfAvailable();

        if (order == UNORDERED) {
            assertThat(gottenWork).hasSize(3);
            assertOffsets(gottenWork, of(0, 1, 2));
        } else {
            assertThat(gottenWork).hasSize(1);
            assertOffsets(gottenWork, of(0));
        }

        //
        wm.onSuccessResult(gottenWork.get(0));

        //
        gottenWork = wm.getWorkIfAvailable();

        if (order == UNORDERED) {
            assertThat(gottenWork).isEmpty();
        } else {
            assertThat(gottenWork).hasSize(1);
            assertOffsets(gottenWork, of(1));
        }

        //
        gottenWork = wm.getWorkIfAvailable();
        assertThat(gottenWork).isEmpty();
    }

    @Test
    void testUnorderedAndDelayed() {
        setupWorkManager(ParallelConsumerOptions.builder()
                .ordering(UNORDERED)
                .build());
        registerSomeWork();

        int max = 2;

        {
            var workRetrieved = wm.getWorkIfAvailable(max);
            assertThat(workRetrieved).hasSize(2);
            assertOffsets(workRetrieved, of(0, 1));

            // pass first, fail second
            WorkContainer<String, String> succeed = workRetrieved.get(0);
            succeed(succeed);
            WorkContainer<String, String> fail = workRetrieved.get(1);
            fail(fail);
        }

        {
            var workRetrieved = wm.getWorkIfAvailable(max);
            assertOffsets(workRetrieved, of(2),
                    "no order restriction, 1's delay won't have passed - should get remaining in queue not yet failed");

            WorkContainer<String, String> succeed = workRetrieved.get(0);
            succeed(succeed);
        }

        {
            var workRetrieved = wm.getWorkIfAvailable(max);
            assertOffsets(workRetrieved, of(), "delay won't have passed so should not retrieve anything");

            advanceClockBySlightlyLessThanDelay();
        }

        {
            var workRetrieved = wm.getWorkIfAvailable(max);
            assertOffsets(workRetrieved, of());

            advanceClockByDelay();
        }

        {
            var workRetrieved = wm.getWorkIfAvailable(max);
            assertOffsets(workRetrieved, of(1),
                    "should retrieve 1 given clock has been advanced and retry delay should be over");
            WorkContainer<String, String> succeed = workRetrieved.get(0);
            succeed(succeed);
        }

        assertThat(successfulWork)
                .extracting(x -> (int) x.getCr().offset())
                .isEqualTo(of(0, 2, 1));
    }

    private void succeed(WorkContainer<String, String> succeed) {
        succeed.onUserFunctionSuccess();
        wm.onSuccessResult(succeed);
    }

    private void succeed(Iterable<WorkContainer<String, String>> succeed) {
        succeed.forEach(this::succeed);
    }

    /**
     * Checks the offsets of the work, matches the offsets in the provided list
     *
     * @deprecated use {@link CommitHistorySubject} or similar instead
     */
    @Deprecated
    private AbstractListAssert<?, List<? extends Integer>, Integer, ObjectAssert<Integer>>
    assertOffsets(List<WorkContainer<String, String>> works, List<Integer> expected, String msg) {
        return assertThat(works)
                .as(msg)
                .extracting(x -> (int) x.getCr().offset())
                .isEqualTo(expected);
    }

    private AbstractListAssert<?, List<? extends Integer>, Integer, ObjectAssert<Integer>>
    assertOffsets(List<WorkContainer<String, String>> works, List<Integer> expected) {
        return assertOffsets(works, expected, "offsets of work given");
    }

    @Test
    public void testOrderedInFlightShouldBlockQueue() {
        ParallelConsumerOptions build = ParallelConsumerOptions.builder().ordering(PARTITION).build();
        setupWorkManager(build);

        assertThat(wm.getOptions().getOrdering()).isEqualTo(PARTITION);

        registerSomeWork();

        int max = 2;

        var works = wm.getWorkIfAvailable(max);
        assertOffsets(works, of(0));
        var w = works.get(0);

        works = wm.getWorkIfAvailable(max);
        assertOffsets(works, of()); // should be blocked by in flight

        succeed(w);

        works = wm.getWorkIfAvailable(max);
        assertOffsets(works, of(1));
    }

    /**
     * Tests failed work delay
     */
    @Test
    void testOrderedAndDelayed() {
        ParallelConsumerOptions<?, ?> build = ParallelConsumerOptions.builder().ordering(PARTITION).build();
        setupWorkManager(build);

        // sanity
        assertThat(wm.getOptions().getOrdering()).isEqualTo(PARTITION);

        registerSomeWork();

        int maxWorkToGet = 2;

        var works = wm.getWorkIfAvailable(maxWorkToGet);

        assertOffsets(works, of(0));

        // fail the work
        var wc = works.get(0);
        fail(wc);

        // nothing available to get
        works = wm.getWorkIfAvailable(maxWorkToGet);
        assertOffsets(works, of());

        // advance clock to make delay pass
        advanceClockByDelay();

        // work should now be ready to take
        works = wm.getWorkIfAvailable(maxWorkToGet);
        assertOffsets(works, of(0));

        wc = works.get(0);
        fail(wc);

        advanceClock(wc.getRetryDelayConfig().minus(ofSeconds(1)));

        works = wm.getWorkIfAvailable(maxWorkToGet);
        assertOffsets(works, of());

        // increased advance to allow for bigger delay under high load during parallel test execution.
        advanceClock(wc.getRetryDelayConfig().plus(ofSeconds(1)));

        works = wm.getWorkIfAvailable(maxWorkToGet);
        assertOffsets(works, of(0));
        succeed(works.get(0));

        assertOffsets(successfulWork, of(0));

        works = wm.getWorkIfAvailable(maxWorkToGet);
        assertOffsets(works, of(1));
        succeed(works.get(0));

        works = wm.getWorkIfAvailable(maxWorkToGet);
        assertOffsets(works, of(2));
        succeed(works.get(0));

        // check all published in the end
        assertOffsets(successfulWork, of(0, 1, 2));
    }

    @Test
    void containerDelay() {
        var wc = new WorkContainer<String, String>(0, mock(ConsumerRecord.class), module);
        assertThat(wc.isDelayPassed()).isTrue(); // when new, there's no delay
        wc.onUserFunctionFailure(new FakeRuntimeException(""));
        assertThat(wc.isDelayPassed()).isFalse();
        advanceClockBySlightlyLessThanDelay();
        assertThat(wc.isDelayPassed()).isFalse();
        advanceClockByDelay();
        ManagedTruth.assertThat(wc).isDelayPassed();
    }

    private void advanceClockBySlightlyLessThanDelay() {
        Duration retryDelay = module.options().getDefaultMessageRetryDelay();
        Duration duration = retryDelay.dividedBy(2);
        getClock().add(duration);
    }

    private void advanceClockByDelay() {
        Duration retryDelay = module.options().getDefaultMessageRetryDelay();
        getClock().add(retryDelay);
    }

    private void advanceClock(Duration by) {
        getClock().add(by);
    }

    @Test
    void insertWrongOrderPreservesOffsetOrdering() {
        ParallelConsumerOptions<?, ?> build = ParallelConsumerOptions.builder().ordering(UNORDERED).build();
        setupWorkManager(build);

        assertThat(wm.getOptions().getOrdering()).isEqualTo(UNORDERED);

        registerSomeWork();

        String key = "key";
        int partition = 0;

        // mess with offset order for insertion
        var rec = new ConsumerRecord<>(INPUT_TOPIC, partition, 10, key, "value");
        var rec2 = new ConsumerRecord<>(INPUT_TOPIC, partition, 6, key, "value");
        var rec3 = new ConsumerRecord<>(INPUT_TOPIC, partition, 8, key, "value");
        Map<TopicPartition, List<ConsumerRecord<String, String>>> m = new HashMap<>();
        m.put(topicPartitionOf(partition), of(rec2, rec3, rec));
        var recs = new ConsumerRecords<>(m);

        //
        registerWork(recs);

        int max = 10;

        var works = wm.getWorkIfAvailable(4);
        assertOffsets(works, of(0, 1, 2, 6));

        // fail some
        fail(works.get(1));
        fail(works.get(3));

        //
        works = wm.getWorkIfAvailable(max);
        assertOffsets(works, of(8, 10));

        //
        advanceClockByDelay();

        //
        works = wm.getWorkIfAvailable(max);
        assertOffsets(works, of(1, 6));
    }

    private void registerWork(ConsumerRecords<String, String> recs) {
        wm.registerWork(new EpochAndRecordsMap<>(recs, wm.getPm()));
    }


    private void fail(WorkContainer<String, String> wc) {
        wc.onUserFunctionFailure(null);
        wm.onFailureResult(wc);
    }

    @Test
    public void maxInFlight() {
        //
        var opts = ParallelConsumerOptions.builder();
        setupWorkManager(opts.build());

        //
        registerSomeWork();

        //
        assertThat(wm.getWorkIfAvailable()).hasSize(1);
        assertThat(wm.getWorkIfAvailable()).isEmpty();
    }

    public static class FluentQueue<T> implements Iterable<T> {
        ArrayDeque<T> work = new ArrayDeque<>();

        Collection<T> add(Collection<T> c) {
            work.addAll(c);
            return c;
        }

        public T poll() {
            return work.poll();
        }

        @Override
        public Iterator<T> iterator() {
            return work.iterator();
        }

        public int size() {
            return work.size();
        }
    }

    @Test
    void orderedByPartitionsParallel() {
        ParallelConsumerOptions<?, ?> build = ParallelConsumerOptions.builder()
                .ordering(PARTITION)
                .build();
        setupWorkManager(build);

        registerSomeWork();

        var partition = 2;
        assignPartition(2);
        var rec = new ConsumerRecord<>(INPUT_TOPIC, partition, 10, "66", "value");
        var rec2 = new ConsumerRecord<>(INPUT_TOPIC, partition, 6, "66", "value");
        var rec3 = new ConsumerRecord<>(INPUT_TOPIC, partition, 8, "66", "value");
        Map<TopicPartition, List<ConsumerRecord<String, String>>> m = new HashMap<>();
        m.put(topicPartitionOf(partition), of(rec2, rec3, rec));
        var recs = new ConsumerRecords<>(m);

        //
        registerWork(recs);

        //
        var works = wm.getWorkIfAvailable();
        assertOffsets(works, of(0, 6));
        successAll(works);

        //
        works = wm.getWorkIfAvailable();
        assertOffsets(works, of(1, 8));
        successAll(works);

        //
        works = wm.getWorkIfAvailable();
        assertOffsets(works, of(2, 10));
        successAll(works);
    }

    private void successAll(List<WorkContainer<String, String>> works) {
        for (WorkContainer<String, String> work : works) {
            wm.onSuccessResult(work);
        }
    }

    @Test
    void orderedByKeyParallel() {
        var build = ParallelConsumerOptions.builder().ordering(KEY).build();
        setupWorkManager(build);

        assertThat(wm.getOptions().getOrdering()).isEqualTo(KEY);

        registerSomeWork();

        var partition = 2;
        assignPartition(2);
        var rec2 = new ConsumerRecord<>(INPUT_TOPIC, partition, 6, "key-a", "value");
        var rec3 = new ConsumerRecord<>(INPUT_TOPIC, partition, 8, "key-b", "value");
        var rec0 = new ConsumerRecord<>(INPUT_TOPIC, partition, 10, "key-a", "value");
        var rec4 = new ConsumerRecord<>(INPUT_TOPIC, partition, 12, "key-c", "value");
        var rec5 = new ConsumerRecord<>(INPUT_TOPIC, partition, 15, "key-a", "value");
        var rec6 = new ConsumerRecord<>(INPUT_TOPIC, partition, 20, "key-c", "value");
        Map<TopicPartition, List<ConsumerRecord<String, String>>> m = new HashMap<>();
        m.put(topicPartitionOf(partition), of(rec2, rec3, rec0, rec4, rec5, rec6));
        var recs = new ConsumerRecords<>(m);

        //
        registerWork(recs);

        //
        var works = wm.getWorkIfAvailable();
        works.sort(Comparator.naturalOrder()); // we actually don't care about the order
        // one record per key
        assertOffsets(works, of(0, 6, 8, 12));
        successAll(works);

        //
        works = wm.getWorkIfAvailable();
        works.sort(Comparator.naturalOrder());
        assertOffsets(works, of(1, 10, 20));
        successAll(works);

        //
        works = wm.getWorkIfAvailable();
        works.sort(Comparator.naturalOrder());
        assertOffsets(works, of(2, 15));
        successAll(works);

        works = wm.getWorkIfAvailable();
        assertOffsets(works, of());
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 5, 10, 20, 30, 50, 1000})
    void highVolumeKeyOrder(int quantity) {
        int uniqueKeys = 100;

        var build = ParallelConsumerOptions.builder()
                .ordering(KEY)
                .build();
        setupWorkManager(build);

        KafkaTestUtils ktu = new KafkaTestUtils(INPUT_TOPIC, null, new LongPollingMockConsumer<>(OffsetResetStrategy.EARLIEST));

        List<Integer> keys = Range.listOfIntegers(uniqueKeys);

        var records = ktu.generateRecords(keys, quantity);
        var flattened = ktu.flatten(records.values());

        int partition = 0;
        var recs = new ConsumerRecords<>(UniMaps.of(topicPartitionOf(partition), flattened));

        assignPartition(partition);

        //
        registerWork(recs);

        //
        long awaiting = wm.getSm().getNumberOfWorkQueuedInShardsAwaitingSelection();
        assertThat(awaiting).isEqualTo(quantity);

        //
        List<WorkContainer<String, String>> work = wm.getWorkIfAvailable();

        //
        ManagedTruth.assertTruth(work).hasSameSizeAs(records);
    }

    @Test
    void treeMapOrderingCorrect() {
        KafkaTestUtils ktu = new KafkaTestUtils(INPUT_TOPIC, null, new LongPollingMockConsumer<>(OffsetResetStrategy.EARLIEST));

        int i = 10;
        var records = ktu.generateRecords(i);

        var treeMap = new TreeMap<Long, WorkContainer<String, String>>();
        for (ConsumerRecord<String, String> record : records) {
            treeMap.put(record.offset(), new WorkContainer<>(0, record, mock(PCModuleTestEnv.class)));
        }

        // read back, assert correct order
        NavigableSet<Long> ascendingOrder = treeMap.navigableKeySet();
        Object[] objects = ascendingOrder.toArray();

        assertThat(objects).containsExactly(0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L);
    }

    /**
     * Checks work management is correct in this respect.
     */
    @Test
    void workQueuesEmptyWhenAllWorkComplete() {
        var build = ParallelConsumerOptions.builder()
                .ordering(UNORDERED)
                .build();
        setupWorkManager(build);
        registerSomeWork();

        //
        var work = wm.getWorkIfAvailable();
        assertThat(work).hasSize(3);

        //
        succeed(work);

        //
        assertThat(wm.getSm().getNumberOfWorkQueuedInShardsAwaitingSelection()).isZero();
        assertThat(wm.getNumberOfIncompleteOffsets()).as("Partition commit queues are now empty").isZero();

        // drain commit queue
        var completedFutureOffsets = wm.collectCommitDataForDirtyPartitions();
        assertThat(completedFutureOffsets).hasSize(1); // coalesces (see log)
        var sync = completedFutureOffsets.values().stream().findFirst().get();
        Truth.assertThat(sync.offset()).isEqualTo(3);
        Truth.assertThat(sync.metadata()).isEmpty();
        PartitionState<String, String> state = wm.getPm().getPartitionState(topicPartitionOf(0));
        Truth.assertThat(state.getAllIncompleteOffsets()).isEmpty();
    }

    /**
     * Tests that the resuming iterator is used correctly
     */
    @ParameterizedTest
    @EnumSource
    void resumesFromNextShard(ParallelConsumerOptions.ProcessingOrder order) {
        Assumptions.assumeFalse(order == KEY); // just want to test ordered vs unordered

        ParallelConsumerOptions<?, ?> build = ParallelConsumerOptions.builder()
                .ordering(order)
                .build();
        setupWorkManager(build);

        registerSomeWork();

        assignPartition(1);
        assignPartition(2);
        Map<TopicPartition, List<ConsumerRecord<String, String>>> m = new HashMap<>();
        var rec = new ConsumerRecord<>(INPUT_TOPIC, 1, 11, "11", "value");
        m.put(topicPartitionOf(1), of(rec));
        var rec2 = new ConsumerRecord<>(INPUT_TOPIC, 2, 21, "21", "value");
        m.put(topicPartitionOf(2), of(rec2));
        var recs = new ConsumerRecords<>(m);
        registerWork(recs);

//        // force ingestion of records - see refactor: Queue unification confluentinc#219
//        wm.tryToEnsureQuantityOfWorkQueuedAvailable(100);

        var workContainersOne = wm.getWorkIfAvailable(1);
        var workContainersTwo = wm.getWorkIfAvailable(1);
        var workContainersThree = wm.getWorkIfAvailable(1);
        var workContainersFour = wm.getWorkIfAvailable(1);

        Truth.assertThat(workContainersOne).hasSize(1);
        Truth.assertThat(workContainersOne.stream().findFirst().get().getTopicPartition().partition()).isEqualTo(0);
        Truth.assertThat(workContainersTwo).hasSize(1);
        Truth.assertThat(workContainersTwo.stream().findFirst().get().getTopicPartition().partition()).isEqualTo(1);
        Truth.assertThat(workContainersThree).hasSize(1);
        Truth.assertThat(workContainersThree.stream().findFirst().get().getTopicPartition().partition()).isEqualTo(2);

        if (order == PARTITION) {
            Truth.assertThat(workContainersFour).isEmpty();
        } else {
            Truth.assertThat(workContainersFour).hasSize(1);
            Optional<WorkContainer<String, String>> work = workContainersFour.stream().findFirst();
            Truth.assertThat(work.get().getTopicPartition().partition()).isEqualTo(0);
            Truth.assertThat(work.get().offset()).isEqualTo(1);
            Truth.assertThat(work.get().getCr().value()).isEqualTo("1");
        }
    }


    /**
     * Checks that when using shards are not starved when there's enough work queued to satisfy poll request from the
     * initial request (without needing to iterate to other shards)
     *
     * @see <a href="https://github.com/confluentinc/parallel-consumer/issues/236">#236</a> Under some conditions, a
     *         shard (by partition or key), can get starved for attention
     */
    @Test
    void starvation() {
        setupWorkManager(ParallelConsumerOptions.builder()
                .ordering(PARTITION)
                .build());

        registerSomeWork(0);
        registerSomeWork(1);
        registerSomeWork(2);

        var allWork = new ArrayList<WorkContainer<String, String>>();

        {
            var work = wm.getWorkIfAvailable(2);
            allWork.addAll(work);

            assertWithMessage("Should be able to get 2 records of work, one from each partition shard")
                    .that(work).hasSize(2);

            //
            var tpOne = work.get(0).getTopicPartition();
            var tpTwo = work.get(1).getTopicPartition();
            assertWithMessage("The partitions should be different")
                    .that(tpOne).isNotEqualTo(tpTwo);

        }

        {
            var work = wm.getWorkIfAvailable(2);
            assertWithMessage("Should be able to get only 1 more, from the third shard")
                    .that(work).hasSize(1);
            allWork.addAll(work);

            //
            var tpOne = work.get(0).getTopicPartition();
        }

        assertWithMessage("TPs all unique")
                .that(allWork.stream()
                        .map(WorkContainer::getTopicPartition)
                        .collect(Collectors.toList()))
                .containsNoDuplicates();

    }

    /**
     * Tests available worker cnt
     */
    @Test
    void testAvailableWorkerCnt() {
        ParallelConsumerOptions<?, ?> build = ParallelConsumerOptions.builder().ordering(PARTITION).build();
        setupWorkManager(build);
        // sanity
        assertThat(wm.getOptions().getOrdering()).isEqualTo(PARTITION);

        registerSomeWork();

        int total = 3;
        int maxWorkToGet = 2;

        var works = wm.getWorkIfAvailable(maxWorkToGet);

        assertThat(wm.getNumberOfWorkQueuedInShardsAwaitingSelection()).isEqualTo(total - works.size());

        // fail the work
        var wc = works.get(0);
        fail(wc);


        // advance clock to make delay pass - the mock clock is the only time source the retry path reads,
        // so there is nothing to wait for. This used to also sleep a real second and hope.
        advanceClockByDelay();

        // work should now be ready to take
        works = wm.getWorkIfAvailable(maxWorkToGet);
        assertThat(wm.getNumberOfWorkQueuedInShardsAwaitingSelection()).isEqualTo(total - works.size());

    }


    // -----------------------------------------------------------------------------------------------------
    // Verdict-free work return (astubbs#242) - a record can go back to scheduling with no verdict at all,
    // without the return counting as a processing attempt. Used by external engines when the process holding
    // a record disappears before reporting on it.
    // -----------------------------------------------------------------------------------------------------

    /**
     * Returns work through the real dispatch path rather than calling the result handler directly, so the
     * branch selection in {@link WorkManager#handleFutureResult} is what is under test.
     */
    private void abandon(WorkContainer<String, String> wc) {
        wc.markAbandoned(wc.getDeliveryCount());
        wm.handleFutureResult(wc);
    }

    private void setupUnordered() {
        setupWorkManager(ParallelConsumerOptions.builder().ordering(UNORDERED).build());
    }

    @Test
    void abandonedWorkBecomesSelectableAgain() {
        setupUnordered();
        registerSomeWork();

        var taken = wm.getWorkIfAvailable(1);
        assertThat(taken).hasSize(1);
        var wc = taken.get(0);

        abandon(wc);

        var retaken = wm.getWorkIfAvailable(1);
        assertThat(retaken)
                .as("work returned without a verdict is immediately selectable again - it earned no retry delay")
                .hasSize(1);
        assertThat(retaken.get(0).offset()).isEqualTo(wc.offset());
    }

    @Test
    void abandoningDoesNotConsumeARetryAttempt() {
        setupUnordered();
        registerSomeWork();

        var wc = wm.getWorkIfAvailable(1).get(0);
        assertThat(wc.getNumberOfFailedAttempts()).isEqualTo(0);

        abandon(wc);

        assertThat(wc.getNumberOfFailedAttempts())
                .as("a dropped connection is not a processing attempt")
                .isEqualTo(0);
        assertThat(wc.getLastFailedAt()).isEmpty();
    }

    /**
     * Covers AE9: a worker killed while holding a record on its second attempt must see that record redelivered
     * still reporting one prior failure, not two.
     */
    @Test
    void abandonAfterAPriorFailureKeepsTheAttemptCount() {
        setupUnordered();
        registerSomeWork();

        var wc = wm.getWorkIfAvailable(1).get(0);
        fail(wc);
        assertThat(wc.getNumberOfFailedAttempts()).isEqualTo(1);

        advanceClockByDelay();
        var retaken = wm.getWorkIfAvailable(1);
        assertThat(retaken).hasSize(1);
        var second = retaken.get(0);
        assertThat(second.offset()).isEqualTo(wc.offset());

        abandon(second);

        assertThat(second.getNumberOfFailedAttempts())
                .as("attempt count is unchanged by a verdict-free return")
                .isEqualTo(1);

        // Without clearing the stale verdict on redelivery, this record still carries
        // maybeUserFunctionSucceeded == false from its earlier failure, takes the failure path, and lands in the
        // retry queue behind a delay it never earned - so it would NOT be selectable here.
        var third = wm.getWorkIfAvailable(1);
        assertThat(third)
                .as("selectable immediately - an abandoned record earns no retry delay, even after a prior failure")
                .hasSize(1);
        assertThat(third.get(0).getNumberOfFailedAttempts())
                .as("redelivered still reporting one prior failure, not two")
                .isEqualTo(1);
    }

    /**
     * The in-flight counter gates the broker poller. Drift in it stalls the consumer silently while it still
     * looks alive, so this asserts the exact number rather than merely that work keeps flowing.
     */
    @Test
    void abandonReturnsTheInFlightCounterToItsPreviousValue() {
        setupUnordered();
        registerSomeWork();

        long baseline = wm.getNumberRecordsOutForProcessing();
        assertThat(baseline).isEqualTo(0);

        var taken = wm.getWorkIfAvailable(2);
        assertThat(taken).hasSize(2);
        assertThat(wm.getNumberRecordsOutForProcessing()).isEqualTo(2);

        abandon(taken.get(0));
        assertThat(wm.getNumberRecordsOutForProcessing())
                .as("exactly one decrement per abandoned record")
                .isEqualTo(1);

        abandon(taken.get(1));
        assertThat(wm.getNumberRecordsOutForProcessing())
                .as("counter is back to where it started, with no work lost")
                .isEqualTo(baseline);
    }

    @Test
    void aDuplicateReturnAfterRedeliveryIsIgnored() {
        setupUnordered();
        registerSomeWork();

        var wc = wm.getWorkIfAvailable(1).get(0);
        long firstDelivery = wc.getDeliveryCount();
        assertThat(wm.getNumberRecordsOutForProcessing()).isEqualTo(1);

        abandon(wc);
        assertThat(wm.getNumberRecordsOutForProcessing()).isEqualTo(0);

        // The control loop drains returns and re-selects in the same iteration, so by the time a duplicate
        // arrives the record is live again on a later delivery.
        var redelivered = wm.getWorkIfAvailable(1);
        assertThat(redelivered).hasSize(1);
        assertThat(redelivered.get(0)).isSameAs(wc);
        assertThat(wm.getNumberRecordsOutForProcessing()).isEqualTo(1);

        // the late duplicate, carrying the delivery it was actually raised for
        wc.markAbandoned(firstDelivery);
        wm.handleFutureResult(wc);

        assertThat(wm.getNumberRecordsOutForProcessing())
                .as("the superseded return is ignored - it must not end the live delivery's flight")
                .isEqualTo(1);
        assertThat(wc.isInFlight())
                .as("the live delivery is still in flight")
                .isTrue();
    }

    @Test
    void aSupersededReturnDoesNotOrphanTheAwaitingSelectionCount() {
        setupUnordered();
        registerSomeWork();

        var wc = wm.getWorkIfAvailable(1).get(0);
        long firstDelivery = wc.getDeliveryCount();
        abandon(wc);

        var redelivered = wm.getWorkIfAvailable(1).get(0);
        wc.markAbandoned(firstDelivery);
        wm.handleFutureResult(wc);

        // the live delivery now completes normally
        succeed(redelivered);

        assertThat(wm.getNumberRecordsOutForProcessing())
                .as("counter nets out - a superseded return must not decrement")
                .isEqualTo(0);
        assertThat(wm.getNumberOfWorkQueuedInShardsAwaitingSelection())
                .as("two of three records remain selectable; a superseded return must not orphan an increment")
                .isEqualTo(2);
    }

    @Test
    void aSupersededReturnOnARevokedPartitionDoesNotDecrement() {
        setupUnordered();
        registerSomeWork();

        var wc = wm.getWorkIfAvailable(1).get(0);
        long firstDelivery = wc.getDeliveryCount();
        abandon(wc);
        var redelivered = wm.getWorkIfAvailable(1).get(0);
        assertThat(wm.getNumberRecordsOutForProcessing()).isEqualTo(1);

        // the partition goes away while the duplicate is still in flight
        wm.onPartitionsRevoked(UniLists.of(topicPartitionOf(0)));

        wc.markAbandoned(firstDelivery);
        wm.handleFutureResult(wc);

        assertThat(wm.getNumberRecordsOutForProcessing())
                .as("the superseded check runs before the stale-partition branch, which decrements unconditionally")
                .isEqualTo(1);
    }

    @Test
    void workWithNeitherVerdictNorAbandonMarkerStillThrows() {
        setupUnordered();
        registerSomeWork();

        var wc = wm.getWorkIfAvailable(1).get(0);

        assertThatThrownBy(() -> wm.handleFutureResult(wc))
                .as("an empty verdict with no abandon marker is still the bug it always was")
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("without a success flag");
    }

    /**
     * Covers AE15: every worker disconnecting is not an end-of-life signal. The returned records stay in
     * scheduling rather than being dropped or retried.
     */
    @Test
    void everyWorkerDisconnectingLeavesAllWorkInScheduling() {
        setupUnordered();
        registerSomeWork();

        var taken = wm.getWorkIfAvailable(3);
        assertThat(taken).hasSize(3);
        assertThat(wm.getNumberOfWorkQueuedInShardsAwaitingSelection()).isEqualTo(0);

        // the whole fleet goes away without reporting on anything
        taken.forEach(this::abandon);

        assertThat(wm.getNumberRecordsOutForProcessing()).isEqualTo(0);
        assertThat(wm.getNumberOfWorkQueuedInShardsAwaitingSelection())
                .as("all three records are back awaiting selection, none discarded")
                .isEqualTo(3);

        var retaken = wm.getWorkIfAvailable(3);
        assertThat(retaken)
                .as("and all three are handed out again once a worker reconnects")
                .hasSize(3);
    }

    /**
     * Revoking a partition whose record is parked in retry back-off used to leave the shard's available-work
     * counter permanently one too high: {@link ProcessingShard#remove(long)} only deducted for records that were
     * {@code isAvailableToTakeAsWork()}, and a record waiting out a retry delay is not - even though
     * {@link ProcessingShard#markAvailableAgain()} had already counted it. The retry queue entry, which is what
     * cancels that increment out in
     * {@link ShardManager#getNumberOfWorkQueuedInShardsAwaitingSelection()}, is removed on the same path, so the
     * increment is left with nothing to offset it.
     * <p>
     * Drift in this direction is the one the clamp never caught, and it throttles record intake and can stop
     * {@code drain()} from ever transitioning to closing.
     */
    @Test
    void revokingARecordParkedInRetryBackoffLeavesNoPhantomAwaitingSelection() {
        setupUnordered();
        registerSomeWork();

        var taken = wm.getWorkIfAvailable(3);
        assertThat(taken).hasSize(3);

        // one record parked in retry back-off, the rest gone
        fail(taken.get(0));
        succeed(taken.get(1));
        succeed(taken.get(2));

        assertThat(wm.getNumberOfWorkQueuedInShardsAwaitingSelection())
                .as("the one surviving record is waiting out its retry delay, so nothing is selectable yet")
                .isZero();

        wm.onPartitionsRevoked(UniLists.of(topicPartitionOf(0)));

        assertThat(wm.getSm().sumOfShardAvailableCounters())
                .as("the shard holds no records at all, so its raw available counter must be zero - not clamped to it")
                .isZero();
        assertThat(wm.getNumberOfWorkQueuedInShardsAwaitingSelection())
                .as("the partition is gone; there is no record left anywhere to select")
                .isZero();
    }

    /**
     * The invariant the conservation figure exists to hold: {@code admitted - retired} must equal the number of
     * records the shards are actually holding, after every way a record can arrive and every way it can leave.
     * <p>
     * A conservation figure is only as good as its enumeration of the departure paths - miss one and it leaks,
     * with no clamp to catch it - so this drives all of them through {@link WorkManager} in one sequence:
     * success, failure into retry back-off, retry redelivery, abandonment without a verdict, a stale sweep
     * triggered by a partition being re-assigned underneath live work, a partition revocation that takes both a
     * parked record and one still out at a worker, and the stale result that worker later hands back.
     */
    @Test
    void theConservationFigureMatchesTheShardsThroughEveryKindOfDeparture() {
        setupUnordered();

        registerSomeWork(0);
        registerSomeWork(1);

        assertConservationHolds("after admitting two partitions' worth of records");
        assertThat(wm.getNumberOfRecordsInShards()).isEqualTo(6);

        var taken = wm.getWorkIfAvailable(6);
        assertThat(taken).hasSize(6);
        assertConservationHolds("with every record out at a worker");
        assertThat(wm.getNumberOfRecordsInShards())
                .as("selection is not a departure - the record is still the system's responsibility")
                .isEqualTo(6);

        var partitionZero = workOn(taken, 0);
        var partitionOne = workOn(taken, 1);

        // the ordinary departure
        succeed(partitionZero.get(0));
        assertConservationHolds("after a success");
        assertThat(wm.getNumberOfRecordsInShards()).isEqualTo(5);

        // failure parks the record for retry - it is still held, so still in the system
        fail(partitionZero.get(1));
        assertConservationHolds("after a failure");
        assertThat(wm.getNumberOfRecordsInShards()).isEqualTo(5);

        // returned with no verdict at all - immediately selectable again, also still held
        abandon(partitionZero.get(2));
        assertConservationHolds("after an abandonment");
        assertThat(wm.getNumberOfRecordsInShards()).isEqualTo(5);

        // the retry falls due and the abandoned record is selectable, so both go back out
        advanceClockByDelay();
        var redelivered = wm.getWorkIfAvailable(6);
        assertThat(redelivered)
                .as("the parked retry and the abandoned record are both selectable again")
                .hasSize(2);
        assertConservationHolds("after a retry redelivery");
        assertThat(wm.getNumberOfRecordsInShards()).isEqualTo(5);

        // partition 0 is re-assigned underneath its live work, bumping the epoch: everything still held for it
        // is now stale and gets swept, including records that are out at a worker right now
        assignPartition(0);
        assertConservationHolds("after a stale sweep took records that were out at a worker");
        assertThat(wm.getNumberOfRecordsInShards())
                .as("both of partition 0's remaining records were swept as stale")
                .isEqualTo(3);

        // partition 1 goes away holding one successful record's worth of history, one parked retry and one
        // record still out at a worker
        succeed(partitionOne.get(0));
        fail(partitionOne.get(1));
        assertConservationHolds("before revoking partition 1");
        assertThat(wm.getNumberOfRecordsInShards()).isEqualTo(2);

        wm.onPartitionsRevoked(UniLists.of(topicPartitionOf(1)));
        assertConservationHolds("after a revocation that took a parked record and one out at a worker");
        assertThat(wm.getNumberOfRecordsInShards())
                .as("the revoked partition's records are gone, however they were being held")
                .isZero();

        // the worker finally reports on the record whose partition was revoked out from under it
        wm.handleFutureResult(partitionOne.get(2));
        assertConservationHolds("after a stale result was handed back");
        assertThat(wm.getNumberOfRecordsInShards())
                .as("the record was already retired at revocation - a stale return must not retire it twice")
                .isZero();

        var population = wm.getSm().getRecordPopulation();
        assertThat(population.getAdmittedTotal())
                .as("six records were admitted, once each")
                .isEqualTo(6);
        assertThat(population.getRetiredTotal())
                .as("and every one of them was retired exactly once, by whichever path took it")
                .isEqualTo(6);
        assertThat(wm.getSm().sumOfShardAvailableCounters())
                .as("the shards are empty, so the raw available counter must be exactly zero without a clamp")
                .isZero();
    }

    /**
     * The load gate reads the conservation figure, so it has to agree with the shards' real contents rather than
     * with a counter that describes them - including in the revoke-a-parked-retry sequence that leaves the old
     * available-work counter high.
     */
    @Test
    void theLoadGateAgreesWithTheShardsAfterARevocation() {
        setupUnordered();
        registerSomeWork();

        var taken = wm.getWorkIfAvailable(3);
        fail(taken.get(0));
        succeed(taken.get(1));
        succeed(taken.get(2));

        assertThat(wm.getNumberOfWorkableRecordsInSystem())
                .as("one record is held, but parked in retry back-off, so it is not workable")
                .isZero();

        wm.onPartitionsRevoked(UniLists.of(topicPartitionOf(0)));

        assertConservationHolds("after revoking a partition holding a parked retry");
        assertThat(wm.getNumberOfWorkableRecordsInSystem())
                .as("nothing is held any more, so the poller must not be told the pipeline is loaded")
                .isZero();
        assertThat(wm.isSufficientlyLoaded())
                .as("an empty system is never sufficiently loaded")
                .isFalse();
    }

    /**
     * Holds the O(1) conservation figure against an O(n) scan of what the shards actually contain. The two are
     * deliberately computed by different means - if they can be made to disagree, the conservation figure has a
     * departure path it does not know about.
     */
    private void assertConservationHolds(String stage) {
        assertThat(wm.getNumberOfRecordsInShards())
                .as("conservation figure (admitted - retired) vs a scan of the shards, %s", stage)
                .isEqualTo(wm.getSm().countRecordsInShardsByScan());
    }

    private List<WorkContainer<String, String>> workOn(List<WorkContainer<String, String>> work, int partition) {
        var onPartition = work.stream()
                .filter(wc -> wc.getTopicPartition().partition() == partition)
                .sorted(Comparator.comparingLong(WorkContainer::offset))
                .collect(Collectors.toList());
        assertThat(onPartition).hasSize(3);
        return onPartition;
    }

}
