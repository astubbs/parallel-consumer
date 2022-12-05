package io.confluent.parallelconsumer.internal;

import io.confluent.csid.utils.JavaUtils;
import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.state.Batch;
import io.confluent.parallelconsumer.state.ProcessingShard;
import io.confluent.parallelconsumer.state.WorkContainer;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;

import java.util.LinkedList;
import java.util.List;
import java.util.NavigableSet;
import java.util.TreeSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static io.confluent.parallelconsumer.state.ShardManager.retryQueueWorkContainerComparator;
import static lombok.AccessLevel.PRIVATE;

/**
 * @author Antony Stubbs
 */
@RequiredArgsConstructor
@FieldDefaults(makeFinal = true, level = PRIVATE)
public class CentralQueue<K, V> {

    ParallelConsumerOptions<?, ?> options;

    Batcher<WorkContainer<K, V>> batcher = new Batcher<>();

    BlockingQueue<Batch<WorkContainer<K, V>>> mainQueue = new LinkedBlockingQueue<>();

    // todo move comparator
    NavigableSet<WorkContainer<K, V>> retryQueue = new TreeSet<>(retryQueueWorkContainerComparator);

    public Batch<WorkContainer<K, V>> take() throws InterruptedException {
        if (retryQueue.isEmpty()) {
            return mainQueue.take();
        } else {
            return makeBatchHybrid();
        }
    }

    // todo actually make a hybrid of retries and norms?
    private Batch<WorkContainer<K, V>> makeBatchHybrid() {
        var atMostFromRetryQueue = getAtMostFromRetryQueue(options.getBatchSize());
        var batches = batcher.makeBatchesAsBatch(atMostFromRetryQueue);
        if (batches.size() != 1) {
            throw new IllegalStateException("Hybrid batcher should only return one batch");
        }
        var first = JavaUtils.getFirst(batches);
        return first.get();
    }

    private List<WorkContainer<K, V>> getAtMostFromRetryQueue(int batchSize) {
        var got = new LinkedList<WorkContainer<K, V>>();
        while (!retryQueue.isEmpty() && got.size() < batchSize) {
            var first = retryQueue.first();
            // todo check this is all you need to check, encapsulate
            if (first.isDelayPassed()) {
                got.add(first);
            }
        }
        return got;
    }

    public void queueWorkForProcessing(ProcessingShard<K, V>.WorkRequestResult workIfAvailable) {
        var workTaken = workIfAvailable.getWorkTaken();
        var batches = batcher.makeBatchesAsBatch(workTaken);
        mainQueue.addAll(batches);
    }

    public void addToRetry(WorkContainer<K, V> wc) {
//        var batch = batcher.makeBatchesAsBatch(wc);
        retryQueue.add(wc);
    }

    public void removeWorkFromShardFor(WorkContainer<K, V> consumerRecord) {
        retryQueue.remove(consumerRecord);
    }
}
