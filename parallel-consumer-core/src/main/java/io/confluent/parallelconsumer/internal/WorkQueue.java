package io.confluent.parallelconsumer.internal;

import io.confluent.parallelconsumer.state.WorkContainer;

import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import static io.confluent.parallelconsumer.internal.PCWorker.DEFUALT_QUEUE_SIZE;

public class WorkQueue<K, V> {

    BlockingQueue<List<WorkContainer<K, V>>> queue = new ArrayBlockingQueue<>(DEFUALT_QUEUE_SIZE);

    public int size() {
        return queue.size();
    }

    public List<WorkContainer<K, V>> take() throws InterruptedException {
        return queue.take();
    }

    public void add(List<WorkContainer<K, V>> work) {
        queue.add(work);
    }
}
