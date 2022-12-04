package io.confluent.parallelconsumer.internal;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.parallelconsumer.state.ProcessingShard;
import io.confluent.parallelconsumer.state.WorkContainer;
import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @author Antony Stubbs
 */
@Slf4j
@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
public class WorkQueue<K, V> {

    BlockingQueue<ProcessingShard<K, V>> shardQueue = new LinkedBlockingQueue<>();

    public int size() {
        return shardQueue.size();
    }

    public List<WorkContainer<K, V>> poll() throws InterruptedException {
        int quantity = 100;

        while (true) {
            var shard = shardQueue.take();
            try {
                if (!shard.isEmpty()) {
                    var workIfAvailable = shard.getWorkIfAvailable(quantity);

                    if (!workIfAvailable.isEmpty()) {
//                        return new Batch<>(workIfAvailable);
                        return workIfAvailable;
                    }
                }
            } finally {
                // only add back to queue if not empty
                if (!shard.isEmpty()) {
                    shardQueue.add(shard);
                }
            }
        }
    }

    public void add(ProcessingShard<K, V> shard) {
        shardQueue.add(shard);
    }

}