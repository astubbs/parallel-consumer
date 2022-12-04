package io.confluent.parallelconsumer.internal;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.parallelconsumer.state.ProcessingShard;
import io.confluent.parallelconsumer.state.WorkContainer;
import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;
import lombok.experimental.NonFinal;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.NavigableSet;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * @author Antony Stubbs
 */
@Slf4j
@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
public class WorkQueue<K, V> {

    NavigableSet<ProcessingShard<K, V>> shardQueue = new TreeSet<>();

    ReentrantLock newWorkLockMaker = new ReentrantLock();

    //    @NonFinal
    Condition newWorkEvent = newWorkLockMaker.newCondition();

//    PCModule<K, V> module;

    @NonFinal
    ProcessingShard<K, V> lastShard;

    public int size() {
        return shardQueue.size();
    }

    public void onWorkAdded(ProcessingShard<K, V> shard) throws InterruptedException {
        newWorkLockMaker.lock();
        try {
            newWorkEvent.signalAll();
        } finally {
            newWorkLockMaker.unlock();
        }
    }

    public List<WorkContainer<K, V>> poll() throws InterruptedException {
        int quantity = 100;
        while (true) {

            SortedSet<ProcessingShard<K, V>> tailset;
            if (lastShard == null) {
                tailset = shardQueue;
            } else {
                tailset = shardQueue.tailSet(lastShard);
            }

            var shardSetToIterate = new TreeSet<>(tailset);
            var iterator = shardSetToIterate.iterator();
            while (iterator.hasNext()) {
                //var shard = shardQueue.take();

                var shard = iterator.next();
                lastShard = shard;

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
                        // add it to the back
                        shardQueue.add(shard);
                    }
                }
            }

            // if we get here, we've exhausted the queue
            newWorkLockMaker.lock();
            try {
                newWorkEvent.await(1, SECONDS);
            } finally {
                newWorkLockMaker.unlock();
            }
        }

    }

    public void addIfMissing(ProcessingShard<K, V> shard) {
        if (!shardQueue.contains(shard)) {
            shardQueue.add(shard);
        }
    }

}