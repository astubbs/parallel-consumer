package io.confluent.parallelconsumer.internal;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.parallelconsumer.state.ProcessingShard;
import io.confluent.parallelconsumer.state.QueuedWorkManager;
import io.confluent.parallelconsumer.state.WorkContainer;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.Value;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;

import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * @author Antony Stubbs
 */
@Slf4j
@RequiredArgsConstructor
@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
// todo rename worker
public class PCWorker<K, V, R> {

    SimpleMeterRegistry metricsRegistry = new SimpleMeterRegistry();

    Timer userFunctionTimer = metricsRegistry.timer("user.function");

    //    WorkQueue<K, V> workQueue = new WorkQueue<>();

    Queue<NewWorkMessage> workQueue = new LinkedList<>();

    PCWorkerPool<K, V, R> parentPool;

    QueuedWorkManager<K, V> wm;

    Batcher<WorkContainer<K, V>> batcher = new Batcher<>();

    private LinkedBlockingQueue<NewWorkMessage> inbox = new LinkedBlockingQueue<>();

    // todo audit interruptions with finally blocks
    public void loop() {
        while (!Thread.currentThread().isInterrupted()) {
            try {
//                Batch<K, V> work = acquireFromWm();
                var work = acquireFromQueue();

                // todo rectify - drainTo?
                var ll = work.stream().flatMap(x -> x.getValue().stream()).collect(Collectors.toList());

                var batches = batcher.makeBatches(ll);

//                var work = acquireFromWmBatch();
//                var batches = batcher.makeBatches(work.getValues());

                process(batches);
            } catch (InterruptedException e) {
                log.info("Interrupted");
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                log.error("Error acquiring work from work manager", e);
            }
        }
    }

//    private Batch<WorkContainer<K, V>> acquireFromWmBatch() throws InterruptedException {
//        return wm.take();
//    }

//    private Batch<K, V> acquireFromWm() throws InterruptedException {
//        return wm.take();
//    }

    private void process(List<List<WorkContainer<K, V>>> listOfBatches) {
        userFunctionTimer.record(() -> {
                    var functionRunner = parentPool.getRunner();
//                    if (functionRunner.isPresent()) {
//                        functionRunner.get().run(work);
                    for (var batch : listOfBatches) {
                        functionRunner.run(batch);
                    }
//                    } else {
//                        throw new IllegalStateException("Function runner not set");
//                    }
                }
        );
    }

    private List<NewWorkMessage> acquireFromQueue() throws InterruptedException {
        processNewWorkBlocking();
        var work = new LinkedList<NewWorkMessage>();
        while (!workQueue.isEmpty()) {
            work.add(workQueue.poll());
        }
        return work;
    }

    /**
     * Blocks until a message is received
     */
    private void processNewWorkBlocking() throws InterruptedException {
        inbox.add(inbox.take()); // wait for first
        inbox.drainTo(workQueue, inbox.size()); // drain the rest, but up to a limit
        log.trace("Message received, messages drained: {}", workQueue.size());
    }

    // todo plug this in
    public int getQueueCapacity(Timer workRetrievalTimer) {
//        return 100 - workQueue.size();
        return 100;
//        return calculateQuantityShouldHaveInQueue(workRetrievalTimer) - workQueue.size();
    }

    private int calculateQuantityShouldHaveInQueue(Timer workRetrievalTimer) {
        var retrieval = workRetrievalTimer.mean(NANOSECONDS);
        var processing = userFunctionTimer.mean(NANOSECONDS);
        var quantity = retrieval / processing;
        return (int) quantity * 2;
    }

//    public void enqueue(List<WorkContainer<K, V>> work) {
//        workQueue.add(work);
//    }

//    /**
//     * Add to the collection of {@link ProcessingShard}s this Worker is responsible for
//     */
//    public void addShardIfMissing(ProcessingShard<K, V> shard) {
//        workQueue.addIfMissing(shard);
//    }
//
//    public void onWorkAdded(ProcessingShard<K, V> processingShard) throws InterruptedException {
//        workQueue.onWorkAdded(processingShard);
//    }

    public void newWorkMessage(ProcessingShard<K, V> shard, List<WorkContainer<K, V>> workList) {
        if (workList.isEmpty()) {
            throw new IllegalArgumentException("Empty work list");
        }

        NewWorkMessage msg = new NewWorkMessage(shard, workList);
        this.inbox.add(msg);
    }

//    public void newWorkMessage(List<WorkContainer<K, V>> p) {
//        // todo resolve null
//        this.inbox.add(new NewWork(null, p));
//    }


    @Value
    public class NewWorkMessage {

        ProcessingShard<K, V> shard;

        List<WorkContainer<K, V>> value;
    }
}

