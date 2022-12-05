package io.confluent.parallelconsumer.internal;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import io.confluent.parallelconsumer.state.QueuedWorkManager;
import io.confluent.parallelconsumer.state.WorkContainer;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.experimental.NonFinal;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.TimeUnit;

/**
 * @author Antony Stubbs
 */
@Slf4j
@RequiredArgsConstructor
@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
// todo rename worker
public class PCWorker<K, V, R> {

    public static final String WORKER_PREFIX = "worker";

    /**
     * Minimum number of timing measurements before we can calculate a good value
     */
    static int NUMBER_OF_MEASUREMENTS_CUTOFF = 5;

    /**
     * Use a large initial value to get things kicked off
     */
    static int INITIAL_MIN_QUEUE_SIZE = 100;

    private static int workerCount = 0;

    SimpleMeterRegistry metricsRegistry = new SimpleMeterRegistry();

    @Getter
    private final int workerId = workerCount++;

    //    WorkQueue<K, V> workQueue = new WorkQueue<>();
//    Queue<WorkContainer<K, V>> workQueue = new LinkedList<>();

    Timer processingWorkQueueTimer = metricsRegistry.timer("user.function");

    PCWorkerPool<K, V, R> parentPool;

    QueuedWorkManager<K, V> wm;

    Batcher<WorkContainer<K, V>> batcher = new Batcher<>();

//    private LinkedBlockingQueue<NewWorkMessage> inbox = new LinkedBlockingQueue<>();

    DistributionSummary workQueueSizeDistribution =
            DistributionSummary.builder(WORKER_PREFIX + ".workQueue.size").register(metricsRegistry);

//    private Batch<WorkContainer<K, V>> acquireFromWmBatch() throws InterruptedException {
//        return wm.take();
//    }

//    private Batch<K, V> acquireFromWm() throws InterruptedException {
//        return wm.take();
//    }

    RateLimiter loopPerfStats = new RateLimiter();

    @NonFinal
    public int LOADING_MULTIPLE = 2;

    private Timer spentWaitingForNewWorkTimer = metricsRegistry.timer(WORKER_PREFIX + ".wait.newWork");

    private CentralQueue centralQueue;

//    private void process(List<List<WorkContainer<K, V>>> listOfBatches) {
//        var start = Timer.start(metricsRegistry);
//
//        var functionRunner = parentPool.getRunner();
//
//        for (var batch : listOfBatches) {
//            functionRunner.run(batch);
//        }
//
//        start.stop(processingWorkQueueTimer);
//    }

    // todo audit interruptions with finally blocks
    public void loop() {
        Runnable stats = () -> log.debug("Loop stats: blocking time for new work: {} ns",
                (int) spentWaitingForNewWorkTimer.mean(TimeUnit.NANOSECONDS));

        while (!Thread.currentThread().isInterrupted()) {
            try {
//                Batch<K, V> work = acquireFromWm();
//                var work = acquireFromQueue();

//                processNewWorkBlocking();


                // todo rectify - drainTo?
//                var ll = work.stream().flatMap(x -> x.getValue().stream()).collect(Collectors.toList());

//                var batches = batcher.makeBatches(ll);

//                var work = acquireFromWmBatch();
//                var batches = batcher.makeBatches(work.getValues());

                processWorkQueueFromCentral();

                if (log.isDebugEnabled()) {
                    loopPerfStats.performIfNotLimited(stats);
                }

            } catch (InterruptedException e) {
                log.info("Interrupted");
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                log.error("Error acquiring work from work manager", e);
            }
        }
    }

//    private List<NewWorkMessage> pollFromQueue() throws InterruptedException {
//        processNewWorkBlocking();
//        var work = new LinkedList<NewWorkMessage>();
//        while (!workQueue.isEmpty()) {
//            work.add(workQueue.poll());
//        }
//        return work;
//    }

//    /**
//     * Blocks until a message is received
//     */
//    private void processNewWorkBlocking() throws InterruptedException {
//        var drain = new LinkedList<NewWorkMessage>();
//
//        var start = Timer.start();
//        var first = inbox.take();// wait for first
//        start.stop(spentWaitingForNewWorkTimer);
//
//        drain.add(first);
//
//        // drain if still not empty
//        if (!inbox.isEmpty()) { // protects from acquiring the lock again
//            var inboxSize = inbox.size();
//            inbox.drainTo(drain, inboxSize); // drain the rest, but up to a limit
//        }
//
//        // process
//        for (var msg : drain) {
//            workQueue.addAll(msg.getValue());
//        }
//    }

    private void processWorkQueueFromCentral() throws InterruptedException {
//        if (workQueue.isEmpty()) {
//            return;
//        }

        var take = Timer.start();
        var work = centralQueue.take();
        take.stop(spentWaitingForNewWorkTimer);


        var run = Timer.start(metricsRegistry);

        var functionRunner = parentPool.getRunner();

//        var workQueueSize = workQueue.size();
//
//        while (!workQueue.isEmpty()) {


//            var listOfBatches = batcher.makeBatches(work);
//            for (var batch : listOfBatches) {
        functionRunner.run(work.getValues());
//            }
//        }

        // update metrics
        run.stop(processingWorkQueueTimer);
//        workQueueSizeDistribution.record(workQueueSize);
    }

//    private void processWorkQueue() {
//        if (workQueue.isEmpty()) {
//            return;
//        }
//
//        var start = Timer.start(metricsRegistry);
//
//        var functionRunner = parentPool.getRunner();
//
//        var workQueueSize = workQueue.size();
//
//        while (!workQueue.isEmpty()) {
//
//            var work = pollFromQueue(1);
//            var listOfBatches = batcher.makeBatches(work);
//            for (var batch : listOfBatches) {
//                functionRunner.run(batch);
//            }
//        }
//
//        // update metrics
//        start.stop(processingWorkQueueTimer);
//        workQueueSizeDistribution.record(workQueueSize);
//    }

//    private List<WorkContainer<K, V>> pollFromQueue(int howMany) {
//        var work = new LinkedList<WorkContainer<K, V>>();
//        while (!workQueue.isEmpty() && work.size() < howMany) {
//            work.add(workQueue.poll());
//        }
//        return work;
//    }

//    public int getQueueCapacity(Timer workRetrievalTimer) {
////        return 100 - workQueue.size();
////        return 100000;
//        var calculatedTargetQueueSize = calculateQuantityShouldHaveInQueue(workRetrievalTimer);
//        var targetSize = calculatedTargetQueueSize * LOADING_MULTIPLE;
//        var currentQueueSize = getCurrentQueueWorkContainerCount();
//        var remainingCapacity = targetSize - currentQueueSize;
//
//        log.trace("Target capacity: {}, current q size: {}, remaining {}",
//                targetSize,
//                currentQueueSize,
//                remainingCapacity);
//
//        return remainingCapacity;
//    }

//    private int calculateQuantityShouldHaveInQueue(Timer workRetrievalTimer) {
//        if (workRetrievalTimer.count() < NUMBER_OF_MEASUREMENTS_CUTOFF || processingWorkQueueTimer.count() < NUMBER_OF_MEASUREMENTS_CUTOFF) {
//            return INITIAL_MIN_QUEUE_SIZE;
//        }
//
//        var retrievalNS = workRetrievalTimer.mean(NANOSECONDS);
//
//        var averageTimeProcessingWorkQueueNS = processingWorkQueueTimer.mean(NANOSECONDS);
//        var averageWorkContainersInQueueCOUNT = workQueueSizeDistribution.mean();
//        var processingTimePerWorkContainerNS = averageTimeProcessingWorkQueueNS / averageWorkContainersInQueueCOUNT;
//
//        var quantity = retrievalNS / processingTimePerWorkContainerNS;
//
//        var rawCalculatedCapacity = (int) quantity;
//
//
//        if (log.isDebugEnabled()) {
//            loopPerfStats.performIfNotLimited(() ->
//                    log.debug("Worker: {}, " +
//                                    "Units required to keep busy: {}, " +
//                                    "control loop time: {} ms," +
//                                    "worker avg queue size: {}, " +
//                                    "processing time: {} ms, " +
//                                    "avg time per work container: {} microSeconds",
//                            getWorkerId(),
//                            rawCalculatedCapacity,
//                            (int) workRetrievalTimer.mean(MILLISECONDS),
//                            (int) averageWorkContainersInQueueCOUNT,
//                            (int) processingWorkQueueTimer.mean(MILLISECONDS),
//                            (int) processingTimePerWorkContainerNS / 1000
//                    )
//            );
//        }
//
//        return rawCalculatedCapacity;
//    }

//    private int getCurrentQueueWorkContainerCount() {
//        return workQueue.size() + inbox.stream().mapToInt(x -> x.getValue().size()).sum();
//    }

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

//    /**
//     * todo docs
//     * <p>
//     * todo extract external API interface
//     * <p>
//     * External thread safe API
//     */
//    @ThreadSafe
//    public void newWorkMessage(ProcessingShard<K, V> shard, List<WorkContainer<K, V>> workList) {
//        if (workList.isEmpty()) {
//            throw new IllegalArgumentException("Empty work list");
//        }
//
//        NewWorkMessage msg = new NewWorkMessage(shard, workList);
//        this.inbox.add(msg);
//    }

//    public void newWorkMessage(List<WorkContainer<K, V>> p) {
//        // todo resolve null
//        this.inbox.add(new NewWork(null, p));
//    }

//    @Value
//    public class NewWorkMessage {
//
//        ProcessingShard<K, V> shard;
//
//        List<WorkContainer<K, V>> value;
//    }
}

