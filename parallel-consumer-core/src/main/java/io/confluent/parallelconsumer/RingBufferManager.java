package io.confluent.parallelconsumer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.function.Consumer;
import java.util.function.Function;

@Slf4j
@RequiredArgsConstructor
public class RingBufferManager<K, V> {

    private final ParallelConsumerOptions<K, V> options;
    private final WorkManager<K, V> wm;
    private final ParallelEoSStreamProcessor<K, V> pc;
    private final ThreadPoolExecutor threadPoolExecutor;
    private Semaphore semaphore;


    public <R> void startRingBuffer(
//            final BlockingQueue<Runnable> ringbuffer,
            final Function<ConsumerRecord<K, V>, List<R>> usersFunction,
            final Consumer<R> callback) {

        DynamicLoadFactor dynamicLoadFactor = new DynamicLoadFactor();
        int currentSize = dynamicLoadFactor.getCurrent();
        Thread supervisorThread = Thread.currentThread();
        semaphore = new Semaphore(options.getNumberOfThreads() * 2, true);

        Runnable runnable = () -> {
            Thread.currentThread().setName(RingBufferManager.class.getSimpleName());
            boolean lastRunDirty = true;
            while (supervisorThread.isAlive()) {
                try {
                    int toGet = options.getNumberOfThreads() * currentSize * 2; // ensure we always have more waiting to queue
                    // todo make sure work is gotten in large batches, and only when buffer is small enough - not every loop
                    List<WorkContainer<K, V>> workContainers = wm.maybeGetWork(toGet);
                    boolean clean = wm.isClean();
//                    if(lastRunDirty )
                    int got = workContainers.size();
                    log.debug("Got {}, req {}", got, toGet);
                    if (workContainers.isEmpty()) {
                        waitForRecordsAvailable();
                    }
                    int size = threadPoolExecutor.getQueue().size();
//                    if (got == 0 && size > 0) {
                    if (got == 0) {
                        log.info("{}", threadPoolExecutor);
                        log.info("got none");

                        if (size > 0) {
                            var processingShards = wm.getProcessingShards();
                            log.debug("Nothing available to process, waiting on dirty state of shards");
                            synchronized (processingShards) {
                                processingShards.wait();//refactor
                            }
                        }
                    }
                    for (final WorkContainer<K, V> work : workContainers) {
                        Runnable run = () -> {
                            try {
                                pc.userFunctionRunner(usersFunction, callback, work);
                                pc.handleFutureResult(work);
                            } finally {
                                log.debug("Releasing ticket");
                                semaphore.release();
                            }
                        };
//                        while (!ringbuffer.contains(run)) {
//                                ringbuffer.put(run);
                        submit(run, work);

//                        }

//                        log.info("Loop, yield");
                        Thread.sleep(1); // for testing - remove
                    }
                } catch (Exception e) {
                    log.error("Unknown error", e);
                }
            }
        };

        Executors.newSingleThreadExecutor().submit(runnable);
    }

    private void waitForRecordsAvailable() throws InterruptedException {
        Integer queued = wm.getWorkQueuedInMailboxCount();
        if (!(queued > 0)) { // pre-render this view in the fly in WM - shouldn't need to recount every loop - it's a very tight loop
            log.debug("empty, no work to be gotten, wait to be notified");
            synchronized (wm.getWorkInbox()) {
                wm.getWorkInbox().wait();
            }
            log.debug("finished wait");
        }
    }

    void submit(final Runnable run, final WorkContainer<K, V> work) {
        if (semaphore.availablePermits() < 1) {
            log.debug("Probably Blocking putting work into ring buffer until there is capacity");
        }
//        try{
        semaphore.acquireUninterruptibly();
        log.debug("Ticket acquired (remaining: {}), submitting {}", semaphore.availablePermits(), work);
//        } catch (InterruptedException e) {
//            log.debug("Interrupted waiting to put", e);
//        }
        threadPoolExecutor.submit(run);
    }

}