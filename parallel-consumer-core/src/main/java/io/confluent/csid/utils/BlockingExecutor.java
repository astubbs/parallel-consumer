package io.confluent.csid.utils;

import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.*;

/**
 * An Executor class wraps around a ThreadPoolExecutor and uses a Semaphore to block then the work queue is full.
 * <p>
 * Minor tweaks for our use - we use an enqueueing timeout, so Controller can wake up at a given time to perform a
 * commit.
 *
 * @author Antony Stubbs
 * @author Java Concurrency in Practice by Brian Goetz
 * @see https://stackoverflow.com/questions/2001086/how-to-make-threadpoolexecutors-submit-method-block-if-it-is-saturated
 */
@Slf4j
public class BlockingExecutor {

    private final ThreadPoolExecutor executor;
    private final Semaphore semaphore;

    /**
     * Max size of the work queue
     */
    private final int intendedQueueSize;

    public BlockingExecutor(int queueSize, int corePoolSize, int maxPoolSize, int keepAliveTime, TimeUnit unit, ThreadFactory factory) {
        this.intendedQueueSize = queueSize;
        BlockingQueue<Runnable> queue = new LinkedBlockingQueue<>();
        this.executor = new ThreadPoolExecutor(corePoolSize, maxPoolSize, keepAliveTime, unit, queue, factory);
        executor.prestartAllCoreThreads();
        this.semaphore = new Semaphore(queueSize + maxPoolSize);
    }

    /**
     * Safely waits  for the queue to have space, then submit the task. If the timeout is reached, the task is still
     * enqueue for processing, and then we return.
     * <p>
     * Do not call this method if the {@link #hasCapacity()} is false, if you do, the time-out may always fire, and you
     * will continually add single elements to the queue.
     *
     * @see #hasCapacity()
     */
    public <R> Future<R> submit(Callable<? extends R> task, Duration timeout) throws InterruptedException {
        var acquired = semaphore.tryAcquire(timeout.toMillis(), TimeUnit.MILLISECONDS);

        if (!acquired) {
            log.debug("Timeout while waiting to enqueue task, will enqueue and return");
        }

        try {
            return executor.submit(() -> {
                try {
                    return task.call();
                } finally {
                    semaphore.release();
                }
            });
        } catch (RejectedExecutionException e) {
            // will never be thrown with an unbounded buffer (underlying queue) (LinkedBlockingQueue)
            semaphore.release();
            throw new RuntimeException("Unexpected RejectedExecutionException", e);
        }
    }

    /**
     * Blocks until a slot there is execution capacity.
     * <p>
     * Does not guarantee that {@link #submit} will not block for capacity. However, if only a single thread enqueues
     * work, it will still have capacity when submit is called.
     */
    public boolean waitForCapacity(Duration timeout) throws InterruptedException {
        if (hasCapacity()) {
            // optimistic check, as we don't guarantee submit will not block after we return
            return true;
        }

        try {
            // wait to acquire a token
            return semaphore.tryAcquire(intendedQueueSize, timeout.toMillis(), TimeUnit.MILLISECONDS);
        } finally {
            // return the token straight away
            semaphore.release();
        }
    }

    public int getCapacity() {
        return semaphore.availablePermits();
    }

    public boolean hasCapacity() {
        return getQueueSize() < intendedQueueSize;
    }

    public List<Runnable> shutdownNow() {
        return executor.shutdownNow();
    }

    public boolean awaitTermination(long toSeconds, TimeUnit seconds) throws InterruptedException {
        return executor.awaitTermination(toSeconds, seconds);
    }

    public int getQueueSize() {
        var waitingThreads = semaphore.getQueueLength();
        return semaphore.availablePermits() - intendedQueueSize + waitingThreads;
    }

    public int getActiveCount() {
        return executor.getActiveCount();
    }

}
