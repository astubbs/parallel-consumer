package io.confluent.parallelconsumer.connect;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

import java.util.Collection;
import java.util.concurrent.locks.ReentrantLock;

/**
 * One {@link SinkTask} and the lock that keeps it single-threaded.
 *
 * <p>A sink task is not thread-safe, and Connect's own runtime never needs to say so because
 * {@code WorkerSinkTask} drives it from one loop. Here the calls arrive from different threads - {@code put}
 * from a worker in Parallel Consumer's pool, and the lifecycle callbacks from whichever thread the
 * dispatcher owns - so the exclusion has to be explicit. The lock also supplies the happens-before edge
 * between them, which a connector accumulating state across {@code put} calls depends on.
 *
 * <p>This class deliberately does <b>not</b> call {@code preCommit}, {@code flush}, or any offset
 * machinery. Completion here means the callback returned, which is not a durability claim; composing task
 * watermarks with Parallel Consumer's frontier is a later design and is out of scope for this proof.
 */
@Slf4j
public class PcSinkTaskLane {

    @Getter
    private final SinkTask task;

    /** Non-fair on purpose: there is no ordering claim between lanes, only exclusion within one. */
    private final ReentrantLock lock = new ReentrantLock();

    public PcSinkTaskLane(final SinkTask task) {
        this.task = task;
    }

    /**
     * Delivers one record's worth of work to the task, holding the lock across the whole call.
     *
     * <p>Held across the whole {@code put} rather than around it: a connector may buffer inside the call,
     * and releasing early would let a second worker enter while the first is mid-write.
     */
    public void put(final Collection<SinkRecord> records) {
        lock.lock();
        try {
            task.put(records);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Runs a lifecycle callback under the same lock, so {@code open}, {@code close} and {@code stop} can
     * never interleave with an in-flight {@code put}.
     */
    public void runExclusively(final Runnable callback) {
        lock.lock();
        try {
            callback.run();
        } finally {
            lock.unlock();
        }
    }

    /** Visible for tests: whether some thread currently holds this lane. */
    boolean isHeld() {
        return lock.isLocked();
    }
}
