package bz.stub.parallelconsumer.connect;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

import java.util.Collection;
import java.util.Map;
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
 * <p>{@link #put} returning is <b>not</b> a durability claim - a connector may still be holding the record
 * in memory. {@link #preCommit} is where durability is asked for, and
 * {@link PcSinkTaskDurabilityBarrier} is what interprets the answer. This class only guarantees exclusion
 * and the happens-before edge; it draws no conclusion from either call.
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
     * Asks this task what it has durably written, under the same lock as {@code put}.
     *
     * <p>The lock is not optional: {@code preCommit} reads the state {@code put} is writing, and a
     * connector's implementation typically flushes a buffer that an interleaved {@code put} is still adding
     * to. Connect's own runtime needs no such lock only because one loop drives both.
     *
     * <p><b>The map handed in must contain only offsets this lane received.</b> {@code SinkTask}'s base
     * implementation is {@code flush(offsets); return offsets;} - and {@code flush}'s own base body is a
     * bare {@code return} - so for a connector that overrides neither, this call is a pure echo. Handing it
     * a partition-wide map would therefore not merely return an over-claiming watermark: for a connector
     * that overrides {@code flush}, it actively instructs a flush to offsets this lane never saw.
     *
     * @return whatever the task returned, unmodified - interpreting it is
     *         {@link PcSinkTaskDurabilityBarrier}'s job, not this class's
     */
    public Map<TopicPartition, OffsetAndMetadata> preCommit(
            final Map<TopicPartition, OffsetAndMetadata> laneOffsets) {
        lock.lock();
        try {
            return task.preCommit(laneOffsets);
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
}
