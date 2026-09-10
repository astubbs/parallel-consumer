package bz.stub.parallelconsumer.fluent;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

/**
 * The thread-local a facade throw writes its intent into, and the retry-delay provider reads it back out of (KTD4).
 *
 * <h2>Why a thread-local, and why keyed rather than a single slot</h2>
 * The engine calls the retry-delay provider <b>synchronously, inside the failure path, on the thread that threw</b> -
 * {@code WorkContainer.onUserFunctionFailure} asks {@code getRetryDelayConfig}, which calls the provider. So the
 * thread that ran the function is the thread that will be asked, which is what makes a thread-local sufficient and
 * a shared map unnecessary.
 * <p>
 * It is <b>keyed by topic, partition and offset</b> rather than being one slot, because a failed batch marks every
 * container in it failed on that one thread, in a loop, and asks the provider once per container. A single slot
 * would answer every record in the batch with the last writer's intent. Two partitions of one topic routinely carry
 * the same offset number, and two topics carry the same partition number, so the key needs all three.
 *
 * <h2>Lifecycle</h2>
 * An entry is removed when it is read. {@link #clearThread()} at the top of each dispatch bounds what a
 * never-read entry can cost: by the time a worker thread starts its next dispatch, the provider call for the
 * throw it made last time has already happened, inside the engine's failure path, on this same thread. So anything
 * still in the map at that point was never going to be read at all.
 *
 * @see RouteDispatcher#retryDelayFor(String, int, long)
 */
final class RetryIntents {

    /**
     * A plain {@link HashMap}: it is only ever touched by the one thread it belongs to.
     */
    private static final ThreadLocal<Map<String, RetryIntent>> INTENTS = ThreadLocal.withInitial(HashMap::new);

    private RetryIntents() {
    }

    static void retry(ConsumerRecord<?, ?> record, Duration delay) {
        record(record, RetryIntent.retryAfter(delay));
    }

    static void park(ConsumerRecord<?, ?> record, Duration delay) {
        record(record, RetryIntent.parkFor(delay));
    }

    private static void record(ConsumerRecord<?, ?> record, RetryIntent intent) {
        INTENTS.get().put(key(record.topic(), record.partition(), record.offset()), intent);
    }

    /**
     * Reads this thread's intent for one record and clears it, so the record's <em>next</em> failure cannot be
     * answered with this one's delay.
     *
     * @return null when nothing was recorded, which is the wrapper letting an ordinary failure through
     */
    static RetryIntent take(String topic, int partition, long offset) {
        return INTENTS.get().remove(key(topic, partition, offset));
    }

    /**
     * Drops whatever this thread still holds. Called at the top of each dispatch, and by tests.
     */
    static void clearThread() {
        INTENTS.get().clear();
    }

    /**
     * Visible for tests: how many intents this thread is still holding.
     */
    static int sizeForThisThread() {
        return INTENTS.get().size();
    }

    private static String key(String topic, int partition, long offset) {
        return topic + '-' + partition + '@' + offset;
    }
}
