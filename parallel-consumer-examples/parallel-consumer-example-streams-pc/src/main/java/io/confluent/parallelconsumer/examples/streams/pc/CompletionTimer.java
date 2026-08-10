package io.confluent.parallelconsumer.examples.streams.pc;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Times each record's completion relative to the moment the first record entered the processing chain.
 * <p>
 * <b>Measured from first entry, not from the produce timestamp.</b> That deliberately excludes two sources
 * of noise which have nothing to do with the dispatch seam: producer batching, and the gap while the
 * topology starts up and gets its partition assigned. What remains is time spent waiting inside the chain,
 * which is exactly what head-of-line blocking is.
 *
 * @author Antony Stubbs
 */
final class CompletionTimer {

    private final AtomicLong firstStartNanos = new AtomicLong();

    private final List<Completion> completions = Collections.synchronizedList(new ArrayList<>());

    private final ConcurrentHashMap<String, Boolean> seen = new ConcurrentHashMap<>();

    void markStarted() {
        firstStartNanos.compareAndSet(0L, System.nanoTime());
    }

    /**
     * When the last record of all finished, so the run can report whether the whole batch drained sooner
     * and not only whether the fast records did.
     * <p>
     * Without this the demo would be reporting the metric that flatters it, on the records that flatter it.
     * If PC's pool had made the blocker itself slower, or the batch as a whole longer, the fast-record
     * distribution would look identical and nothing on screen would say so.
     */
    private final AtomicLong lastCompletionNanos = new AtomicLong();

    void markCompleted(final String key, final String value) {
        long now = System.nanoTime();
        lastCompletionNanos.accumulateAndGet(now, Math::max);
        long elapsedMillis = (now - firstStartNanos.get()) / 1_000_000L;
        // Guarded against double counting: a redelivery would otherwise add a second, much later sample for
        // a record that had already been processed, and quietly fatten the tail.
        if (seen.putIfAbsent(key + "/" + value, Boolean.TRUE) == null) {
            completions.add(new Completion(value, elapsedMillis));
        }
    }

    int completed() {
        return completions.size();
    }

    /** Time to drain every record, blocker included, on the same clock as the per-record latencies. */
    long totalDrainMillis() {
        return (lastCompletionNanos.get() - firstStartNanos.get()) / 1_000_000L;
    }

    /**
     * Every record except the blocker.
     * <p>
     * The blocker is excluded by <em>value</em>, not by key, because in the single-key control every record
     * carries the blocker's key - excluding by key there would discard the entire sample and leave a
     * distribution of nothing.
     */
    List<Long> fastRecordLatencies() {
        List<Long> out = new ArrayList<>();
        synchronized (completions) {
            for (Completion completion : completions) {
                if (!ArmRunner.BLOCKER_VALUE.equals(completion.value)) {
                    out.add(completion.elapsedMillis);
                }
            }
        }
        return out;
    }

    private static final class Completion {

        private final String value;

        private final long elapsedMillis;

        Completion(final String value, final long elapsedMillis) {
            this.value = value;
            this.elapsedMillis = elapsedMillis;
        }
    }
}
