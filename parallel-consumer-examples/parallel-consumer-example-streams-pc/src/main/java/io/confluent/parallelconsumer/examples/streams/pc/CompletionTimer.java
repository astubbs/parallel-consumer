package io.confluent.parallelconsumer.examples.streams.pc;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import java.util.ArrayList;
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
 * <p>
 * <b>Kept deliberately in step with the private {@code CompletionTimer} inside
 * {@code HeadOfLineBlockingBenchmarkTest}</b> in {@code parallel-consumer-streams}, so the demo and the
 * regression test time the same thing the same way. This one additionally tracks the whole-batch drain.
 * The two are separate because a {@code src/main} module cannot import another module's test classes.
 * Change one and check the other, because nothing else will.
 *
 * @author Antony Stubbs
 */
final class CompletionTimer {

    private final AtomicLong firstStartNanos = new AtomicLong();

    /**
     * Keyed by record identity, which is what makes the map both the sample set and the de-duplicator.
     * <p>
     * A redelivery would otherwise add a second, much later sample for a record that had already been
     * processed, and quietly fatten the tail. Keying the samples themselves means the guard cannot fall out
     * of step with the thing it guards.
     */
    private final ConcurrentHashMap<String, Completion> completions = new ConcurrentHashMap<>();

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
        long elapsedMillis = (now - firstStartNanos.get()) / 1_000_000L;
        // The drain clock advances only on a FIRST completion, for the same reason the samples do. A
        // redelivery of the last record would otherwise push the whole-batch figure later while leaving
        // the per-record distribution untouched, and the two headline numbers would disagree with each
        // other with nothing on screen to say why.
        if (completions.putIfAbsent(key + "/" + value, new Completion(value, elapsedMillis)) == null) {
            lastCompletionNanos.accumulateAndGet(now, Math::max);
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
        for (Completion completion : completions.values()) {
            if (!ArmRunner.BLOCKER_VALUE.equals(completion.value)) {
                out.add(completion.elapsedMillis);
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
