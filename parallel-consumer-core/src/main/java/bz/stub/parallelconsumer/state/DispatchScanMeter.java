package bz.stub.parallelconsumer.state;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import java.util.concurrent.atomic.LongAdder;

/**
 * How many shard entries the dispatch scan has examined, across every pass and every shard.
 * <p>
 * <b>Why this exists at all.</b> {@link ProcessingShard#getWorkIfAvailable} walks a shard's entries looking for
 * work to hand out, and the two ordering modes present it with opposite shapes: {@code UNORDERED} shards by
 * topic-partition, so one shard holds every in-flight record for its partition, while {@code KEY} shards by record
 * key, so a distinct-key workload gets one shard per record. A change to the scan can easily make one of those
 * shapes quadratic while leaving the other linear - and every correctness test passes either way, because the
 * records all still get delivered. This counter is what makes that difference observable.
 * <p>
 * <b>Why a counter and not a stopwatch.</b> The obvious guard is to time both modes and compare, and that was
 * tried first. It does not work in this repo: the core module runs test METHODS in parallel
 * ({@code junit.jupiter.execution.parallel.enabled} with a dynamic factor of 20, see the module pom), and the two
 * arms are not equally sensitive to that. {@code UNORDERED} walks one large map and is memory-bound, so it
 * degrades far harder under cache and memory-bandwidth contention than {@code KEY}, which leaves each tiny map
 * almost immediately. The measured consequence: on an unchanged tree the ratio sat at 0.75-1.0 when the test ran
 * alone and 1.13-4.91 inside the suite, on identical settings. Neither taking the fastest of several runs, nor
 * comparing the two arms as a ratio, nor switching from wall time to per-thread CPU time, nor {@code @Isolated}
 * removed it - because none of them addresses work the process genuinely performs. A count does: it is the same
 * number on a loaded machine and an idle one.
 * <p>
 * <b>Why it is shared rather than per-shard.</b> An empty shard is removed from
 * {@link ShardManager#processingShards}, so a per-shard counter would be discarded along with it. That would
 * undercount {@code KEY} - where nearly every shard empties and is removed - far more than {@code UNORDERED},
 * which is precisely the asymmetry the counter is meant to detect. One instance per {@link ShardManager} survives
 * shard turnover.
 * <p>
 * Monotonic and never reset: a caller that wants a delta takes two readings and subtracts, the same way
 * {@link RecordPopulation} is used. {@link LongAdder} for the same reason it does - written on every examined
 * entry, read rarely.
 *
 * @author Antony Stubbs
 * @see ProcessingShard#getWorkIfAvailable
 * @see RecordPopulation
 */
public class DispatchScanMeter {

    private final LongAdder entriesExamined = new LongAdder();

    /** One entry of one shard was looked at by the dispatch scan. */
    void onEntryExamined() {
        entriesExamined.increment();
    }

    /**
     * Total entries examined since this instance was created.
     *
     * @return a monotonically increasing count; subtract two readings for the cost of a single scan
     */
    public long getEntriesExamined() {
        return entriesExamined.sum();
    }
}
