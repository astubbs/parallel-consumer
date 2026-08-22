package bz.stub.parallelconsumer.state;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.ParallelConsumerOptions.ProcessingOrder;
import bz.stub.parallelconsumer.internal.EpochAndRecordsMap;
import bz.stub.parallelconsumer.internal.PCModuleTestEnv;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import pl.tlinkowski.unij.api.UniLists;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;

/**
 * Pins how many shard entries the dispatch scan examines in each ordering mode.
 * <p>
 * WHY THIS EXISTS. The two modes put the same records into opposite shard shapes: {@code UNORDERED}
 * shards by topic-partition, so a handful of shards each hold every in-flight record for their
 * partition, while {@code KEY} shards by record key, so a distinct-key workload gets one shard per
 * record holding one entry. Dispatch walks one of those shapes on every pass, so a change to the scan
 * can easily make one shape cheap and the other quadratic - and nothing would notice, because every
 * correctness test passes either way.
 * <p>
 * That is not hypothetical. A 21% gap between the modes was reported from a real benchmark on
 * 2026-08-21 and a fix was written, tested and merged on the strength of it, before like-for-like runs
 * showed the two are 0.9% apart and the original comparison had been taken at two different record
 * counts. See docs/inflight/perf-throughput-regression-since-0-3.md.
 * <p>
 * WHY IT COUNTS RATHER THAN TIMES, WHICH IS THE WHOLE STORY OF THIS TEST. It originally timed both modes and
 * compared them, and it spent a day reporting a regression against code that a bisect then showed had not
 * changed. Timing cannot work here: this module runs test METHODS in parallel (see
 * {@code junit.parallelism.configuration.parameters} in the module pom, dynamic factor 20), and the two arms are
 * not equally sensitive to that. UNORDERED walks one map holding every in-flight record and is memory-bound; KEY
 * leaves each one-entry map immediately. Under contention the first degrades far harder, so the comparison moves
 * without the code moving - measured at 0.75-1.0 alone against 1.13-4.91 in-suite on an unchanged tree.
 * <p>
 * Taking the fastest of several runs, expressing it as a ratio, switching to per-thread CPU time and marking the
 * class {@code @Isolated} were all tried and none of them fixed it, because none addresses work the process
 * genuinely does. {@link DispatchScanMeter} counts entries the scan examined, which is the same number on a
 * loaded machine and an idle one - so this test no longer has a timing budget, a warm-up, or a tolerance for
 * noise.
 * <p>
 * WHAT THE NUMBERS TURNED OUT TO BE, which is not what this test originally assumed. Both modes now examine
 * exactly one entry per record. UNORDERED did NOT: it examined 20.5 per record on this workload and was genuinely
 * quadratic, because in-flight records stay in the shard and every pass restarted from the beginning of it. That
 * is what {@link ShardOccupancy} removed, and the history is kept on {@link #EXPECTED_UNORDERED_EXAMINATIONS}
 * because the number moving is the only thing this test is for. The first version of this test asserted the two
 * modes cost "the same" within a factor of four, which was calibrated against the stopwatch and was simply wrong
 * about the code. The counts are exact and deterministic, so they are asserted exactly.
 *
 * @author Antony Stubbs
 * @see ProcessingShard#getWorkIfAvailable
 */
class OrderingModeDispatchParityTest {

    static final String TOPIC = "ordering-parity-topic";
    static final TopicPartition TP = new TopicPartition(TOPIC, 0);

    /** Enough that a per-pass rescan of the in-flight set is measurable if one exists. */
    static final int RECORDS = 20_000;
    static final int BATCH = 500;

    /** {@link #RECORDS} taken {@link #BATCH} at a time. */
    static final int PASSES = RECORDS / BATCH;

    /**
     * KEY examines exactly one entry per record, because a distinct-key workload puts each record in its own
     * shard and the scan leaves after the head.
     * <p>
     * <b>The busy-shard in-flight guard was expected to lower this and does not, which is worth knowing before
     * anyone concludes the guard is inert.</b> That guard skips an ordered shard entirely when it already has a
     * record out at a worker, so a skipped shard is an entry never examined - but this workload never gives it
     * the chance. Records here are taken and never returned, and {@code LoopingResumingIterator} resumes each
     * pass from where the previous one stopped, so each of the {@link #RECORDS} shards is entered exactly once
     * and none is ever revisited while occupied. That the guard does fire, and does suppress examinations, is
     * asserted against this same meter by
     * {@link ShardInFlightCountTest#anOrderedShardWithWorkInFlightIsNotEnteredAtAll}.
     */
    static final long EXPECTED_KEY_EXAMINATIONS = RECORDS;

    /**
     * UNORDERED now examines exactly one entry per record, the same as KEY.
     * <p>
     * <b>It used to be {@code BATCH x (1 + 2 + ... + PASSES)} - 410,000 for the 20,000 records below, or 20.5 per
     * record</b> - because every in-flight record stayed in the shard and each pass restarted from the beginning
     * of it: pass <i>k</i> walked past the {@code (k-1) x BATCH} records already taken before it reached new ones.
     * The previous revision of this constant asserted that quadratic figure deliberately, and said that a LOWER
     * number meant someone had landed the optimisation and should update it saying which.
     * <p>
     * <b>Which change did it: {@link ShardOccupancy}</b> - the unordered dispatch path walks an index of the
     * offsets no worker is holding instead of the shard's whole entry map, so the in-flight prefix is not in the
     * path at all. That is why the count collapses to one per record here and, more to the point, why it stops
     * growing with concurrency: this workload never completes a record, so the old figure grew with the record
     * count, and in a running consumer the same cost grew with {@code maxConcurrency}. It was the mechanism behind
     * the direct-pull engine's collapse at 5,000 workers - 440 examinations per record with a SINGLE scanner,
     * against 1.00 at ten. See {@code docs/inflight/perf-direct-pull-collapse-is-the-scan.md}.
     * <p>
     * <b>The ordering enforcement this used to be entangled with is untouched</b>, and that is deliberate: the
     * ordered modes still walk the entry map, because in-flight records staying visible to that walk is how a
     * {@code KEY} shard excludes a second taker. Only the unordered path, which has no ordering to enforce, reads
     * the index.
     * <p>
     * So if this number CHANGES again, that is still the signal. Higher means the unordered scan has started
     * looking at entries the index should have kept out of its way.
     */
    static final long EXPECTED_UNORDERED_EXAMINATIONS = RECORDS;

    @Test
    void keyAndUnorderedDispatchExamineTheExpectedNumberOfEntries() {
        // no warm-up and no repeats: a count needs neither, which is most of the point
        long keyExamined = dispatchAll(ProcessingOrder.KEY);
        long unorderedExamined = dispatchAll(ProcessingOrder.UNORDERED);

        assertWithMessage(
                "KEY dispatch examined %s shard entries for %s records. It must stay at exactly one per record: "
                        + "each distinct key gets its own shard, and the scan leaves after the head. Anything more "
                        + "means the scan is walking entries it has no reason to look at, and because KEY is the "
                        + "mode with the MOST shards, that cost is multiplied by the shard count.",
                keyExamined, RECORDS)
                .that(keyExamined).isEqualTo(EXPECTED_KEY_EXAMINATIONS);

        assertWithMessage(
                "UNORDERED dispatch examined %s shard entries for %s records, expected exactly one each. The "
                        + "unordered path walks ShardOccupancy's index of unheld offsets, so the in-flight prefix "
                        + "is not in its way at all. HIGHER means that index has stopped keeping records out of "
                        + "the scan's path - and the cost of that is not the ratio you see here, it is that the "
                        + "figure starts growing with maxConcurrency again, which is what collapsed the "
                        + "direct-pull engine at 5,000 workers. This is a COUNT, not a timing: machine load "
                        + "cannot move it, so do not re-run hoping it passes.",
                unorderedExamined, RECORDS)
                .that(unorderedExamined).isEqualTo(EXPECTED_UNORDERED_EXAMINATIONS);
    }

    private long dispatchAll(ProcessingOrder ordering) {
        var module = new PCModuleTestEnv(ParallelConsumerOptions.<String, String>builder()
                .ordering(ordering)
                .build());
        WorkManager<String, String> wm = module.workManager();
        wm.onPartitionsAssigned(UniLists.of(TP));
        var sm = wm.getSm();

        List<ConsumerRecord<String, String>> recs = new ArrayList<>(RECORDS);
        for (int i = 0; i < RECORDS; i++) {
            // one distinct key per record: the shape that makes the two modes maximally different
            recs.add(new ConsumerRecord<>(TOPIC, 0, i, "key-" + i, "value-" + i));
        }
        Map<TopicPartition, List<ConsumerRecord<String, String>>> m = new HashMap<>();
        m.put(TP, recs);
        wm.registerWork(new EpochAndRecordsMap<>(new ConsumerRecords<>(m), wm.getPm()));

        int taken = 0;
        long before = sm.getDispatchScanMeter().getEntriesExamined();
        while (taken < RECORDS) {
            var batch = wm.getWorkIfAvailable(BATCH);
            if (batch.isEmpty()) {
                break;
            }
            taken += batch.size();
        }
        long examined = sm.getDispatchScanMeter().getEntriesExamined() - before;

        // If a mode stops handing out work early the comparison is meaningless, so fail loudly rather than
        // reporting a cheap run that did less.
        assertThat(taken).isEqualTo(RECORDS);
        return examined;
    }
}
