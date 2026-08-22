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
 * WHAT THE NUMBERS TURNED OUT TO BE, which is not what this test originally assumed. KEY examines exactly one
 * entry per record. UNORDERED examines 20.5 per record on this workload - it is genuinely quadratic, because
 * in-flight records stay in the shard and every pass restarts from the beginning of it. The first version of this
 * test asserted the two modes cost "the same" within a factor of four, which was calibrated against the stopwatch
 * and was simply wrong about the code. The counts are exact and deterministic, so they are asserted exactly; see
 * {@link #EXPECTED_UNORDERED_EXAMINATIONS} for why that quadratic cost is not a defect and what to do if the
 * number ever moves.
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
     * UNORDERED examines {@code BATCH x (1 + 2 + ... + PASSES)}, because every in-flight record stays in the shard
     * and each pass restarts from the beginning of it: pass <i>k</i> walks past the {@code (k-1) x BATCH} records
     * already taken before it reaches new ones.
     * <p>
     * <b>That is quadratic, it is known, and this test asserts it rather than tolerating it.</b> In-flight records
     * remaining in the shard is HOW ORDERING IS ENFORCED - the scan has to see them to know the shard is blocked -
     * so it is not a defect to be fixed in passing. Two branches have measured attempts at removing the rescan:
     * {@code perf/split-shard-inflight} (10x cheaper dispatch, 0% end to end) and {@code perf/resume-shard-scan}
     * (+0.2%). Neither shipped, because at any realistic per-record handler delay this cost is invisible - the
     * end-to-end benchmark puts the two modes 0.9% apart.
     * <p>
     * So if this number CHANGES, that is the signal. Lower means someone landed the optimisation, and this
     * constant should be updated deliberately along with a note saying which. Higher means the scan got worse.
     */
    static final long EXPECTED_UNORDERED_EXAMINATIONS = (long) BATCH * PASSES * (PASSES + 1) / 2;

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
                "UNORDERED dispatch examined %s shard entries, expected %s (BATCH x PASSES x (PASSES+1) / 2, with "
                        + "BATCH=%s and PASSES=%s). LOWER means the per-pass rescan of the in-flight set has been "
                        + "removed or reduced - that is a WIN, update this constant and say which change did it. "
                        + "HIGHER means the scan examines more than the whole in-flight prefix, which nothing "
                        + "should need to. This is a COUNT, not a timing: machine load cannot move it, so do not "
                        + "re-run hoping it passes.",
                unorderedExamined, EXPECTED_UNORDERED_EXAMINATIONS, BATCH, PASSES)
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
