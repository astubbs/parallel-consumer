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
 * Guards the measured finding that {@code KEY} and {@code UNORDERED} dispatch at the same cost.
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
 * WHY IT IS A RATIO AND NOT A THRESHOLD. An absolute figure would encode this machine's speed and
 * would flake on any other. A ratio between two modes measured back to back in the same JVM cancels
 * machine speed, JIT state and load, and leaves the thing actually under test: whether one shard shape
 * costs more to walk than the other.
 * <p>
 * WHY THE TOLERANCE IS WIDE. It is deliberately not tuned to the 0.9% that was measured. A tight bound
 * on a timing test in CI is a flake generator, and this test is not trying to detect drift - it is
 * trying to detect a scan that has become superlinear in one shape, which shows up as multiples, not
 * percentages. If this ever fails, the cause is structural and the benchmark in {@code bench/} is where
 * to quantify it.
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

    /**
     * Measured on this workload: the clean ratio sits at about 2.3, and injecting a redundant full-map
     * walk into the UNORDERED path alone moves it to about 6.4. Four sits between them with margin on
     * both sides.
     * <p>
     * The bound is NOT 1.0, because the two modes do not in fact cost the same to dispatch - UNORDERED
     * walks one shard holding every in-flight record, KEY walks many shards holding one each, and the
     * first is a little over twice the second here. That difference is real and it is fine: at any
     * realistic per-record handler delay it disappears entirely, and the end-to-end benchmark puts the
     * two modes 0.9% apart. This test guards the SHAPE of the cost, not its absence.
     */
    static final double MAX_RATIO = 4.0;

    /**
     * Timing runs are taken more than once and the FASTEST is used. A minimum is the stable estimator
     * for elapsed time: it is the run least contaminated by GC, scheduling and JIT, whereas a mean
     * drags with every unrelated hiccup on the machine. Without this the ratio moved enough between
     * runs to sit either side of the bound, which is a flake generator rather than a guard.
     */
    static final int TIMING_RUNS = 3;

    @Test
    void keyAndUnorderedCostTheSameToDispatch() {
        // warm both paths before either is timed, so neither pays for class loading and JIT
        dispatchAll(ProcessingOrder.KEY);
        dispatchAll(ProcessingOrder.UNORDERED);

        long keyNanos = fastestDispatch(ProcessingOrder.KEY);
        long unorderedNanos = fastestDispatch(ProcessingOrder.UNORDERED);

        double ratio = (double) unorderedNanos / keyNanos;
        assertWithMessage(
                "UNORDERED dispatch took %sms and KEY took %sms over %s records in batches of %s "
                        + "(ratio %s). The two shard shapes should cost the same to walk; a large ratio "
                        + "means the dispatch scan has become superlinear in one of them. Quantify with "
                        + "bench/run-bisect.sh before changing this bound.",
                unorderedNanos / 1_000_000, keyNanos / 1_000_000, RECORDS, BATCH, ratio)
                .that(ratio).isLessThan(MAX_RATIO);

        // and the same in the other direction, so a regression in KEY is caught too
        assertWithMessage("KEY dispatch took %sms against UNORDERED's %sms (inverse ratio %s)",
                keyNanos / 1_000_000, unorderedNanos / 1_000_000, 1 / ratio)
                .that(1 / ratio).isLessThan(MAX_RATIO);
    }

    private long fastestDispatch(ProcessingOrder ordering) {
        long best = Long.MAX_VALUE;
        for (int i = 0; i < TIMING_RUNS; i++) {
            best = Math.min(best, dispatchAll(ordering));
        }
        return best;
    }

    /**
     * Takes every record without ever completing one, so the in-flight set grows the whole way and each
     * pass has more of it to walk past. That growth is the condition being measured; completing records
     * would remove them from the shard and hide it.
     *
     * @return nanoseconds spent inside dispatch alone - registration is outside the timed region
     */
    private long dispatchAll(ProcessingOrder ordering) {
        var module = new PCModuleTestEnv(ParallelConsumerOptions.<String, String>builder()
                .ordering(ordering)
                .build());
        WorkManager<String, String> wm = module.workManager();
        wm.onPartitionsAssigned(UniLists.of(TP));

        List<ConsumerRecord<String, String>> recs = new ArrayList<>(RECORDS);
        for (int i = 0; i < RECORDS; i++) {
            // one distinct key per record: the shape that makes the two modes maximally different
            recs.add(new ConsumerRecord<>(TOPIC, 0, i, "key-" + i, "value-" + i));
        }
        Map<TopicPartition, List<ConsumerRecord<String, String>>> m = new HashMap<>();
        m.put(TP, recs);
        wm.registerWork(new EpochAndRecordsMap<>(new ConsumerRecords<>(m), wm.getPm()));

        int taken = 0;
        long start = System.nanoTime();
        while (taken < RECORDS) {
            var batch = wm.getWorkIfAvailable(BATCH);
            if (batch.isEmpty()) {
                break;
            }
            taken += batch.size();
        }
        long elapsed = System.nanoTime() - start;

        // If a mode stops handing out work early the timing comparison is meaningless, so fail loudly
        // rather than reporting a fast run that did less.
        assertThat(taken).isEqualTo(RECORDS);
        return elapsed;
    }
}
