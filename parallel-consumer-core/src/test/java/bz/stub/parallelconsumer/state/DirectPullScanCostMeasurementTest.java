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
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * MEASUREMENT ONLY - not a guard test. Separates the two candidate causes of the direct-pull engine's collapse at
 * high concurrency, using {@link DispatchScanMeter} so machine load cannot move the answer.
 * <p>
 * The two hypotheses need different fixes, so they have to be told apart before anything is built:
 * <ol>
 *     <li><b>Claim contention</b> - N workers scan, most lose the compare-and-set in
 *     {@link WorkContainer#onQueueingForExecution()}, park, retry. Cost is proportional to <em>failed claims</em>,
 *     so it needs more than one scanner to exist at all.</li>
 *     <li><b>The scan itself, multiplied by N</b> - under {@code UNORDERED} one shard holds every in-flight record
 *     for its partition, and the scan walks that whole prefix to find the first free entry. Cost is proportional to
 *     <em>in-flight per shard</em>, and is fully present with a single scanner.</li>
 * </ol>
 * <b>The two variables are deliberately decoupled here</b>, which is what makes this a controlled experiment rather
 * than a re-run of the benchmark. In the engine the number of scanning workers and the number of records in flight
 * are the same number, so no observation of the engine can separate them. Here the workers only <em>select</em>; a
 * single controller thread holds exactly {@code inFlight} records out at any instant and completes the oldest to
 * make room. So {@code scanners} and {@code inFlight} move independently, and {@code scanners = 1} is a
 * contention-free control arm at any in-flight depth.
 * <p>
 * Enable with {@code -Dpc.scanCost=true}; it is otherwise skipped, because it takes minutes and asserts nothing.
 *
 * @author Antony Stubbs
 * @see ProcessingShard#getWorkIfAvailable
 * @see DispatchScanMeter
 */
@EnabledIfSystemProperty(named = "pc.scanCost", matches = "true")
class DirectPullScanCostMeasurementTest {

    static final String TOPIC = "scan-cost-topic";

    /** Matches the ten-partition benchmark topic the direct-pull sweep used. */
    static final int SHARDS = Integer.getInteger("pc.scanCost.shards", 10);

    /** How many records each arm processes before its cost is read off. */
    static final int RECORDS = Integer.getInteger("pc.scanCost.records", 20_000);

    @Test
    void sweep() throws Exception {
        String[] arms = System.getProperty("pc.scanCost.arms",
                "1x10,1x100,1x1000,1x5000,10x10,100x100,1000x1000,5000x5000")
                .split(",");
        System.out.println("SCANCOST scanners,inFlight,shards,records,examinations,examinationsPerRecord,wallMs");
        for (String arm : arms) {
            String[] parts = arm.trim().split("x");
            int scanners = Integer.parseInt(parts[0]);
            int inFlight = Integer.parseInt(parts[1]);
            Result r = run(scanners, inFlight, RECORDS);
            System.out.printf("SCANCOST %d,%d,%d,%d,%d,%.2f,%d%n",
                    scanners, inFlight, SHARDS, RECORDS, r.examinations,
                    r.examinations / (double) RECORDS, r.wallMs);
            System.out.flush();
        }
    }

    static class Result {
        long examinations;
        long wallMs;
    }

    /**
     * @param scanners how many threads call {@link WorkManager#getWorkIfAvailable(int)} concurrently
     * @param inFlight how many records are claimed but not yet completed at any instant - held exactly, so it is a
     *                 setting rather than an emergent property of handler timing
     */
    Result run(int scanners, int inFlight, int records) throws Exception {
        var module = new PCModuleTestEnv(ParallelConsumerOptions.<String, String>builder()
                .ordering(ProcessingOrder.UNORDERED)
                .build());
        WorkManager<String, String> wm = module.workManager();
        List<TopicPartition> tps = new ArrayList<>();
        for (int p = 0; p < SHARDS; p++) {
            tps.add(new TopicPartition(TOPIC, p));
        }
        wm.onPartitionsAssigned(tps);

        // Registered up front, so every arm faces the same buffer depth. If the buffer differed between arms it,
        // and not the scan, could be what the numbers were comparing.
        int perShard = records / SHARDS;
        for (int p = 0; p < SHARDS; p++) {
            registerBatch(wm, p, 0, perShard);
        }

        var taken = new ConcurrentLinkedQueue<WorkContainer<String, String>>();
        // Claimed but not yet completed. Capped at inFlight, so a scanner reserves its slot BEFORE scanning and the
        // depth can never overshoot - which is what makes the in-flight depth a setting rather than a measurement.
        var outstanding = new AtomicInteger();
        var stop = new AtomicBoolean();

        var meter = wm.getSm().getDispatchScanMeter();
        long examinedBefore = meter.getEntriesExamined();
        long start = System.currentTimeMillis();

        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < scanners; i++) {
            Thread t = new Thread(() -> {
                while (!stop.get()) {
                    if (outstanding.incrementAndGet() > inFlight) {
                        outstanding.decrementAndGet();
                        Thread.yield();
                        continue;
                    }
                    var work = wm.getWorkIfAvailable(1);
                    if (work.isEmpty()) {
                        outstanding.decrementAndGet();
                        // The real worker parks on a condition here. Parking would make the arm's cost depend on
                        // wake-up timing; the count is what is being measured, so it yields.
                        Thread.yield();
                    } else {
                        taken.addAll(work);
                    }
                }
            }, "scanner-" + i);
            t.setDaemon(true);
            threads.add(t);
        }
        threads.forEach(Thread::start);

        // The controller: the only thread allowed to return results, exactly as in the engine
        // (WorkManager#handleFutureResult is control-thread only).
        Deque<WorkContainer<String, String>> holding = new ArrayDeque<>();
        int completed = 0;
        int drained = 0;
        while (completed < records) {
            WorkContainer<String, String> wc;
            while ((wc = taken.poll()) != null) {
                holding.addLast(wc);
                drained++;
            }
            // Completing merely because the hand-off queue happens to be empty would let the in-flight depth
            // collapse to one, and the arm would then measure a depth nobody asked for. Hold until the window is
            // genuinely full - except in the tail, once every record has been claimed and no more can arrive.
            boolean tail = drained >= records;
            if (!holding.isEmpty() && (outstanding.get() >= inFlight || tail)) {
                var done = holding.pollFirst();
                done.onUserFunctionSuccess();
                wm.handleFutureResult(done);
                outstanding.decrementAndGet();
                completed++;
            } else if (holding.isEmpty()) {
                Thread.yield();
            }
        }
        stop.set(true);

        long wall = System.currentTimeMillis() - start;
        long examined = meter.getEntriesExamined() - examinedBefore;

        for (Thread t : threads) {
            t.join(10_000);
        }

        var r = new Result();
        r.examinations = examined;
        r.wallMs = wall;
        return r;
    }

    private void registerBatch(WorkManager<String, String> wm, int partition, long fromOffset, int count) {
        List<ConsumerRecord<String, String>> recs = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            long off = fromOffset + i;
            recs.add(new ConsumerRecord<>(TOPIC, partition, off, "key-" + off, "value-" + off));
        }
        Map<TopicPartition, List<ConsumerRecord<String, String>>> m = new HashMap<>();
        m.put(new TopicPartition(TOPIC, partition), recs);
        wm.registerWork(new EpochAndRecordsMap<>(new ConsumerRecords<>(m), wm.getPm()));
    }
}
