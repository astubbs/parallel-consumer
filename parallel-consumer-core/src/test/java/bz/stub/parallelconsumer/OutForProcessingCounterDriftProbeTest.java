package bz.stub.parallelconsumer;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.state.WorkManager;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static bz.stub.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.UNORDERED;
import static com.google.common.truth.Truth.assertWithMessage;
import static org.awaitility.Awaitility.await;
import static pl.tlinkowski.unij.api.UniLists.of;

/**
 * MEASUREMENT PROBE, not intended to merge as-is: does {@code WorkManager#numberRecordsOutForProcessing} drift
 * (stay permanently above the true number of records out with the worker pool) after partitions are revoked while
 * records are in flight? See confluentinc#857 and
 * docs/solutions/test-flakiness/pc-silent-stall-under-contention-2026-07-29.md.
 * <p>
 * Method: block the user function on a gate so records are genuinely out with the pool, then drive the engine's own
 * rebalance-listener path ({@code onPartitionsRevoked} then {@code onPartitionsAssigned}, as the broker poll thread
 * would during a real rebalance), release the gate, wait for true quiescence (no user function running, nothing
 * awaiting selection, counter stable), and compare the counter against ground truth (zero records out with the
 * pool). Repeated for several revoke cycles so a per-cycle leak accumulates visibly.
 */
@Slf4j
@Timeout(120)
class OutForProcessingCounterDriftProbeTest {

    static final String TOPIC = "drift-probe";
    static final TopicPartition TP = new TopicPartition(TOPIC, 0);
    static final int CYCLES = 5;
    static final int RECORDS_PER_CYCLE = 20;

    MockConsumer<String, String> mockConsumer;
    ParallelEoSStreamProcessor<String, String> pc;

    /** Number of threads currently inside the user function - the ground truth for "genuinely in flight". */
    final AtomicInteger inUserFunction = new AtomicInteger();
    final AtomicInteger processedCount = new AtomicInteger();
    /** Gate the user function blocks on; swapped per cycle. Starts open. */
    volatile CountDownLatch gate = new CountDownLatch(0);

    @AfterEach
    void tearDown() {
        if (pc != null && !pc.isClosedOrFailed()) {
            pc.close();
        }
    }

    @Test
    void revokeWhileRecordsInFlightDoesNotDriftTheCounter() {
        mockConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        var options = ParallelConsumerOptions.<String, String>builder()
                .consumer(mockConsumer)
                .ordering(UNORDERED) // several records in flight from the single partition
                .maxConcurrency(4)
                .build();
        pc = new ParallelEoSStreamProcessor<>(options);
        pc.subscribe(of(TOPIC));
        // the manual rebalance dance MockConsumer requires - see MockConsumerTestBase
        mockConsumer.rebalance(Collections.singletonList(TP));
        pc.onPartitionsAssigned(of(TP));
        mockConsumer.updateBeginningOffsets(Collections.singletonMap(TP, 0L));

        pc.poll(context -> {
            inUserFunction.incrementAndGet();
            try {
                gate.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                inUserFunction.decrementAndGet();
                processedCount.incrementAndGet();
            }
        });

        WorkManager<String, String> wm = pc.getWm();
        long offset = 0;
        List<String> cycleReports = new ArrayList<>();
        List<Integer> quiescedCounters = new ArrayList<>();

        for (int cycle = 0; cycle < CYCLES; cycle++) {
            gate = new CountDownLatch(1); // close the gate for this cycle

            for (int i = 0; i < RECORDS_PER_CYCLE; i++) {
                mockConsumer.addRecord(new ConsumerRecord<>(TOPIC, 0, offset, "k" + offset, "v" + offset));
                offset++;
            }

            // wait until records are genuinely out with the pool (workers blocked inside the user function)
            await().atMost(Duration.ofSeconds(30)).until(() -> inUserFunction.get() > 0);
            int counterBeforeRevoke = wm.getNumberRecordsOutForProcessing();
            int trueInFlightBeforeRevoke = inUserFunction.get();

            // Revoke while in flight, then reassign - the engine's own listener path, as the broker poll
            // thread runs it during a rebalance. The mock's assignment is deliberately left untouched:
            // PC's epoch bump and state truncation are driven entirely by these listener calls, and swapping
            // the MockConsumer assignment from the test thread races the poll thread's own pause/resume
            // bookkeeping (measured: IllegalStateException "No current assignment" killing pc-broker-poll) -
            // an interleaving impossible in production, where the listener runs inside poll() on that thread.
            pc.onPartitionsRevoked(of(TP));
            pc.onPartitionsAssigned(of(TP));

            // release the in-flight work; stale containers now return through the mailbox
            gate.countDown();

            // wait for true quiescence: nothing in the user function, nothing awaiting selection,
            // and the counter stable for 2s (mailbox fully drained by the controller)
            int quiescedCounter = awaitQuiescenceAndReadCounter(wm);
            quiescedCounters.add(quiescedCounter);
            String report = String.format(
                    "cycle %d: beforeRevoke counter=%d trueInFlight=%d | afterQuiesce counter=%d trueInFlight=%d",
                    cycle, counterBeforeRevoke, trueInFlightBeforeRevoke, quiescedCounter, inUserFunction.get());
            log.warn("DRIFT-PROBE {}", report);
            cycleReports.add(report);
        }

        // liveness check: with the gate open, a fresh batch must still flow end to end
        int processedSoFar = processedCount.get();
        for (int i = 0; i < RECORDS_PER_CYCLE; i++) {
            mockConsumer.addRecord(new ConsumerRecord<>(TOPIC, 0, offset, "k" + offset, "v" + offset));
            offset++;
        }
        await().atMost(Duration.ofSeconds(30)).until(() -> processedCount.get() >= processedSoFar + RECORDS_PER_CYCLE);
        log.warn("DRIFT-PROBE liveness: processed {} records after final revoke cycle (total {})",
                processedCount.get() - processedSoFar, processedCount.get());

        cycleReports.forEach(r -> log.warn("DRIFT-PROBE SUMMARY {}", r));

        assertWithMessage("numberRecordsOutForProcessing at quiescence (ground truth in-flight = 0) per cycle")
                .that(quiescedCounters)
                .isEqualTo(of(0, 0, 0, 0, 0));
        assertWithMessage("PC control thread must survive the revoke cycles")
                .that(pc.getFailureCause()).isNull();
    }

    /**
     * The other branch of the stale check: partition revoked and NOT reassigned, so returning containers meet
     * {@code RemovedPartitionState} (partition-not-assigned) rather than an epoch mismatch. This is exactly the
     * case the astubbs#29 fix's comment worries about ("after pm.onPartitionsRevoked, entries will be gone").
     */
    @Test
    void revokeWithoutReassignStillBalancesTheCounter() {
        mockConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        var options = ParallelConsumerOptions.<String, String>builder()
                .consumer(mockConsumer)
                .ordering(UNORDERED)
                .maxConcurrency(4)
                .build();
        pc = new ParallelEoSStreamProcessor<>(options);
        pc.subscribe(of(TOPIC));
        mockConsumer.rebalance(Collections.singletonList(TP));
        pc.onPartitionsAssigned(of(TP));
        mockConsumer.updateBeginningOffsets(Collections.singletonMap(TP, 0L));

        pc.poll(context -> {
            inUserFunction.incrementAndGet();
            try {
                gate.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                inUserFunction.decrementAndGet();
                processedCount.incrementAndGet();
            }
        });

        WorkManager<String, String> wm = pc.getWm();
        gate = new CountDownLatch(1);
        for (int i = 0; i < RECORDS_PER_CYCLE; i++) {
            mockConsumer.addRecord(new ConsumerRecord<>(TOPIC, 0, i, "k" + i, "v" + i));
        }
        await().atMost(Duration.ofSeconds(30)).until(() -> inUserFunction.get() > 0);
        int counterBeforeRevoke = wm.getNumberRecordsOutForProcessing();

        pc.onPartitionsRevoked(of(TP)); // revoke only - partition state is gone for good
        gate.countDown();

        int quiescedCounter = awaitQuiescenceAndReadCounter(wm);
        log.warn("DRIFT-PROBE revoke-only: beforeRevoke counter={} | afterQuiesce counter={} trueInFlight={}",
                counterBeforeRevoke, quiescedCounter, inUserFunction.get());

        assertWithMessage("counter at quiescence after revoke-without-reassign (ground truth in-flight = 0)")
                .that(quiescedCounter).isEqualTo(0);
        assertWithMessage("PC control thread must survive the revoke")
                .that(pc.getFailureCause()).isNull();
    }

    /**
     * Waits (bounded, without throwing) until ground truth says nothing is out with the pool and the counter has
     * been stable for 2 seconds, then returns the counter value - so a drifted counter is measured, not just
     * failed on.
     */
    private int awaitQuiescenceAndReadCounter(WorkManager<String, String> wm) {
        long deadlineNanos = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        int lastValue = Integer.MIN_VALUE;
        long stableSinceNanos = System.nanoTime();
        while (System.nanoTime() < deadlineNanos) {
            int counter = wm.getNumberRecordsOutForProcessing();
            boolean truthQuiet = inUserFunction.get() == 0
                    && wm.getNumberOfWorkQueuedInShardsAwaitingSelection() == 0;
            if (counter != lastValue) {
                lastValue = counter;
                stableSinceNanos = System.nanoTime();
            }
            if (truthQuiet && System.nanoTime() - stableSinceNanos > Duration.ofSeconds(2).toNanos()) {
                return counter;
            }
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        log.warn("DRIFT-PROBE quiescence deadline hit; counter={} inUserFunction={} awaitingSelection={}",
                wm.getNumberRecordsOutForProcessing(), inUserFunction.get(),
                wm.getNumberOfWorkQueuedInShardsAwaitingSelection());
        return wm.getNumberRecordsOutForProcessing();
    }
}
