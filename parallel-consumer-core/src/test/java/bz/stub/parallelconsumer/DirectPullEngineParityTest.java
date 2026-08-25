package bz.stub.parallelconsumer;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions.ProcessingOrder;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;

/**
 * The direct-pull engine driven end to end, against the behaviour it is supposed to be equivalent to.
 * <p>
 * WHY THIS EXISTS. {@code DirectPullWorkerPool} shipped onto this branch as roughly five hundred lines with no
 * dedicated tests, and it is reachable only through the {@code pc.directPull} system property - so the whole
 * engine is invisible to an ordinary suite run, which is exactly the gap
 * {@code docs/inflight/test-ci-opt-in-paths-unexercised.md} describes. These tests turn the option ON in their
 * own options rather than reading the property, so the engine is exercised by the DEFAULT suite as well as by
 * a {@code -Dpc.directPull=true} run. That is deliberate: an opt-in path nothing exercises is an untested
 * path however many other tests exist.
 * <p>
 * WHAT IS ASSERTED is the user-visible contract, not the engine's mechanism - exactly-once delivery, the
 * ordering guarantee, that a pause stops delivery, and that a draining close finishes what it took. All four
 * are things direct pull must agree with the shipped engine on, so the same assertions would hold for either.
 *
 * @author Antony Stubbs
 * @see bz.stub.parallelconsumer.internal.DirectPullWorkerPool
 */
@Slf4j
class DirectPullEngineParityTest extends ParallelEoSStreamProcessorTestBase {

    static final int RECORDS = 500;

    /**
     * Every offset delivered to the user function, in delivery order, with duplicates preserved - a set would
     * silently absorb the very defect this is looking for.
     */
    final List<Long> deliveries = Collections.synchronizedList(new ArrayList<>());

    private void setupDirectPull(ProcessingOrder ordering, int maxConcurrency) {
        setupParallelConsumerInstance(ParallelConsumerOptions.<String, String>builder()
                .ordering(ordering)
                .maxConcurrency(maxConcurrency)
                .directPullEngine(true)
                .build());
    }

    private void produce(int count, java.util.function.IntFunction<String> keyFor) {
        for (int i = 0; i < count; i++) {
            consumerSpy.addRecord(ktu.makeRecord(keyFor.apply(i), "v-" + i));
        }
    }

    /**
     * The headline guarantee, and the one a double-claim would break: every record reaches the user function
     * exactly once.
     * <p>
     * Deliveries are collected as a LIST and counted per offset, so a record handed to two workers shows up as
     * a count of two rather than being absorbed by a set. The commit assertion is the other half - a record
     * delivered but never accounted for would leave the frontier short.
     */
    @Test
    @SneakyThrows
    void everyRecordIsDeliveredExactlyOnceUnderDirectPull() {
        setupDirectPull(ProcessingOrder.UNORDERED, 8);
        parallelConsumer.poll(context -> deliveries.add(context.offset()));

        produce(RECORDS, i -> "key-" + i);

        Awaitility.waitAtMost(defaultTimeout)
                .alias(RECORDS + " records delivered")
                .until(() -> deliveries.size() >= RECORDS);

        // give any duplicate a chance to show up rather than declaring success the instant the count is met
        awaitForSomeLoopCycles(3);

        assertWithMessage("offsets delivered more than once: %s", duplicates())
                .that(duplicates()).isEmpty();
        assertWithMessage("exactly the produced records were delivered, no more and no fewer")
                .that(deliveries).hasSize(RECORDS);
        awaitForCommit(RECORDS);
    }

    /**
     * The ordering guarantee, at the level a user sees it: two records with the same key must never be inside
     * the user function at the same time, and they must arrive in offset order.
     * <p>
     * Under the shipped engine a single-threaded control loop is the only thing that selects work, so this
     * holds by construction. Under direct pull every worker selects concurrently, and the guarantee rests
     * instead on two things that are nowhere named as the ordering lock: {@code ProcessingShard} breaking its
     * scan after the head record whether or not it managed to claim it, and
     * {@code WorkContainer.onQueueingForExecution()} returning whether the claim was won. This test is worth
     * having BECAUSE it passes - it is what catches the next rework of selection quietly dropping either.
     */
    @Test
    @SneakyThrows
    void sameKeyRecordsAreNeitherConcurrentNorOutOfOrderUnderDirectPull() {
        int keys = 10;
        int perKey = 20;
        setupDirectPull(ProcessingOrder.KEY, 8);

        Map<String, AtomicInteger> concurrentPerKey = new ConcurrentHashMap<>();
        Map<String, Long> lastOffsetPerKey = new ConcurrentHashMap<>();
        List<String> violations = Collections.synchronizedList(new ArrayList<>());

        parallelConsumer.poll(context -> {
            String key = context.getSingleConsumerRecord().key();
            long offset = context.offset();
            int concurrent = concurrentPerKey.computeIfAbsent(key, k -> new AtomicInteger()).incrementAndGet();
            try {
                if (concurrent > 1) {
                    violations.add(key + " had " + concurrent + " records in the user function at once, at offset " + offset);
                }
                // hold the key long enough that a second record for it would overlap rather than merely follow
                Thread.sleep(1);
                Long previous = lastOffsetPerKey.put(key, offset);
                if (previous != null && offset <= previous) {
                    violations.add(key + " went backwards: offset " + offset + " after " + previous);
                }
                deliveries.add(offset);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                concurrentPerKey.get(key).decrementAndGet();
            }
        });

        for (int r = 0; r < perKey; r++) {
            final int round = r;
            produce(keys, i -> "key-" + i + "-round-marker");
            log.trace("produced round {}", round);
        }

        int total = keys * perKey;
        Awaitility.waitAtMost(defaultTimeout)
                .alias(total + " records delivered")
                .until(() -> deliveries.size() >= total);

        assertWithMessage("KEY ordering was violated under concurrent pull: %s", violations)
                .that(violations).isEmpty();
        assertWithMessage("and no record was delivered twice: %s", duplicates())
                .that(duplicates()).isEmpty();
    }

    /**
     * A pause must stop delivery, and a resume must deliver what was buffered exactly once.
     * <p>
     * The count after the resume is asserted as an EXACT total against the records produced, which is what
     * turns this from a liveness check into a double-delivery check: an engine that redelivered one record
     * during the pause/resume transition overshoots and fails, where a "reached at least N" assertion would
     * pass.
     */
    @Test
    @SneakyThrows
    void pausingStopsDeliveryAndResumingDeliversTheRestExactlyOnce() {
        int firstSet = 100;
        int secondSet = 100;
        setupDirectPull(ProcessingOrder.UNORDERED, 4);
        parallelConsumer.poll(context -> deliveries.add(context.offset()));

        produce(firstSet, i -> "first-" + i);
        Awaitility.waitAtMost(defaultTimeout).until(() -> deliveries.size() >= firstSet);
        awaitForCommit(firstSet);

        parallelConsumer.pauseIfRunning();
        awaitForOneLoopCycle();
        int deliveredWhenPaused = deliveries.size();

        produce(secondSet, i -> "second-" + i);
        awaitForSomeLoopCycles(3);

        assertWithMessage("nothing new may be delivered while paused - direct pull has no pre-loaded queue to "
                + "drain, so this is exact")
                .that(deliveries.size()).isEqualTo(deliveredWhenPaused);

        parallelConsumer.resumeIfPaused();

        Awaitility.waitAtMost(defaultTimeout)
                .alias("both sets delivered after resume")
                .until(() -> deliveries.size() >= firstSet + secondSet);
        awaitForSomeLoopCycles(3);

        assertWithMessage("exactly both sets, so nothing was redelivered across the pause: duplicates were %s",
                duplicates())
                .that(deliveries).hasSize(firstSet + secondSet);
    }

    /**
     * Shutdown, where direct pull's workers are parked on a condition rather than sitting in an executor's
     * queue. A draining close has to wake them, let them finish, and commit the result.
     */
    @Test
    @SneakyThrows
    void aDrainingCloseFinishesEverythingItTookAndCommitsIt() {
        setupDirectPull(ProcessingOrder.UNORDERED, 8);
        parallelConsumer.poll(context -> deliveries.add(context.offset()));

        produce(RECORDS, i -> "key-" + i);

        Awaitility.waitAtMost(defaultTimeout).until(() -> deliveries.size() >= RECORDS);

        parallelConsumer.closeDrainFirst();

        assertWithMessage("a draining close leaves nothing delivered twice: %s", duplicates())
                .that(duplicates()).isEmpty();
        assertThat(deliveries).hasSize(RECORDS);
        assertCommitsContains(pl.tlinkowski.unij.api.UniLists.of(RECORDS));
    }

    /**
     * @return offsets delivered more than once, with their delivery counts
     */
    private Map<Long, Integer> duplicates() {
        Map<Long, Integer> counts = new LinkedHashMap<>();
        synchronized (deliveries) {
            for (Long offset : deliveries) {
                counts.merge(offset, 1, Integer::sum);
            }
        }
        Map<Long, Integer> dupes = new LinkedHashMap<>();
        counts.forEach((offset, count) -> {
            if (count > 1) {
                dupes.put(offset, count);
            }
        });
        return dupes;
    }
}
