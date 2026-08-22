package bz.stub.parallelconsumer;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions.AdaptiveConcurrencyMode;
import bz.stub.parallelconsumer.ParallelConsumerOptions.ProcessingOrder;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;

/**
 * Adaptive concurrency under {@code ENFORCE}, driven end to end, against the behaviour it must not change.
 * <p>
 * WHY THIS EXISTS. {@code ENFORCE} takes the in-flight target away from {@code maxConcurrency} and hands it to a
 * control law that moves it every second - so the number governing dispatch, the poller gate and the drain is no
 * longer a constant. Everything a user is promised has to survive that, and none of it is asserted by the
 * controller's own tests, which stop at the controller. These tests turn the mode ON in their OWN options rather
 * than reading {@code pc.adaptiveConcurrency}, so the mode is exercised by the DEFAULT suite as well as by the
 * execution-mode lane - and, more to the point, {@code ENFORCE} is exercised at all: the system property may
 * select at most {@code OBSERVE}, so a lane alone could never reach enforcement.
 * <p>
 * WHAT IS ASSERTED is the user-visible contract, not the controller's mechanism - exactly-once delivery, the
 * ordering guarantee, that a pause stops delivery, and that a draining close finishes what it took. The
 * controller's internals belong to {@code AdaptiveConcurrencyModeTest} and the admission unit tests; asserting
 * them here would make this a second copy of those rather than a parity check.
 * <p>
 * The seed is deliberately BELOW {@code maxConcurrency}: starting at the ceiling would run the whole suite at
 * today's static width and prove nothing about a target that moves.
 *
 * @author Antony Stubbs
 * @see ParallelConsumerOptions#getAdaptiveConcurrencyMode()
 */
@Slf4j
class AdaptiveConcurrencyParityTest extends EngineParityTestBase {

    static final int RECORDS = 500;

    /** The ceiling the controller adapts under - explicit, so no adaptive-default substitution is in play. */
    static final int MAX_CONCURRENCY = 8;

    /** Where the live target starts: below the ceiling, so dispatch really is admission-bound from record one. */
    static final int INITIAL_TARGET = 2;

    /**
     * The mode is asserted on the built options before the consumer is started, so this suite cannot quietly
     * become a second run of the default engine - which is precisely what it would look like if a future default,
     * a validation change, or the {@code pc.adaptiveConcurrency} property downgraded the request. That the ENGINE
     * then serves the requested mode (rather than warning and deactivating) is
     * {@code AdaptiveConcurrencyCapabilityTest}'s pin; the resolved flag is not reachable from this package.
     */
    private void setupAdaptive(ProcessingOrder ordering) {
        var options = ParallelConsumerOptions.<String, String>builder()
                .ordering(ordering)
                .maxConcurrency(MAX_CONCURRENCY)
                .adaptiveConcurrencyMode(AdaptiveConcurrencyMode.ENFORCE)
                .adaptiveConcurrencyInitialTarget(INITIAL_TARGET)
                .build();
        assertWithMessage("fixture: this suite is only a parity check if enforcement is actually on")
                .that(options.getAdaptiveConcurrencyMode()).isEqualTo(AdaptiveConcurrencyMode.ENFORCE);
        setupParallelConsumerInstance(options);
    }

    /**
     * The headline guarantee, and the one a moving target is most likely to break: every record reaches the user
     * function exactly once.
     * <p>
     * A contracting target must throttle intake without dropping what it already admitted, and a growing one must
     * not re-admit anything on the way up. Deliveries are collected as a LIST and counted per offset, so a record
     * handed out twice shows as a count of two rather than being absorbed by a set.
     */
    @Test
    @SneakyThrows
    void everyRecordIsDeliveredExactlyOnceUnderAdaptiveConcurrency() {
        setupAdaptive(ProcessingOrder.UNORDERED);
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
     * The ordering guarantee, at the level a user sees it: two records with the same key must never be inside the
     * user function at the same time, and they must arrive in offset order.
     * <p>
     * Worth having BECAUSE it passes. Ordering rests on shard selection, and the admission target is a bound on
     * how much of that selection's output is dispatched - a bound applied in the wrong place (per shard rather
     * than per pass, say) would let a shard's head record be re-offered while its predecessor was still running.
     */
    @Test
    @SneakyThrows
    void sameKeyRecordsAreNeitherConcurrentNorOutOfOrderUnderAdaptiveConcurrency() {
        int keys = 10;
        int perKey = 20;
        setupAdaptive(ProcessingOrder.KEY);

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

        assertWithMessage("KEY ordering was violated under an adaptive target: %s", violations)
                .that(violations).isEmpty();
        assertWithMessage("and no record was delivered twice: %s", duplicates())
                .that(duplicates()).isEmpty();
    }

    /**
     * A pause must stop delivery, and a resume must deliver what was buffered exactly once.
     * <p>
     * The count after the resume is asserted as an EXACT total against the records produced, which is what turns
     * this from a liveness check into a double-delivery check. It is also where the controller's pause handling
     * shows up as user-visible behaviour: a paused interval discards the in-progress sample window, and the
     * resume must not stall waiting on a target that never moves again.
     */
    @Test
    @SneakyThrows
    void pausingStopsDeliveryAndResumingDeliversTheRestExactlyOnce() {
        int firstSet = 100;
        int secondSet = 100;
        setupAdaptive(ProcessingOrder.UNORDERED);
        parallelConsumer.poll(context -> deliveries.add(context.offset()));

        produce(firstSet, i -> "first-" + i);
        Awaitility.waitAtMost(defaultTimeout).until(() -> deliveries.size() >= firstSet);
        awaitForCommit(firstSet);

        parallelConsumer.pauseIfRunning();
        awaitForOneLoopCycle();
        int deliveredWhenPaused = deliveries.size();

        produce(secondSet, i -> "second-" + i);
        awaitForSomeLoopCycles(3);

        assertWithMessage("nothing new may be delivered while paused, whatever the admission target says")
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
     * Shutdown, which is where a contracted target would do the most damage if it were left in force: a draining
     * close has to finish everything it took, and finish it inside the drain timeout rather than trickling it out
     * at whatever width the controller happened to have settled on.
     */
    @Test
    @SneakyThrows
    void aDrainingCloseFinishesEverythingItTookAndCommitsIt() {
        setupAdaptive(ProcessingOrder.UNORDERED);
        parallelConsumer.poll(context -> deliveries.add(context.offset()));

        produce(RECORDS, i -> "key-" + i);

        Awaitility.waitAtMost(defaultTimeout).until(() -> deliveries.size() >= RECORDS);

        parallelConsumer.closeDrainFirst();

        assertWithMessage("a draining close leaves nothing delivered twice: %s", duplicates())
                .that(duplicates()).isEmpty();
        assertThat(deliveries).hasSize(RECORDS);
        assertCommitsContains(pl.tlinkowski.unij.api.UniLists.of(RECORDS));
    }
}
