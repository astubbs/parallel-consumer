package io.confluent.parallelconsumer.integrationTests;

/*-
 * Copyright (C) 2020-2026 Confluent, Inc.
 */

import io.confluent.parallelconsumer.ParallelEoSStreamProcessor;
import io.confluent.parallelconsumer.integrationTests.utils.KafkaClientUtils.GroupOption;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static io.confluent.csid.utils.LatchTestUtils.awaitLatch;
import static io.confluent.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.UNORDERED;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;
import static pl.tlinkowski.unij.api.UniLists.of;

/**
 * Integration-level guard for the drain-path fix: a member draining for close must not stall the GROUP.
 * <p>
 * <b>Semantics under test</b> (see
 * {@code docs/solutions/test-flakiness/pc-silent-stall-under-contention-2026-07-29.md}):
 * <ul>
 *   <li>A draining member <b>correctly keeps its partition assignment</b> while it finishes in-flight work
 *   so it can commit before leaving - that is what delivers no/near-zero duplicates. Releasing at drain
 *   start would let a sibling immediately reprocess all in-flight work.</li>
 *   <li>But the drainer must stay <b>rebalance-responsive</b>: if another member joins mid-drain, the
 *   group's rebalance must complete promptly (the drainer participates, commits what is done, hands over
 *   cleanly). The pre-fix defect made the drainer protocol-absent ({@code consumer.poll()} never invoked
 *   once draining), so a mid-drain join froze the ENTIRE group until the drain ended or
 *   {@code max.poll.interval.ms} evicted the zombie.</li>
 * </ul>
 * Scenario: PC-A consumes alone, is put into a long drain (in-flight work parked on a latch), then PC-B
 * joins the same group mid-drain. Asserts:
 * <ol>
 *   <li><b>Group liveness</b>: B makes consumption progress while A is still draining (pre-fix: the whole
 *   group is frozen waiting on A).</li>
 *   <li><b>Clean drain</b>: once the parked work is released, A's close completes promptly.</li>
 *   <li><b>Correctness ledger</b>: no records are lost, and B does NOT wholesale-reprocess A's work -
 *   duplicates stay bounded to A's uncommitted tail (commit interval + in-flight), proving the drain
 *   hand-over honoured A's commits.</li>
 * </ol>
 * This is also the W1 "skateboard" scenario of the Chaos Pain Suite design
 * ({@code docs/plans/2026-07-30-001-feat-chaos-pain-suite-design-plan.md}).
 */
@Timeout(180)
@Testcontainers
@Slf4j
class DrainingMemberRebalanceIT extends BrokerIntegrationTest<String, String> {

    private static final int TO_PRODUCE = 1_000;

    @BeforeEach
    void setup() {
        numPartitions = 4; // several partitions so a completed rebalance can spread work across members
        setupTopic();
    }

    @Test
    void groupMakesProgressWhileMemberDrains() throws InterruptedException {
        List<String> producedKeys = produceMessages(TO_PRODUCE);

        // ---- PC-A: sole member; its user function can be switched to park in-flight work on a latch
        Set<String> processedByA = ConcurrentHashMap.newKeySet();
        AtomicBoolean parkNewWork = new AtomicBoolean(false);
        CountDownLatch someWorkParked = new CountDownLatch(1);
        CountDownLatch releaseParkedWork = new CountDownLatch(1);

        ParallelEoSStreamProcessor<String, String> pcA = getKcu().buildPc(UNORDERED, GroupOption.REUSE_GROUP);
        pcA.subscribe(of(getTopic()));
        pcA.poll(recordContexts -> {
            if (parkNewWork.get()) {
                someWorkParked.countDown();
                awaitLatch(releaseParkedWork);
            }
            processedByA.add(recordContexts.key());
        });

        // A is healthy and consuming alone
        await().atMost(30, SECONDS).untilAsserted(() ->
                assertWithMessage("A should be consuming before we start the drain")
                        .that(processedByA.size()).isAtLeast(50));

        // ---- park in-flight work, then start a long drain. A may have already consumed the whole initial
        // batch (UNORDERED x high concurrency is fast), so produce fresh records AFTER arming the park so
        // some work is guaranteed to be in-flight when the drain starts.
        parkNewWork.set(true);
        producedKeys.addAll(produceMessages(200, "parked-"));
        awaitLatch(someWorkParked); // at least one worker is now blocked in-flight -> drain cannot complete
        Thread drainingCloser = new Thread(pcA::closeDrainFirst, "test-draining-closer");
        drainingCloser.start();

        // ---- PC-B joins the SAME group while A is mid-drain -> forces a rebalance
        Set<String> processedByB = ConcurrentHashMap.newKeySet();
        ParallelEoSStreamProcessor<String, String> pcB = getKcu().buildPc(UNORDERED, GroupOption.REUSE_GROUP);
        pcB.subscribe(of(getTopic()));
        pcB.poll(recordContexts -> processedByB.add(recordContexts.key()));

        try {
            // 1) GROUP LIVENESS: the rebalance must complete and B must make progress while A still drains.
            //    Pre-fix, A is protocol-absent: the join blocks on A up to max.poll.interval.ms (5 min
            //    default) and B consumes NOTHING inside this window.
            // Threshold is ANY progress (>=1): the property is that the rebalance completes and B consumes
            // AT ALL while A drains - pre-fix, B gets NOTHING (rebalance blocked on the zombie). A higher
            // bar races A's bounded drain window (worker-pool force-interrupt) under load for no extra
            // discrimination.
            await().alias("group progress while a member drains")
                    .atMost(45, SECONDS)
                    .pollInterval(Duration.ofMillis(200))
                    .failFast(pcB::isClosedOrFailed)
                    .untilAsserted(() -> {
                        assertWithMessage("B (joined mid-drain) should be consuming while A drains")
                                .that(processedByB.size()).isAtLeast(1);
                        assertWithMessage("A should still be draining while B progresses (else this run " +
                                "did not exercise the mid-drain window and cannot discriminate)")
                                .that(drainingCloser.isAlive()).isTrue();
                    });
        } finally {
            // 2) CLEAN DRAIN: release the parked work; A must finish closing promptly
            releaseParkedWork.countDown();
        }

        drainingCloser.join(Duration.ofSeconds(60).toMillis());
        assertWithMessage("A's drain should complete promptly once in-flight work finishes")
                .that(drainingCloser.isAlive()).isFalse();

        // 3) CORRECTNESS LEDGER: run B to completion, then account for every record
        await().alias("all records consumed by the group (no loss)")
                .atMost(60, SECONDS)
                .failFast(pcB::isClosedOrFailed)
                .untilAsserted(() -> {
                    Set<String> union = ConcurrentHashMap.newKeySet();
                    union.addAll(processedByA);
                    union.addAll(processedByB);
                    assertWithMessage("at-least-once: every produced record processed by some member")
                            .that(union).containsAtLeastElementsIn(producedKeys);
                });

        // Duplicates must be bounded to A's uncommitted tail (commit interval is 1s in buildPc + in-flight
        // work), NOT a wholesale reprocess of A's records - which is what release-at-drain-start (or
        // ignoring A's commits) would produce. A processed >= 50 before the drain even started; allow a
        // generous uncommitted tail but reject anything close to full reprocessing.
        Set<String> duplicates = ConcurrentHashMap.newKeySet();
        duplicates.addAll(processedByA);
        duplicates.retainAll(processedByB);
        log.info("Ledger: A processed {}, B processed {}, duplicates {} (bounded tail expected)",
                processedByA.size(), processedByB.size(), duplicates.size());
        assertWithMessage("hand-over must honour A's commits: duplicates bounded to the uncommitted " +
                "tail, not a wholesale reprocess of A's %s records", processedByA.size())
                .that(duplicates.size())
                .isLessThan(Math.max(100, processedByA.size() / 2));

        pcB.close();
    }
}
