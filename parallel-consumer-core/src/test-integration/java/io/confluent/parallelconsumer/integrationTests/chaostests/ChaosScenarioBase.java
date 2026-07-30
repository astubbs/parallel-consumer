package io.confluent.parallelconsumer.integrationTests.chaostests;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import io.confluent.parallelconsumer.integrationTests.BrokerIntegrationTest;
import io.confluent.parallelconsumer.integrationTests.utils.ManagedPCInstance;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.truth.Truth.assertWithMessage;

/**
 * Shared scaffolding for Chaos Pain Suite scenarios (W1 churn storm, W4 revoke-under-work, ...): the
 * keyed producer, the heavy-tailed NON-interruptible user function, coverage checks, and fleet settling.
 * Scenario classes own their chaos shape (conductor weights/ticks, fleet size, commit mode) - this base
 * owns the mechanics every scenario shares, so scenarios can't drift apart on them.
 */
@Slf4j
abstract class ChaosScenarioBase extends BrokerIntegrationTest<String, String> {

    /**
     * A processing function with a heavy tail: every {@code heavyEvery}-th record dwells
     * {@code heavySleep} NON-interruptibly (sleep-until-deadline). PC's close path force-interrupts
     * stuck workers after ~5s (awaitTermination -> shutdownNow), which would cap every drain/stall at
     * seconds and shrink the windows the probes discriminate on. Real-world slow work often ignores
     * interrupts too (JDBC, native calls, CPU loops).
     */
    protected ManagedPCInstance newInstance(ManagedPCInstance.Config config,
                                            int heavyEvery, Duration heavySleep,
                                            AtomicLong totalConsumed, Queue<String> allConsumed) {
        return new ManagedPCInstance(config, getKcu(), key -> {
            if (isHeavyKey(key, heavyEvery)) {
                long deadline = System.currentTimeMillis() + heavySleep.toMillis();
                boolean interrupted = false;
                long left;
                while ((left = deadline - System.currentTimeMillis()) > 0) {
                    try {
                        Thread.sleep(Math.min(left, 1_000));
                    } catch (InterruptedException e) {
                        interrupted = true; // note it, keep dwelling until the deadline
                    }
                }
                if (interrupted) {
                    Thread.currentThread().interrupt();
                }
            }
            totalConsumed.incrementAndGet();
            allConsumed.add(key);
        });
    }

    /** key format is "key-N"; every heavyEvery-th record is heavy. */
    protected static boolean isHeavyKey(String key, int heavyEvery) {
        int n = Integer.parseInt(key.substring(key.indexOf('-') + 1));
        return n > 0 && n % heavyEvery == 0;
    }

    /** Coverage check is expensive at scale - callers only evaluate once the counter says it's plausible. */
    protected boolean allConsumedCovers(Set<String> expectedKeys, Queue<String> allConsumed) {
        var unique = new HashSet<>(allConsumed);
        return unique.containsAll(expectedKeys);
    }

    protected void produceRange(String topic, int fromInclusive, int toExclusive, Set<String> expectedKeys) {
        try (Producer<String, String> producer = getKcu().createNewProducer(false)) {
            List<Future<RecordMetadata>> sends = new ArrayList<>();
            for (int i = fromInclusive; i < toExclusive; i++) {
                String key = "key-" + i;
                expectedKeys.add(key);
                sends.add(producer.send(new ProducerRecord<>(topic, key, "v-" + i)));
            }
            for (Future<RecordMetadata> send : sends) {
                send.get();
            }
            log.info("Produced [{}..{})", fromInclusive, toExclusive);
        } catch (Exception e) {
            throw new RuntimeException("Producer failed at range [" + fromInclusive + ".." + toExclusive + ")", e);
        }
    }

    /** Close every fleet member that chaos left running, classifying (not asserting on) close errors. */
    protected void settleFleet(ChaosConductor conductor) {
        for (ManagedPCInstance pc : conductor.getFleet()) {
            try {
                if (pc.getParallelConsumer() != null && !pc.getParallelConsumer().isClosedOrFailed()) {
                    pc.getParallelConsumer().close();
                }
            } catch (Exception e) {
                log.warn("Settle-close of instance {}: {}", pc.getInstanceId(), e.getMessage());
            }
        }
    }

    /** Shared finally-block epilogue: stop chaos and the probe, join the background producer, settle
     * the fleet, log the run summary. Runs on both the pass and fail path - it must only tear down and
     * report, never assert (asserting here would mask the primary failure). */
    protected void settleRun(ChaosConductor conductor, ProgressProbe probe, Thread producerThread,
                             AtomicLong totalConsumed) throws InterruptedException {
        conductor.stop();
        List<String> violations = probe.stop();
        producerThread.join(10_000);
        settleFleet(conductor);
        log.info("Run summary: consumed={} (unique tracking via correctness ledger), probe violations={}",
                totalConsumed.get(), violations);
    }

    /** The suite-wide verdict, identical for every scenario by design: probes must be violation-free
     * (each violation carries its own diagnosis), and the correctness ledger must balance - no loss
     * ever, duplicates bounded per disturbance. The seed in every message replays the schedule. */
    protected void assertScenarioSlos(ProgressProbe probe, ChaosConductor conductor, long seed,
                                      Set<String> expectedKeys, Queue<String> allConsumed) {
        assertWithMessage("chaos probes must be violation-free (each violation carries the diagnosis; " +
                "seed %s replays this schedule)", seed)
                .that(probe.getViolations()).isEmpty();

        int disturbances = (int) conductor.getTimeline().stream()
                .filter(entry -> entry.contains("STOP_") || entry.contains("RESTART")).count();
        List<String> ledgerProblems = ProgressProbe.ledger(expectedKeys, allConsumed,
                Math.max(disturbances, 1), /* perDisturbanceAllowance */ 5_000);
        assertWithMessage("correctness ledger must balance (seed %s)", seed)
                .that(ledgerProblems).isEmpty();
    }
}
