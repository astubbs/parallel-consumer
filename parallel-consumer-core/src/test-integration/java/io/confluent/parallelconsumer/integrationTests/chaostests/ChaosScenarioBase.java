package io.confluent.parallelconsumer.integrationTests.chaostests;

/*-
 * Copyright (C) 2020-2026 Confluent, Inc. and contributors
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
}
