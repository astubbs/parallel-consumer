package io.confluent.parallelconsumer.integrationTests.utils;

/*-
 * Copyright (C) 2020-2026 Confluent, Inc. and contributors
 */

import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.ParallelConsumerOptions.CommitMode;
import io.confluent.parallelconsumer.ParallelConsumerOptions.ProcessingOrder;
import io.confluent.parallelconsumer.ParallelEoSStreamProcessor;
import lombok.Builder;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.DisconnectException;
import org.apache.kafka.common.errors.WakeupException;

import java.nio.channels.ClosedChannelException;
import java.time.Duration;
import java.util.Optional;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static io.confluent.parallelconsumer.internal.AbstractParallelEoSStreamProcessor.MDC_INSTANCE_ID;
import static pl.tlinkowski.unij.api.UniLists.of;

/**
 * Manages the lifecycle of a {@link ParallelEoSStreamProcessor} instance in multi-instance
 * integration tests. Handles creation, start, stop, toggle (for chaos monkey), and restart
 * with proper exception classification.
 * <p>
 * Each call to {@link #run()} creates a fresh PC + consumer, so restarts don't carry over
 * stale state from the previous instance. This simulates what a real supervisor would do
 * (start a new process).
 * <p>
 * On restart, checks the previous PC's failure cause:
 * <ul>
 *   <li>Expected close exceptions (see {@link #isExpectedCloseException}) → logged at WARN, restart allowed</li>
 *   <li>Unexpected exceptions → thrown as RuntimeException (fails the test — acts as a canary for real bugs)</li>
 * </ul>
 *
 * @see io.confluent.parallelconsumer.integrationTests.MultiInstanceRebalanceTest
 */
@Slf4j
@Getter
@ToString
public class ManagedPCInstance implements Runnable {

    private static final AtomicInteger ID_GENERATOR = new AtomicInteger();

    private final int instanceId;
    private final Config config;
    private final KafkaClientUtils kcu;

    @Getter
    private volatile ParallelEoSStreamProcessor<String, String> parallelConsumer;
    private volatile boolean started = false;

    @ToString.Exclude
    private final Queue<String> consumedKeys = new ConcurrentLinkedQueue<>();

    /** Callback invoked for each consumed record — lets the test track overall progress */
    @ToString.Exclude
    private final Consumer<String> onConsumed;

    public ManagedPCInstance(Config config, KafkaClientUtils kcu, Consumer<String> onConsumed) {
        this.config = config;
        this.kcu = kcu;
        this.onConsumed = onConsumed;
        this.instanceId = ID_GENERATOR.getAndIncrement();
    }

    @Override
    public void run() {
        org.slf4j.MDC.put(MDC_INSTANCE_ID, "Runner-" + instanceId);

        // Wait for the previous PC's threads to fully stop before creating a new one.
        // Without this, two broker poll threads can briefly coexist, causing
        // ConcurrentModificationException on the KafkaConsumer.
        if (parallelConsumer != null) {
            int waitMs = 0;
            while (!parallelConsumer.isClosedOrFailed() && waitMs < 10_000) {
                try {
                    Thread.sleep(100);
                    waitMs += 100;
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
            if (waitMs >= 10_000) {
                log.warn("Instance {} previous PC did not close within 10s, proceeding anyway", instanceId);
            }
        }

        started = true;
        log.info("Running consumer instance {}", instanceId);

        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, config.maxPoll);
        if (config.useCooperativeAssignor) {
            consumerProps.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
                    "org.apache.kafka.clients.consumer.CooperativeStickyAssignor");
        }
        KafkaConsumer<String, String> newConsumer = kcu.createNewConsumer(false, consumerProps);

        this.parallelConsumer = new ParallelEoSStreamProcessor<>(ParallelConsumerOptions.<String, String>builder()
                .ordering(config.order)
                .consumer(newConsumer)
                .commitMode(config.commitMode)
                .maxConcurrency(config.maxConcurrency)
                .build());

        this.parallelConsumer.setTimeBetweenCommits(Duration.ofSeconds(1));
        this.parallelConsumer.setMyId(Optional.of("PC-" + instanceId));
        this.parallelConsumer.subscribe(of(config.inputTopic));

        parallelConsumer.poll(record -> {
            if (config.pollDelayMs > 0) {
                try {
                    Thread.sleep(config.pollDelayMs);
                } catch (InterruptedException e) {
                    // ignore — shutdown in progress
                }
            }
            consumedKeys.add(record.key());
            onConsumed.accept(record.key());
        });
    }

    public void stop() {
        log.info("Stopping instance {}", instanceId);
        started = false;
        parallelConsumer.close();
    }

    /**
     * Restart: checks the previous PC's failure cause, classifies it, then resubmits to the executor.
     * Expected close exceptions are logged. Unexpected exceptions fail the test.
     */
    public void start(ExecutorService pcExecutor) {
        if (parallelConsumer != null) {
            Exception failureCause = parallelConsumer.getFailureCause();
            if (failureCause != null) {
                if (isExpectedCloseException(failureCause)) {
                    log.warn("Instance {} had expected close exception (restarting): {}",
                            instanceId, failureCause.getMessage());
                } else {
                    throw new RuntimeException(
                            "Instance " + instanceId + " died from unexpected error: " + failureCause.getMessage(),
                            failureCause);
                }
            }
        }
        log.info("Starting instance {}", instanceId);
        pcExecutor.submit(this);
    }

    public void toggle(ExecutorService pcExecutor) {
        if (started) {
            stop();
        } else {
            start(pcExecutor);
        }
    }

    public void close() {
        log.info("Closing instance {}", instanceId);
        stop();
    }

    /**
     * Whitelist-only exception classification. Walks the cause chain looking for known
     * close-related exceptions. Everything not on the whitelist is treated as an unexpected
     * bug that should fail the test.
     */
    public static boolean isExpectedCloseException(Throwable t) {
        Throwable current = t;
        while (current != null) {
            if (current instanceof InterruptedException ||
                    current instanceof WakeupException ||
                    current instanceof DisconnectException ||
                    current instanceof ClosedChannelException ||
                    current instanceof TimeoutException) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }

    /**
     * Configuration for a managed PC instance. Use the builder.
     */
    @Builder
    @Getter
    public static class Config {
        @Builder.Default private final int maxPoll = 500;
        private final CommitMode commitMode;
        private final ProcessingOrder order;
        private final String inputTopic;
        @Builder.Default private final int pollDelayMs = 0;
        @Builder.Default private final int maxConcurrency = 10;
        @Builder.Default private final boolean useCooperativeAssignor = false;
    }
}
