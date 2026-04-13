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
    @Getter
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

        // Wait for the previous PC to fully close — including its internal threads finishing
        // and the KafkaConsumer being closed on the poll thread. PC.close() blocks until
        // the control thread finishes, which waits for the poll thread (brokerPollSubsystem
        // .closeAndWait), which closes the consumer. So by the time isClosedOrFailed() returns
        // true, the consumer should be fully closed and deregistered from the group.
        // See #857.
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

        // started flag is set in start(), not here — prevents double-submission
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

    /** True while a background close is in progress — prevents toggle from restarting prematurely */
    private volatile boolean closePending = false;

    public void stop() {
        log.info("Stopping instance {}", instanceId);
        started = false;
        parallelConsumer.close();
    }

    /**
     * Non-blocking stop: signals close and returns immediately. The close completes
     * in a background thread. Use this from the chaos monkey so it isn't blocked for
     * 30-40s while the PC shuts down. The {@link #closePending} flag prevents
     * {@link #toggle} from restarting until close finishes.
     */
    public void stopAsync() {
        log.info("Async stopping instance {}", instanceId);
        started = false;
        closePending = true;
        var pcToClose = parallelConsumer;
        new Thread(() -> {
            try {
                pcToClose.close();
            } catch (Exception e) {
                log.warn("Instance {} background close error: {}", instanceId, e.getMessage());
            } finally {
                closePending = false;
            }
        }, "pc-close-" + instanceId).start();
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
        started = true; // set BEFORE submit so next toggle() sees it — prevents double-submission
        log.info("Starting instance {}", instanceId);
        pcExecutor.submit(this);
    }

    public void toggle(ExecutorService pcExecutor) {
        if (closePending) {
            log.trace("Instance {} toggle skipped — close still pending", instanceId);
            return;
        }
        if (started) {
            stopAsync(); // non-blocking so the chaos monkey isn't frozen during close
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
