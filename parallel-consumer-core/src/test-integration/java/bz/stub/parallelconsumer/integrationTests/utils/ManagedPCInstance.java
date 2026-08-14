package bz.stub.parallelconsumer.integrationTests.utils;

/*-
 * Copyright (C) 2020-2026 Confluent, Inc. and contributors
 * Modifications Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.ParallelConsumerOptions.CommitMode;
import bz.stub.parallelconsumer.ParallelConsumerOptions.ProcessingOrder;
import bz.stub.parallelconsumer.ParallelEoSStreamProcessor;
import lombok.AccessLevel;
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
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static bz.stub.parallelconsumer.internal.AbstractParallelEoSStreamProcessor.MDC_INSTANCE_ID;
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
 * @see bz.stub.parallelconsumer.integrationTests.MultiInstanceRebalanceTest
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

    /**
     * Single-flight guard for the start window: claimed by {@link #start} before submitting, released
     * by {@link #run} once the PC is up. Without it, a stop/restart pair drawn while an earlier
     * restart is still parked in {@code run()}'s close-wait loop submits {@code run()} twice, and the
     * two invocations race on {@link #parallelConsumer} - leaving one PC orphaned (a group member
     * nobody ever closes) and two threads closing the other's {@code KafkaConsumer}. That is the
     * {@code ChaosRevokeUnderWorkCooperativeIT} failure under seed 8291601231857558952:
     * {@code ConcurrentModificationException: KafkaConsumer is not safe for multi-threaded access}
     * plus {@code ZOMBIE_MEMBER/REBALANCE_BLOCKED}.
     */
    @ToString.Exclude
    @Getter(AccessLevel.NONE) // class-level @Getter would hand out the mutable guard itself
    private final AtomicBoolean startInFlight = new AtomicBoolean(false);

    /**
     * Set by a stop, cleared by a start. A {@code run()} still queued from an earlier start reads it
     * after the close-wait loop and aborts, rather than bringing up a PC the conductor believes is
     * stopped and will therefore never close.
     * <p>
     * This is the logical negation of {@link #started} at every write site except a rejected
     * submission's rollback - harmless there, because no {@code run()} is in flight to abort. It is
     * kept as its own field deliberately: {@code run()}'s abort then reads as a positive
     * ("a stop happened") rather than a double negative, and the two names keep the toggle protocol
     * and the queued-start abort separable. Do not "simplify" it away by folding it into
     * {@code started} - getting that polarity backwards silently restores the double-submission race
     * this class exists to prevent.
     */
    @Getter(AccessLevel.NONE) // internal signal, like the two guards above and below
    private volatile boolean stopRequested = false;

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
        try {

            // Wait for the previous PC to fully close — including its internal threads finishing
            // and the KafkaConsumer being closed on the poll thread. PC.close() blocks until
            // the control thread finishes, which waits for the poll thread (brokerPollSubsystem
            // .closeAndWait), which closes the consumer. So by the time isClosedOrFailed() returns
            // true, the consumer should be fully closed and deregistered from the group.
            // See confluentinc#857.
            if (parallelConsumer != null) {
                int waitMs = 0;
                // bail out of the wait as soon as a stop lands: an aborting run() creates no
                // replacement PC, so it does not need the old one to have finished closing, and
                // this runs on a bounded work-stealing pool where sleeping holds a carrier thread
                while (!parallelConsumer.isClosedOrFailed() && waitMs < 10_000 && !stopRequested) {
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

            // A stop drawn while this start was queued wins: bringing up a PC now would leave the
            // conductor believing the instance is stopped, so nothing would ever close it.
            if (stopRequested) {
                log.info("Instance {} start aborted - stopped while this start was queued", instanceId);
                return;
            }

            // started flag is set in start(), not here — prevents double-submission
            log.info("Running consumer instance {}", instanceId);

            Properties consumerProps = new Properties();
            consumerProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, config.maxPoll);
            if (config.useCooperativeAssignor) {
                consumerProps.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
                        "org.apache.kafka.clients.consumer.CooperativeStickyAssignor");
            }
            if (config.extraConsumerProps != null) {
                consumerProps.putAll(config.extraConsumerProps); // scenario-specific overrides win last
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

            // Re-check: the pre-build check above is not enough. start() is fire-and-forget, so the
            // conductor thread is free to draw a stop while this method is between that check and
            // here - building and subscribing a PC is real work, not an instant. A stop landing in
            // that window captured the OLD parallelConsumer (or null) and returned, so without this
            // the instance would end up RUNNING a PC the conductor believes is stopped, which
            // nothing would ever close.
            if (stopRequested) {
                log.info("Instance {} start aborted after build - stopped while the PC was coming up",
                        instanceId);
                closeQuietly(this.parallelConsumer);
                return;
            }

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
        } finally {
            // release the start window - a further start() may now submit (see startInFlight)
            startInFlight.set(false);
            // pool threads are reused across instances - do not leak this instance id into later runners (PR astubbs#83 review)
            org.slf4j.MDC.remove(MDC_INSTANCE_ID);
        }
    }

    /**
     * The PCs this instance currently has a closer running for. Keyed by PC, not by instance: the
     * hazard being guarded is two threads inside <em>one</em> {@code KafkaConsumer}
     * ({@code ConcurrentModificationException} against its poll thread), which is a property of the
     * PC, not of the instance that owns it.
     * <p>
     * An instance-wide flag would be both too strong and unsafe. Because {@code start()} does not
     * wait for a close to finish, a restart can build a second PC while the first is still
     * closing - and an instance-wide flag would then refuse the second PC's close entirely, leaving
     * it open with nothing to retry it: an unaccounted group member, the very failure this class
     * exists to prevent. Two <em>different</em> PCs closing at once was always safe.
     */
    @ToString.Exclude
    @Getter(AccessLevel.NONE) // callers get the isClosePending() snapshot below, not the guard itself
    private final Set<ParallelEoSStreamProcessor<String, String>> closingPcs = ConcurrentHashMap.newKeySet();

    /** True while a background close is in progress for any of this instance's PCs. */
    public boolean isClosePending() {
        return !closingPcs.isEmpty();
    }

    /**
     * Record that this instance is stopping, without closing anything. For the drain path, which
     * closes the PC itself via {@code closeDrainFirst()} and so never goes through
     * {@link #stop}/{@link #stopAsync} - leaving {@link #stopRequested} unset, and the queued-start
     * abort in {@link #run} inert for the most frequently drawn stop action.
     */
    public void markStopRequested() {
        stopRequested = true;
        started = false;
    }

    /** Close a PC we are abandoning, without letting its failure mask the reason we abandoned it. */
    private void closeQuietly(ParallelEoSStreamProcessor<String, String> pc) {
        if (pc == null || !closingPcs.add(pc)) {
            return;
        }
        try {
            pc.close();
        } catch (Exception e) {
            log.warn("Instance {} close of abandoned PC failed: {}", instanceId, e.getMessage());
        } finally {
            closingPcs.remove(pc);
        }
    }

    public void stop() {
        log.info("Stopping instance {}", instanceId);
        stopRequested = true;
        started = false;
        var pcToClose = parallelConsumer;
        if (pcToClose == null) {
            return;
        }
        // the same one-closer-per-PC rule stopAsync() enforces: a synchronous close racing the
        // background one would put two threads inside the same KafkaConsumer, which is the
        // ConcurrentModificationException this class was hardened against
        if (!closingPcs.add(pcToClose)) {
            log.info("Instance {} stop skipped - this PC is already being closed", instanceId);
            return;
        }
        try {
            pcToClose.close();
        } finally {
            closingPcs.remove(pcToClose);
        }
    }

    /**
     * Non-blocking stop: signals close and returns immediately. The close completes
     * in a background thread. Use this from the chaos monkey so it isn't blocked for
     * 30-40s while the PC shuts down. The close-in-progress state prevents
     * {@link #toggle} from restarting until close finishes.
     */
    public void stopAsync() {
        stopRequested = true;
        started = false;
        var pcToClose = parallelConsumer;
        if (pcToClose == null) {
            log.info("Instance {} async stop skipped - never started", instanceId);
            return;
        }
        // one closer per PC: a second concurrent close() puts two threads inside the same
        // KafkaConsumer, which throws ConcurrentModificationException against the poll thread.
        // A *different* PC of this instance is a separate close and must not be refused here.
        if (!closingPcs.add(pcToClose)) {
            log.info("Instance {} async stop skipped - this PC is already being closed", instanceId);
            return;
        }
        log.info("Async stopping instance {}", instanceId);
        Thread closer = new Thread(() -> {
            try {
                pcToClose.close();
            } catch (Exception e) {
                log.warn("Instance {} background close error: {}", instanceId, e.getMessage());
            } finally {
                closingPcs.remove(pcToClose);
            }
        }, "pc-close-" + instanceId);
        // daemon, like chaos-conductor and chaos-drain-N: a close() that hangs under an injected
        // broker failure must not keep the forked test JVM alive after the run has finished
        closer.setDaemon(true);
        closer.start();
    }

    /**
     * Restart: checks the previous PC's failure cause, classifies it, then resubmits to the executor.
     * Expected close exceptions are logged. Unexpected exceptions fail the test.
     * <p>
     * Single-flight: refuses (returning {@code false}) while an earlier start is still in flight, so
     * one instance can never have two concurrent {@link #run()} invocations racing on
     * {@link #parallelConsumer}. Callers must treat {@code false} as "still stopped" - see
     * {@link #startInFlight}.
     *
     * @return true if this call submitted the start, false if it was refused
     */
    public boolean start(ExecutorService pcExecutor) {
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
        if (!startInFlight.compareAndSet(false, true)) {
            log.warn("Instance {} start refused - an earlier start is still in flight", instanceId);
            return false;
        }
        stopRequested = false;
        started = true; // set BEFORE submit so next toggle() sees it — prevents double-submission
        log.info("Starting instance {}", instanceId);
        try {
            pcExecutor.submit(this);
        } catch (RuntimeException e) {
            started = false;
            startInFlight.set(false); // never strand the guard on a rejected submission
            throw e;
        }
        return true;
    }

    /**
     * Test hook: seed the PC without bringing one up, so the close-path guards can be exercised
     * without a broker. Production callers get their PC from {@link #run()}.
     */
    void setParallelConsumerForTest(ParallelEoSStreamProcessor<String, String> pc) {
        this.parallelConsumer = pc;
    }

    public void toggle(ExecutorService pcExecutor) {
        if (isClosePending()) {
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
        /** Scenario-specific consumer property overrides, applied last (e.g. a low max.poll.interval.ms). */
        private final Properties extraConsumerProps;
    }
}
