package bz.stub.parallelconsumer.integrationTests.chaostests.scenario;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Publishes keyed records at a rate a scenario can change while it runs - the throughput ramp, and the
 * key spread that populates shards.
 * <p>
 * Deliberately written against a {@link RecordSink} rather than a Kafka {@code Producer}: the pacing and
 * the rate changes are the part worth testing, and a sink keeps that testable with no broker. The
 * broker-backed sink is one lambda over the existing {@code KafkaClientUtils} producer - this class does
 * NOT reimplement topic creation or producer construction (AGENTS.md: duplicating that is how a
 * one-second timeout drifted in and became a flaky-CI source).
 */
@Slf4j
public class WorkloadPublisher {

    /** Where records go. One lambda over an existing producer, or a counter in a test. */
    @FunctionalInterface
    public interface RecordSink {
        void send(String key, String value);
    }

    /**
     * Pacing slice. Short enough that a rate change takes effect promptly, long enough that a high rate
     * does not turn into a busy loop.
     */
    private static final long SLICE_MS = 50;

    private final RecordSink sink;
    /** Records cycle over this many distinct keys, so a scenario can spread work across shards. */
    private final int keySpace;
    private final String keyPrefix;

    private volatile int ratePerSecond;
    private volatile boolean running;
    private Thread thread;

    @Getter
    private final AtomicLong published = new AtomicLong();

    public WorkloadPublisher(RecordSink sink, int keySpace, String keyPrefix, int initialRatePerSecond) {
        if (sink == null) throw new IllegalArgumentException("a workload publisher needs a sink");
        if (keySpace < 1) throw new IllegalArgumentException("keySpace must be at least 1, was " + keySpace);
        this.sink = sink;
        this.keySpace = keySpace;
        this.keyPrefix = keyPrefix == null ? "key-" : keyPrefix;
        setRatePerSecond(initialRatePerSecond);
    }

    public void setRatePerSecond(int recordsPerSecond) {
        if (recordsPerSecond < 0) {
            throw new IllegalArgumentException("publish rate cannot be negative, was " + recordsPerSecond);
        }
        this.ratePerSecond = recordsPerSecond;
    }

    public int getRatePerSecond() {
        return ratePerSecond;
    }

    public synchronized void start() {
        if (running) return;
        running = true;
        thread = new Thread(this::publishLoop, "scenario-workload-publisher");
        thread.setDaemon(true);
        thread.start();
    }

    /** Stops the pacing thread and waits briefly for it, so a caller sees a quiesced producer. */
    public synchronized void stop() {
        running = false;
        Thread t = thread;
        if (t != null) {
            t.interrupt();
            try {
                t.join(5_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            thread = null;
        }
    }

    /**
     * Sends one slice's worth per pass, budgeted against MEASURED elapsed time rather than the nominal
     * slice length: on a loaded machine the pacing thread is descheduled, and budgeting by nominal slices
     * would silently deliver a fraction of the requested rate. Carries the fractional remainder between
     * slices, so a rate that does not divide evenly into slices averages out rather than truncating to
     * zero, and caps the carry at one second's worth so a long stall catches up rather than bursting
     * unboundedly.
     */
    private void publishLoop() {
        double carry = 0;
        long lastNanos = System.nanoTime();
        while (running) {
            try {
                Thread.sleep(SLICE_MS);
                long now = System.nanoTime();
                double elapsedSeconds = (now - lastNanos) / 1_000_000_000.0;
                lastNanos = now;
                int rate = ratePerSecond;
                carry = Math.min(carry + rate * elapsedSeconds, rate);
                int toSend = (int) carry;
                carry -= toSend;
                for (int i = 0; i < toSend && running; i++) {
                    long n = published.getAndIncrement();
                    sink.send(keyPrefix + (n % keySpace), "v-" + n);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (Exception e) {
                log.warn("Workload publish failed (continuing): {}", e.toString());
            }
        }
    }
}
