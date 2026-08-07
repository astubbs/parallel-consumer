package io.confluent.parallelconsumer.integrationTests.chaostests.scenario;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * A user function whose behaviour a scenario can change while it runs: how long it dwells, what
 * proportion of records it fails, and whether one particular key fails every single time.
 * <p>
 * Wraps the delegate the test actually wants to run (the progress counter, the ledger append) and
 * decorates it - so it drops straight into {@code ManagedPCInstance}'s {@code Consumer<String>} slot.
 * <p>
 * <b>Failures are chosen by key, not by a random draw.</b> Two reasons. First, a shared RNG here would
 * be consumed by many worker threads in nondeterministic order, so the run would not replay even with a
 * fixed seed - and it would perturb nothing else only by luck. Second, key-derived failure is the
 * behaviour that actually demonstrates the thing: the same key fails on every delivery, so its shard
 * backs up behind retry backoff instead of skipping around at random.
 */
@Slf4j
public class ScriptedFunction implements Consumer<String> {

    /** Granularity of the failure proportion: 1 in 10,000, enough for "fail 0.1% of records". */
    private static final int PROPORTION_RESOLUTION = 10_000;

    /** Thrown to make PC retry a record. Named so a log reader knows it was induced, not a real defect. */
    public static class InducedFailure extends RuntimeException {
        public InducedFailure(String message) {
            super(message);
        }
    }

    private final Consumer<String> delegate;

    private volatile long delayMs;
    private volatile int failureThreshold; // records with keyBucket < this fail
    private volatile String failingKey;

    @Getter
    private final AtomicLong inducedFailures = new AtomicLong();
    @Getter
    private final AtomicLong pinnedKeyFailures = new AtomicLong();
    @Getter
    private final AtomicLong processed = new AtomicLong();

    public ScriptedFunction(Consumer<String> delegate) {
        if (delegate == null) throw new IllegalArgumentException("a scripted function needs a delegate to wrap");
        this.delegate = delegate;
    }

    public void setDelay(Duration delay) {
        this.delayMs = delay == null ? 0 : Math.max(0, delay.toMillis());
    }

    public Duration getDelay() {
        return Duration.ofMillis(delayMs);
    }

    public void setFailureProportion(double proportion) {
        if (proportion < 0 || proportion > 1) {
            throw new IllegalArgumentException("failure proportion must be in [0,1], was " + proportion);
        }
        this.failureThreshold = (int) Math.round(proportion * PROPORTION_RESOLUTION);
    }

    public double getFailureProportion() {
        return failureThreshold / (double) PROPORTION_RESOLUTION;
    }

    public void setFailingKey(String key) {
        this.failingKey = key;
    }

    public String getFailingKey() {
        return failingKey;
    }

    @Override
    public void accept(String key) {
        long dwell = delayMs;
        if (dwell > 0) {
            try {
                Thread.sleep(dwell);
            } catch (InterruptedException e) {
                // shutdown in progress - restore the flag and let the delegate/PC see it
                Thread.currentThread().interrupt();
            }
        }

        String pinned = failingKey;
        if (pinned != null && pinned.equals(key)) {
            pinnedKeyFailures.incrementAndGet();
            inducedFailures.incrementAndGet();
            throw new InducedFailure("scripted failure: key '" + key + "' is pinned to fail every delivery");
        }
        if (keyBucket(key) < failureThreshold) {
            inducedFailures.incrementAndGet();
            throw new InducedFailure("scripted failure: key '" + key + "' falls in the failing "
                    + getFailureProportion() + " proportion");
        }

        delegate.accept(key);
        processed.incrementAndGet();
    }

    /**
     * Stable bucket in {@code [0, PROPORTION_RESOLUTION)} for a key. Deliberately a pure function of the
     * key: the same key is either always in the failing proportion or never in it, at a given setting.
     * <p>
     * The murmur3 finalizer is not decoration. Workload keys are near-consecutive ("key-0", "key-1", ...)
     * and {@code String.hashCode} maps them to near-consecutive integers, so taking those modulo the
     * resolution puts a whole run of keys in one contiguous band - a 0.5 proportion measured 10 failures
     * in 200 keys before this mixing was added. Without avalanche, a requested proportion is not the
     * delivered proportion.
     */
    static int keyBucket(String key) {
        int h = key == null ? 0 : key.hashCode();
        h ^= (h >>> 16);
        h *= 0x85ebca6b;
        h ^= (h >>> 13);
        h *= 0xc2b2ae35;
        h ^= (h >>> 16);
        return Math.floorMod(h, PROPORTION_RESOLUTION);
    }
}
