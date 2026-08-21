package bz.stub.parallelconsumer.client.demo;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import java.time.Duration;

/**
 * What one arm achieved: how long it took, and over how many records.
 *
 * @author Antony Stubbs
 */
public final class ArmResult {

    private final String arm;

    private final Duration elapsed;

    private final int processed;

    public ArmResult(String arm, Duration elapsed, int processed) {
        this.arm = arm;
        this.elapsed = elapsed;
        this.processed = processed;
    }

    public String arm() {
        return arm;
    }

    public Duration elapsed() {
        return elapsed;
    }

    public int processed() {
        return processed;
    }

    /** Throughput, which is the only figure this demo reports; see {@link DemoBroker#seed}. */
    public double ratePerSecond() {
        double seconds = elapsed.toNanos() / 1_000_000_000d;
        return seconds > 0 ? processed / seconds : 0;
    }
}
