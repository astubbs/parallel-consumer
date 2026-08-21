package bz.stub.parallelconsumer.client.demo;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import java.time.Duration;

/**
 * What one arm achieved: which client drove it, how long it took, over how many records, and over
 * how many distinct keys.
 *
 * <h2>The arm has a name and a client, and they are not the same thing</h2>
 *
 * {@code AK core} is a <em>category</em> - "that language's own Kafka client" - and a reader cannot
 * judge a comparison without knowing which library actually produced the number. It is
 * {@code KafkaConsumer} here, {@code franz-go} in Go, {@code rdkafka} in Ruby. So the table prints
 * both, as {@code arm (client)}, while everything that has to <em>match</em> across runs - the
 * baseline lookup, the integration test's expectations - keys off {@link #arm()} alone.
 *
 * @author Antony Stubbs
 */
public final class ArmResult {

    private final String arm;

    private final String client;

    private final Duration elapsed;

    private final int processed;

    private final int uniqueKeys;

    public ArmResult(String arm, String client, Duration elapsed, int processed, int uniqueKeys) {
        this.arm = arm;
        this.client = client;
        this.elapsed = elapsed;
        this.processed = processed;
        this.uniqueKeys = uniqueKeys;
    }

    /** The arm's stable name - the thing every run, table and test agrees on. */
    public String arm() {
        return arm;
    }

    /** What actually drove it, named so the row is judgeable: {@code KafkaConsumer}, {@code this client}. */
    public String client() {
        return client;
    }

    /** How the arm is shown to a reader: the role and the library it ran. */
    public String label() {
        return arm + " (" + client + ")";
    }

    public Duration elapsed() {
        return elapsed;
    }

    public int processed() {
        return processed;
    }

    /**
     * The distinct keys this arm saw.
     * <p>
     * Deterministic, unlike everything else in the row: the backlog is seeded over a fixed key space
     * ({@link DemoBroker#expectedUniqueKeys}), so every arm - and every language - replaying the
     * same records reports the same number. That is what makes it comparable when a rate is not.
     */
    public int uniqueKeys() {
        return uniqueKeys;
    }

    /** Throughput, which is the only <em>measured</em> figure this demo reports; see {@link DemoBroker#seed}. */
    public double ratePerSecond() {
        double seconds = elapsed.toNanos() / 1_000_000_000d;
        return seconds > 0 ? processed / seconds : 0;
    }
}
