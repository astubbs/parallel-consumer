package io.confluent.parallelconsumer.streams.benchmark;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

/**
 * One record of a generated workload: what to send, when to send it, and what it costs to process.
 * <p>
 * <b>The cost travels inside the payload.</b> The processor reads it back out by parsing the JSON, which makes
 * the parse load-bearing rather than decorative - a benchmark whose "realistic payload" is never actually read
 * is measuring a sleep with a string attached. It also means both arms cannot possibly disagree about what a
 * record cost, because the cost is in the record.
 *
 * @author Antony Stubbs
 */
public final class GeneratedRecord {

    /**
     * The authorisation-shaped envelope. Field order is fixed so a padded payload's size is predictable, and
     * the domain fields are real ones rather than filler - this is what a card-network authorisation carries.
     */
    private static final String TEMPLATE =
            "{\"authId\":\"auth-%08d\",\"card\":\"%s\",\"amountPence\":%d,\"merchant\":\"merchant-%03d\","
                    + "\"country\":\"%s\",\"mcc\":%d,\"blockNanos\":%d,\"spinNanos\":%d,\"idx\":%d,\"pad\":\"%s\"}";

    /**
     * One in ten authorisations is cross-border. Present because a realistic record has fields the processor
     * branches on, not because the benchmark measures the branch.
     */
    private static final String[] COUNTRIES = {"GB", "GB", "GB", "GB", "GB", "GB", "GB", "GB", "GB", "FR"};

    private final int index;
    private final String key;
    private final long blockingNanos;
    private final long spinNanos;
    private final long arrivalOffsetNanos;
    private final String value;

    GeneratedRecord(final int index,
                    final String key,
                    final long blockingNanos,
                    final long spinNanos,
                    final long arrivalOffsetNanos,
                    final int payloadBytes) {
        this.index = index;
        this.key = key;
        this.blockingNanos = blockingNanos;
        this.spinNanos = spinNanos;
        this.arrivalOffsetNanos = arrivalOffsetNanos;
        this.value = renderValue(index, key, blockingNanos, spinNanos, payloadBytes);
    }

    public int getIndex() {
        return index;
    }

    public String getKey() {
        return key;
    }

    /**
     * How long this record's processing must BLOCK - a downstream call that the thread cannot proceed through.
     */
    public long getBlockingNanos() {
        return blockingNanos;
    }

    /**
     * How long this record's processing must SPIN - real CPU work that competes for a core.
     */
    public long getSpinNanos() {
        return spinNanos;
    }

    /**
     * Total service cost, which is what a report should compare arms on: it is identical in both arms by
     * construction, so any difference in end-to-end latency is time the record spent waiting rather than working.
     */
    public long getCostNanos() {
        return blockingNanos + spinNanos;
    }

    /**
     * Nanoseconds after the load generator starts at which this record should be sent. Always zero in backlog
     * mode.
     */
    public long getArrivalOffsetNanos() {
        return arrivalOffsetNanos;
    }

    public String getValue() {
        return value;
    }

    private static String renderValue(final int index,
                                      final String key,
                                      final long blockingNanos,
                                      final long spinNanos,
                                      final int payloadBytes) {
        // Derived from the index, so the amount and merchant vary the way a real feed does without any of it
        // correlating with the key - see BenchmarkWorkload's note on why cost must not derive from the key.
        long amountPence = 150L + (index * 977L) % 250_000L;
        int merchant = index % 500;
        String country = COUNTRIES[index % COUNTRIES.length];
        int mcc = 5000 + (index % 400);

        String unpadded = String.format(TEMPLATE, index, key, amountPence, merchant, country, mcc,
                blockingNanos, spinNanos, index, "");
        int padding = Math.max(0, payloadBytes - unpadded.length());
        StringBuilder pad = new StringBuilder(padding);
        for (int i = 0; i < padding; i++) {
            // Not random: a random pad would make the payload incompressible in a way that varies per record,
            // and the broker's own batching would then differ between two runs of the same configuration.
            pad.append((char) ('a' + (i % 26)));
        }
        return String.format(TEMPLATE, index, key, amountPence, merchant, country, mcc,
                blockingNanos, spinNanos, index, pad);
    }
}
