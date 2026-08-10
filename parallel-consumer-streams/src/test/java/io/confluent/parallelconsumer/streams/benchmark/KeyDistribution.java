package io.confluent.parallelconsumer.streams.benchmark;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import java.util.Random;

/**
 * How a workload's records are spread over its keys - the axis that decides how much concurrency Parallel
 * Consumer's KEY ordering is allowed to find, and therefore the axis most able to embarrass it.
 * <p>
 * <b>Uniform is the unrealistic one.</b> Every generator in this repository before this class drew keys with
 * {@code random.nextInt(keys.size())}, which is the friendliest possible input: perfectly even shards, no hot
 * key, maximum available concurrency. Real keyspaces - merchants, accounts, devices, sessions - are power-law
 * shaped, and under KEY ordering a hot key is a serial queue that no worker pool can open up. So
 * {@link #ZIPF} exists to make the benchmark harder, not to make it prettier.
 * <p>
 * {@link #SINGLE} is the floor case and the falsifier: with one key, KEY ordering permits at most one record in
 * flight, so PC must show no advantage. If it does, every other cell is measuring a faster harness rather than
 * key concurrency, and they all have to be withdrawn.
 *
 * @author Antony Stubbs
 */
public enum KeyDistribution {

    /**
     * Every record on one key. The floor: KEY ordering forbids concurrency entirely.
     */
    SINGLE {
        @Override
        int sampleKeyIndex(final Random random, final int keyCount, final double exponent) {
            return 0;
        }

        @Override
        int effectiveKeyCount(final int requestedKeyCount, final int recordCount) {
            return 1;
        }
    },

    /**
     * Keys drawn evenly. The friendliest realistic-looking input, kept as the reference point the skewed cell
     * is compared against rather than as a claim about real traffic.
     */
    UNIFORM {
        @Override
        int sampleKeyIndex(final Random random, final int keyCount, final double exponent) {
            return random.nextInt(keyCount);
        }
    },

    /**
     * Zipf-distributed keys: the {@code r}-th most popular key takes a share proportional to
     * {@code 1 / r^exponent}. At {@code exponent = 1} over {@code K} keys the head key takes {@code 1/H(K)} of
     * the traffic, where {@code H(K)} is the {@code K}-th harmonic number - roughly 19% at {@code K = 100}.
     * <p>
     * Sampled by inverse CDF over a precomputed cumulative table. Exact at the cardinalities in play here, and
     * it needs no dependency - which matters because this repository bans its only fixture-data library and has
     * no statistics library on the tree at all.
     */
    ZIPF {
        @Override
        int sampleKeyIndex(final Random random, final int keyCount, final double exponent) {
            throw new UnsupportedOperationException("ZIPF samples from a precomputed table - see sampler()");
        }
    },

    /**
     * One key per record. The ceiling: maximum available concurrency, and the case a sceptic correctly points
     * out is not what their traffic looks like.
     */
    HIGH_CARDINALITY {
        @Override
        int sampleKeyIndex(final Random random, final int keyCount, final double exponent) {
            throw new UnsupportedOperationException("HIGH_CARDINALITY is positional - see sampler()");
        }

        @Override
        int effectiveKeyCount(final int requestedKeyCount, final int recordCount) {
            return recordCount;
        }
    };

    /**
     * How many distinct keys this distribution actually produces, which is not always what was requested -
     * {@link #SINGLE} collapses to one and {@link #HIGH_CARDINALITY} expands to the record count. Reported
     * rather than assumed, because the distinct-key count is the ceiling on concurrency under KEY ordering and
     * a report that stated the requested figure would be stating the wrong ceiling.
     */
    int effectiveKeyCount(final int requestedKeyCount, final int recordCount) {
        return requestedKeyCount;
    }

    abstract int sampleKeyIndex(Random random, int keyCount, double exponent);

    /**
     * A sampler bound to this distribution's parameters. Built once per workload so the Zipf cumulative table
     * is computed once rather than per record.
     *
     * @param keyCount the effective key count, from {@link #effectiveKeyCount(int, int)}
     * @param exponent the Zipf exponent; ignored by the other distributions
     */
    Sampler sampler(final int keyCount, final double exponent) {
        if (this == HIGH_CARDINALITY) {
            return (random, recordIndex) -> recordIndex;
        }
        if (this == ZIPF) {
            final double[] cumulative = zipfCumulative(keyCount, exponent);
            return (random, recordIndex) -> {
                double draw = random.nextDouble();
                // Linear scan. The key counts here are in the hundreds, and a binary search would trade
                // readability for time nobody is measuring - the generator runs before the clock starts.
                for (int rank = 0; rank < cumulative.length; rank++) {
                    if (draw <= cumulative[rank]) {
                        return rank;
                    }
                }
                return cumulative.length - 1;
            };
        }
        return (random, recordIndex) -> sampleKeyIndex(random, keyCount, exponent);
    }

    private static double[] zipfCumulative(final int keyCount, final double exponent) {
        double[] cumulative = new double[keyCount];
        double normaliser = 0d;
        for (int rank = 1; rank <= keyCount; rank++) {
            normaliser += 1d / Math.pow(rank, exponent);
        }
        double running = 0d;
        for (int rank = 1; rank <= keyCount; rank++) {
            running += (1d / Math.pow(rank, exponent)) / normaliser;
            cumulative[rank - 1] = running;
        }
        // Guard the last bucket against floating-point shortfall, so a draw of exactly 1.0 has somewhere to go.
        cumulative[keyCount - 1] = 1d;
        return cumulative;
    }

    /**
     * Draws one key index. Positional distributions ignore the random source; random ones ignore the index.
     */
    interface Sampler {
        int keyIndex(Random random, int recordIndex);
    }
}
