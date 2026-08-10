package io.confluent.parallelconsumer.streams.benchmark;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

/**
 * The load generator: one deterministic, parameterised description of a workload, and the record list both
 * arms replay.
 * <p>
 * <b>Generated once, replayed twice.</b> The whole method of this module's benchmarks is comparing arms that
 * differ in exactly one term, and the cheapest way to lose that property is to generate the input separately
 * per arm. So a workload produces its records once and both arms are fed the same list - identical keys,
 * identical payloads, identical per-record costs, identical arrival offsets. The only difference between the
 * arms is {@code PcDispatchSwitch}.
 *
 * <h2>Per-record cost is drawn by record INDEX, never by key</h2>
 * This is the load-bearing decision in the class, and it is not an aesthetic one.
 * {@code docs/solutions/best-practices/control-arms-vary-exactly-one-term.md} records this exact defect being
 * found in {@code HeadOfLineBlockingBenchmarkTest}: cost was selected by key, so the single-key control changed
 * cardinality <em>and</em> cost distribution, and its p50 came out at 19568ms against the experiment's 1865ms.
 * <p>
 * A realistic domain invites the same mistake even harder, because in the real world cost genuinely does
 * correlate with the record - a premium card gets a deeper check, a big merchant gets a heavier lookup. Modelling
 * that faithfully would make the key-distribution axis vary two terms and the skew result would be
 * uninterpretable. So cost here is drawn from the record's position in the stream, which is independent of its
 * key by construction, and {@code BenchmarkWorkloadTest} asserts the independence rather than trusting this
 * comment.
 *
 * <h2>Cost is a distribution, not a constant</h2>
 * Lognormal, parameterised by the p50 and p99 an operator would actually quote for a service call: a tight body
 * with a long right tail. A constant cost is both unrealistic and quietly flattering - with equal costs a pool
 * of N drains exactly N-fold, whereas a tail leaves some workers stuck on slow records while others turn over,
 * which is what real pools look like.
 *
 * <h2>Arrival is a mode, not a second generator</h2>
 * {@code ratePerSecond == 0} means "all at once": every arrival offset is zero, the producer sends the lot
 * before the topology starts, and the run is a cold-start backlog catch-up. Any positive rate paces the sends on
 * exponential inter-arrival times - a Poisson process, which is bursty, and bursts create queueing in both arms
 * rather than in neither.
 *
 * @author Antony Stubbs
 * @see KeyDistribution
 */
public final class BenchmarkWorkload {

    /**
     * Fixed by default so two runs of the same configuration see the same input, and a figure that moved
     * between runs is a real change rather than a different draw.
     */
    public static final long DEFAULT_SEED = 20260811L;

    static final String PROPERTY_PREFIX = "pc.bench.";

    /**
     * Standard normal quantile at 0.99, used to solve the lognormal's sigma from a quoted p50/p99 pair.
     */
    private static final double Z99 = 2.3263478740408408d;

    private final String name;
    private final int recordCount;
    private final KeyDistribution keyDistribution;
    private final int requestedKeyCount;
    private final double zipfExponent;
    private final Duration costP50;
    private final Duration costP99;
    private final double blockingFraction;
    private final int payloadBytes;
    private final double ratePerSecond;
    private final long seed;

    private BenchmarkWorkload(final Builder builder) {
        this.name = builder.name;
        this.recordCount = builder.recordCount;
        this.keyDistribution = builder.keyDistribution;
        this.requestedKeyCount = builder.keyCount;
        this.zipfExponent = builder.zipfExponent;
        this.costP50 = builder.costP50;
        this.costP99 = builder.costP99;
        this.blockingFraction = builder.blockingFraction;
        this.payloadBytes = builder.payloadBytes;
        this.ratePerSecond = builder.ratePerSecond;
        this.seed = builder.seed;
    }

    public static Builder builder(final String name) {
        return new Builder(name);
    }

    /**
     * A builder pre-loaded from {@code -Dpc.bench.*} system properties, so {@code bin/streams-benchmark.sh} can
     * re-run any experiment under a different configuration without editing a test.
     * <p>
     * The properties are the defaults, not an override of what a test asks for: a test that sets a parameter
     * explicitly is stating a term of its experiment and must win over ambient configuration, or the same
     * committed test would measure different things on different machines without saying so.
     */
    public static Builder fromSystemProperties(final String name) {
        Builder builder = new Builder(name);
        builder.recordCount = intProperty("records", builder.recordCount);
        builder.keyCount = intProperty("keys", builder.keyCount);
        builder.zipfExponent = doubleProperty("skew", builder.zipfExponent);
        builder.costP50 = millisProperty("costP50Ms", builder.costP50);
        builder.costP99 = millisProperty("costP99Ms", builder.costP99);
        builder.blockingFraction = doubleProperty("blockingFraction", builder.blockingFraction);
        builder.payloadBytes = intProperty("payloadBytes", builder.payloadBytes);
        builder.ratePerSecond = doubleProperty("rate", builder.ratePerSecond);
        builder.seed = longProperty("seed", builder.seed);
        String distribution = System.getProperty(PROPERTY_PREFIX + "keyDistribution");
        if (distribution != null) {
            builder.keyDistribution = KeyDistribution.valueOf(distribution.trim().toUpperCase(java.util.Locale.ROOT));
        }
        return builder;
    }

    public String getName() {
        return name;
    }

    public int getRecordCount() {
        return recordCount;
    }

    public KeyDistribution getKeyDistribution() {
        return keyDistribution;
    }

    public double getZipfExponent() {
        return zipfExponent;
    }

    public double getBlockingFraction() {
        return blockingFraction;
    }

    public double getRatePerSecond() {
        return ratePerSecond;
    }

    public boolean isBacklog() {
        return ratePerSecond <= 0d;
    }

    /**
     * The distinct-key count this workload will actually produce - the ceiling on concurrency under KEY
     * ordering, and therefore the number a report must state rather than the count that was requested.
     */
    public int effectiveKeyCount() {
        return keyDistribution.effectiveKeyCount(requestedKeyCount, recordCount);
    }

    /**
     * Generates the records. Deterministic: same parameters and seed, same list, every time.
     */
    public List<GeneratedRecord> generate() {
        int keyCount = effectiveKeyCount();
        KeyDistribution.Sampler sampler = keyDistribution.sampler(keyCount, zipfExponent);

        // Three independent streams from one seed, so that changing one axis cannot shift another's draws.
        // Sharing a single Random would make the cost sequence depend on how many key draws preceded it, and
        // "I changed the key distribution and the costs moved" is precisely the co-variation this class exists
        // to prevent.
        Random keyRandom = new Random(seed);
        Random costRandom = new Random(seed + 1);
        Random arrivalRandom = new Random(seed + 2);

        double mu = Math.log(costP50.toNanos());
        double sigma = (Math.log(costP99.toNanos()) - mu) / Z99;

        List<GeneratedRecord> records = new ArrayList<>(recordCount);
        long arrivalOffsetNanos = 0L;
        double meanInterArrivalNanos = isBacklog() ? 0d : 1_000_000_000d / ratePerSecond;

        for (int index = 0; index < recordCount; index++) {
            String key = keyFor(sampler.keyIndex(keyRandom, index));

            long costNanos = Math.max(1L, Math.round(Math.exp(mu + sigma * costRandom.nextGaussian())));
            long blockingNanos = Math.round(costNanos * blockingFraction);
            long spinNanos = costNanos - blockingNanos;

            if (!isBacklog()) {
                // Exponential inter-arrival times: a Poisson process. Bursty on purpose - a perfectly even
                // arrival is the one input shape that never queues, and never queueing is the property the
                // stock arm is supposed to struggle with.
                double uniform = Math.max(1e-12d, arrivalRandom.nextDouble());
                arrivalOffsetNanos += Math.round(-Math.log(uniform) * meanInterArrivalNanos);
            }

            records.add(new GeneratedRecord(index, key, blockingNanos, spinNanos, arrivalOffsetNanos, payloadBytes));
        }
        return Collections.unmodifiableList(records);
    }

    /**
     * Zero-padded so keys sort readably in logs and so every key is the same length - a varying key length
     * would put a few extra bytes of serialisation on the hot keys, which is a small cost co-varying with the
     * skew axis.
     */
    private static String keyFor(final int keyIndex) {
        return String.format("card-%08d", keyIndex);
    }

    /**
     * The distinct keys actually present, for a report that states the real ordering ceiling.
     */
    public static Set<String> distinctKeys(final List<GeneratedRecord> records) {
        Set<String> keys = new HashSet<>();
        for (GeneratedRecord record : records) {
            keys.add(record.getKey());
        }
        return keys;
    }

    @Override
    public String toString() {
        return String.format("%s[records=%d keys=%s(%d, skew=%.2f) cost=p50 %dms/p99 %dms blockingFraction=%.2f "
                        + "payload=%dB arrival=%s seed=%d]",
                name, recordCount, keyDistribution, effectiveKeyCount(), zipfExponent,
                costP50.toMillis(), costP99.toMillis(), blockingFraction, payloadBytes,
                isBacklog() ? "backlog" : String.format("%.0f/s", ratePerSecond), seed);
    }

    private static int intProperty(final String suffix, final int fallback) {
        String raw = System.getProperty(PROPERTY_PREFIX + suffix);
        return raw == null ? fallback : Integer.parseInt(raw.trim());
    }

    private static long longProperty(final String suffix, final long fallback) {
        String raw = System.getProperty(PROPERTY_PREFIX + suffix);
        return raw == null ? fallback : Long.parseLong(raw.trim());
    }

    private static double doubleProperty(final String suffix, final double fallback) {
        String raw = System.getProperty(PROPERTY_PREFIX + suffix);
        return raw == null ? fallback : Double.parseDouble(raw.trim());
    }

    private static Duration millisProperty(final String suffix, final Duration fallback) {
        String raw = System.getProperty(PROPERTY_PREFIX + suffix);
        return raw == null ? fallback : Duration.ofMillis(Long.parseLong(raw.trim()));
    }

    /**
     * Every default here is a claim about a realistic workload, so each one is defended where it is set rather
     * than in a comment somewhere else.
     */
    public static final class Builder {

        private final String name;

        /**
         * Enough that p99 is a tail statistic rather than the maximum in disguise - at 2000 the p99 is the
         * twentieth-worst sample, not the worst.
         */
        private int recordCount = 2_000;

        private KeyDistribution keyDistribution = KeyDistribution.ZIPF;

        /**
         * Small enough that a hot key genuinely bites at this record count, large enough to exceed any worker
         * pool this module would run.
         */
        private int keyCount = 200;

        /**
         * Zipf s=1.0. Not picked for roundness: Twitter published the fitted Zipf exponent for each of 54
         * production cache clusters, whose median is 1.21 with an interquartile range of 0.85 to 1.60, so 1.0
         * sits inside a measured distribution rather than beside it.
         * <p>
         * <b>It sits below that median deliberately, and that is the direction that flatters this benchmark's
         * subject.</b> Lower skew spreads the keyspace, which gives KEY ordering more concurrency to find. The
         * matrix sweeps upward from here to 1.5 - still inside the observed IQR, and the point at which a
         * single key takes roughly a third of the whole stream - so the unfavourable end is measured rather
         * than avoided.
         * <p>
         * Counter-evidence worth knowing: CacheLib's published workloads include a social graph at 0.55 and a
         * storage workload that is not Zipfian at all. Power-law keys are the common case, not the only one.
         */
        private double zipfExponent = 1.0d;

        /**
         * A service call an enrichment stage would actually make.
         * <p>
         * Precedent in this repository: the card-payment screening example fixes its fraud-scoring call at
         * 200ms. Published production figures for the same shape of call sit around here or below - Adyen's
         * internal inference endpoints at 20ms p50, Uber's internal RPC at roughly 10ms p99, Cloudflare's
         * feature service well under a millisecond at the median. Expressed as a distribution rather than a
         * constant, which is the part those benchmarks usually skip.
         */
        private Duration costP50 = Duration.ofMillis(20);

        /**
         * A 10:1 p99/p50 spread.
         * <p>
         * Conservative against the published range rather than generous. Google's own guidance suggests a p99
         * within three to five times the median, while its SRE book uses twenty- and fifty-fold tails as
         * unremarkable examples, and measured extremes reach far past that. Adyen quotes 20ms p50 against
         * 100ms p99 for internal inference; Dean and Barroso's fan-out table shows 40ms p50 against 140ms p99.
         * Ten is comfortably inside all of that.
         * <p>
         * It is also deliberately not heavier: a very long tail would let a handful of records dominate the
         * run, which rebuilds the single-blocker shape this benchmark exists to get away from.
         */
        private Duration costP99 = Duration.ofMillis(200);

        /**
         * Fully blocking by default: this is the case the seam is for, and it is the honest starting point
         * because the profile axis then sweeps AWAY from it towards the cases where PC does nothing.
         */
        private double blockingFraction = 1.0d;

        /**
         * A JSON business event of unremarkable size. Big enough that parsing it is real work, small enough to
         * be typical rather than a stress test of the serialiser.
         * <p>
         * <b>This is the weakest-sourced parameter here, and it is better to say so than to dress it up.</b>
         * No named operator publishes a percentile distribution of Kafka record sizes. The nearest thing is a
         * workload specification putting payment events at 512 bytes to 2 kilobytes, and that document is
         * itself an author's construction rather than a measured trace. So this figure is chosen, not cited -
         * which is why the matrix sweeps it rather than asserting on one value, and why the published survey
         * critique of benchmarks that fix a uniform one-kilobyte record applies to this one too unless it does.
         */
        private int payloadBytes = 512;

        /**
         * Backlog by default. See the class javadoc: a cold-start backlog removes arrival rate as a variable
         * entirely, which makes it a cleaner experiment than a paced one rather than a cruder one.
         */
        private double ratePerSecond = 0d;

        private long seed = DEFAULT_SEED;

        private Builder(final String name) {
            if (name == null || name.trim().isEmpty()) {
                throw new IllegalArgumentException("name must be set - it labels the workload in every report");
            }
            this.name = name;
        }

        public Builder recordCount(final int value) {
            this.recordCount = value;
            return this;
        }

        public Builder keyDistribution(final KeyDistribution value) {
            this.keyDistribution = value;
            return this;
        }

        public Builder keyCount(final int value) {
            this.keyCount = value;
            return this;
        }

        public Builder zipfExponent(final double value) {
            this.zipfExponent = value;
            return this;
        }

        public Builder cost(final Duration p50, final Duration p99) {
            this.costP50 = p50;
            this.costP99 = p99;
            return this;
        }

        public Builder blockingFraction(final double value) {
            this.blockingFraction = value;
            return this;
        }

        public Builder payloadBytes(final int value) {
            this.payloadBytes = value;
            return this;
        }

        public Builder ratePerSecond(final double value) {
            this.ratePerSecond = value;
            return this;
        }

        public Builder seed(final long value) {
            this.seed = value;
            return this;
        }

        public BenchmarkWorkload build() {
            require(recordCount >= 1, "recordCount must be at least 1, was " + recordCount);
            require(keyCount >= 1, "keyCount must be at least 1, was " + keyCount);
            require(zipfExponent >= 0d, "zipfExponent must not be negative, was " + zipfExponent);
            require(costP50 != null && !costP50.isNegative() && !costP50.isZero(),
                    "costP50 must be positive, was " + costP50);
            require(costP99 != null && costP99.compareTo(costP50) >= 0,
                    "costP99 (" + costP99 + ") must be at least costP50 (" + costP50 + ") - a p99 below the "
                            + "median is not a distribution, it is a typo, and it would silently invert the tail");
            require(blockingFraction >= 0d && blockingFraction <= 1d,
                    "blockingFraction must be in [0, 1], was " + blockingFraction);
            require(payloadBytes >= 64, "payloadBytes must be at least 64 - below that the JSON envelope alone "
                    + "exceeds the target and the requested size stops meaning anything. Was " + payloadBytes);
            require(ratePerSecond >= 0d, "ratePerSecond must not be negative (0 means backlog), was " + ratePerSecond);
            return new BenchmarkWorkload(this);
        }

        private static void require(final boolean condition, final String message) {
            if (!condition) {
                throw new IllegalArgumentException(message);
            }
        }
    }
}
