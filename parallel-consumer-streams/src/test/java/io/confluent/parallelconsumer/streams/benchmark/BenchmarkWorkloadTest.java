package io.confluent.parallelconsumer.streams.benchmark;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.within;

/**
 * Proves the generator produces what the benchmarks claim it produces, before any benchmark spends a minute
 * measuring it.
 * <p>
 * <b>This is the instrumentation-reached-the-run rule applied to a fixture.</b> A skew axis whose generator
 * silently produced uniform keys would report "skew has no effect" and look exactly like a real finding. The
 * head-key share is therefore checked against the analytic Zipf value here, in the fast surefire lane, rather
 * than inferred from a benchmark result.
 *
 * @author Antony Stubbs
 */
class BenchmarkWorkloadTest {

    private static final int RECORDS = 20_000;

    @Test
    void sameSeedAndParametersProduceAnIdenticalList() {
        List<GeneratedRecord> first = defaults().build().generate();
        List<GeneratedRecord> second = defaults().build().generate();

        assertThat(first).hasSameSizeAs(second);
        for (int i = 0; i < first.size(); i++) {
            assertThat(first.get(i).getKey()).isEqualTo(second.get(i).getKey());
            assertThat(first.get(i).getValue()).isEqualTo(second.get(i).getValue());
            assertThat(first.get(i).getCostNanos()).isEqualTo(second.get(i).getCostNanos());
            assertThat(first.get(i).getArrivalOffsetNanos()).isEqualTo(second.get(i).getArrivalOffsetNanos());
        }
    }

    @Test
    void aDifferentSeedProducesADifferentList() {
        List<GeneratedRecord> first = defaults().build().generate();
        List<GeneratedRecord> second = defaults().seed(BenchmarkWorkload.DEFAULT_SEED + 1).build().generate();

        assertThat(keyCounts(first)).isNotEqualTo(keyCounts(second));
    }

    @Test
    void singleCollapsesToOneKeyAndReportsThatAsItsCeiling() {
        BenchmarkWorkload workload = defaults().keyDistribution(KeyDistribution.SINGLE).keyCount(200).build();

        assertThat(BenchmarkWorkload.distinctKeys(workload.generate())).hasSize(1);
        assertThat(workload.effectiveKeyCount())
                .as("the ordering ceiling a report states must be the one the data actually has, not the one "
                        + "that was requested")
                .isEqualTo(1);
    }

    @Test
    void highCardinalityGivesEveryRecordItsOwnKey() {
        BenchmarkWorkload workload = defaults()
                .keyDistribution(KeyDistribution.HIGH_CARDINALITY)
                .recordCount(500)
                .build();

        assertThat(BenchmarkWorkload.distinctKeys(workload.generate())).hasSize(500);
        assertThat(workload.effectiveKeyCount()).isEqualTo(500);
    }

    @Test
    void uniformSpreadsRecordsEvenlyOverTheRequestedKeys() {
        Map<String, Integer> counts = keyCounts(defaults()
                .keyDistribution(KeyDistribution.UNIFORM)
                .keyCount(100)
                .build()
                .generate());

        assertThat(counts).hasSize(100);
        double expectedPerKey = RECORDS / 100d;
        for (Integer count : counts.values()) {
            assertThat(count).isCloseTo((int) expectedPerKey, within((int) (expectedPerKey * 0.35)));
        }
    }

    /**
     * The analytic check. At exponent 1 the head key's share is {@code 1 / H(K)}; if the sampler were quietly
     * uniform the share would be {@code 1 / K}, which at K=200 is 0.5% against the expected 17% - a gap no
     * tolerance could hide.
     */
    @Test
    void zipfPutsTheAnalyticShareOnTheHeadKey() {
        int keys = 200;
        Map<String, Integer> counts = keyCounts(defaults()
                .keyDistribution(KeyDistribution.ZIPF)
                .keyCount(keys)
                .zipfExponent(1.0d)
                .build()
                .generate());

        double harmonic = 0d;
        for (int rank = 1; rank <= keys; rank++) {
            harmonic += 1d / rank;
        }
        double expectedHeadShare = 1d / harmonic;
        double measuredHeadShare = counts.values().stream().mapToInt(Integer::intValue).max().orElse(0) / (double) RECORDS;

        assertThat(measuredHeadShare)
                .as("Zipf s=1 over %d keys puts 1/H(%d) = %.4f of the traffic on the head key. Measuring the "
                        + "uniform share (%.4f) instead would mean the skew axis is not reaching the data, and "
                        + "every 'skew has no effect' result would be an artefact",
                        keys, keys, expectedHeadShare, 1d / keys)
                .isCloseTo(expectedHeadShare, within(expectedHeadShare * 0.15));
    }

    @Test
    void raisingTheZipfExponentRaisesTheHeadShareMonotonically() {
        double previous = 0d;
        for (double exponent : new double[]{0.5d, 1.0d, 1.5d, 2.0d}) {
            Map<String, Integer> counts = keyCounts(defaults()
                    .keyDistribution(KeyDistribution.ZIPF)
                    .keyCount(200)
                    .zipfExponent(exponent)
                    .build()
                    .generate());
            double headShare = counts.values().stream().mapToInt(Integer::intValue).max().orElse(0) / (double) RECORDS;
            assertThat(headShare)
                    .as("head-key share at exponent %.1f must exceed the share at the previous exponent", exponent)
                    .isGreaterThan(previous);
            previous = headShare;
        }
    }

    /**
     * R6, and the single most important assertion in this class.
     * <p>
     * If cost correlated with key, changing the key distribution would change the cost distribution too, and
     * every skew result would be measuring two terms at once. This module has already been bitten by exactly
     * that defect - see {@code control-arms-vary-exactly-one-term.md}, where a cost-selected-by-key fixture put
     * the control's p50 at 19568ms against the experiment's 1865ms.
     */
    @Test
    void perRecordCostIsIndependentOfTheKeyEvenUnderStrongSkew() {
        List<GeneratedRecord> records = defaults()
                .keyDistribution(KeyDistribution.ZIPF)
                .keyCount(200)
                .zipfExponent(1.5d)
                .build()
                .generate();

        Map<String, List<Long>> costsByKey = new HashMap<>();
        for (GeneratedRecord record : records) {
            costsByKey.computeIfAbsent(record.getKey(), key -> new ArrayList<>()).add(record.getCostNanos());
        }

        String hottestKey = costsByKey.entrySet().stream()
                .max((a, b) -> Integer.compare(a.getValue().size(), b.getValue().size()))
                .orElseThrow(IllegalStateException::new)
                .getKey();

        double overallMean = records.stream().mapToLong(GeneratedRecord::getCostNanos).average().orElse(0d);
        double hottestKeyMean = costsByKey.get(hottestKey).stream().mapToLong(Long::longValue).average().orElse(0d);

        assertThat(hottestKeyMean)
                .as("the hottest key carries %d of %d records; if its mean cost differed from the overall mean "
                        + "then the key-distribution axis would be varying cost as well as cardinality, and no "
                        + "skew result could be attributed to skew",
                        costsByKey.get(hottestKey).size(), records.size())
                .isCloseTo(overallMean, within(overallMean * 0.15));
    }

    @Test
    void theDrawnCostDistributionMatchesTheRequestedPercentiles() {
        List<GeneratedRecord> records = defaults()
                .cost(Duration.ofMillis(20), Duration.ofMillis(200))
                .build()
                .generate();

        List<Long> costsMillis = new ArrayList<>();
        for (GeneratedRecord record : records) {
            costsMillis.add(record.getCostNanos() / 1_000_000L);
        }
        LatencyDistribution costs = new LatencyDistribution("costs", costsMillis);

        assertThat(costs.p50()).as("requested p50 was 20ms, measured %s", costs).isCloseTo(20L, within(4L));
        assertThat(costs.p99()).as("requested p99 was 200ms, measured %s", costs).isCloseTo(200L, within(60L));
        assertThat(costs.max())
                .as("a lognormal has a genuine right tail - a maximum equal to the p99 would mean the draw is "
                        + "not tailed and the workload is a constant wearing a distribution's clothes")
                .isGreaterThan(costs.p99());
    }

    @Test
    void blockingFractionSplitsTheCostAndNothingIsLost() {
        for (double fraction : new double[]{0d, 0.5d, 1d}) {
            List<GeneratedRecord> records = defaults().blockingFraction(fraction).recordCount(2_000).build().generate();

            long blocking = records.stream().mapToLong(GeneratedRecord::getBlockingNanos).sum();
            long spinning = records.stream().mapToLong(GeneratedRecord::getSpinNanos).sum();
            long total = records.stream().mapToLong(GeneratedRecord::getCostNanos).sum();

            assertThat(blocking + spinning)
                    .as("the split must conserve total cost at fraction %.1f, or the profile axis would also be "
                            + "varying how much work there is", fraction)
                    .isEqualTo(total);
            assertThat(blocking / (double) total)
                    .as("measured blocking share at requested fraction %.1f", fraction)
                    .isCloseTo(fraction, within(0.01d));
        }
    }

    @Test
    void backlogModeGivesEveryRecordAZeroArrivalOffset() {
        List<GeneratedRecord> records = defaults().ratePerSecond(0d).build().generate();

        assertThat(records).allMatch(record -> record.getArrivalOffsetNanos() == 0L);
        assertThat(defaults().ratePerSecond(0d).build().isBacklog()).isTrue();
    }

    @Test
    void pacedModeProducesNonDecreasingPoissonArrivals() {
        double rate = 500d;
        List<GeneratedRecord> records = defaults().ratePerSecond(rate).build().generate();

        long previous = -1L;
        for (GeneratedRecord record : records) {
            assertThat(record.getArrivalOffsetNanos()).isGreaterThanOrEqualTo(previous);
            previous = record.getArrivalOffsetNanos();
        }

        double meanInterArrivalNanos = records.get(records.size() - 1).getArrivalOffsetNanos() / (double) (records.size() - 1);
        assertThat(meanInterArrivalNanos)
                .as("a Poisson process at %.0f/s has a mean inter-arrival of %.0fns", rate, 1e9 / rate)
                .isCloseTo(1e9 / rate, within(1e9 / rate * 0.1d));
    }

    @Test
    void thePayloadIsParseableJsonOfAboutTheRequestedSize() {
        GeneratedRecord record = defaults().payloadBytes(512).build().generate().get(0);

        assertThat(record.getValue()).startsWith("{").endsWith("}").contains("\"card\":\"" + record.getKey() + "\"");
        assertThat(record.getValue().length()).isEqualTo(512);
        assertThat(record.getValue()).contains("\"blockNanos\":" + record.getBlockingNanos());
    }

    @Test
    void aLargePayloadRequestIsHonoured() {
        assertThat(defaults().payloadBytes(8_192).build().generate().get(0).getValue()).hasSize(8_192);
    }

    @Test
    void invalidParametersFailWithAMessageNamingTheParameter() {
        assertThatThrownBy(() -> defaults().recordCount(0).build())
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("recordCount");
        assertThatThrownBy(() -> defaults().ratePerSecond(-1d).build())
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("ratePerSecond");
        assertThatThrownBy(() -> defaults().zipfExponent(-0.5d).build())
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("zipfExponent");
        assertThatThrownBy(() -> defaults().blockingFraction(1.5d).build())
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("blockingFraction");
        assertThatThrownBy(() -> defaults().cost(Duration.ofMillis(100), Duration.ofMillis(10)).build())
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("costP99");
        assertThatThrownBy(() -> defaults().payloadBytes(16).build())
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("payloadBytes");
    }

    @Test
    void theDescriptionStatesEveryTermAnArmWasRunWith() {
        assertThat(defaults().build().toString())
                .contains("records=" + RECORDS)
                .contains("ZIPF")
                .contains("blockingFraction")
                .contains("backlog")
                .contains("seed=" + BenchmarkWorkload.DEFAULT_SEED);
    }

    /**
     * The demonstration's documentation tells a reader to try {@code --skew 2.0}. If the test's own value
     * outranked the flag, the run would report a configuration nobody asked for while looking perfectly
     * healthy - the same silent-wrong-experiment failure that the runner script's unknown-flag guard exists to
     * prevent, arriving by a different route.
     */
    @Test
    void anOverrideAppliedLastBeatsTheTestsOwnValue() {
        String property = BenchmarkWorkload.PROPERTY_PREFIX + "skew";
        String previous = System.getProperty(property);
        try {
            System.setProperty(property, "2.0");

            assertThat(BenchmarkWorkload.builder("demo").zipfExponent(1.0d).applySystemPropertyOverrides()
                    .build().getZipfExponent())
                    .as("applySystemPropertyOverrides() is called LAST, so the command line wins")
                    .isEqualTo(2.0d);

            assertThat(BenchmarkWorkload.fromSystemProperties("cell").zipfExponent(1.0d).build().getZipfExponent())
                    .as("fromSystemProperties() loads defaults FIRST, so an experiment sweeping this axis keeps "
                            + "the value that names its cell")
                    .isEqualTo(1.0d);
        } finally {
            if (previous == null) {
                System.clearProperty(property);
            } else {
                System.setProperty(property, previous);
            }
        }
    }

    private static BenchmarkWorkload.Builder defaults() {
        return BenchmarkWorkload.builder("unit").recordCount(RECORDS);
    }

    private static Map<String, Integer> keyCounts(final List<GeneratedRecord> records) {
        Map<String, Integer> counts = new HashMap<>();
        for (GeneratedRecord record : records) {
            counts.merge(record.getKey(), 1, Integer::sum);
        }
        return counts;
    }
}
