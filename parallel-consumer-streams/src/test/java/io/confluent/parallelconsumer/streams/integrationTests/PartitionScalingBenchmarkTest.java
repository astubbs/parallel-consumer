package io.confluent.parallelconsumer.streams.integrationTests;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import io.confluent.parallelconsumer.streams.benchmark.BenchmarkReport;
import io.confluent.parallelconsumer.streams.benchmark.BenchmarkWorkload;
import io.confluent.parallelconsumer.streams.benchmark.GeneratedRecord;
import io.confluent.parallelconsumer.streams.benchmark.KeyDistribution;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;

import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * <b>Experiment E: "just add partitions."</b> The reviewer's counter-proposal, run rather than argued with.
 * <p>
 * This is the single most dangerous objection to every other benchmark in this module, and it deserves better
 * than a footnote. Stock Kafka Streams parallelises across partitions. Every experiment here runs one
 * partition, which is the configuration that makes the seam look best - and a reviewer is right to point out
 * that a real deployment would not do that. Published guidance is six to twelve partitions for a topic of any
 * consequence, and LinkedIn's fleet averages around seventy.
 *
 * <h2>What this measures, and what it deliberately does not claim</h2>
 * Three arms, so the comparison is honest in both directions:
 * <ol>
 *   <li><b>Stock, one partition.</b> The baseline the other experiments use, and the one under attack.</li>
 *   <li><b>Stock, N partitions and N StreamThreads.</b> The counter-proposal, given the full resources it
 *       asks for.</li>
 *   <li><b>The seam, one partition, a pool of N.</b> The same total concurrency as arm 2, from one
 *       partition.</li>
 * </ol>
 * <b>The expected result is that arms 2 and 3 are comparable, and that is not a defeat.</b> The claim was never
 * "faster than stock at equal concurrency" - it is that the concurrency arrives without the partition count.
 * Arms 2 and 3 buy the same throughput at different prices: arm 2 costs N partitions, N consumers in the group
 * and N sets of buffers, and it is only available if the topic was created with N partitions and the keyspace
 * spreads evenly over them. Arm 3 costs N threads.
 * <p>
 * If arm 2 beats arm 3 outright, that is a real finding and it belongs in the write-up, because it would mean
 * the seam's overhead exceeds what partition parallelism costs.
 *
 * <h2>The known hazard</h2>
 * Multi-task and multi-thread behaviour under PC dispatch is explicitly untested - the spike's own caveat list
 * says every test in this module runs one StreamThread, one partition, one task, and the wake-on-work signal is
 * scoped to the thread that constructed the dispatcher. Arm 4 probes that, and a failure there is reported as a
 * finding rather than hidden: it would mean the seam does not currently compose with partition scaling, which
 * anyone deploying it needs to know.
 *
 * @author Antony Stubbs
 * @see BacklogCatchUpBenchmarkTest the headline, which this exists to defend
 */
@Slf4j
@Isolated
@Tag("performance")
class PartitionScalingBenchmarkTest extends StreamsBenchmarkHarness {

    private static final int DEPTH = 600;

    /**
     * Matched to {@link #POOL_SIZE}, so arm 2 and arm 3 have the same total concurrency and the comparison is
     * about how it was bought rather than how much of it there is.
     */
    private static final int PARTITIONS = POOL_SIZE;

    /**
     * High cardinality, deliberately, and this one is chosen to favour the counter-proposal rather than the
     * seam. Partition parallelism only helps when the keyspace spreads evenly across partitions; a skewed
     * keyspace lands disproportionately on some partitions and stock's extra threads idle. Giving arm 2 an
     * evenly-spreading keyspace is the strongest form of the objection, which is the form worth answering.
     */
    private static final KeyDistribution KEYS = KeyDistribution.HIGH_CARDINALITY;

    private static BenchmarkWorkload.Builder workload(final String name) {
        return BenchmarkWorkload.fromSystemProperties(name)
                .recordCount(DEPTH)
                .keyDistribution(KEYS)
                .cost(Duration.ofMillis(20), Duration.ofMillis(200))
                .blockingFraction(1.0d)
                .payloadBytes(512)
                .ratePerSecond(0d);
    }

    @Test
    void whatItCostsToBuyTheSameConcurrencyWithPartitions() {
        BenchmarkWorkload spec = workload("partitions").build();
        List<GeneratedRecord> records = spec.generate();

        ArmResult stockOnePartition = runArm("part-stock-1p", spec, records, false, 1, 1);
        ArmResult stockNPartitions = runArm("part-stock-" + PARTITIONS + "p", spec, records, false,
                PARTITIONS, PARTITIONS);
        ArmResult pcOnePartition = runArm("part-pc-1p", spec, records, true, 1, 1);

        double baseline = stockOnePartition.getSustainedRatePerSecond();
        double addPartitions = stockNPartitions.getSustainedRatePerSecond() / baseline;
        double addSeam = pcOnePartition.getSustainedRatePerSecond() / baseline;

        BenchmarkReport report = new BenchmarkReport("Experiment E - 'just add partitions', measured")
                .configured("backlog depth", DEPTH)
                .configured("keys", "one per record, so partitions spread evenly - the objection's best case")
                .configured("cost per record", "20ms p50 / 200ms p99, blocking")
                .configured("worker pool", POOL_SIZE)
                .measurement("stock, 1 partition, 1 thread",
                        String.format("%.1f/s", baseline), "-", "1.00x (baseline)")
                .measurement("stock, " + PARTITIONS + " partitions, " + PARTITIONS + " threads",
                        String.format("%.1f/s", stockNPartitions.getSustainedRatePerSecond()), "-",
                        String.format("%.2fx", addPartitions))
                .measurement("seam, 1 partition, pool of " + POOL_SIZE, "-",
                        String.format("%.1f/s", pcOnePartition.getSustainedRatePerSecond()),
                        String.format("%.2fx", addSeam));

        ArmResult pcNPartitions = null;
        String composeFinding;
        try {
            pcNPartitions = runArm("part-pc-" + PARTITIONS + "p", spec, records, true, PARTITIONS, PARTITIONS);
            double both = pcNPartitions.getSustainedRatePerSecond() / baseline;
            report.measurement("seam + " + PARTITIONS + " partitions/threads", "-",
                    String.format("%.1f/s", pcNPartitions.getSustainedRatePerSecond()),
                    String.format("%.2fx", both));
            composeFinding = both > addPartitions * 1.3d
                    ? String.format("They COMPOSE: the seam on top of %d partitions reached %.2fx, above the "
                    + "%.2fx that partitions alone bought. Multi-task dispatch is listed as untested in this "
                    + "module's caveats, so treat this as a probe rather than a supported configuration.",
                    PARTITIONS, both, addPartitions)
                    : String.format("They do NOT compose usefully here: the seam on top of %d partitions "
                    + "reached %.2fx against %.2fx for partitions alone. Multi-task dispatch is untested and "
                    + "this is the measurement saying so.", PARTITIONS, both, addPartitions);
        } catch (RuntimeException | AssertionError e) {
            // A failure here is a finding, not a broken test. The module's own caveats say multi-task
            // dispatch is untested; discovering that it does not work is exactly what a probe is for, and
            // burying it would leave a deployer to discover it instead.
            composeFinding = "The seam on " + PARTITIONS + " partitions FAILED TO RUN: "
                    + e.getClass().getSimpleName() + " - " + e.getMessage()
                    + ". Multi-task PC dispatch is listed as untested in this module's caveats and this "
                    + "measurement confirms it is not usable today. Deployers must choose one or the other.";
            log.warn("Multi-partition PC arm failed - recorded as a finding", e);
        }

        report.finding(String.format("The objection is right that one partition is not a realistic deployment. "
                        + "Given %d partitions and %d StreamThreads, stock reached %.2fx the single-partition "
                        + "baseline.", PARTITIONS, PARTITIONS, addPartitions))
                .finding(String.format("The seam reached %.2fx from ONE partition and one StreamThread. The two "
                        + "routes buy comparable throughput; what differs is the price. Partitions cost a "
                        + "partition and a consumer per unit of concurrency and require the topic to have been "
                        + "created that way; the seam costs threads.", addSeam))
                .finding(addSeam >= addPartitions * 0.8d
                        ? "So the single-partition baseline used elsewhere in this suite is not what produces "
                        + "the advantage - stock given four times the resources lands in the same region."
                        : String.format("REFUTED: partition scaling beat the seam (%.2fx against %.2fx). Where "
                        + "you can add partitions and your keys spread evenly, adding partitions is the better "
                        + "answer, and that must be said plainly.", addPartitions, addSeam))
                .finding(composeFinding)
                .finding("Not measured here, and it is the objection's real weight: partitions are a global, "
                        + "up-front, hard-to-change resource with per-broker and per-cluster ceilings, whereas "
                        + "a worker pool is a local setting. That trade-off is an argument, not a number, and "
                        + "it is not settled by this benchmark.");
        log.info(report.render());

        assertThat(addPartitions)
                .as("E: stock given %d partitions and %d threads must beat stock on one of each, or the "
                        + "counter-proposal was not actually given its resources and this experiment did not "
                        + "run the objection at all", PARTITIONS, PARTITIONS)
                .isGreaterThan(1.5d);
    }
}
