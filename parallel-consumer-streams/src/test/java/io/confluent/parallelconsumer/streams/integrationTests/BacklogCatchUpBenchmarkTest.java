package io.confluent.parallelconsumer.streams.integrationTests;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import io.confluent.parallelconsumer.streams.PcDispatchSwitch;
import io.confluent.parallelconsumer.streams.benchmark.BenchmarkReport;
import io.confluent.parallelconsumer.streams.benchmark.BenchmarkWorkload;
import io.confluent.parallelconsumer.streams.benchmark.GeneratedRecord;
import io.confluent.parallelconsumer.streams.benchmark.KeyDistribution;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * <b>Experiment C: cold-start backlog catch-up.</b> The topology starts against a topic that already holds a
 * backlog, and drains it. How long until we are caught up?
 * <p>
 * This is the second benchmark in this module, and its job is not to beat the first one's 57x. Its job is to
 * measure the same property on a workload nobody chose to flatter it, so that "synthetic, unfair, false
 * advertising" has nowhere to land.
 *
 * <h2>Why a backlog is a better experiment than a rate, not a cruder one</h2>
 * <ul>
 *   <li><b>It removes arrival rate as a variable.</b> Work is always available, so nothing is ever waiting on
 *       the broker and what is left is processing concurrency.</li>
 *   <li><b>It is the situation operators actually sweat about.</b> Restart after downtime, a new consumer
 *       group, a replay, recovery after an incident, a rebalance handing a partition to an instance that is
 *       behind.</li>
 *   <li><b>It was expected to neutralise this module's own most recent optimisation. It does the opposite,
 *       and that is the most important thing measured here.</b> The prediction was that a full backlog would
 *       leave wake-on-work idle, because the poll would never have to wait - which would have made this result
 *       independent of a fix landed days earlier. Measured, the split-wait branch fires on 94% of records, and
 *       the control arm in {@link #howMuchOfTheAdvantageSurvivesWithoutWakeOnWork()} shows the advantage
 *       collapsing from 3.76x to <b>1.31x</b> without it. A backlog keeps the <em>broker</em> supplied, but
 *       Parallel Consumer's max concurrency still bounds what one pass may take, so a StreamThread that blocks
 *       for a full poll budget is not there to refill the pool when a worker finishes, and the pool starves.
 *       Concurrent dispatch only pays if something keeps it fed. Both ship and both default on, so the
 *       headline is what a user gets - but it must never be described as independent of the poll fix.</li>
 * </ul>
 *
 * <h2>The statistic, chosen before the run</h2>
 * <b>The sustained catch-up rate carries the claim</b>; time-to-drain is reported but asserted on nowhere.
 * Time-to-drain includes a fixed startup cost - assignment, first poll, and Parallel Consumer's own
 * {@code DynamicLoadFactor} which deliberately does not scale for the first two seconds of any run - whose
 * share shrinks as the backlog deepens, so a claim asserted on it would move with backlog depth rather than
 * with the seam. The sustained rate trims the first and last decile of the drain and is invariant to depth,
 * which is what makes {@link #theAdvantageIsARateNotAnArtefactOfDepth()} able to check it at three depths.
 * <p>
 * See {@code docs/solutions/best-practices/choose-the-statistic-that-states-the-claim.md} for why this module
 * chooses a statistic deliberately rather than reporting a percentile and asserting on whatever it gives.
 *
 * @author Antony Stubbs
 * @see HeadOfLineBlockingBenchmarkTest the first benchmark, and the one this exists to make credible
 * @see WorkloadMatrixBenchmarkTest where the coverage lives
 */
@Slf4j
// PcDispatchSwitch is process-wide, and the JUnit config runs at a dynamic factor of 20 - a concurrently
// running class would both flip the switch and contend for the cores being measured.
@Isolated
@Tag("performance")
class BacklogCatchUpBenchmarkTest extends StreamsBenchmarkHarness {

    /**
     * The primary depth. Large enough that the drain lasts long enough for a trimmed window to mean something,
     * small enough that both arms of every sweep finish inside a coffee break.
     */
    private static final int DEPTH = 1_200;

    /**
     * The sweep. Three depths spanning an order of magnitude, which is enough to see whether the ratio moves
     * with depth - the whole question {@link #theAdvantageIsARateNotAnArtefactOfDepth()} exists to settle.
     */
    private static final int[] DEPTH_SWEEP = {200, 1_200, 3_000};

    /**
     * A discarded pass before anything is measured, so neither arm pays for JIT the other avoided. Small,
     * because its job is to reach compiled code and open the broker connections, not to be a measurement.
     */
    private static final int WARM_UP_DEPTH = 150;

    /**
     * Wide, and justified rather than tuned. The pool is four, so the ceiling is four; asserting 1.5x leaves
     * generous room for a loaded machine while staying far above what a null result could reach.
     */
    private static final double MIN_SUSTAINED_RATE_IMPROVEMENT = 1.5d;

    /**
     * The workload every test here varies from. Skewed keys by default, because that is what real traffic
     * looks like and because skew is the axis that costs Parallel Consumer most.
     */
    /**
     * The scenario's defaults, then the reader's overrides - {@code applySystemPropertyOverrides()} last, so a
     * flag typed on the command line actually takes effect rather than being silently outranked.
     * <p>
     * The depth sweep re-applies its own {@code recordCount} afterwards, because sweeping depth IS that
     * experiment and an ambient {@code --records} would collapse three points into one.
     */
    private static BenchmarkWorkload.Builder backlogWorkload(final String name, final int depth) {
        return BenchmarkWorkload.builder(name)
                .recordCount(depth)
                .keyDistribution(KeyDistribution.ZIPF)
                .keyCount(200)
                .zipfExponent(1.0d)
                .ratePerSecond(0d)
                .applySystemPropertyOverrides();
    }

    /**
     * The headline. Both arms, one backlog, one term varied.
     */
    @Test
    void aBacklogDrainsFasterWithTheSeamOn() {
        warmUp();

        BenchmarkWorkload workload = backlogWorkload("catchup", DEPTH).build();
        List<GeneratedRecord> records = workload.generate();

        ArmResult stock = runArm("catchup-stock", workload, records, false);
        ArmResult pc = runArm("catchup-pc", workload, records, true);

        double rateRatio = pc.getSustainedRatePerSecond() / stock.getSustainedRatePerSecond();
        double drainRatio = stock.getTimeToDrainMillis() / (double) pc.getTimeToDrainMillis();
        long secondsSaved = (stock.getTimeToDrainMillis() - pc.getTimeToDrainMillis()) / 1000L;

        log.info(new BenchmarkReport("Experiment C1 - cold-start backlog catch-up")
                .configured("backlog depth", DEPTH)
                .configured("workload", workload)
                .configured("worker pool", POOL_SIZE)
                .configured("partitions / threads", "1 / 1")
                .configured("ordering ceiling", stock.getDistinctKeys() + " distinct keys")
                .measurement("sustained catch-up rate", rate(stock), rate(pc), String.format("%.2fx", rateRatio))
                .measurement("time to drain", stock.getTimeToDrainMillis() + "ms",
                        pc.getTimeToDrainMillis() + "ms", String.format("%.2fx", drainRatio))
                .measurement("in-chain latency p50", stock.getInChainLatency().p50() + "ms",
                        pc.getInChainLatency().p50() + "ms",
                        BenchmarkReport.ratio(pc.getInChainLatency().p50(), stock.getInChainLatency().p50()))
                .measurement("in-chain latency p99", stock.getInChainLatency().p99() + "ms",
                        pc.getInChainLatency().p99() + "ms",
                        BenchmarkReport.ratio(pc.getInChainLatency().p99(), stock.getInChainLatency().p99()))
                .measurement("split poll waits", String.valueOf(stock.getSplitPollWaits()),
                        String.valueOf(pc.getSplitPollWaits()), "-")
                .finding(String.format("Catching up on a %d-record backlog took %ds with the seam on against "
                        + "%ds with it off - %ds saved.", DEPTH, pc.getTimeToDrainMillis() / 1000L,
                        stock.getTimeToDrainMillis() / 1000L, secondsSaved))
                .finding(String.format("The rate ratio is %.2fx against a worker pool of %d, so the run was %s.",
                        rateRatio, POOL_SIZE,
                        rateRatio >= POOL_SIZE * 0.85d ? "pool-limited - more workers would still help"
                                : "handover-limited or tail-limited, not saturating the pool"))
                .finding(String.format("In-chain latency is nearly identical in the two arms (p50 %dms vs %dms). "
                        + "That is not a null result, it is the evidence for the statistic: once a record has "
                        + "entered the chain it costs what it costs in either arm, and the queueing a backlog "
                        + "creates happens BEFORE entry. Latency percentiles cannot see it; the rate can.",
                        stock.getInChainLatency().p50(), pc.getInChainLatency().p50()))
                .finding(String.format("Wake-on-work took the split-wait branch %d times against %d records "
                                + "dispatched (%.0f%%). %s", pc.getSplitPollWaits(), pc.getRecordsDispatchedToPool(),
                        100d * pc.getSplitPollWaits() / Math.max(1L, pc.getRecordsDispatchedToPool()),
                        pc.getSplitPollWaits() < pc.getRecordsDispatchedToPool() / 4
                                ? "It barely participated, so this result does not rest on that optimisation."
                                : "REFUTED - the prediction was that a saturated backlog would barely use it. "
                                + "It is heavily in play, so how much of this result it accounts for is a "
                                + "question to measure rather than assume. See C3."))
                .render());

        assertThat(rateRatio)
                .as("C1: under a saturated backlog the stock arm hands the partition over one record at a time, "
                        + "so its catch-up rate is bounded by one record per mean cost however deep the queue "
                        + "is. Stock %.1f/s vs PC %.1f/s", stock.getSustainedRatePerSecond(),
                        pc.getSustainedRatePerSecond())
                .isGreaterThan(MIN_SUSTAINED_RATE_IMPROVEMENT);

        assertThat(rateRatio)
                .as("C1: and it cannot exceed the worker pool of %d - a ratio above that would mean the gain is "
                        + "coming from something other than concurrency, and the result would have to be "
                        + "withdrawn rather than published", POOL_SIZE)
                .isLessThanOrEqualTo(POOL_SIZE * 1.15d);
    }

    /**
     * P2. The claim being tested is subtle and it partly contradicts the intuition that the advantage compounds
     * with backlog depth.
     * <p>
     * The <em>absolute</em> time saved does compound, without limit. The <em>time-to-drain ratio</em> rises with
     * depth, because the fixed startup cost is amortised. But the <em>sustained-rate ratio</em> should be flat:
     * both arms are throughput-limited from the first second, stock at roughly one record per mean cost and PC
     * at roughly {@code poolSize} records per mean cost, and there is nothing left in that ratio to compound.
     * <p>
     * A moving rate ratio would mean either that the statistic is not measuring what it claims, or that
     * something depth-dependent is in play that this model does not contain. Either would be a more interesting
     * finding than the confirmation.
     */
    @Test
    void theAdvantageIsARateNotAnArtefactOfDepth() {
        warmUp();

        BenchmarkReport report = new BenchmarkReport("Experiment C2 - does the advantage depend on backlog depth?")
                .configured("depths swept", java.util.Arrays.toString(DEPTH_SWEEP))
                .configured("worker pool", POOL_SIZE);

        double[] rateRatios = new double[DEPTH_SWEEP.length];
        double[] drainRatios = new double[DEPTH_SWEEP.length];

        for (int i = 0; i < DEPTH_SWEEP.length; i++) {
            int depth = DEPTH_SWEEP[i];
            // recordCount re-applied after the overrides: sweeping depth is this experiment, so an ambient
            // --records would collapse the three points into one and the sweep would prove nothing.
            BenchmarkWorkload workload = backlogWorkload("depth" + depth, depth).recordCount(depth).build();
            List<GeneratedRecord> records = workload.generate();

            ArmResult stock = runArm("depth" + depth + "-stock", workload, records, false);
            ArmResult pc = runArm("depth" + depth + "-pc", workload, records, true);

            rateRatios[i] = pc.getSustainedRatePerSecond() / stock.getSustainedRatePerSecond();
            drainRatios[i] = stock.getTimeToDrainMillis() / (double) pc.getTimeToDrainMillis();

            report.measurement("depth " + depth + " sustained rate", rate(stock), rate(pc),
                            String.format("%.2fx", rateRatios[i]))
                    .measurement("depth " + depth + " time to drain", stock.getTimeToDrainMillis() + "ms",
                            pc.getTimeToDrainMillis() + "ms", String.format("%.2fx", drainRatios[i]))
                    .measurement("depth " + depth + " seconds saved", "-", "-",
                            (stock.getTimeToDrainMillis() - pc.getTimeToDrainMillis()) / 1000L + "s");
        }

        double rateSpread = spread(rateRatios);
        report.finding(String.format("Sustained-rate ratio across depths %s: %s (spread %.2fx).",
                        java.util.Arrays.toString(DEPTH_SWEEP), format(rateRatios), rateSpread))
                .finding(String.format("Time-to-drain ratio across the same depths: %s - it rises with depth "
                        + "because the fixed startup cost is amortised, which is exactly why it is reported "
                        + "and not asserted on.", format(drainRatios)))
                .finding(rateSpread < 0.5d
                        ? "HELD: the rate ratio is flat in depth, so it is a property of the dispatch mechanism "
                        + "rather than an artefact of how much work was queued."
                        : "REFUTED: the rate ratio moved with depth. Something depth-dependent is in play that "
                        + "the model does not contain, and that is the more interesting result.");
        log.info(report.render());

        assertThat(rateSpread)
                .as("C2: the sustained-rate ratio must be roughly the same at every depth, or it is not the "
                        + "depth-invariant statistic it was chosen for. Measured %s", format(rateRatios))
                .isLessThan(1.0d);
    }

    /**
     * <b>C3, and it exists because C1 refuted the prediction that led to it.</b>
     * <p>
     * The plan predicted that a saturated backlog would barely use wake-on-work, on the reasoning that with
     * work always available the poll never has to wait - which would have meant this benchmark's headline owed
     * nothing to this module's own most recent optimisation. The first measured run refuted that flatly: the
     * split-wait branch was taken on roughly nine records in ten.
     * <p>
     * The mechanism is now obvious in hindsight and was not predicted. A backlog keeps the <em>broker</em>
     * supplied, but Parallel Consumer's max concurrency still bounds what the StreamThread may take in one
     * pass, so the thread returns to the poll while its workers are mid-flight and finds itself waiting on
     * them rather than on the broker. Saturating the topic does not saturate the thread.
     * <p>
     * So the counter is not evidence and this test does not use it. Instead it varies wake-on-work as its
     * single term - the same control-arm discipline applied to a different question - and measures how much of
     * the advantage survives without it. That is the honest form of "does this result depend on your own
     * optimisation": a measurement, not an assumption.
     */
    @Test
    void howMuchOfTheAdvantageSurvivesWithoutWakeOnWork() {
        warmUp();

        BenchmarkWorkload workload = backlogWorkload("wow", DEPTH).build();
        List<GeneratedRecord> records = workload.generate();

        ArmResult stock = runArm("wow-stock", workload, records, false);

        ArmResult pcWithout;
        try {
            PcDispatchSwitch.setWakeOnWork(false);
            pcWithout = runArm("wow-pc-nowake", workload, records, true);
        } finally {
            PcDispatchSwitch.setWakeOnWork(true);
        }
        ArmResult pcWith = runArm("wow-pc-wake", workload, records, true);

        double withWake = pcWith.getSustainedRatePerSecond() / stock.getSustainedRatePerSecond();
        double withoutWake = pcWithout.getSustainedRatePerSecond() / stock.getSustainedRatePerSecond();
        double attributable = withWake - withoutWake;

        log.info(new BenchmarkReport("Experiment C3 - how much of the backlog result is wake-on-work?")
                .configured("backlog depth", DEPTH)
                .configured("varied term", "pc.streams.wakeOnWork.enabled, seam ON in both PC arms")
                .measurement("stock (seam off)", rate(stock), "-", "-")
                .measurement("PC, wake-on-work OFF", rate(stock), rate(pcWithout),
                        String.format("%.2fx", withoutWake))
                .measurement("PC, wake-on-work ON", rate(stock), rate(pcWith), String.format("%.2fx", withWake))
                .measurement("split poll waits (ON arm)", "0", String.valueOf(pcWith.getSplitPollWaits()), "-")
                .finding(String.format("REFUTED, and this test is the replacement for the prediction that "
                        + "failed: a saturated backlog does NOT idle the wake-on-work path. It fired on %.0f%% "
                        + "of records, because PC's max concurrency - not the broker - is what sends the "
                        + "StreamThread back to a poll it then has to wait out.",
                        100d * pcWith.getSplitPollWaits() / Math.max(1L, pcWith.getRecordsDispatchedToPool())))
                .finding(String.format("Of the %.2fx advantage, %.2fx is present with wake-on-work switched "
                                + "off. The optimisation accounts for the remaining %.2fx.",
                        withWake, withoutWake, attributable))
                .finding(withoutWake > MIN_SUSTAINED_RATE_IMPROVEMENT
                        ? "So the headline survives the removal of this module's own recent optimisation, which "
                        + "is the claim C1 wanted to make - now measured rather than assumed."
                        : "So the headline does NOT survive without wake-on-work, and any quotation of it must "
                        + "say so. That is a materially weaker claim than C1's framing implied.")
                .render());

        assertThat(withoutWake)
                .as("C3: with wake-on-work off the seam must still beat stock, or the backlog advantage is an "
                        + "artefact of the poll optimisation rather than of concurrent dispatch. Stock %.1f/s "
                        + "vs PC-without-wake %.1f/s", stock.getSustainedRatePerSecond(),
                        pcWithout.getSustainedRatePerSecond())
                .isGreaterThan(1.0d);
    }

    /**
     * P10. Whichever arm runs second inherits the other's warm-up, so the arms are run in both orders and both
     * ratios reported. This is the only one of the three warm-up defences that can actually falsify the others:
     * if the orders disagree, the measured difference is JVM warm-up and the headline has to be withdrawn.
     */
    @Test
    void armOrderDoesNotChangeTheAnswer() {
        warmUp();

        BenchmarkWorkload workload = backlogWorkload("order", DEPTH).build();
        List<GeneratedRecord> records = workload.generate();

        ArmResult stockFirst = runArm("order-a-stock", workload, records, false);
        ArmResult pcSecond = runArm("order-a-pc", workload, records, true);
        double stockThenPc = pcSecond.getSustainedRatePerSecond() / stockFirst.getSustainedRatePerSecond();

        ArmResult pcFirst = runArm("order-b-pc", workload, records, true);
        ArmResult stockSecond = runArm("order-b-stock", workload, records, false);
        double pcThenStock = pcFirst.getSustainedRatePerSecond() / stockSecond.getSustainedRatePerSecond();

        double disagreement = Math.abs(stockThenPc - pcThenStock);
        log.info(new BenchmarkReport("Experiment C4 - is the result JVM warm-up?")
                .configured("backlog depth", DEPTH)
                .measurement("ratio, stock arm first", "-", "-", String.format("%.2fx", stockThenPc))
                .measurement("ratio, PC arm first", "-", "-", String.format("%.2fx", pcThenStock))
                .measurement("disagreement", "-", "-", String.format("%.2fx", disagreement))
                .finding(disagreement < 0.75d
                        ? "HELD: running the arms in either order gives the same answer, so the difference is "
                        + "the seam and not the order the JIT warmed them in."
                        : "REFUTED: the two orders disagree. The measured difference includes JVM warm-up and "
                        + "the headline must be withdrawn until that is fixed.")
                .render());

        assertThat(disagreement)
                .as("C4: stock-then-PC measured %.2fx and PC-then-stock measured %.2fx. A large disagreement "
                        + "means the number is warm-up rather than the seam", stockThenPc, pcThenStock)
                .isLessThan(1.5d);
    }

    /**
     * A discarded pass, on the same topology shape, before any arm that will be reported. Runs both seam
     * settings so neither arm is the one that pays to compile the shared path.
     */
    private void warmUp() {
        BenchmarkWorkload workload = backlogWorkload("warmup", WARM_UP_DEPTH)
                .cost(java.time.Duration.ofMillis(3), java.time.Duration.ofMillis(15))
                .build();
        List<GeneratedRecord> records = workload.generate();
        runArm("warmup-stock", workload, records, false);
        runArm("warmup-pc", workload, records, true);
        log.info("=== warm-up complete, discarded. Measured arms start now.");
    }

    private static String rate(final ArmResult result) {
        return String.format("%.1f/s", result.getSustainedRatePerSecond());
    }

    private static String format(final double[] ratios) {
        StringBuilder out = new StringBuilder("[");
        for (int i = 0; i < ratios.length; i++) {
            out.append(String.format("%.2fx", ratios[i]));
            if (i < ratios.length - 1) {
                out.append(", ");
            }
        }
        return out.append("]").toString();
    }

    private static double spread(final double[] values) {
        double min = Double.MAX_VALUE;
        double max = -Double.MAX_VALUE;
        for (double value : values) {
            min = Math.min(min, value);
            max = Math.max(max, value);
        }
        return max - min;
    }
}
