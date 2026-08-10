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
 * <b>Experiment D: the synthetic matrix.</b> Openly synthetic, and that is the right thing for it to be - it is
 * measuring properties, not telling a story. "Realistic" means a different thing to every reader, so one domain
 * cannot carry the argument alone; this is where the coverage lives.
 * <p>
 * <b>Cells where Parallel Consumer does nothing, or loses, are results.</b> They are published rather than
 * tuned away, and two of them carry assertions to that effect: the single-key floor, and CPU-bound work at
 * equal thread count. Those two are the falsifiers - if the seam wins where it structurally cannot, every other
 * cell is measuring a faster harness and has to be withdrawn.
 *
 * <h2>Three axes, swept one at a time</h2>
 * A full cross product is twenty-four cells at two arms each, which nobody would ever run and which would
 * therefore be a benchmark in name only. Each axis is swept while the others hold at a stated centre point:
 * skewed keys, fully blocking work, a 512-byte payload, one partition, one StreamThread. The centre point is
 * named here so a reader knows what everything else was pinned to.
 *
 * <h2>The backlog runner is reused deliberately</h2>
 * Every cell is a cold-start backlog drain, so arrival rate is not a variable anywhere in the matrix and each
 * cell differs from its neighbour in exactly the axis being swept. Paced arrival is exercised in
 * {@link PaymentAuthorisationBenchmarkTest}, where the steady-state case belongs.
 *
 * @author Antony Stubbs
 * @see BacklogCatchUpBenchmarkTest the headline
 * @see HeadOfLineBlockingBenchmarkTest the first, deliberately isolating, benchmark
 */
@Slf4j
@Isolated
@Tag("performance")
class WorkloadMatrixBenchmarkTest extends StreamsBenchmarkHarness {

    /**
     * Smaller than the headline experiment's, because the matrix runs eleven cells at two arms each and a
     * benchmark nobody will wait for does not get run. Still large enough that the trimmed rate window holds
     * more than two hundred completions.
     */
    private static final int DEPTH = 300;

    private static final int KEYS = 100;

    private static final int SMALL_PAYLOAD = 512;

    private static final int LARGE_PAYLOAD = 8_192;

    /**
     * The ceiling the two negative-control cells must stay under. Not 1.0: the PC path still hands records
     * through a pool even when it cannot parallelise them, so exact parity would be a stronger claim than the
     * mechanism supports. This asserts the absence of a <em>material</em> advantage.
     */
    private static final double NO_MATERIAL_ADVANTAGE = 1.35d;

    private static BenchmarkWorkload.Builder cell(final String name) {
        return BenchmarkWorkload.fromSystemProperties(name)
                .recordCount(DEPTH)
                .keyDistribution(KeyDistribution.ZIPF)
                .keyCount(KEYS)
                .zipfExponent(1.0d)
                .cost(Duration.ofMillis(20), Duration.ofMillis(200))
                .blockingFraction(1.0d)
                .payloadBytes(SMALL_PAYLOAD)
                .ratePerSecond(0d);
    }

    /**
     * Axis 1: key distribution. This is the axis a sceptic reaches for first, because it is where the existing
     * benchmark is weakest - it gave every fast record its own key, and real keyspaces do not look like that.
     * <p>
     * It also subsumes the cardinality sweep that was planned as Experiment B and never written, adding the
     * dimension that plan lacked: skew.
     */
    @Test
    void keyDistributionDecidesHowMuchConcurrencyThereIsToFind() {
        BenchmarkReport report = new BenchmarkReport("Experiment D1 - key distribution")
                .configured("held constant", "blocking work, 512B payload, 1 partition, 1 StreamThread")
                .configured("backlog depth", DEPTH)
                .configured("worker pool", POOL_SIZE);

        double single = measure(report, "single key", cell("d1-single")
                .keyDistribution(KeyDistribution.SINGLE));
        double zipf = measure(report, "zipf s=1.0, " + KEYS + " keys", cell("d1-zipf"));
        double zipfHeavy = measure(report, "zipf s=1.5, " + KEYS + " keys", cell("d1-zipf15")
                .zipfExponent(1.5d));
        double uniform = measure(report, "uniform, " + KEYS + " keys", cell("d1-uniform")
                .keyDistribution(KeyDistribution.UNIFORM));
        double highCardinality = measure(report, "one key per record", cell("d1-highcard")
                .keyDistribution(KeyDistribution.HIGH_CARDINALITY));

        report.finding(String.format("Single key is the floor at %.2fx: KEY ordering permits one record in "
                + "flight, so there is no concurrency for the seam to find and it must not win here.", single))
                .finding(String.format("Skew costs real advantage. Uniform %.2fx against zipf s=1.0 %.2fx "
                        + "against zipf s=1.5 %.2fx - the hotter the head key, the more of the stream is a "
                        + "serial queue no pool can open up.", uniform, zipf, zipfHeavy))
                .finding(String.format("One key per record is the ceiling at %.2fx, and it is the shape a "
                        + "sceptic correctly says their traffic does not have.", highCardinality))
                .finding(zipfHeavy <= zipf + 0.35d
                        ? "HELD: raising the skew lowered the advantage."
                        : "REFUTED: raising the skew did not lower the advantage, which means either the skew "
                        + "is not reaching the shards or something else dominates at this depth.");
        log.info(report.render());

        assertThat(single)
                .as("D1: with every record on ONE key, PC's KEY ordering allows at most one in flight, so the "
                        + "seam cannot confer an advantage. If it does, every other cell in this class is "
                        + "measuring a faster harness rather than key concurrency and must be withdrawn")
                .isLessThan(NO_MATERIAL_ADVANTAGE);

        assertThat(highCardinality)
                .as("D1: the advantage must rise as keys spread out - single %.2fx vs one-key-per-record %.2fx",
                        single, highCardinality)
                .isGreaterThan(single);
    }

    /**
     * Axis 2: what the per-record work actually is. The most important axis for credibility, because
     * demonstrating where the seam does <em>not</em> help is most of what makes the cells where it does help
     * believable.
     * <p>
     * <b>The received wisdom is tested here rather than assumed.</b> "PC's advantage is for work that blocks,
     * so a CPU-bound workload should show little or no gain" is the framing this benchmark was commissioned
     * with, and on an idle machine it is wrong: stock runs one record at a time per StreamThread whether that
     * thread is blocked or computing, so a pool spreads CPU work over spare cores just as it spreads waits. The
     * honest negative control is CPU-bound work on a machine with no spare cores, which is what
     * {@link #cpuBoundWorkOnASaturatedMachineIsTheRealNegativeControl()} builds.
     */
    @Test
    void theBlockingFractionDecidesWhetherThereIsAnyAdvantageAtAll() {
        BenchmarkReport report = new BenchmarkReport("Experiment D2 - processing profile")
                .configured("held constant", "zipf s=1.0 keys, 512B payload, 1 partition, 1 StreamThread")
                .configured("backlog depth", DEPTH)
                .configured("available cores", Runtime.getRuntime().availableProcessors())
                .configured("worker pool", POOL_SIZE);

        double blocking = measure(report, "fully blocking (b=1.0)", cell("d2-block").blockingFraction(1.0d));
        double mixed = measure(report, "mixed (b=0.5)", cell("d2-mixed").blockingFraction(0.5d));
        double cpuBound = measure(report, "CPU-bound (b=0.0), idle box", cell("d2-cpu").blockingFraction(0.0d));

        report.finding(String.format("Blocking %.2fx, mixed %.2fx, CPU-bound %.2fx on an otherwise idle "
                + "%d-core machine.", blocking, mixed, cpuBound, Runtime.getRuntime().availableProcessors()))
                .finding(cpuBound < NO_MATERIAL_ADVANTAGE
                        ? "HELD, matching the received wisdom: CPU-bound work gained nothing."
                        : String.format("REFUTED, and this is the more instructive outcome: CPU-bound work "
                        + "still gained %.2fx on an idle machine. Stock processes one record at a time per "
                        + "StreamThread whether the thread is blocked or computing, so a worker pool spreads "
                        + "computation across spare cores exactly as it spreads waits. 'PC only helps blocking "
                        + "work' is too strong - what PC parallelises is work the StreamThread cannot proceed "
                        + "past, and a busy core is one of those. The real negative control holds the THREAD "
                        + "COUNT equal instead, and is measured separately in D3.", cpuBound))
                .finding(mixed >= Math.min(blocking, cpuBound) - 0.35d && mixed <= Math.max(blocking, cpuBound) + 0.35d
                        ? "HELD: the mixed cell falls between the two pure cells, as an Amdahl split predicts."
                        : "REFUTED: the mixed cell falls outside the bracket set by the pure cells.");
        log.info(report.render());

        assertThat(blocking)
                .as("D2: blocking work is the case the seam exists for and must show an advantage here, or "
                        + "nothing else in this suite is worth reading")
                .isGreaterThan(1.5d);
    }

    /**
     * Axis 2, continued: the genuine negative control - <b>at equal thread count</b>.
     *
     * <h2>Two failed attempts at this control, both recorded because they are instructive</h2>
     * The first attempt made the CPU work a deadline-bounded spin, which is a busy-wait for a fixed
     * <em>duration</em> and therefore a sleep wearing a spin's clothes. Contention could not possibly show up,
     * and this control failing is what caught it - see {@code StreamsBenchmarkHarness.spinFor}.
     * <p>
     * The second attempt burned eleven of twelve cores with background threads, and with the fixture now
     * correct it still measured 3.43x. That refutation is the more interesting one, because this time the
     * <em>experimental design</em> was wrong rather than the fixture: <b>a machine cannot be saturated against
     * a fair scheduler by adding elastic background load.</b> The scheduler divides cores among runnable
     * threads, so an arm with fifteen threads takes a larger total share than an arm with twelve however busy
     * the box already was. Loading the machine does not neutralise the seam's extra threads; it only makes
     * everyone's slice thinner.
     *
     * <h2>What actually controls it</h2>
     * Hold the <b>thread count</b> equal instead. Stock gets {@link #POOL_SIZE} partitions and
     * {@link #POOL_SIZE} StreamThreads; the seam gets one partition, one StreamThread and a pool of
     * {@link #POOL_SIZE}. Both arms then have the same number of runnable threads doing the same total work,
     * and the question becomes the honest one: <b>does the seam buy anything that threads alone would not?</b>
     * <p>
     * For CPU-bound work the answer should be no, and that is the boundary of the claim. The seam's value was
     * never more throughput per thread - it is that the concurrency arrives without needing the partitions,
     * which is what {@link PartitionScalingBenchmarkTest} prices.
     */
    @Test
    void cpuBoundWorkAtEqualThreadCountIsTheRealNegativeControl() {
        int cores = Runtime.getRuntime().availableProcessors();

        BenchmarkWorkload spec = cell("d3-cpu-equal")
                .blockingFraction(0.0d)
                // Evenly-spread keys, so the stock arm's extra partitions are all fed. A skewed keyspace would
                // leave some of them idle and hand the seam an advantage that came from the data rather than
                // the dispatch - the control would then be varying two terms.
                .keyDistribution(KeyDistribution.HIGH_CARDINALITY)
                .build();
        List<GeneratedRecord> records = spec.generate();

        ArmResult stockManyThreads = runArm("d3-stock-" + POOL_SIZE + "t", spec, records, false,
                POOL_SIZE, POOL_SIZE);
        ArmResult pcOneThread = runArm("d3-pc-1t", spec, records, true, 1, 1);

        double atEqualThreads =
                pcOneThread.getSustainedRatePerSecond() / stockManyThreads.getSustainedRatePerSecond();

        log.info(new BenchmarkReport("Experiment D3 - CPU-bound, at equal thread count")
                .configured("available cores", cores)
                .configured("stock arm", POOL_SIZE + " partitions, " + POOL_SIZE + " StreamThreads")
                .configured("seam arm", "1 partition, 1 StreamThread, pool of " + POOL_SIZE)
                .configured("held constant", "one key per record, 512B payload, b=0.0, backlog " + DEPTH)
                .measurement("CPU-bound, equal threads",
                        String.format("%.1f/s", stockManyThreads.getSustainedRatePerSecond()),
                        String.format("%.1f/s", pcOneThread.getSustainedRatePerSecond()),
                        String.format("%.2fx", atEqualThreads))
                .finding(String.format("Given the same number of threads, the seam measured %.2fx against "
                        + "stock. %s", atEqualThreads, atEqualThreads < NO_MATERIAL_ADVANTAGE
                        ? "HELD: this is where the seam does nothing. For CPU-bound work throughput is a "
                        + "function of how many threads are computing, and the seam does not create cores. "
                        + "Its advantage elsewhere comes from getting those threads without the partitions, "
                        + "not from the threads being more productive."
                        : "REFUTED: the seam gained even at equal thread count on CPU-bound work, which this "
                        + "model does not explain and which needs chasing before any other cell is quoted."))
                .finding("Two earlier attempts at this control failed and both are recorded rather than "
                        + "discarded: a deadline-bounded spin (which is a sleep), and saturating the machine "
                        + "with background load (which a fair scheduler defeats, because it divides cores by "
                        + "runnable threads and the seam's arm simply has more of them).")
                .render());

        assertThat(atEqualThreads)
                .as("D3: for CPU-bound work at equal thread count the seam must not confer a material "
                        + "advantage - it does not create cores. Stock %.1f/s on %d threads vs seam %.1f/s on "
                        + "1 StreamThread and a pool of %d", stockManyThreads.getSustainedRatePerSecond(),
                        POOL_SIZE, pcOneThread.getSustainedRatePerSecond(), POOL_SIZE)
                .isLessThan(NO_MATERIAL_ADVANTAGE);
    }

    /**
     * Axis 3: data shape. Serialisation and parsing are CPU work on the same thread as everything else, so a
     * bigger payload raises the non-blocking share of each record and pulls the cell towards the CPU end of
     * axis 2.
     */
    @Test
    void payloadSizeDilutesTheAdvantage() {
        BenchmarkReport report = new BenchmarkReport("Experiment D4 - data shape")
                .configured("held constant", "zipf s=1.0 keys, blocking work, 1 partition, 1 StreamThread")
                .configured("backlog depth", DEPTH)
                .configured("worker pool", POOL_SIZE);

        double small = measure(report, SMALL_PAYLOAD + "B JSON", cell("d4-small").payloadBytes(SMALL_PAYLOAD));
        double large = measure(report, LARGE_PAYLOAD + "B JSON", cell("d4-large").payloadBytes(LARGE_PAYLOAD));

        report.finding(String.format("%dB payload %.2fx against %dB payload %.2fx.",
                        SMALL_PAYLOAD, small, LARGE_PAYLOAD, large))
                .finding(large < small - 0.25d
                        ? "HELD: a larger payload dilutes the advantage, because parse and serialise are CPU "
                        + "work that scales with size and is not what the seam parallelises away."
                        : String.format("REFUTED: growing the payload sixteenfold did not dilute the advantage "
                        + "(%.2fx against %.2fx - no material difference, and if anything the wrong way). The "
                        + "prediction assumed serialisation cost was a meaningful share of a record's work; at "
                        + "a %dms median service call, parsing even %dB of JSON is a rounding error beside it. "
                        + "Payload size would only matter here if the per-record service cost were far smaller, "
                        + "which is a different workload rather than a different data shape.",
                        large, small, 20, LARGE_PAYLOAD));
        log.info(report.render());
    }

    /**
     * Runs both arms of one cell and returns the sustained-rate ratio, adding the row to the report.
     * <p>
     * One generated list per cell, replayed into both arms - the cell's own guarantee that its two arms differ
     * in the seam and nothing else.
     */
    private double measure(final BenchmarkReport report, final String label, final BenchmarkWorkload.Builder builder) {
        BenchmarkWorkload workload = builder.build();
        List<GeneratedRecord> records = workload.generate();

        ArmResult stock = runArm(workload.getName() + "-stock", workload, records, false);
        ArmResult pc = runArm(workload.getName() + "-pc", workload, records, true);

        double ratio = pc.getSustainedRatePerSecond() / stock.getSustainedRatePerSecond();
        report.measurement(label,
                String.format("%.1f/s", stock.getSustainedRatePerSecond()),
                String.format("%.1f/s", pc.getSustainedRatePerSecond()),
                String.format("%.2fx", ratio));
        // Whole-batch drain, in every cell, next to the rate. The two can disagree - a rate is trimmed and
        // excludes startup, a drain is total wall clock - and where they disagree that IS the finding, because
        // total wall clock is the most natural thing for a reader to measure and needs no explanation.
        report.measurement("  ^ whole-batch drain", stock.getTimeToDrainMillis() + "ms",
                pc.getTimeToDrainMillis() + "ms",
                String.format("%.2fx", stock.getTimeToDrainMillis() / (double) pc.getTimeToDrainMillis()));
        return ratio;
    }

    /**
     * <b>The no-penalty claim, checked on the statistic a reader would actually compute.</b>
     * <p>
     * The module's headline promise for the degenerate case is "no penalty when you fall back to traditional
     * Kafka Streams usage", and the evidence behind it is a <em>median per-record</em> figure that wake-on-work
     * moved from about 0.70x to about 0.99x. Whole-batch drain time on a single key was never re-measured after
     * that fix - and on the pre-wake-on-work branch it stood at <b>0.57x</b>, far worse than the median said.
     * <p>
     * That gap matters more than it looks. Total wall clock is the first thing a sceptical reader measures for
     * themselves, it needs no explanation of percentiles, and a claim that holds for one statistic but not for
     * the obvious one is the easiest possible way to be caught out. So this test asks the question directly and
     * prints the answer in words, whichever way it goes.
     * <p>
     * A divergence is a real result rather than a defect: a workload can improve per-record latency while
     * getting slower end to end, because the pool handoff and completion feedback cost something per record and
     * nothing recovers that cost when there is no concurrency to win.
     */
    @Test
    void onASingleKeyDoesWholeBatchDrainReachParity() {
        BenchmarkWorkload workload = cell("d5-single").keyDistribution(KeyDistribution.SINGLE).build();
        List<GeneratedRecord> records = workload.generate();

        ArmResult stock = runArm("d5-single-stock", workload, records, false);
        ArmResult pc = runArm("d5-single-pc", workload, records, true);

        double drainRatio = stock.getTimeToDrainMillis() / (double) pc.getTimeToDrainMillis();
        double rateRatio = pc.getSustainedRatePerSecond() / stock.getSustainedRatePerSecond();
        double medianRatio = stock.getInChainLatency().p50() / (double) Math.max(1L, pc.getInChainLatency().p50());

        log.info(new BenchmarkReport("Experiment D5 - the no-penalty claim, on whole-batch drain")
                .configured("keys", "ONE - the degenerate case, where key ordering forbids concurrency")
                .configured("backlog depth", DEPTH)
                .configured("wake-on-work", "ON (this branch includes the fix)")
                .configured("worker pool", POOL_SIZE)
                .measurement("whole-batch drain", stock.getTimeToDrainMillis() + "ms",
                        pc.getTimeToDrainMillis() + "ms", String.format("%.2fx", drainRatio))
                .measurement("sustained rate", String.format("%.1f/s", stock.getSustainedRatePerSecond()),
                        String.format("%.1f/s", pc.getSustainedRatePerSecond()), String.format("%.2fx", rateRatio))
                .measurement("per-record median", stock.getInChainLatency().p50() + "ms",
                        pc.getInChainLatency().p50() + "ms", String.format("%.2fx", medianRatio))
                .finding(String.format("On one key, whole-batch drain measured %.2fx (%dms stock against %dms "
                                + "with the seam on). %s", drainRatio, stock.getTimeToDrainMillis(),
                        pc.getTimeToDrainMillis(), drainRatio >= 0.9d
                                ? "Parity reached on the statistic a reader computes first, so the no-penalty "
                                + "claim survives its most natural test."
                                : "PARITY NOT REACHED. The no-penalty claim holds for the per-record median but "
                                + "NOT for total wall clock, and it must be narrowed to say which statistic it "
                                + "is about."))
                .finding(Math.abs(drainRatio - medianRatio) > 0.2d
                        ? String.format("The statistics DISAGREE: drain %.2fx against per-record median %.2fx. "
                        + "A workload can improve per-record latency while getting slower end to end, because "
                        + "the pool handoff costs something per record and there is no concurrency here to pay "
                        + "it back. Anyone deciding whether to adopt this needs both numbers.", drainRatio,
                        medianRatio)
                        : String.format("Drain (%.2fx) and per-record median (%.2fx) agree, so the claim does "
                        + "not depend on which one is quoted.", drainRatio, medianRatio))
                .render());

        assertThat(drainRatio)
                .as("D5: on a single key the seam cannot parallelise anything, so it must not be materially "
                        + "SLOWER either - that is the 'no penalty on the degenerate case' claim, tested on "
                        + "whole-batch drain rather than on the median it was originally made about. Stock "
                        + "%dms vs seam %dms", stock.getTimeToDrainMillis(), pc.getTimeToDrainMillis())
                .isGreaterThan(0.75d);
    }
}
