package bz.stub.parallelconsumer.examples.streams.pc;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import java.util.Locale;
import java.util.Optional;
import java.util.OptionalLong;

/**
 * A {@link DemoSection} that runs the same workload twice, varying only the dispatch seam, and prints the
 * two latency distributions side by side.
 * <p>
 * Both of this branch's sections are instances of this class differing in one field, which is the point:
 * the headline result and the control that keeps it honest are the same measurement applied to workloads
 * that differ in key cardinality alone. Building them from one class is what stops them drifting into two
 * subtly different experiments whose comparison would mean nothing.
 *
 * @author Antony Stubbs
 */
final class LatencyScenario implements DemoSection {

    /**
     * What this section predicts, stated as something the run can contradict.
     * <p>
     * <b>The control's prediction is parity, not a loss, and getting that wrong is why this enum exists.</b>
     * The demo was inherited from a branch without wake-on-work, where the single-key arm lost outright and
     * a plain "did PC win?" test was a sound check on it. With the split poll wait present that arm sits on
     * the line, so a direction test flags roughly half of all healthy runs and teaches the reader to ignore
     * it. A band is the honest shape of "no effect".
     */
    enum Expectation {

        /**
         * The headline. Falsified by anything under {@link #MUCH_FASTER_THRESHOLD}, and <b>not</b> merely
         * by a ratio at or below 1.00x.
         * <p>
         * That weaker test is what this originally said, and a sabotage run - the seam left switched off in
         * the arm that was supposed to enable it - measured 1.01x and slipped through it, printing "a fast
         * record no longer waits for an unrelated slow one" over a run in which every fast record waited.
         * A check that a broken run can satisfy is not a check.
         */
        PC_MUCH_FASTER,

        /**
         * The negative control. One key means at most one record in flight, so the seam has nothing to
         * exploit and the two arms should land together - on either side, since which one wins a tie is
         * decided by whatever the machine was doing. Only a difference bigger than
         * {@link #NO_DIFFERENCE_BAND} is worth a reader's attention.
         */
        NO_MATERIAL_DIFFERENCE
    }

    /**
     * How far from 1.00x the control may land before the run says so.
     * <p>
     * Set from measurement rather than taste: across four consecutive runs on one machine the control's
     * median ratio read 1.19x, 0.99x, 0.98x and 0.99x, and the 1.19x came from the STOCK arm of that run
     * being slow - two stock arms doing byte-identical work differed by the same 1.20x. So the band has to
     * clear the run's own noise floor, and the demo prints that floor beside the verdict rather than asking
     * anyone to take this number on trust.
     */
    private static final double NO_DIFFERENCE_BAND = 0.25;

    /**
     * How far ahead PC has to be for the headline verdict to be printed.
     * <p>
     * Set between the two populations rather than at 1.00x: the head-of-line median ratio measured 14.5x,
     * 19.0x, 14.8x, 18.6x and 17.9x across five runs on one machine, while a run with the seam switched off
     * measures around 1.00x and the noise floor around 1.02x. 2.0x sits in the wide empty space between
     * them, so the check cannot be satisfied by a broken run and cannot be tripped by a slow one.
     */
    private static final double MUCH_FASTER_THRESHOLD = 2.0;

    private final String title;

    private final String armPrefix;

    private final String claim;

    private final String reading;

    private final boolean allOneKey;

    private final Expectation expectation;

    private final String summaryTitle;

    private final String verdict;

    /** Held so the closing summary can restate the result without re-running anything. */
    private ArmResult stockResult;

    private ArmResult pcResult;

    /**
     * Six of the eight parameters are prose strings and Java has no named arguments, so every one of them is
     * documented here: the javadoc is the only thing standing between a call site and a silent transposition
     * of two pieces of explanatory text.
     *
     * @param title        the section heading, printed as the demonstration starts
     * @param summaryTitle the heading this section's result appears under in the closing summary
     * @param armPrefix    prefix for the arms' topic and application ids, so each arm is isolated
     * @param claim        what this section asserts, in the reader's terms, printed before the arms run so
     *                     the numbers are read against a stated prediction rather than rationalised
     *                     afterwards
     * @param reading      how to interpret the result, printed after
     * @param verdict      the one-line result restated in the closing summary
     * @param allOneKey    put every record on one key, removing the key concurrency PC dispatch depends on
     * @param expectation  the shape of result this section predicts, checked against what it measured, so
     *                     the fixed prose above cannot be printed over a run that went another way
     */
    LatencyScenario(final String title,
                    final String summaryTitle,
                    final String armPrefix,
                    final String claim,
                    final String reading,
                    final String verdict,
                    final boolean allOneKey,
                    final Expectation expectation) {
        this.title = title;
        this.summaryTitle = summaryTitle;
        this.armPrefix = armPrefix;
        this.claim = claim;
        this.reading = reading;
        this.verdict = verdict;
        this.allOneKey = allOneKey;
        this.expectation = expectation;
    }

    /**
     * This section's STOCK arm median, for the demo's noise floor.
     * <p>
     * Stock Kafka Streams hands records over one at a time regardless of key, and the per-record cost here
     * is chosen by value rather than by key, so the stock arms of the two sections do byte-identical work in
     * the same order. Their difference is therefore pure run-to-run variance, measured inside this very run
     * - which is the only honest way to say whether a small difference elsewhere means anything.
     *
     * @return empty if this section has not run
     */
    OptionalLong stockMedianMillis() {
        return stockResult == null ? OptionalLong.empty() : OptionalLong.of(stockResult.latencies().p50());
    }

    @Override
    public String title() {
        return title;
    }

    @Override
    public void run(final DemoBroker broker) {
        Console.line("");
        Console.line("  Workload: 1 record costing %,dms at the head of ONE partition, then %d records "
                        + "costing %,dms each.",
                ArmRunner.SLOW_COST.toMillis(), ArmRunner.FAST_RECORDS, ArmRunner.FAST_COST.toMillis());
        Console.line("  Keys    : %s", allOneKey
                ? "every record on the SAME key as the slow one"
                : "every fast record on its OWN key, all different from the slow one");
        Console.wrapped("  Measured: ", "ALL " + ArmRunner.FAST_RECORDS + " fast-record latencies share ONE "
                + "t=0, the instant the first "
                + "record entered the topology. So each figure is elapsed-since-start, and what it captures "
                + "is queue position rather than service time - which is exactly what head-of-line blocking "
                + "is. Starting the clock at first entry rather than at produce keeps producer batching and "
                + "topology startup out of the measurement.");
        Console.wrapped("  Claim   : ", claim);
        Console.line("");
        Console.line("  Varying one deliberate term between the arms: PcDispatchSwitch, off then on.");
        Console.line("  Same JVM, same broker, same patched classes, same topology, same record set.");
        Console.line("  Not controlled: ORDER. Stock runs first, and no arm meets the machine in the same "
                + "state as another.");
        Console.line("  The NOISE FLOOR in the summary measures how much that alone is worth in this run.");

        Console.subSection("Arm 1 of 2: STOCK Kafka Streams dispatch (PcDispatchSwitch OFF)");
        stockResult = ArmRunner.runArm(broker, armPrefix + "-stock", false, allOneKey);
        printArm(stockResult, false);

        Console.subSection("Arm 2 of 2: PARALLEL CONSUMER dispatch (PcDispatchSwitch ON, pool of "
                + ArmRunner.POOL_SIZE + ")");
        pcResult = ArmRunner.runArm(broker, armPrefix + "-pc", true, allOneKey);
        printArm(pcResult, true);

        printComparison(stockResult, pcResult);
        Console.line("");
        Console.wrapped("  Reading : ", reading);
    }

    @Override
    public void printSummary() {
        if (stockResult == null || pcResult == null) {
            return;
        }
        Latencies stock = stockResult.latencies();
        Latencies pc = pcResult.latencies();

        Console.line("");
        Console.line("  %s", summaryTitle);
        summaryRow("fastest fast record", stock.min(), pc.min());
        summaryRow("median  fast record", stock.p50(), pc.p50());
        summaryRow("whole batch drained", stockResult.totalDrainMillis(), pcResult.totalDrainMillis());
        // Measured, not asserted: printed here so the per-section evidence is the reading taken in this
        // run rather than the reading the demo expects to take.
        Console.line("    %-20s %,9d stock  ->  %,9d PC   (of %d records)", "through PC's pool",
                stockResult.dispatchedToPool(), pcResult.dispatchedToPool(), ArmRunner.TOTAL_RECORDS);

        // The verdict below is fixed prose describing the expected result. Printing it over a run that went
        // another way would be the demo telling the reader what it wanted to find, so the measurement is
        // checked against the expectation first.
        double medianRatio = stock.p50() / (double) Math.max(1, pc.p50());
        contradiction(expectation, medianRatio).ifPresent(unexpected ->
                Console.wrapped("    !! ", "UNEXPECTED: " + unexpected + " The fixed reading below describes "
                        + "the result this arm was predicted to give, and this run did not give it. Trust "
                        + "the numbers above, not the sentence below."));
        Console.wrapped("    => ", verdict);
    }

    /**
     * True only when this section's two arms read exactly as the demo's evidence claim requires: every
     * record through the pool in the PC arm, none in the stock arm, and nothing dropped by the epoch
     * filter on the way.
     */
    @Override
    public boolean dispatchEvidenceHolds() {
        return stockResult != null
                && pcResult != null
                && stockResult.dispatchedToPool() == 0
                && pcResult.dispatchedToPool() == ArmRunner.TOTAL_RECORDS
                && !pcResult.hasWorkManagerDrop();
    }

    /**
     * Whether this run contradicts what the section predicted, and if so how, in a sentence.
     * <p>
     * <b>Static and package-private so it can be tested, which is not incidental.</b> A passing demo run
     * never enters the failure branch, and the demo itself is behind {@code -Pdemo} so the ordinary build
     * never runs it at all - this decision would otherwise have no coverage anywhere. That is exactly how
     * its first version shipped a threshold a broken run could satisfy.
     *
     * @return empty when the run is consistent with the expectation
     */
    static Optional<String> contradiction(final Expectation expectation, final double medianRatio) {
        switch (expectation) {
            case PC_MUCH_FASTER:
                return medianRatio >= MUCH_FASTER_THRESHOLD ? Optional.empty()
                        : Optional.of(String.format(Locale.ROOT, "this section's median ratio was %.2fx, "
                        + "short of the %.1fx this claim needs - and a ratio near 1.00x is what a run with "
                        + "the seam switched off looks like, so check the dispatch counters above first.",
                        medianRatio, MUCH_FASTER_THRESHOLD));
            case NO_MATERIAL_DIFFERENCE:
                return Math.abs(medianRatio - 1.0) <= NO_DIFFERENCE_BAND ? Optional.empty()
                        : Optional.of(String.format(Locale.ROOT, "this section's median ratio was %.2fx, "
                        + "further from parity than the %.0f%% this control tolerates - in EITHER "
                        + "direction, because either would mean something is acting here that key "
                        + "concurrency cannot explain.", medianRatio, NO_DIFFERENCE_BAND * 100));
            default:
                throw new IllegalStateException("Unhandled expectation " + expectation);
        }
    }

    private static void summaryRow(final String label, final long stock, final long pc) {
        Console.line("    %-20s %,9dms stock  ->  %,9dms PC   %6.2fx", label, stock, pc,
                stock / (double) Math.max(1, pc));
    }

    private void printArm(final ArmResult result, final boolean expectPcDispatch) {
        Latencies latencies = result.latencies();
        Console.line("    fast-record latency   n=%d  min=%,dms  p50=%,dms  p99=%,dms",
                latencies.count(), latencies.min(), latencies.p50(), latencies.p99());
        // The whole-batch figure, so nobody has to wonder whether the fast records were made quicker at
        // the expense of the batch.
        Console.line("    whole batch drained   %,dms for all %d records, blocker included",
                result.totalDrainMillis(), ArmRunner.TOTAL_RECORDS);
        Console.line("    dispatch counters     offered=%d accepted=%d dispatchedToPool=%d ok=%d failed=%d",
                result.offeredToWorkManager(), result.acceptedByWorkManager(), result.dispatchedToPool(),
                result.completedSuccessfully(), result.failed());
        // Separate line because it is a separate mechanism. The seam and the split poll wait arrive on the
        // same branch and would otherwise be credited to each other; these two numbers say which one ran.
        Console.line("    wake-on-work          splitPollWaits=%d wakesOnWork=%d",
                result.splitPollWaits(), result.wakesOnWork());

        // The counters are the evidence that the arm ran the path it says it ran, so they are checked here
        // rather than merely printed for the reader to audit.
        long dispatched = result.dispatchedToPool();
        if (expectPcDispatch) {
            if (dispatched == ArmRunner.TOTAL_RECORDS) {
                Console.line("    ^ all %d records went through PC's worker pool. This counter is incremented "
                        + "at exactly", ArmRunner.TOTAL_RECORDS);
                Console.line("      one place in the codebase, so it cannot read non-zero unless PC really "
                        + "drove this arm.");
            } else {
                Console.line("    ^ WARNING: expected %d records through the pool, saw %d. Treat this arm's "
                        + "numbers with suspicion.", ArmRunner.TOTAL_RECORDS, dispatched);
            }
            if (result.hasWorkManagerDrop()) {
                Console.line("    ^ WARNING: offered != accepted, so records were dropped by the epoch "
                        + "filter rather than processed.");
            }
            // Reported rather than made fatal: the demo's claim is about the dispatch seam, and this arm is
            // still a valid PC arm without the split wait. But it would be a DIFFERENT PC arm, and the
            // negative control's result in particular turns on which of the two ran.
            if (result.splitPollWaits() == 0) {
                Console.line("    ^ WARNING: wake-on-work never ran in this arm. Read the control below as "
                        + "the seam WITHOUT the split poll wait.");
            } else {
                Console.line("    ^ the split poll wait ran %d times and %d of those ended on a worker "
                        + "completion,", result.splitPollWaits(), result.wakesOnWork());
                Console.line("      rather than on the poll budget running out.");
            }
        } else if (dispatched == 0) {
            Console.line("    ^ zero records through PC's pool, which is what a genuine stock arm looks "
                    + "like.");
            // The split wait is entered only when PC has work outstanding on this thread, so a stock arm
            // reading non-zero would mean the previous arm's StreamThread was still alive and counting.
            if (result.splitPollWaits() != 0) {
                Console.line("    ^ WARNING: a stock arm took the split poll wait %d times, which it cannot "
                        + "do. A previous arm's thread is still running.", result.splitPollWaits());
            }
        } else {
            Console.line("    ^ WARNING: this arm was supposed to be stock, but %d records reached PC's "
                    + "pool.", dispatched);
        }
    }

    private void printComparison(final ArmResult stockResult, final ArmResult pcResult) {
        Latencies stock = stockResult.latencies();
        Latencies pc = pcResult.latencies();

        Console.line("");
        Console.line("  %-16s %14s %14s %22s", "fast record", "STOCK", "PC", "ratio (>1 = PC better)");
        Console.line("  %-16s %14s %14s %22s", "----------------", "--------------", "--------------",
                "----------------------");
        // Minimum first, because it is the statistic that states the claim: "a fast record does not have to
        // wait for the slow one" is falsified if even the luckiest one waited.
        printRow("min (fastest)", stock.min(), pc.min());
        printRow("p50 (median)", stock.p50(), pc.p50());
        printRow("p99 (slowest)", stock.p99(), pc.p99());
    }

    private static void printRow(final String label, final long stock, final long pc) {
        double ratio = stock / (double) Math.max(1, pc);
        Console.line("  %-16s %,12dms %,12dms %21.2fx", label, stock, pc, ratio);
    }
}
