package io.confluent.parallelconsumer.examples.streams.pc;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

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

    private final String title;

    private final String armPrefix;

    private final String claim;

    private final String reading;

    private final boolean allOneKey;

    private final String summaryTitle;

    private final String verdict;

    /** Held so the closing summary can restate the result without re-running anything. */
    private ArmResult stockResult;

    private ArmResult pcResult;

    /**
     * Six of the seven parameters are prose strings and Java has no named arguments, so every one of them is
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
     */
    LatencyScenario(final String title,
                    final String summaryTitle,
                    final String armPrefix,
                    final String claim,
                    final String reading,
                    final String verdict,
                    final boolean allOneKey) {
        this.title = title;
        this.summaryTitle = summaryTitle;
        this.armPrefix = armPrefix;
        this.claim = claim;
        this.reading = reading;
        this.verdict = verdict;
        this.allOneKey = allOneKey;
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
        Console.line("  Not controlled: ORDER. Stock runs first, so PC meets a warmer JVM. The control "
                + "section bounds that.");

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

        // The verdict below is fixed prose describing the expected direction. Printing it over a result
        // that went the other way would be the demo telling the reader what it wanted to find, so the
        // measurement is checked against the expectation first.
        boolean pcWonOnMedian = pc.p50() < stock.p50();
        if (pcWonOnMedian == allOneKey) {
            Console.wrapped("    !! ", "UNEXPECTED: this section's median went the other way. The fixed "
                    + "reading below describes the direction this arm was predicted to take, and this run "
                    + "did not take it. Trust the numbers above, not the sentence below.");
        }
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
        } else if (dispatched == 0) {
            Console.line("    ^ zero records through PC's pool, which is what a genuine stock arm looks "
                    + "like.");
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
