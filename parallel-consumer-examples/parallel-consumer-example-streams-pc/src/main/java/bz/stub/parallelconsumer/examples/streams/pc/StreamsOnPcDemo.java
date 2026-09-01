package bz.stub.parallelconsumer.examples.streams.pc;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.streams.PcDispatchSwitch;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.OptionalLong;

/**
 * Runnable demonstration that Parallel Consumer can drive a Kafka Streams topology, and that doing so
 * removes head-of-line blocking within a partition.
 * <p>
 * Run it with:
 * <pre>
 * ./mvnw -Pdemo -pl parallel-consumer-examples/parallel-consumer-example-streams-pc -am \
 *        -DskipTests process-classes
 * </pre>
 * It starts its own Kafka broker in Docker, so nothing needs to be running first.
 * <p>
 * <b>What this is for.</b> {@code PcDrivenStreamsDispatchTest} in {@code parallel-consumer-streams} already
 * asserts that dispatch happens and that the output is unchanged, and a test is the right place to defend
 * that against regression. A test is a poor place to be convinced by it: it reports pass or fail to a
 * machine, and a reader who did not run it is being asked to take the claim on trust. This prints the
 * evidence instead - which classes are actually loaded, which records actually went through Parallel
 * Consumer, and what the latencies actually were, in both arms, in one run.
 * <p>
 * <b>The dispatch switch is turned on EXPLICITLY here, in both directions.</b> It defaults off, so this
 * demo is exactly the opt-in it describes; and stating it in the stock arm too means neither arm inherits
 * the default, which is documented as moving once unsupported topology shapes are refused.
 * <p>
 * <b>It also prints the arms that do not flatter us</b>, for the same reason: a negative control that must
 * show no advantage ({@link #singleKeyControl()}), and a noise floor measured inside the same run
 * ({@link #printNoiseFloor}).
 *
 * @author Antony Stubbs
 * @see ClasspathGuard which makes the silent failure mode impossible
 * @see DemoSection the seam a later branch extends
 */
public final class StreamsOnPcDemo {

    private StreamsOnPcDemo() {
    }

    public static void main(final String[] args) {
        long startedAt = System.nanoTime();
        printIntroduction();

        // FIRST, and fatal. If the patched classes lost the classpath race then Parallel Consumer is not
        // driving anything, both arms below are stock Kafka Streams, and every number printed after this
        // point would be noise presented as a result. Nothing throws in that situation, which is exactly
        // why it has to be checked rather than assumed.
        ClasspathGuard.verifyPatchedStreamsIsLoaded();

        // Held as the concrete type as well as in the list, because the noise floor below is derived from
        // these two sections' stock arms and that is not something the DemoSection contract offers.
        LatencyScenario headOfLine = headOfLineBlocking();
        LatencyScenario singleKey = singleKeyControl();
        List<DemoSection> sections = new ArrayList<>(Arrays.asList(headOfLine, singleKey));

        Console.section("Starting a broker");
        DemoBroker broker = DemoBroker.start();
        try {
            int number = 0;
            for (DemoSection section : sections) {
                number++;
                Console.section("Demonstration " + number + " of " + sections.size() + ": " + section.title());
                section.run(broker);
            }
        } finally {
            // Process-wide static state: leaving it wherever the last arm put it would silently change what
            // anything else in this JVM measured.
            PcDispatchSwitch.resetToDefault();
            broker.close();
        }

        printClosing();
        printSummary(sections, headOfLine, singleKey, broker, System.nanoTime() - startedAt);
    }

    /**
     * How much this run's numbers move for no reason at all, measured inside the run itself.
     * <p>
     * <b>The two STOCK arms do byte-identical work.</b> Stock Kafka Streams hands a partition's records over
     * one at a time whatever key they carry, and the per-record cost here is chosen by value rather than by
     * key, so the head-of-line section's stock arm and the control's stock arm process the same 25 records
     * the same way. Any difference between them is variance: JIT state, page cache, whatever else the
     * machine is doing, and arm position within the run.
     * <p>
     * That makes it the yardstick for every small difference printed above, and the demo has already needed
     * it. One run read 1.19x on the negative control - which reads as PC winning an arm it should not - and
     * the two stock arms in that same run differed by 1.20x. The control had measured nothing; the run was
     * noisy, and only a floor derived from the same run could say so.
     */
    private static void printNoiseFloor(final LatencyScenario headOfLine, final LatencyScenario singleKey) {
        OptionalLong firstMedian = headOfLine.stockMedianMillis();
        OptionalLong secondMedian = singleKey.stockMedianMillis();
        if (!firstMedian.isPresent() || !secondMedian.isPresent()) {
            return;
        }
        long first = firstMedian.getAsLong();
        long second = secondMedian.getAsLong();
        double ratio = Math.max(first, second) / (double) Math.max(1, Math.min(first, second));

        Console.line("");
        Console.line("  NOISE FLOOR (the two STOCK arms, which do identical work - stock ignores keys)");
        Console.line("    %-20s %,9dms       ->  %,9dms      %6.2fx", "median  fast record", first, second,
                ratio);
        Console.wrapped("    => ", String.format(Locale.ROOT, "this run's numbers move by %.2fx for no "
                + "reason at all. Read every ratio above against it: a difference smaller than this is "
                + "the machine, not the seam.", ratio));
    }

    /**
     * The last thing printed, and deliberately self-contained.
     * <p>
     * A reader who ran this in a terminal has a few hundred lines above them and no reason to know which
     * ones mattered. Everything needed to state what happened is repeated here, so the bottom of the
     * output is a complete answer on its own.
     */
    private static void printSummary(final Iterable<DemoSection> sections,
                                     final LatencyScenario headOfLine,
                                     final LatencyScenario singleKey,
                                     final DemoBroker broker,
                                     final long elapsedNanos) {
        Console.section("SUMMARY");
        Console.line("");
        // Repeated here, not just referenced. Without the workload a reader cannot tell that the stock
        // "fastest" figure is essentially the blocker's own cost, and so cannot sanity-check the ratio.
        Console.line("  Workload: 1 record costing %,dms then %d costing %,dms, ONE partition, one stream "
                        + "thread, PC pool of %d.",
                ArmRunner.SLOW_COST.toMillis(), ArmRunner.FAST_RECORDS, ArmRunner.FAST_COST.toMillis(),
                ArmRunner.POOL_SIZE);
        Console.line("  Latencies below share one t=0, the instant the first record entered the topology.");
        Console.line("");

        // Asked of the sections rather than restated from the constants they were run with. The counter
        // claim is the demo's core evidence, so the summary must not be able to print it over a run where
        // an arm above already warned that the counters disagreed.
        boolean evidenceHolds = true;
        for (DemoSection section : sections) {
            evidenceHolds &= section.dispatchEvidenceHolds();
        }

        if (evidenceHolds) {
            Console.line("  Evidence this was really Parallel Consumer driving Kafka Streams:");
            Console.line("    - the patched StreamTask loaded from parallel-consumer-streams, not the "
                    + "kafka-streams jar, and it carries the dispatch seam");
            Console.line("    - %d of %d records went through PC's worker pool in each PC arm, and 0 in "
                    + "each stock arm", ArmRunner.TOTAL_RECORDS, ArmRunner.TOTAL_RECORDS);
        } else {
            Console.banner("EVIDENCE FAILED - DO NOT QUOTE THESE NUMBERS");
            Console.wrapped("  ", "At least one arm above printed a counter WARNING, so this run did NOT "
                    + "read the dispatch counters the way it needs to. The latencies below are still what "
                    + "was measured, but nothing here establishes which code path produced them. Read the "
                    + "per-arm counter lines above before drawing any conclusion.");
        }

        for (DemoSection section : sections) {
            section.printSummary();
        }
        printNoiseFloor(headOfLine, singleKey);

        Console.line("");
        Console.wrapped("  Caveats : ", "the comparison is within ONE partition, and the workload is "
                + "blocking IO, which is the case PC is for - CPU-bound work would not behave this way. "
                + "This is not a claim that PC is faster than Kafka Streams - it is a claim that a "
                + "partition is no longer a serialisation point.");
        Console.line("");
        Console.wrapped("            ", "Quote the MEDIAN, not the fastest. The fastest row approaches the "
                + "workload's own cost ratio by construction, so it demonstrates that the blocking is gone "
                + "without measuring how much is typically saved. The median is the number that answers "
                + "that.");
        Console.line("");
        Console.line("  Run time %,.0fs%s. Details and limitations: this module's README.md.",
                elapsedNanos / 1_000_000_000.0,
                broker.wasReused() ? " (broker reused)" : " (including broker startup)");
        Console.line("");
    }

    /**
     * The claim. Fast records on their own keys, sitting behind one slow record in the same partition.
     */
    private static LatencyScenario headOfLineBlocking() {
        return new LatencyScenario(
                "Head-of-line blocking, with and without PC dispatch",
                "HEAD-OF-LINE BLOCKING (fast records on their own keys)",
                "hol",
                "under stock dispatch every fast record waits for the slow one; under PC dispatch it does not",
                "stock Kafka Streams hands a partition over one record at a time, in PartitionGroup"
                        + ".nextRecord(), so a cheap record queued behind an expensive one waits for it even "
                        + "though they share nothing but a partition. PC dispatch gives each key its own "
                        + "in-flight slot, so the fast records go straight through.",
                "a fast record no longer waits for an unrelated slow one in the same partition.",
                false,
                LatencyScenario.Expectation.PC_MUCH_FASTER);
    }

    /**
     * The negative control, and the arm this demo would be dishonest without.
     * <p>
     * PC's key ordering permits at most one in-flight record per key, so with every record on a single key
     * the seam has no concurrency to exploit and must confer no advantage. What that buys is the licence to
     * read the headline as key concurrency: if PC won here too, the gain above could as easily be a
     * generally faster path, a warm-up artefact or a measurement error, and both results would be void.
     * <p>
     * <b>The prediction is parity, and on the branch this demo came from it was a loss.</b> There, PC
     * finished this arm around 0.7x - a real cost, because Kafka Streams polls and processes on one thread,
     * so a blocked poll stalled dispatch that stock had nothing to lose by blocking. The split poll wait
     * shipped with the seam removes that, and the arm now lands on the line. Measured across four
     * consecutive runs here at 1.19x, 0.99x, 0.98x and 0.99x - and the 1.19x run's two stock arms, which do
     * identical work, differed by 1.20x, so that reading was the machine rather than the seam. The demo
     * prints that floor with the verdict for exactly this reason.
     */
    private static LatencyScenario singleKeyControl() {
        return new LatencyScenario(
                // The expectation goes in the HEADING, not after the table. A reader who meets these numbers
                // cold reads them as a result, and by the time the explanation arrives they have already
                // formed an impression.
                "Negative control - PC should show NO ADVANTAGE here (same workload, every record on ONE key)",
                "NEGATIVE CONTROL (every record on ONE key, so PC has no concurrency to exploit)",
                "onekey",
                "PC dispatch should confer NO advantage here, because one key means one in-flight record",
                "this is the arm that licenses reading the first result as key concurrency rather than as a "
                        + "generally faster path. Expect the two arms to land together, on either side of "
                        + "parity - which one wins a tie is decided by the machine, so compare the gap with "
                        + "the noise floor in the summary rather than with zero.",
                "with no key concurrency available, PC neither gains nor loses. The headline above is "
                        + "therefore key concurrency, not a generally faster path.",
                true,
                LatencyScenario.Expectation.NO_MATERIAL_DIFFERENCE);
    }

    private static void printIntroduction() {
        Console.section("Kafka Streams, driven by Parallel Consumer");
        Console.line("");
        Console.line("  Stock Kafka Streams parallelises across PARTITIONS. Within one partition it hands");
        Console.line("  records to the topology strictly one at a time, so one expensive record delays every");
        Console.line("  record queued behind it - whatever key they carry, and however cheap they are. Where");
        Console.line("  per-record cost is blocking IO, that serialisation buys nothing: records on different");
        Console.line("  keys are independent.");
        Console.line("");
        Console.line("  This module replaces that hand-off with Parallel Consumer's work manager, so records");
        Console.line("  on different keys in the SAME partition are processed concurrently, while per-key");
        Console.line("  ordering and offset-commit correctness are preserved.");
        Console.line("");
        Console.line("  The topology in this demo is ordinary Kafka Streams code with no Parallel Consumer");
        Console.line("  API in it. Taking the dependency is the entire integration.");
        Console.line("");
        Console.line("  The seam is OFF by default and this demo turns it on explicitly, per arm. That is");
        Console.line("  the honest default while unsupported topology shapes - joins, windows, punctuation,");
        Console.line("  exactly-once - are still dispatched rather than refused. This demo's topology is a");
        Console.line("  stateless map, which is inside what the seam supports.");
    }

    /**
     * The two things a reader could otherwise get wrong on their own: why the control arm reaches parity
     * rather than losing, and what this run does not entitle anyone to conclude. The rest of the framing is
     * in the summary below and the README.
     */
    private static void printClosing() {
        Console.section("Why the control arm reaches parity, and what is NOT being claimed");
        Console.line("");
        Console.wrapped("  ", "Kafka Streams polls and processes on ONE thread. Blocking in poll() for the "
                + "full poll.ms costs stock dispatch nothing - there is by definition no processing that "
                + "thread could be doing instead. Hand records to a worker pool and that inverts: workers "
                + "complete during the poll wait, and neither those completions nor the records they "
                + "unblock can move until poll returns. The seam alone would therefore charge every record "
                + "that wait, and on the branch this demo came from it did - the single-key arm finished "
                + "around 0.7x.");
        Console.line("");
        Console.wrapped("  ", "This build ships the split poll wait with the seam: a short poll, then a wait "
                + "on our own condition that a worker completion ends. The splitPollWaits and wakesOnWork "
                + "counters printed against each PC arm are that mechanism reporting itself, and the "
                + "control arm landing on parity rather than behind is what it bought. poll.ms is left at "
                + "its default deliberately - tuning it down would narrow the same gap by mitigation "
                + "instead, and these numbers are what default configuration gives you.");
        Console.line("");
        Console.wrapped("  ", "Nothing here is claimed about multiple partitions, multiple stream threads, "
                + "rebalancing, exactly-once, windowing, joins, punctuation or state stores. This build "
                + "does not REFUSE those shapes either - it would dispatch one and quietly give you wrong "
                + "behaviour, which is why the switch this demo turns on defaults to off. The topology "
                + "below is a stateless map, and the tested envelope is one partition, one stream thread, "
                + "one task. See astubbs#255.");
    }
}
