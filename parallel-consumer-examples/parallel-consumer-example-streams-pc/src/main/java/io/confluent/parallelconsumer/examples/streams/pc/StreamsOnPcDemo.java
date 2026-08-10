package io.confluent.parallelconsumer.examples.streams.pc;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import io.confluent.parallelconsumer.streams.PcDispatchSwitch;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Runnable demonstration that Parallel Consumer can drive a Kafka Streams topology, and that doing so
 * removes head-of-line blocking within a partition.
 * <p>
 * Run it with:
 * <pre>
 * ./mvnw -Pdemo -pl parallel-consumer-examples/parallel-consumer-example-streams-pc -am \
 *        -DskipTests -Dcopyright.skip=true process-classes
 * </pre>
 * It starts its own Kafka broker in Docker, so nothing needs to be running first.
 * <p>
 * <b>What this is for.</b> The same effect is already asserted by
 * {@code HeadOfLineBlockingBenchmarkTest} in {@code parallel-consumer-streams}, and a test is the right
 * place to defend it against regression. A test is a poor place to be convinced by it: it reports pass or
 * fail to a machine, and a reader who did not run it is being asked to take the claim on trust. This prints
 * the evidence instead - which classes are actually loaded, which records actually went through Parallel
 * Consumer, and what the latencies actually were, in both arms, in one run.
 * <p>
 * <b>It also prints the result that does not flatter us</b>, for the same reason. See
 * {@link #singleKeyControl()}.
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

        List<DemoSection> sections = new ArrayList<>(Arrays.asList(
                headOfLineBlocking(),
                singleKeyControl()));

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
        printSummary(sections, broker, System.nanoTime() - startedAt);
    }

    /**
     * The last thing printed, and deliberately self-contained.
     * <p>
     * A reader who ran this in a terminal has a few hundred lines above them and no reason to know which
     * ones mattered. Everything needed to state what happened is repeated here, so the bottom of the
     * output is a complete answer on its own.
     */
    private static void printSummary(final List<DemoSection> sections,
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
        Console.line("  Evidence this was really Parallel Consumer driving Kafka Streams:");
        Console.line("    - the patched StreamTask loaded from parallel-consumer-streams, not the "
                + "kafka-streams jar");
        Console.line("    - %d of %d records went through PC's worker pool in each PC arm, and 0 in each "
                + "stock arm", ArmRunner.TOTAL_RECORDS, ArmRunner.TOTAL_RECORDS);

        for (DemoSection section : sections) {
            section.printSummary();
        }

        Console.line("");
        Console.wrapped("  Caveats : ", "the comparison is within ONE partition, and the workload is "
                + "blocking IO. This is not a claim that PC is faster than Kafka Streams - it is a claim "
                + "that a partition is no longer a serialisation point.");
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
    private static DemoSection headOfLineBlocking() {
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
                false);
    }

    /**
     * The negative control, and the arm this demo would be dishonest without.
     * <p>
     * PC's key ordering permits at most one in-flight record per key, so with every record on a single key
     * the seam has no concurrency to exploit and must confer no advantage. Two things follow from running
     * it. If PC still won here, the gain measured above would be coming from something other than key
     * concurrency - a warm-up artefact, a faster path, a measurement error - and both results would be void.
     * And because PC does not win here, the reader gets to see what it costs when it cannot help.
     */
    private static DemoSection singleKeyControl() {
        return new LatencyScenario(
                // The expectation goes in the HEADING, not after the table. A reader who meets these numbers
                // cold reads them as a regression, and by the time the explanation arrives they have already
                // formed the wrong impression.
                "Negative control - PC is EXPECTED TO LOSE here (same workload, every record on ONE key)",
                "NEGATIVE CONTROL (every record on ONE key, so PC has no concurrency to exploit)",
                "onekey",
                "PC dispatch should confer NO advantage here, because one key means one in-flight record",
                "this is the arm that licenses reading the first result as key concurrency rather than as a "
                        + "generally faster path. Expect PC to be no better, and to be somewhat WORSE - the "
                        + "next section explains why.",
                "with no key concurrency available, PC is slightly SLOWER. That is expected, and it is a "
                        + "real current limitation rather than noise.",
                true);
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
    }

    /**
     * The one thing a reader could otherwise get wrong on their own: why the control arm loses, and why
     * that is not being tuned away. The rest of the framing is in the summary below and the README.
     */
    private static void printClosing() {
        Console.section("Why the control arm shows PC behind");
        Console.line("");
        Console.wrapped("  ", "Kafka Streams couples polling and processing on one thread. Blocking for up "
                + "to poll.ms costs stock dispatch nothing, because the same thread would have been "
                + "processing anyway. Under the seam the workers finish in the background, but their "
                + "completions cannot be drained until the poll returns, so that wait becomes dispatch "
                + "latency charged per record. Most of the single-key penalty is that poll wait.");
        Console.line("");
        Console.wrapped("  ", "This demo deliberately does NOT tune poll.ms. A lower value would narrow the "
                + "penalty and widen the headline gap, but it is a mitigation rather than a fix, and these "
                + "numbers are what default configuration gives you today. The fix is to end the poll wait "
                + "when a worker completes, which is a later change and is not claimed here.");
        Console.line("");
        Console.wrapped("  ", "Nothing here is claimed about multiple partitions, multiple stream threads, "
                + "rebalancing, exactly-once, windowing, joins or state stores. This module is alpha; the "
                + "tested envelope is one partition, one stream thread, one task. See astubbs#255.");
    }
}
