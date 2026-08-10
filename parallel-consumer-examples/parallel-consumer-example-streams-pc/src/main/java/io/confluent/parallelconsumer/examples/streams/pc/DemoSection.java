package io.confluent.parallelconsumer.examples.streams.pc;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

/**
 * One thing the demo demonstrates, start to finish, including printing its own result.
 * <p>
 * <b>This is the extension seam.</b> The branches that build on this one each add a capability that wants
 * showing, and they should be able to add a section by writing one class and adding one line to the list in
 * {@link StreamsOnPcDemo}, without reshaping anything already here. That is why the contract is "run and
 * report" rather than "return a latency distribution": the next sections are not all comparisons. A branch
 * that refuses unsupported Kafka Streams constructs, for instance, demonstrates itself by provoking an
 * exception and printing the message, which has no arms and no percentiles.
 * <p>
 * Sections run sequentially and share one broker, and may assume the classpath has already been verified.
 *
 * @author Antony Stubbs
 * @see LatencyScenario the comparison-shaped implementation, used by both of this branch's sections
 */
interface DemoSection {

    /** Shown as the section heading, so it should say what is about to be demonstrated. */
    String title();

    void run(DemoBroker broker);

    /**
     * Restates this section's result in the closing summary, in a few lines that stand on their own.
     * <p>
     * A reader who scrolls to the bottom, or who scrolls back up through Kafka's startup logging looking
     * for the point, should find everything they need in one block rather than having to reassemble it
     * from the sections. Default is silence, for a section whose result is not a number.
     */
    default void printSummary() {
        // Nothing by default.
    }
}
