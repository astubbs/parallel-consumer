package io.confluent.parallelconsumer.examples.streams.pc;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import java.security.CodeSource;
import java.security.ProtectionDomain;

/**
 * Proves, before anything is measured, that this JVM is actually running the patched Kafka Streams.
 * <p>
 * <b>The failure mode this exists for is silence, not error.</b> {@code parallel-consumer-streams} ships
 * patched copies of a handful of Apache Kafka classes in Kafka's own package, inside its ordinary jar, and
 * they take effect only by winning classpath order over the stock {@code kafka-streams} jar. When they lose,
 * nothing throws. Kafka Streams runs exactly as it always did, the demo completes, and every number it prints
 * is a comparison of stock against stock that happens to look plausible. A reader cannot tell the difference
 * from the output, which is precisely why the check has to be in the output.
 * <p>
 * So this runs first and it is fatal. Degrading to a warning would preserve the failure this class exists to
 * remove.
 *
 * @author Antony Stubbs
 */
final class ClasspathGuard {

    /**
     * The one that matters, because it is where the dispatch decision is taken. Named separately as well as
     * listed below, so the classloader comparison further down does not silently depend on it staying at a
     * particular position in the array.
     */
    private static final String STREAM_TASK = "org.apache.kafka.streams.processor.internals.StreamTask";

    /**
     * Patched by this fork, and therefore expected to load from {@code parallel-consumer-streams}. A check of
     * {@link #STREAM_TASK} alone would pass while the rest of the patch set lost, so all four are named.
     */
    private static final String[] PATCHED_CLASSES = {
            STREAM_TASK,
            "org.apache.kafka.streams.processor.internals.ProcessorContextImpl",
            "org.apache.kafka.streams.processor.internals.AbstractProcessorContext",
            "org.apache.kafka.streams.processor.internals.RecordCollectorImpl",
    };

    /**
     * NOT patched, and therefore expected to keep loading from the stock jar. Without this the guard could
     * not tell "our four classes won" from "the whole of Kafka Streams got replaced", and the second would
     * mean something has gone very wrong with the build.
     */
    private static final String UNPATCHED_SIBLING = "org.apache.kafka.streams.processor.internals.StreamThread";

    private static final String PATCHED_ARTIFACT = "parallel-consumer-streams";

    private static final String STOCK_ARTIFACT = "kafka-streams";

    /**
     * A field {@code pc-streams.patch} adds to {@code StreamTask} and that exists in no Apache Kafka
     * release. Asking for it by name turns "the class came from the right jar" into "the class carries the
     * patch", which are different questions with the same happy answer and different failure modes.
     */
    private static final String DISPATCH_SEAM_FIELD = "pcDispatcher";

    private ClasspathGuard() {
    }

    /**
     * Verifies the patched classes are the ones loaded, printing the evidence either way.
     *
     * @throws IllegalStateException if the stock jar won, or the classes are split across loaders
     */
    static void verifyPatchedStreamsIsLoaded() {
        Console.section("Classpath check: is this really the patched Kafka Streams?");

        Class<?> streamTask = load(STREAM_TASK);
        Class<?> streamThread = load(UNPATCHED_SIBLING);

        for (String className : PATCHED_CLASSES) {
            Class<?> patched = load(className);
            String location = codeSourceOf(patched);
            Console.line("  patched   %-28s <- %s", simpleName(className), location);
            if (!locationIsPatched(location)) {
                failLoudly(simpleName(className), location);
            }
            // Checked for EVERY patched class, not just the one below. A split runtime package breaks
            // package-private access between the patched half and the stock half, and any of the four
            // could be the one that landed in the wrong loader.
            if (patched.getClassLoader() != streamThread.getClassLoader()) {
                throw new IllegalStateException(className + " and " + UNPATCHED_SIBLING + " loaded from "
                        + "DIFFERENT classloaders, which splits the runtime package despite the names "
                        + "matching, and breaks package-private access between them.");
            }
        }

        // Provenance is not the same question as content. A build where the patch step silently applied
        // nothing still ships classes from parallel-consumer-streams - they would just be pristine Kafka,
        // and every check above would pass while the seam this demo measures did not exist. So the seam
        // itself is asked for by name.
        verifyDispatchSeamIsPresent(streamTask);

        String siblingLocation = codeSourceOf(streamThread);
        Console.line("  unpatched %-28s <- %s", simpleName(UNPATCHED_SIBLING), siblingLocation);
        Console.line("            (this one SHOULD be unpatched - it is the control proving the check can "
                + "tell the two apart)");
        if (!siblingLocation.contains(STOCK_ARTIFACT)) {
            throw new IllegalStateException(UNPATCHED_SIBLING + " did not load from the stock kafka-streams "
                    + "jar but from " + siblingLocation + ". The patched classes are supposed to REPLACE "
                    + PATCHED_CLASSES.length + " classes and coexist with the rest of Kafka Streams, so this "
                    + "means the build is not what this demo assumes.");
        }

        Console.line("");
        Console.line("  Verdict: PATCHED. StreamTask carries the '%s' field the patch adds, so the dispatch",
                DISPATCH_SEAM_FIELD);
        Console.line("  seam is present in the loaded bytecode and can actually take effect.");
        Console.line("  All share one classloader, so the patched and stock halves are one runtime package.");
    }

    /**
     * The predicate that decides patched from stock, kept separate so it can be exercised without a JVM
     * whose classpath is deliberately broken.
     * <p>
     * Note the second clause. A location matching {@code parallel-consumer-streams} is not enough on its
     * own, because a path can contain both names.
     */
    static boolean locationIsPatched(final String location) {
        return location.contains(PATCHED_ARTIFACT) && !location.contains(STOCK_ARTIFACT);
    }

    /**
     * Asks the loaded {@code StreamTask} whether it actually carries the patch, rather than inferring it
     * from where the class came from.
     * <p>
     * {@code pcDispatcher} is added by {@code pc-streams.patch} and exists in no Apache Kafka release, so
     * its presence cannot be produced by anything except a successful patch application.
     */
    private static void verifyDispatchSeamIsPresent(final Class<?> streamTask) {
        try {
            streamTask.getDeclaredField(DISPATCH_SEAM_FIELD);
        } catch (NoSuchFieldException e) {
            Console.line("");
            Console.banner("THIS DEMO IS MEANINGLESS - THE PATCH DID NOT APPLY");
            Console.line("");
            Console.line("  StreamTask loaded from %s as expected, but it has no '%s' field, so it is",
                    PATCHED_ARTIFACT, DISPATCH_SEAM_FIELD);
            Console.line("  pristine Apache Kafka source that was compiled without the patch being applied.");
            Console.line("  Every arm below would be stock Kafka Streams measured against itself.");
            Console.line("");
            Console.line("  Most likely cause: a stale target/ from a build where the patch step failed.");
            Console.line("  Fix: rebuild with 'clean' so the patch is reapplied from src/main/patch.");
            Console.line("");
            throw new IllegalStateException("StreamTask came from " + PATCHED_ARTIFACT + " but carries no '"
                    + DISPATCH_SEAM_FIELD + "' field, so the patch did not apply and the dispatch seam is "
                    + "absent.", e);
        }
    }

    private static void failLoudly(final String className, final String location) {
        Console.line("");
        Console.banner("THIS DEMO IS MEANINGLESS - STOP READING THE NUMBERS");
        Console.line("");
        Console.line("  %s loaded from:", className);
        Console.line("    %s", location);
        Console.line("");
        Console.line("  That is the STOCK kafka-streams jar, not %s.", PATCHED_ARTIFACT);
        Console.line("");
        Console.line("  The patched classes lost the classpath race, so Parallel Consumer is NOT driving");
        Console.line("  anything. Both arms below would run identical stock Kafka Streams, and any");
        Console.line("  difference printed between them would be measurement noise dressed up as a result.");
        Console.line("");
        Console.line("  Most likely cause: this module's pom.xml declares org.apache.kafka:kafka-streams");
        Console.line("  directly, or declares it before parallel-consumer-streams. The patched classes win");
        Console.line("  by classpath ORDER alone, and Maven orders direct dependencies by declaration order.");
        Console.line("  Fix: declare parallel-consumer-streams FIRST and let kafka-streams arrive");
        Console.line("  transitively. Do not exclude it - the patch set is only %d classes and needs the",
                PATCHED_CLASSES.length);
        Console.line("  other thousand from the stock jar.");
        Console.line("");
        throw new IllegalStateException(className + " loaded from the stock kafka-streams jar (" + location
                + "), so the PC dispatch seam is absent and no number this demo could print would mean "
                + "anything.");
    }

    private static Class<?> load(final String className) {
        try {
            return Class.forName(className);
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException("Could not load " + className + ". Kafka Streams is not on the "
                    + "classpath at all, which means this module's dependencies are wrong.", e);
        }
    }

    /**
     * Where a class was actually loaded from. This is the only question that distinguishes a working demo
     * from a convincing one, so it is asked of the class object rather than inferred from the build.
     */
    private static String codeSourceOf(final Class<?> type) {
        ProtectionDomain protectionDomain = type.getProtectionDomain();
        if (protectionDomain == null) {
            return "<no protection domain>";
        }
        CodeSource codeSource = protectionDomain.getCodeSource();
        if (codeSource == null || codeSource.getLocation() == null) {
            return "<no code source>";
        }
        return codeSource.getLocation().toString();
    }

    private static String simpleName(final String className) {
        return className.substring(className.lastIndexOf('.') + 1);
    }
}
