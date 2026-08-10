package io.confluent.parallelconsumer.examples.streams.pc;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Covers the one predicate the whole demo rests on.
 * <p>
 * {@link ClasspathGuard} decides whether every number the demo prints means anything, and in a passing run
 * it only ever exercises its own success path. A typo that made {@link ClasspathGuard#locationIsPatched}
 * always return true would therefore be invisible: the guard would print its reassuring verdict, the demo
 * would compare stock Kafka Streams against itself, and nothing anywhere would object. These cases pin the
 * predicate in the direction a live run cannot reach.
 * <p>
 * Deliberately free of Docker and of Kafka. This runs in the ordinary build, unlike the demo itself, which
 * is gated behind {@code -Pdemo}.
 *
 * @author Antony Stubbs
 */
class ClasspathGuardTest {

    /** A real code source from a passing run: the reactor's own build output for the patched module. */
    private static final String PATCHED_REACTOR_OUTPUT =
            "file:/home/dev/parallel-consumer/parallel-consumer-streams/target/classes/";

    /** A real code source from a passing run once the module is consumed as a published artifact. */
    private static final String PATCHED_JAR =
            "file:/home/dev/.m2/repository/bz/stub/parallelconsumer/parallel-consumer-streams/"
                    + "0.6.0.0-SNAPSHOT/parallel-consumer-streams-0.6.0.0-SNAPSHOT.jar";

    /** The failure this guard exists for: the stock jar won the classpath race. */
    private static final String STOCK_JAR =
            "file:/home/dev/.m2/repository/org/apache/kafka/kafka-streams/3.9.2/kafka-streams-3.9.2.jar";

    @Test
    void acceptsThePatchedModulesOwnBuildOutput() {
        assertThat(ClasspathGuard.locationIsPatched(PATCHED_REACTOR_OUTPUT)).isTrue();
    }

    @Test
    void acceptsThePatchedModulePackagedAsAJar() {
        assertThat(ClasspathGuard.locationIsPatched(PATCHED_JAR)).isTrue();
    }

    @Test
    void rejectsTheStockKafkaStreamsJar() {
        assertThat(ClasspathGuard.locationIsPatched(STOCK_JAR)).isFalse();
    }

    /**
     * The case a single {@code contains("parallel-consumer-streams")} check would wave through.
     * <p>
     * Maven's local repository often sits under a checkout, so the stock jar's path can legitimately carry
     * the patched module's name in a parent directory while still being the stock jar. The predicate has to
     * reject on the stock name being present, not merely accept on the patched name being present.
     */
    @Test
    void rejectsTheStockJarEvenWhenThePathAlsoNamesThePatchedModule() {
        String stockJarUnderAPatchedNamedDirectory =
                "file:/home/dev/parallel-consumer-streams/.m2/repository/org/apache/kafka/kafka-streams/"
                        + "3.9.2/kafka-streams-3.9.2.jar";

        assertThat(ClasspathGuard.locationIsPatched(stockJarUnderAPatchedNamedDirectory)).isFalse();
    }

    /**
     * A class with no code source reports a placeholder rather than throwing, and a placeholder must not be
     * mistaken for evidence of a patched classpath.
     */
    @Test
    void rejectsAPlaceholderLocation() {
        assertThat(ClasspathGuard.locationIsPatched("<no code source>")).isFalse();
        assertThat(ClasspathGuard.locationIsPatched("<no protection domain>")).isFalse();
    }
}
