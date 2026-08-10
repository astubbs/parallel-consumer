package io.confluent.parallelconsumer.streams;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/**
 * Proves the classpath-shadowing premise this whole module rests on: the classes we generate and compile win
 * over the ones in the {@code kafka-streams} jar, while their un-generated siblings still come from the jar.
 * <p>
 * This exists because the technique is <em>silent</em> when it fails. If the jar's copy were to win, every
 * behavioural test downstream would still pass - it would simply be testing stock Kafka Streams and proving
 * nothing at all. That is the single most likely way this module produces a false positive, so it is asserted
 * directly rather than inferred from behaviour.
 *
 * @see io.confluent.parallelconsumer.streams.integrationTests.ShadowedStreamsControlTest
 */
@Slf4j
class ShadowedClassLoadingTest {

    /**
     * The classes listed in {@code patched.classes} in this module's pom. Keep in sync: a class that is
     * generated but missing here is unguarded, and one listed here but not generated fails loudly below.
     * <p>
     * Named as strings rather than as class literals because two of them - {@code KGroupedStreamImpl} and
     * {@code CogroupedKStreamImpl} - are package-private and cannot be referenced from here at all. Strings
     * also make this list a direct transcription of the pom property it has to match, which is the thing that
     * actually has to stay in step.
     */
    private static final String[] GENERATED = {
            "org.apache.kafka.streams.processor.internals.StreamTask",
            "org.apache.kafka.streams.processor.internals.AbstractProcessorContext",
            "org.apache.kafka.streams.processor.internals.ProcessorContextImpl",
            "org.apache.kafka.streams.processor.internals.RecordCollectorImpl",
            // Wake-on-work (astubbs#255): StreamThread owns the poll wait, so splitting that wait has to
            // happen here. It is also why the jar-resident sibling for this package below is TaskManager.
            "org.apache.kafka.streams.processor.internals.StreamThread",
            // The unsupported-surface refusal (astubbs#255). The interfaces carry @DoNotCall/@Deprecated and
            // the impls carry the runtime throw - and these sit in two packages of their own, so the
            // same-runtime-package assertion below is doing new work rather than repeating itself.
            "org.apache.kafka.streams.kstream.KStream",
            "org.apache.kafka.streams.kstream.KTable",
            "org.apache.kafka.streams.kstream.KGroupedStream",
            "org.apache.kafka.streams.kstream.CogroupedKStream",
            "org.apache.kafka.streams.kstream.internals.KStreamImpl",
            "org.apache.kafka.streams.kstream.internals.KTableImpl",
            "org.apache.kafka.streams.kstream.internals.KGroupedStreamImpl",
            "org.apache.kafka.streams.kstream.internals.CogroupedKStreamImpl",
    };

    /**
     * Classes we deliberately do <em>not</em> generate. They must still load from the jar - that is what makes
     * this "shadowing" rather than "a fork": the two sets have to coexist in one runtime package.
     * <p>
     * One per package we generate into, because "same runtime package" is a per-package property and the
     * generated set spans three packages. Pairing each generated class with a jar-resident class in
     * <em>its own</em> package is what keeps that assertion a real coexistence check.
     * <p>
     * The {@code processor.internals} entry was {@code StreamThread} until wake-on-work (astubbs#255) had to
     * patch the poll wait, which is {@code StreamThread}'s to own. {@code TaskManager} replaces it as the
     * honest control: public, in the same package, and already reached into by the patch
     * ({@code TaskManager.executeAndMaybeSwallow}, from the patched {@code StreamTask.close}) without ever
     * being generated - so it proves the two sets really do coexist rather than merely sitting side by side
     * unused.
     */
    private static final String[] JAR_RESIDENT_SIBLINGS = {
            "org.apache.kafka.streams.processor.internals.TaskManager",
            "org.apache.kafka.streams.kstream.Materialized",
            "org.apache.kafka.streams.kstream.internals.ConsumedInternal",
    };

    @Test
    void generatedClassesWinOverTheJar() {
        for (String name : GENERATED) {
            Class<?> generated = load(name);
            URL location = codeSourceOf(generated);
            log.info("{} loaded from {}", generated.getSimpleName(), location);

            assertThat(location.toString())
                    .as("%s must load from this module's compiled output, not the kafka-streams jar. "
                                    + "If this is a jar URL, classpath ordering has put the jar first and the whole "
                                    + "this module is measuring stock Kafka Streams - every downstream result would be a "
                                    + "false positive.",
                            generated.getName())
                    .doesNotContain(".jar")
                    .contains("/classes/");
        }
    }

    @Test
    void unGeneratedSiblingsStillComeFromTheJar() {
        for (String name : JAR_RESIDENT_SIBLINGS) {
            Class<?> jarResident = load(name);
            URL location = codeSourceOf(jarResident);
            log.info("{} loaded from {}", jarResident.getSimpleName(), location);

            assertThat(location.toString())
                    .as("%s is not in patched.classes, so it must still come from the jar. If it does not, "
                                    + "something is generating more than the declared set.",
                            jarResident.getName())
                    .contains("kafka-streams")
                    .endsWith(".jar");
        }
    }

    /**
     * Package-private access into Kafka internals only works if the generated classes and the jar's classes land
     * in the same runtime package - same package name <em>and</em> same classloader. Different classloaders would
     * split the package and break that access even though the names match.
     */
    @Test
    void generatedAndJarClassesShareOneRuntimePackage() {
        for (String name : GENERATED) {
            Class<?> generated = load(name);

            // Asserted against the declared set rather than against the sibling looked up below: the lookup
            // selects its sibling BY package name, so comparing the two afterwards could never fail.
            assertThat(generated.getPackage().getName())
                    .as("%s is generated into a package this test does not know about. Every generated package "
                                    + "needs a declared jar-resident sibling, or its coexistence is unproven.",
                            generated.getName())
                    .isIn(generatedPackages());

            Class<?> jarSibling = jarResidentSiblingOf(generated);
            assertThat(generated.getClassLoader())
                    .as("%s must share a classloader with %s, or they are in different runtime packages and "
                                    + "package-private access fails despite the matching package name",
                            generated.getName(), jarSibling.getName())
                    .isSameAs(jarSibling.getClassLoader());
        }
    }

    /**
     * The list here has to match {@code patched.classes} in this module's pom, and a comment saying so is not a
     * check. Surefire passes the property through so this can be one.
     */
    @Test
    void theGeneratedListMatchesThePomProperty() {
        String property = System.getProperty("patched.classes");
        assertThat(property)
                .as("patched.classes was not passed through by surefire, so this test cannot tell whether the "
                        + "list above is still complete - fix the pom rather than deleting this test")
                .isNotBlank();

        List<String> fromPom = new ArrayList<>();
        for (String path : property.split(",")) {
            fromPom.add(path.trim().replace(".java", "").replace('/', '.'));
        }

        assertThat(Arrays.asList(GENERATED))
                .as("GENERATED and patched.classes have drifted apart. A class in the pom but not here is "
                        + "generated and unproven; one here but not in the pom is loaded from the jar and every "
                        + "assertion about it is a lie.")
                .containsExactlyInAnyOrderElementsOf(fromPom);
    }

    private static List<String> generatedPackages() {
        List<String> packages = new ArrayList<>();
        for (String name : JAR_RESIDENT_SIBLINGS) {
            packages.add(load(name).getPackage().getName());
        }
        return packages;
    }

    /**
     * The jar-resident class that has to coexist with this generated one, i.e. the one in its own package.
     * Fails rather than falls back: a generated class in a package with no declared jar sibling would
     * otherwise be checked against nothing, and the assertion would pass while proving nothing.
     */
    private static Class<?> jarResidentSiblingOf(Class<?> generated) {
        for (String name : JAR_RESIDENT_SIBLINGS) {
            Class<?> candidate = load(name);
            if (candidate.getPackage().getName().equals(generated.getPackage().getName())) {
                return candidate;
            }
        }
        throw new AssertionError("No jar-resident sibling declared for package " + generated.getPackage().getName()
                + " (needed by " + generated.getName() + "). Add one to JAR_RESIDENT_SIBLINGS - without it the "
                + "same-runtime-package assertion for that package is vacuous.");
    }

    /**
     * @throws AssertionError naming the class, rather than a bare {@code ClassNotFoundException}: a name in
     *         {@link #GENERATED} that no longer resolves means {@code patched.classes} and this list have
     *         drifted apart, which is exactly what this class exists to catch.
     */
    private static Class<?> load(String name) {
        try {
            return Class.forName(name);
        } catch (ClassNotFoundException e) {
            return fail("%s is listed here but is not on the classpath - patched.classes and this list have "
                    + "drifted apart", name);
        }
    }

    private static URL codeSourceOf(Class<?> type) {
        var protectionDomain = type.getProtectionDomain();
        assertThat(protectionDomain).as("no protection domain for %s", type.getName()).isNotNull();
        var codeSource = protectionDomain.getCodeSource();
        assertThat(codeSource).as("no code source for %s - cannot tell where it was loaded from", type.getName()).isNotNull();
        return codeSource.getLocation();
    }
}
