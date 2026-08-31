package bz.stub.parallelconsumer.streams;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Proves the classpath-shadowing premise this whole module rests on: the classes we generate and compile win
 * over the ones in the {@code kafka-streams} jar, while their un-generated siblings still come from the jar.
 * <p>
 * This exists because the technique is <em>silent</em> when it fails. If the jar's copy were to win, every
 * behavioural test downstream would still pass - it would simply be testing stock Kafka Streams and proving
 * nothing at all. That is the single most likely way this module produces a false positive, so it is asserted
 * directly rather than inferred from behaviour.
 */
@Slf4j
class ShadowedClassLoadingTest {

    /**
     * The classes listed in {@code patched.classes} in this module's pom, asserted to be exactly that list by
     * {@link #theGeneratedListMatchesThePomProperty()} rather than by a comment asking for care.
     * <p>
     * Named as strings rather than as class literals because two of them - {@code KGroupedStreamImpl} and
     * {@code CogroupedKStreamImpl} - are package-private and cannot be referenced from here at all. Strings
     * also make this a direct transcription of the pom property it has to match, which is the thing that
     * actually has to stay in step.
     */
    private static final String[] GENERATED = {
            "org.apache.kafka.streams.processor.internals.AbstractProcessorContext",
            "org.apache.kafka.streams.processor.internals.ProcessorContextImpl",
            "org.apache.kafka.streams.processor.internals.RecordCollectorImpl",
            // Moved up from JAR_RESIDENT_NAMES by the execution seam. The rung below that one (astubbs#379)
            // put them in the jar-resident set precisely so that this assertion would have to flip, visibly,
            // on the day the generated set grew. StreamTask is the seam; StreamThread is the poll wait
            // wake-on-work splits.
            "org.apache.kafka.streams.processor.internals.StreamTask",
            "org.apache.kafka.streams.processor.internals.StreamThread",
            // The refusal envelope (astubbs#255). The interfaces carry @DoNotCall/@Deprecated and the impls
            // carry the runtime throw - and these sit in two packages of their own, so the
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
     * Chosen for adjacency, not convenience: {@code PartitionGroup} is the buffer the seam bypasses,
     * {@code RecordQueue} is reached into by the seam's own record preparation, and {@code TaskManager}
     * constructs {@code StreamTask} on the StreamThread. If generation were ever to sprawl past the declared
     * set, these are the first three it would reach.
     * <p>
     * <b>At least one per package we generate into</b>, because "same runtime package" is a per-package
     * property and the refusal envelope took the generated set into two more packages. {@code Materialized}
     * and {@code ConsumedInternal} are the {@code kstream} and {@code kstream.internals} entries: both are
     * used by the classes we patch there without ever being generated themselves, which is what makes the
     * coexistence claim about those packages a real one rather than a vacuous one.
     * <p>
     * Named as strings rather than imported because two of the sharpest neighbours are package-private, and
     * a check that can only name public classes cannot pick its own subjects.
     */
    private static final String[] JAR_RESIDENT_NAMES = {
            "org.apache.kafka.streams.processor.internals.PartitionGroup",
            "org.apache.kafka.streams.processor.internals.RecordQueue",
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
                                    + "If this is a jar URL, classpath ordering has put the jar first and this module "
                                    + "is measuring stock Kafka Streams - every result downstream of it, including the "
                                    + "upstream-suite oracle, would be a false positive.",
                            generated.getName())
                    .doesNotContain(".jar")
                    .contains("/classes/");
        }
    }

    @Test
    void unGeneratedSiblingsStillComeFromTheJar() {
        for (String name : JAR_RESIDENT_NAMES) {
            Class<?> resident = load(name);
            URL location = codeSourceOf(resident);
            log.info("{} loaded from {}", resident.getSimpleName(), location);

            assertThat(location.toString())
                    .as("%s is not in patched.classes, so it must still come from the jar. If it does not, "
                                    + "something is generating more than the declared set.",
                            resident.getName())
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
        Set<String> coveredPackages = jarResidentPackages();

        for (String name : GENERATED) {
            Class<?> generated = load(name);

            // Asserted against the declared set rather than against the siblings looked up below: those are
            // selected BY package name, so comparing the two afterwards could never fail. What can fail, and
            // is worth failing on, is generating into a package with no declared neighbour at all.
            assertThat(generated.getPackage().getName())
                    .as("%s is generated into a package this test does not know about. Every generated package "
                                    + "needs a declared jar-resident sibling, or its coexistence is unproven.",
                            generated.getName())
                    .isIn(coveredPackages);

            for (Class<?> resident : jarResidentSiblingsOf(generated)) {
                assertThat(generated.getClassLoader())
                        .as("%s must share a classloader with %s, or they are in different runtime packages and "
                                        + "package-private access fails despite the matching package name",
                                generated.getName(), resident.getName())
                        .isSameAs(resident.getClassLoader());
            }
        }
    }

    /**
     * {@link #GENERATED} has to match {@code patched.classes} in this module's pom, and a comment saying so is
     * not a check. Surefire passes the property through so that this can be one.
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

    /**
     * The jar-resident classes that have to coexist with this generated one, i.e. those in its own package.
     * Fails rather than returning empty: a generated class in a package with no declared jar sibling would
     * otherwise be checked against nothing, and the loop over the result would pass while proving nothing.
     */
    private static List<Class<?>> jarResidentSiblingsOf(Class<?> generated) {
        List<Class<?>> siblings = new ArrayList<>();
        for (String name : JAR_RESIDENT_NAMES) {
            Class<?> candidate = load(name);
            if (candidate.getPackage().getName().equals(generated.getPackage().getName())) {
                siblings.add(candidate);
            }
        }
        if (siblings.isEmpty()) {
            throw new AssertionError("No jar-resident sibling declared for package "
                    + generated.getPackage().getName() + " (needed by " + generated.getName() + "). Add one to "
                    + "JAR_RESIDENT_NAMES - without it the same-runtime-package assertion for that package is "
                    + "vacuous.");
        }
        return siblings;
    }

    private static Set<String> jarResidentPackages() {
        Set<String> packages = new LinkedHashSet<>();
        for (String name : JAR_RESIDENT_NAMES) {
            packages.add(load(name).getPackage().getName());
        }
        return packages;
    }

    /**
     * Loads a class through the same classloader that loaded this test, which is the one whose classpath
     * ordering the whole module depends on. A name that does not resolve fails here rather than silently
     * shrinking the check to whatever still exists after a Kafka upgrade - or, for a {@link #GENERATED} entry,
     * after {@code patched.classes} and this list have drifted apart.
     */
    private static Class<?> load(String name) {
        try {
            return Class.forName(name, false, ShadowedClassLoadingTest.class.getClassLoader());
        } catch (ClassNotFoundException e) {
            throw new AssertionError(name + " is named here but does not exist on the classpath - either Kafka "
                    + "has moved or renamed it and this check is now guarding nothing, or patched.classes and "
                    + "this list have drifted apart. Pick another adjacent class rather than deleting the "
                    + "entry.", e);
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
