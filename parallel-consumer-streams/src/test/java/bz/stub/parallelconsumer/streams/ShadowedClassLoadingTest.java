package bz.stub.parallelconsumer.streams;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.processor.internals.AbstractProcessorContext;
import org.apache.kafka.streams.processor.internals.ProcessorContextImpl;
import org.apache.kafka.streams.processor.internals.RecordCollectorImpl;
import org.apache.kafka.streams.processor.internals.StreamTask;
import org.apache.kafka.streams.processor.internals.StreamThread;
import org.junit.jupiter.api.Test;

import java.net.URL;

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
     * The classes listed in {@code patched.classes} in this module's pom. Keep in sync: a class that is
     * generated but missing here is unguarded, and one listed here but not generated fails loudly below.
     */
    private static final Class<?>[] GENERATED = {
            AbstractProcessorContext.class,
            ProcessorContextImpl.class,
            RecordCollectorImpl.class,
    };

    /**
     * Classes we deliberately do <em>not</em> generate. They must still load from the jar - that is what makes
     * this "shadowing" rather than "a fork": the two sets have to coexist in one runtime package.
     * <p>
     * {@link StreamTask} is the interesting one, and it is here rather than above on purpose. It is the class
     * the execution seam will patch next, and it is the immediate neighbour of all three generated classes -
     * it constructs the context and drives the collector. Asserting that it still comes from the jar is
     * therefore the sharpest available check that the generated set is exactly what the pom declares, and it
     * is what will have to change, visibly, when the seam arrives.
     */
    private static final Class<?>[] JAR_RESIDENT = {
            StreamTask.class,
            StreamThread.class,
    };

    @Test
    void generatedClassesWinOverTheJar() {
        for (Class<?> generated : GENERATED) {
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
        for (Class<?> resident : JAR_RESIDENT) {
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
        for (Class<?> generated : GENERATED) {
            for (Class<?> resident : JAR_RESIDENT) {
                assertThat(generated.getPackage().getName())
                        .as("%s must sit in the same package as the jar-resident classes it reaches into", generated.getName())
                        .isEqualTo(resident.getPackage().getName());

                assertThat(generated.getClassLoader())
                        .as("%s must share a classloader with %s, or they are in different runtime packages and "
                                        + "package-private access fails despite the matching package name",
                                generated.getName(), resident.getName())
                        .isSameAs(resident.getClassLoader());
            }
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
