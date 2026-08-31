package bz.stub.parallelconsumer.proxy.protocol;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.proxy.protocol.v1.ClientMessage;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;

/**
 * Pins the codegen {@code <outputDirectory>} override in this module's pom (the {@code generated-protobuf} path -
 * its comment there carries the full reasoning). The plugin's default output lands inside
 * {@code target/generated-sources}, which the root pom's build-helper-maven-plugin registers as a TEST source root
 * in every module - so with the default path, every generated class is compiled twice: once into
 * {@code target/classes} as a main source, and again by testCompile into {@code target/test-classes}. That
 * regression is silent - the build stays green - so this test is what goes red instead: it asserts the generated
 * package's classes appear in the main output and NOT in the test output.
 */
class GeneratedCodePlacementTest {

    @Test
    void generatedClassesAreCompiledExactlyOnceNeverIntoTestClasses() throws Exception {
        // Derived from a generated class rather than hardcoded, so a package rename moves the assertion with it.
        var generatedPackageDir = ClientMessage.class.getPackageName().replace('.', '/');

        var mainClasses = codeSourceOf(ClientMessage.class);
        var testClasses = codeSourceOf(getClass());

        // Positive controls: prove this test is looking at the layout it thinks it is, so the empty assertion
        // below cannot pass vacuously because surefire's classpath shape changed under it. The first is also
        // where the regression itself usually surfaces: once testCompile duplicates the generated classes into
        // test-classes, those copies shadow the main ones on surefire's classpath, so the generated class LOADS
        // from test-classes - measured by breaking the override and watching this assertion go red.
        assertWithMessage("directory %s was loaded from - 'test-classes' here means the generated classes were "
                + "compiled a second time into the test output and are shadowing the main copies",
                ClientMessage.class.getName())
                .that(mainClasses.getFileName().toString()).isEqualTo("classes");
        assertThat(testClasses.getFileName().toString()).isEqualTo("test-classes");
        try (Stream<Path> generatedInMain = Files.list(mainClasses.resolve(generatedPackageDir))) {
            assertThat(generatedInMain.anyMatch(p -> p.toString().endsWith(".class"))).isTrue();
        }

        // The check itself: the duplicate-compile failure mode lands the generated package under test-classes.
        var generatedInTestOutput = testClasses.resolve(generatedPackageDir);
        if (Files.isDirectory(generatedInTestOutput)) {
            try (Stream<Path> walk = Files.walk(generatedInTestOutput)) {
                List<Path> strays = walk.filter(p -> p.toString().endsWith(".class")).toList();
                assertThat(strays).isEmpty();
            }
        }
    }

    private static Path codeSourceOf(Class<?> clazz) throws Exception {
        return Path.of(clazz.getProtectionDomain().getCodeSource().getLocation().toURI());
    }
}
