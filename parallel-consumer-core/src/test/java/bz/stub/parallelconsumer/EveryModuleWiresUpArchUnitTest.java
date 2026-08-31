package bz.stub.parallelconsumer;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.truth.Truth.assertWithMessage;

/**
 * ArchUnit only sees a module that opts in: {@code TestConventionRules} holds the rules, but each module needs its
 * own thin {@code TestConventionsArchTest} to point {@code @AnalyzeClasses} at its packages. Nothing made that
 * opt-in visible, so a module without one was not reported as unprotected - it was reported as nothing at all,
 * which reads exactly like passing.
 *
 * <p>It had already happened. Three of the five example modules - core, vertx and reactor - had test sources and no
 * ArchUnit at all, found only because someone asked whether coverage was complete. This test converts that question
 * into a build failure, and covers modules that do not exist yet, which is the half a one-time audit cannot reach.
 *
 * <p>Why not inherit it from the root pom instead: surefire's {@code dependenciesToScan} can pull a shared test
 * class into every module, but it pulls the WHOLE test-jar, so core's 54 test classes would run in all five
 * modules. Scoping that back needs global {@code <includes>}, which then constrains normal test selection
 * everywhere. A two-line file per module plus this check buys the same guarantee without that blast radius.
 */
@Slf4j
class EveryModuleWiresUpArchUnitTest {

    private static final String ARCH_TEST = "TestConventionsArchTest.java";

    /**
     * The one module that CANNOT wire the shared wrapper, with the reason, because a bare exemption is the
     * hole this test exists to close.
     *
     * <p>{@code TestConventionRules} ships in core's test-jar, and the proxy client's api module is
     * dependency-free by design - on core in any scope, which its own pom states and the direct sibling's
     * {@code bannedDependencies} enforcer rule exists to keep true. So the wrapper is unavailable there, and
     * the choice is between weakening that invariant and naming the module here. This test postdates the
     * branch that module was extracted from (astubbs/parallel-consumer#242), so the collision is new rather
     * than a rule anybody ignored.
     *
     * <p><b>It is not unprotected.</b> The module runs {@code ClientSurfaceArchTest} instead, whose two rules
     * are narrower and stronger than the shared conventions: no transport, engine or Kafka type may appear
     * anywhere in the surface nine other languages mirror. {@link #runsSomeOtherArchUnitWrapper} is what makes
     * this row an exemption rather than a mute - delete that test and the module fails this one again.
     *
     * <p>What lifts it: extracting {@code TestConventionRules} into a small test-support artifact that neither
     * core nor the clients own. Worth doing when a second module hits the same wall, not before.
     */
    private static final List<String> EXEMPT_BECAUSE_THEY_CANNOT_DEPEND_ON_CORES_TEST_JAR =
            Collections.singletonList("parallel-consumer-proxy-clients/parallel-consumer-proxy-client-java/"
                    + "parallel-consumer-proxy-client-java-api/src/test/java");

    @Test
    void everyModuleWithTestSourcesWiresUpArchUnit() throws IOException {
        Path repoRoot = repoRoot();
        List<Path> unprotected;
        try (Stream<Path> paths = Files.walk(repoRoot)) {
            unprotected = paths
                    .filter(p -> p.endsWith(Paths.get("src", "test", "java")))
                    .filter(Files::isDirectory)
                    .filter(p -> !p.toString().contains("/target/"))
                    .filter(EveryModuleWiresUpArchUnitTest::hasJavaSources)
                    .filter(p -> !containsArchTest(p))
                    .map(repoRoot::relativize)
                    .filter(p -> !EXEMPT_BECAUSE_THEY_CANNOT_DEPEND_ON_CORES_TEST_JAR.contains(
                            p.toString().replace(java.io.File.separatorChar, '/')))
                    .collect(Collectors.toList());
        }

        assertWithMessage("every module with test sources needs its own %s - it is two lines pointing "
                        + "@AnalyzeClasses at that module's packages, and without it ArchUnit silently "
                        + "does not run there at all. Copy any existing one and change the package. "
                        + "Unprotected", ARCH_TEST)
                .that(unprotected)
                .isEmpty();
    }

    /**
     * The exemption above is only honest while the exempt module runs ArchUnit under some OTHER wrapper, so
     * that is asserted rather than assumed. Without this, deleting {@code ClientSurfaceArchTest} would leave a
     * module with test sources, no ArchUnit at all, and a row in this file saying that is fine - which is the
     * silent opt-out the whole class exists to prevent, reintroduced by the fix for it.
     */
    @Test
    void everyExemptModuleStillRunsSomeArchUnitWrapper() throws IOException {
        Path repoRoot = repoRoot();
        for (String exempt : EXEMPT_BECAUSE_THEY_CANNOT_DEPEND_ON_CORES_TEST_JAR) {
            Path testJavaDir = repoRoot.resolve(exempt);
            assertWithMessage("an exempt module must still exist: %s", exempt)
                    .that(Files.isDirectory(testJavaDir)).isTrue();
            assertWithMessage("%s is exempt from the shared wrapper because it cannot depend on core's "
                            + "test-jar, NOT from ArchUnit - it must still run some @AnalyzeClasses test of "
                            + "its own, or the exemption is a hole", exempt)
                    .that(runsSomeOtherArchUnitWrapper(testJavaDir)).isTrue();
        }
    }

    /** Whether any test in the tree points ArchUnit at packages, whatever the class is called. */
    private static boolean runsSomeOtherArchUnitWrapper(Path testJavaDir) throws IOException {
        try (Stream<Path> paths = Files.walk(testJavaDir)) {
            return paths.filter(p -> p.getFileName().toString().endsWith(".java"))
                    .anyMatch(p -> readBody(p).contains("@AnalyzeClasses"));
        }
    }

    private static String readBody(Path file) {
        try {
            return new String(Files.readAllBytes(file), java.nio.charset.StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new IllegalStateException("cannot read " + file, e);
        }
    }

    private static boolean hasJavaSources(Path testJavaDir) {
        try (Stream<Path> paths = Files.walk(testJavaDir)) {
            return paths.anyMatch(p -> p.getFileName().toString().endsWith(".java"));
        } catch (IOException e) {
            throw new IllegalStateException("cannot read " + testJavaDir, e);
        }
    }

    /**
     * Accepts a module only when its wrapper is actually WIRED, never merely present. Matching the filename
     * alone would let a placeholder - or a file accidentally stripped of its annotation - satisfy this check
     * while ArchUnit discovers nothing there, which is the exact silent opt-out this test exists to prevent.
     * Found by Codex review of astubbs/parallel-consumer#326, and it is an instance of the rule that same PR
     * adds to docs/agent-harness.md: a check can be right about WHAT to look for and wrong about its scope.
     */
    private static boolean containsArchTest(Path testJavaDir) {
        try (Stream<Path> paths = Files.walk(testJavaDir)) {
            return paths.filter(p -> p.getFileName().toString().equals(ARCH_TEST))
                    .anyMatch(EveryModuleWiresUpArchUnitTest::isWired);
        } catch (IOException e) {
            throw new IllegalStateException("cannot read " + testJavaDir, e);
        }
    }

    /** The wrapper must both point ArchUnit at packages and pull in the shared rules; either alone runs nothing. */
    private static boolean isWired(Path wrapper) {
        try {
            String body = new String(Files.readAllBytes(wrapper), java.nio.charset.StandardCharsets.UTF_8);
            return body.contains("@AnalyzeClasses") && body.contains("ArchTests.in(TestConventionRules.class)");
        } catch (IOException e) {
            throw new IllegalStateException("cannot read " + wrapper, e);
        }
    }

    /** Walks up from the module the test runs in - surefire's working directory - to the reactor root. */
    private static Path repoRoot() {
        Path candidate = Paths.get("").toAbsolutePath();
        // Located by BUILD content - the aggregator pom that declares <module> entries - not by Git metadata.
        // Requiring `.git` fails outright in a source archive or exported tree, where the build is otherwise
        // perfectly runnable and this test would abort before checking any module (Codex review of
        // astubbs/parallel-consumer#326). It also sidesteps the trap that a git WORKTREE stores `.git` as a
        // FILE, so an isDirectory check would walk past the worktree root onto the main checkout entirely.
        while (candidate != null && !isReactorRoot(candidate)) {
            candidate = candidate.getParent();
        }
        assertWithMessage("could not find the reactor root by walking up from %s looking for an aggregator pom",
                Paths.get("").toAbsolutePath()).that(candidate).isNotNull();
        return candidate;
    }

    private static boolean isReactorRoot(Path dir) {
        Path pom = dir.resolve("pom.xml");
        if (!Files.isRegularFile(pom)) {
            return false;
        }
        try {
            return new String(Files.readAllBytes(pom), java.nio.charset.StandardCharsets.UTF_8).contains("<module>");
        } catch (IOException e) {
            return false;
        }
    }
}
