package bz.stub.parallelconsumer;
/*-
 * Copyright (C) 2020-2026 Antony Stubbs and contributors
 */

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;

/**
 * Self-test of the quarantine lane's wiring - the {@link Quarantined} mechanism only works while a set
 * of otherwise-unrelated string sites stay in agreement (annotation meta-tag, pom group exclusion, the
 * gating CI scripts, the lane runner, the workflow job, the release guard). Any one of them drifting in
 * a big refactor breaks the lane SILENTLY - worst case, quarantined tests vanish from both lanes (no
 * red anywhere, coverage just gone). This test pins them all together so drift fails the gating unit
 * suite instead.
 */
class QuarantinedAnnotationContractTest {

    private static final Path REPO_ROOT = RepoRoot.find();

    @Test
    void annotationIsMetaTaggedWithTheQuarantinedTag() {
        Tag tag = Quarantined.class.getAnnotation(Tag.class);
        assertWithMessage("@Quarantined must be meta-annotated @Tag - without it, JUnit group " +
                "filtering ignores the annotation entirely and quarantined tests KEEP GATING")
                .that(tag).isNotNull();
        assertThat(tag.value()).isEqualTo(Quarantined.TAG);
    }

    @Test
    void annotationIsRuntimeRetainedAndTargetsClassesAndMethods() {
        Retention retention = Quarantined.class.getAnnotation(Retention.class);
        assertWithMessage("without RUNTIME retention JUnit cannot see the tag at test-discovery time")
                .that(retention.value()).isEqualTo(RetentionPolicy.RUNTIME);
        Target target = Quarantined.class.getAnnotation(Target.class);
        assertThat(Arrays.asList(target.value())).containsAtLeast(ElementType.TYPE, ElementType.METHOD);
    }

    /**
     * Asserts MEMBERSHIP of the quarantine tag in the default exclusion list, not the list's exact contents.
     * <p>
     * It used to pin the whole literal, `performance,chaos,quarantined`. That made every unrelated lane added
     * to the list - the Lincheck one was the first - fail this test with a message about quarantined tests
     * gating, which is not what had happened. Worse, the obvious repair is to paste the new literal in, and
     * then the test is pinning a list nobody reasoned about. Membership is the actual contract, and it still
     * goes red the moment the tag is dropped, which is the failure this test exists for.
     */
    @Test
    void pomExcludesTheQuarantinedGroupFromDefaultSuites() throws IOException {
        String pom = read(REPO_ROOT.resolve("pom.xml"));
        String open = "<excluded.groups>";
        int start = pom.indexOf(open);
        assertWithMessage("root pom must declare a default excluded.groups").that(start).isAtLeast(0);
        String excluded = pom.substring(start + open.length(), pom.indexOf("</excluded.groups>", start));
        assertWithMessage("root pom's default excluded.groups must contain the quarantine tag - " +
                "otherwise quarantined tests run (and fail) in the gating suites. Found: " + excluded)
                .that(Arrays.asList(excluded.split(","))).contains(Quarantined.TAG);
    }

    /**
     * BOTH test plugins must bind the group properties - a real ce-review P1: only failsafe was
     * wired, so the exclusion was a silent no-op for every UNIT test (a @Quarantined unit test kept
     * running in the gating Unit Tests lane). Verified behaviorally at fix time: a failing
     * quarantined unit test ran 0 times under gating groups and executed under the lane's groups.
     */
    @Test
    void bothSurefireAndFailsafeBindTheGroupProperties() throws IOException {
        String pom = read(REPO_ROOT.resolve("pom.xml"));
        int groupsBindings = countOccurrences(pom, "<groups>${included.groups}</groups>");
        int excludedBindings = countOccurrences(pom, "<excludedGroups>${excluded.groups}</excludedGroups>");
        assertWithMessage("surefire AND failsafe must each bind <groups> (unit-lane filtering is a " +
                "no-op without the surefire binding)").that(groupsBindings).isAtLeast(2);
        assertThat(excludedBindings).isAtLeast(2);
    }

    private static int countOccurrences(String haystack, String needle) {
        int count = 0, idx = 0;
        while ((idx = haystack.indexOf(needle, idx)) != -1) {
            count++;
            idx += needle.length();
        }
        return count;
    }

    /**
     * Membership again, for the same reason as the pom check above: these lists grow as lanes are added,
     * and pinning the whole literal makes an unrelated lane fail a quarantine test.
     */
    @Test
    void gatingCiScriptsExcludeTheQuarantinedGroup() throws IOException {
        for (String script : Arrays.asList("bin/ci-unit-test.sh", "bin/ci-integration-test.sh", "bin/ci-build.sh")) {
            String body = read(REPO_ROOT.resolve(script));
            String flag = "-Dexcluded.groups=";
            int start = body.indexOf(flag);
            assertWithMessage(script + " must pass an explicit -Dexcluded.groups").that(start).isAtLeast(0);
            String value = body.substring(start + flag.length()).split("\\s")[0];
            assertWithMessage(script + " hardcodes its group exclusions (it does not inherit the pom " +
                    "default) and must exclude the quarantine tag. Found: " + value)
                    .that(Arrays.asList(value.split(","))).contains(Quarantined.TAG);
        }
    }

    /**
     * The two lists are maintained by hand in two places, and drift is silent in the direction that
     * matters: a tag the pom excludes but a gating wrapper does not RUNS in the gating suite. Nothing else
     * checks this - the wrappers deliberately do not inherit the pom default, precisely so that a pom edit
     * cannot quietly change what gates.
     */
    @Test
    void gatingCiScriptsExcludeEveryGroupThePomDefaultExcludes() throws IOException {
        String pom = read(REPO_ROOT.resolve("pom.xml"));
        String open = "<excluded.groups>";
        int pomStart = pom.indexOf(open);
        List<String> pomGroups = Arrays.asList(
                pom.substring(pomStart + open.length(), pom.indexOf("</excluded.groups>", pomStart)).split(","));

        for (String script : Arrays.asList("bin/ci-unit-test.sh", "bin/ci-integration-test.sh", "bin/ci-build.sh")) {
            String body = read(REPO_ROOT.resolve(script));
            String flag = "-Dexcluded.groups=";
            String value = body.substring(body.indexOf(flag) + flag.length()).split("\\s")[0];
            assertWithMessage(script + " must exclude every group the pom default excludes, or that group " +
                    "runs in the GATING suite. Pom: " + pomGroups + " script: " + value)
                    .that(Arrays.asList(value.split(","))).containsAtLeastElementsIn(pomGroups);
        }
    }

    @Test
    void quarantineLaneRunnerIncludesOnlyTheQuarantinedGroup() throws IOException {
        String lane = read(REPO_ROOT.resolve("bin/quarantined-test.sh"));
        assertThat(lane).contains("-Dincluded.groups=" + Quarantined.TAG);
        assertWithMessage("the lane must clear the default exclusions or the included group is filtered straight back out")
                .that(lane).contains("-Dexcluded.groups=");
    }

    @Test
    void perPrWorkflowRunsTheAuditAndTheLaneWorkflowRunsTheTests() throws IOException {
        String maven = read(REPO_ROOT.resolve(".github/workflows/maven.yml"));
        assertWithMessage("per-PR audit must enforce the registry")
                .that(maven).contains("bin/check-quarantine-registry.sh");
        assertThat(maven).contains("bin/check-quarantine-owners.sh");
        assertWithMessage("the lane RUN must NOT be in maven.yml - it lives in its own workflow " +
                "with its own trigger set")
                .that(maven).doesNotContain("bin/quarantined-test.sh");
        String lane = read(REPO_ROOT.resolve(".github/workflows/quarantine-lane.yml"));
        assertThat(lane).contains("bin/quarantined-test.sh");
        assertWithMessage("the lane fail-fasts on rule violations before spending a test run")
                .that(lane).contains("bin/check-quarantine-registry.sh");
        assertThat(lane).contains("bin/check-quarantine-owners.sh");
    }

    /**
     * The lane workflow must DECLARE the triggers it exists for - a real bug this test guards: the
     * dispatch trigger was once missing while docs claimed "run it manually", making that impossible.
     */
    @Test
    void laneWorkflowDeclaresItsTriggers() throws IOException {
        String lane = read(REPO_ROOT.resolve(".github/workflows/quarantine-lane.yml"));
        assertWithMessage("lane runs on every PR push (pre-merge attribution)")
                .that(lane).contains("pull_request:");
        assertWithMessage("lane must run after every merge to master (canonical master-state record)")
                .that(lane).contains("push:");
        assertWithMessage("manual lane runs need a declared workflow_dispatch trigger")
                .that(lane).contains("workflow_dispatch:");
    }

    @Test
    void releaseWorkflowBlocksOnQuarantinedTests() throws IOException {
        String release = read(REPO_ROOT.resolve(".github/workflows/release.yml"));
        assertWithMessage("release.yml must refuse to release while any test is quarantined")
                .that(release).contains("Refuse to release with quarantined tests");
        assertWithMessage("the release guard must use the SHARED detection lib, not an inline pattern copy")
                .that(release).contains("quarantine-common.sh");
    }

    @Test
    void registryFileExistsWhereTheCheckScriptsExpectIt() {
        assertThat(Files.exists(REPO_ROOT.resolve("docs/quarantined-tests.md"))).isTrue();
    }

    /**
     * "No quarantine without diagnosis" is only compiler-enforced as far as the attributes EXISTING -
     * empty strings compile fine (ce-review P2). Scan compiled test classes reflectively (no static
     * initialization) and reject blank reason/tracking.
     */
    @Test
    void quarantinedAnnotationsMustCarryNonBlankDiagnosis() throws Exception {
        Path classesDir = Paths.get("target/test-classes").toAbsolutePath();
        if (!Files.exists(classesDir)) {
            return; // running outside a built module - the build always has it
        }
        java.util.List<String> offenders = new java.util.ArrayList<>();
        try (java.util.stream.Stream<Path> walk = Files.walk(classesDir)) {
            for (Path clazz : walk.filter(f -> f.toString().endsWith(".class"))
                    .collect(java.util.stream.Collectors.toList())) {
                byte[] bytes = Files.readAllBytes(clazz);
                if (!new String(bytes, StandardCharsets.ISO_8859_1).contains("Quarantined")) {
                    continue; // cheap constant-pool pre-filter
                }
                String name = classesDir.relativize(clazz).toString()
                        .replace(java.io.File.separatorChar, '.').replaceAll("\\.class$", "");
                try {
                    Class<?> loaded = Class.forName(name, false, getClass().getClassLoader());
                    checkDiagnosis(loaded.getAnnotation(Quarantined.class), name, offenders);
                    for (java.lang.reflect.Method m : loaded.getDeclaredMethods()) {
                        checkDiagnosis(m.getAnnotation(Quarantined.class), name + "." + m.getName(), offenders);
                    }
                } catch (Throwable ignored) {
                    // unloadable class (missing optional dep etc.) - not a quarantine concern
                }
            }
        }
        assertWithMessage("blank reason/tracking defeats 'no quarantine without diagnosis'")
                .that(offenders).isEmpty();
    }

    private static void checkDiagnosis(Quarantined q, String where, java.util.List<String> offenders) {
        if (q == null) return;
        if (q.reason().trim().isEmpty()) offenders.add(where + ": blank reason");
        if (q.tracking().trim().isEmpty()) offenders.add(where + ": blank tracking");
    }

    private static String read(Path path) throws IOException {
        assertWithMessage("wiring file moved/deleted: " + path).that(Files.exists(path)).isTrue();
        return new String(Files.readAllBytes(path), StandardCharsets.UTF_8);
    }
}
