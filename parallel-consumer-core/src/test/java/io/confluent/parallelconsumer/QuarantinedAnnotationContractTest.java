package io.confluent.parallelconsumer;
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

    @Test
    void pomExcludesTheQuarantinedGroupFromDefaultSuites() throws IOException {
        String pom = read(REPO_ROOT.resolve("pom.xml"));
        assertWithMessage("root pom's default excluded.groups must contain the quarantine tag - " +
                "otherwise quarantined tests run (and fail) in the gating suites")
                .that(pom).contains("<excluded.groups>performance," + Quarantined.TAG + "</excluded.groups>");
    }

    @Test
    void gatingCiScriptsExcludeTheQuarantinedGroup() throws IOException {
        for (String script : Arrays.asList("bin/ci-unit-test.sh", "bin/ci-integration-test.sh", "bin/ci-build.sh")) {
            assertWithMessage(script + " hardcodes its group exclusions (it does not inherit the pom " +
                    "default) and must exclude the quarantine tag")
                    .that(read(REPO_ROOT.resolve(script)))
                    .contains("-Dexcluded.groups=performance," + Quarantined.TAG);
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
    void perPrWorkflowRunsTheAuditAndTheNightlyWorkflowRunsTheLane() throws IOException {
        String maven = read(REPO_ROOT.resolve(".github/workflows/maven.yml"));
        assertWithMessage("per-PR audit must enforce the registry")
                .that(maven).contains("bin/check-quarantine-registry.sh");
        assertThat(maven).contains("bin/check-quarantine-owners.sh");
        assertWithMessage("the lane RUN must NOT be in maven.yml - it would list as a skipped check " +
                "on every PR; it lives in its own nightly workflow")
                .that(maven).doesNotContain("bin/quarantined-test.sh");
        String nightly = read(REPO_ROOT.resolve(".github/workflows/quarantine-nightly.yml"));
        assertThat(nightly).contains("bin/quarantined-test.sh");
        assertWithMessage("nightly lane fail-fasts on rule violations before spending a test run")
                .that(nightly).contains("bin/check-quarantine-registry.sh");
        assertThat(nightly).contains("bin/check-quarantine-owners.sh");
    }

    /**
     * The nightly workflow must DECLARE the triggers it exists for - a real bug this test guards: the
     * dispatch trigger was once missing while docs claimed "run it manually", making that impossible.
     */
    @Test
    void nightlyWorkflowDeclaresScheduleAndDispatchTriggers() throws IOException {
        String nightly = read(REPO_ROOT.resolve(".github/workflows/quarantine-nightly.yml"));
        assertWithMessage("nightly lane needs a declared schedule trigger")
                .that(nightly).contains("schedule:");
        assertWithMessage("manual lane runs need a declared workflow_dispatch trigger")
                .that(nightly).contains("workflow_dispatch:");
    }

    @Test
    void releaseWorkflowBlocksOnQuarantinedTests() throws IOException {
        String release = read(REPO_ROOT.resolve(".github/workflows/release.yml"));
        assertWithMessage("release.yml must refuse to release while any test is quarantined")
                .that(release).contains("Refuse to release with quarantined tests");
        assertThat(release).contains("@Quarantined");
    }

    @Test
    void registryFileExistsWhereTheCheckScriptsExpectIt() {
        assertThat(Files.exists(REPO_ROOT.resolve("docs/QUARANTINED_TESTS.md"))).isTrue();
    }

    private static String read(Path path) throws IOException {
        assertWithMessage("wiring file moved/deleted: " + path).that(Files.exists(path)).isTrue();
        return new String(Files.readAllBytes(path), StandardCharsets.UTF_8);
    }
}
