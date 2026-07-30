package io.confluent.parallelconsumer;
/*-
 * Copyright (C) 2020-2026 Antony Stubbs and contributors
 */

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;

/**
 * Behavioural self-test of {@code bin/check-quarantine-registry.sh} - the enforcement that keeps
 * {@code docs/QUARANTINED_TESTS.md} and the {@code @Quarantined} annotations from drifting apart. Runs
 * the real script against temp fixtures (via its {@code QUARANTINE_CHECK_ROOT} override) and asserts
 * exit codes + messages for the consistent, missing-entry, and stale-entry cases, so a refactor of the
 * script (or of the registry's machine-parsed format) that breaks detection fails the gating unit suite.
 */
@DisabledOnOs(OS.WINDOWS) // bash script under test; CI and dev machines are POSIX
class QuarantineRegistryScriptTest {

    @TempDir
    Path fixture;

    @Test
    void consistentRegistryPasses() throws Exception {
        writeAnnotatedTest("SomeQuarantinedIT");
        writeRegistry("- [ ] `SomeQuarantinedIT.someMethod` - diagnosed. **Owner: PR #999**\n");
        Result result = runCheck();
        assertThat(result.output).contains("consistent");
        assertThat(result.exitCode).isEqualTo(0);
    }

    @Test
    void annotatedTestMissingFromRegistryFails() throws Exception {
        writeAnnotatedTest("SomeQuarantinedIT");
        writeRegistry("");
        Result result = runCheck();
        assertWithMessage("a quarantined test with no registry entry must be flagged - output: %s", result.output)
                .that(result.exitCode).isEqualTo(1);
        assertThat(result.output).contains("DRIFT");
        assertThat(result.output).contains("SomeQuarantinedIT");
    }

    @Test
    void staleRegistryEntryWithoutAnnotationFails() throws Exception {
        writeRegistry("- [ ] `GhostIT.gone` - was re-enabled but the entry was forgotten. **Owner: PR #999**\n");
        Result result = runCheck();
        assertWithMessage("a registry entry whose test is no longer annotated must be flagged - output: %s", result.output)
                .that(result.exitCode).isEqualTo(1);
        assertThat(result.output).contains("stale entry");
        assertThat(result.output).contains("GhostIT");
    }

    @Test
    void stringLiteralMentionsOfTheAnnotationDoNotCountAsQuarantined() throws Exception {
        // e.g. this test suite itself mentions the annotation inside string literals - only real
        // line-anchored annotation USAGE may count, or self-tests would block releases forever
        Path src = fixture.resolve("module/src/test/java");
        Files.createDirectories(src);
        String java = "class MetaTest {\n" +
                "    void x() { check(\"@Quarantined(\"); }\n" +
                "}\n";
        Files.write(src.resolve("MetaTest.java"), java.getBytes(StandardCharsets.UTF_8));
        writeRegistry("");
        Result result = runCheck();
        assertWithMessage("literal mention wrongly counted as a quarantined test - output: %s", result.output)
                .that(result.exitCode).isEqualTo(0);
    }

    @Test
    void emptyLaneAndEmptyRegistryPasses() throws Exception {
        writeRegistry("");
        Result result = runCheck();
        assertThat(result.exitCode).isEqualTo(0);
    }

    @Test
    void sameLineStackedAnnotationIsDetected() throws Exception {
        // ce-review P1: `@Test @Quarantined(...)` on one line must still count as quarantined
        writeTestSource("StackedIT", "class StackedIT {\n" +
                "    @Test @Quarantined(reason = \"d\", tracking = \"t\")\n" +
                "    void m() {}\n}\n");
        writeRegistry("");
        Result result = runCheck();
        assertWithMessage("stacked same-line annotation missed - output: %s", result.output)
                .that(result.exitCode).isEqualTo(1);
    }

    @Test
    void fullyQualifiedAnnotationIsDetected() throws Exception {
        writeTestSource("FqIT", "class FqIT {\n" +
                "    @io.confluent.parallelconsumer.Quarantined(reason = \"d\", tracking = \"t\")\n" +
                "    void m() {}\n}\n");
        writeRegistry("");
        Result result = runCheck();
        assertWithMessage("fully-qualified annotation missed - output: %s", result.output)
                .that(result.exitCode).isEqualTo(1);
    }

    @Test
    void multiLineAnnotationArgumentsAreDetected() throws Exception {
        writeTestSource("MultiLineIT", "class MultiLineIT {\n" +
                "    @Quarantined(\n" +
                "            reason = \"long \" +\n" +
                "                    \"diagnosis\",\n" +
                "            tracking = \"docs/x.md\")\n" +
                "    void m() {}\n}\n");
        writeRegistry("- [ ] `MultiLineIT.m` - diagnosed. **Owner: PR #999**\n");
        Result result = runCheck();
        assertWithMessage("multi-line annotation should be consistent - output: %s", result.output)
                .that(result.exitCode).isEqualTo(0);
    }

    @Test
    void secondUndiagnosedMethodCannotRideAlongOnTheFirstEntry() throws Exception {
        // ce-review P1 (reproduced there): 2 annotated methods with 1 entry must FAIL
        writeTestSource("TwoIT", "class TwoIT {\n" +
                "    @Quarantined(reason = \"d\", tracking = \"t\")\n    void one() {}\n" +
                "    @Quarantined(reason = \"d\", tracking = \"t\")\n    void two() {}\n}\n");
        writeRegistry("- [ ] `TwoIT.one` - diagnosed. **Owner: PR #999**\n");
        Result result = runCheck();
        assertWithMessage("2 annotations vs 1 entry must drift - output: %s", result.output)
                .that(result.exitCode).isEqualTo(1);
        // and with both entries present it must pass
        writeRegistry("- [ ] `TwoIT.one` - d. **Owner: PR #999**\n- [ ] `TwoIT.two` - d. **Owner: PR #999**\n");
        assertThat(runCheck().exitCode).isEqualTo(0);
    }

    @Test
    void staleMethodEntryIsFlagged() throws Exception {
        // ce-review P1 (reproduced there): entry for a method that no longer exists must FAIL
        writeAnnotatedTest("SomeQuarantinedIT");
        writeRegistry("- [ ] `SomeQuarantinedIT.someMethod` - d. **Owner: PR #999**\n" +
                "- [ ] `SomeQuarantinedIT.goneMethod` - d. **Owner: PR #999**\n");
        Result result = runCheck();
        assertWithMessage("stale method entry must drift - output: %s", result.output)
                .that(result.exitCode).isEqualTo(1);
        assertThat(result.output).contains("goneMethod");
    }

    // ---- fixture helpers ----

    private void writeAnnotatedTest(String className) throws IOException {
        writeTestSource(className, "class " + className + " {\n" +
                "    @Quarantined(reason = \"diagnosed\", tracking = \"docs/x.md\", fixedBy = \"PR #999\")\n" +
                "    void someMethod() {}\n" +
                "}\n");
    }

    private void writeTestSource(String className, String body) throws IOException {
        Path src = fixture.resolve("module/src/test-integration/java");
        Files.createDirectories(src);
        Files.write(src.resolve(className + ".java"), body.getBytes(StandardCharsets.UTF_8));
    }

    private void writeRegistry(String entries) throws IOException {
        Path docs = fixture.resolve("docs");
        Files.createDirectories(docs);
        String content = "# Quarantined tests - live registry\n\n## Currently quarantined\n\n" + entries;
        Files.write(docs.resolve("QUARANTINED_TESTS.md"), content.getBytes(StandardCharsets.UTF_8));
    }

    private Result runCheck() throws IOException, InterruptedException {
        Path script = RepoRoot.find().resolve("bin/check-quarantine-registry.sh");
        ProcessBuilder pb = new ProcessBuilder("bash", script.toString());
        pb.environment().put("QUARANTINE_CHECK_ROOT", fixture.toString());
        pb.redirectErrorStream(true);
        Process process = pb.start();
        String output = readFully(process.getInputStream());
        assertWithMessage("script under test hung - output so far: %s", output)
                .that(process.waitFor(30, TimeUnit.SECONDS)).isTrue();
        return new Result(process.exitValue(), output);
    }

    private static String readFully(InputStream in) throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        byte[] chunk = new byte[4096];
        int n;
        while ((n = in.read(chunk)) != -1) {
            buffer.write(chunk, 0, n);
        }
        return new String(buffer.toByteArray(), StandardCharsets.UTF_8);
    }

    private static final class Result {
        final int exitCode;
        final String output;

        Result(int exitCode, String output) {
            this.exitCode = exitCode;
            this.output = output;
        }
    }
}
