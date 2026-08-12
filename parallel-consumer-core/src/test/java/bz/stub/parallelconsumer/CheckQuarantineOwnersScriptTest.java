package bz.stub.parallelconsumer;
/*-
 * Copyright (C) 2020-2026 Antony Stubbs and contributors
 */

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Map;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;

/**
 * Behavioural self-test of {@code bin/check-quarantine-owners.sh} - the owner-claim verification that
 * previously had zero coverage (ce-review P2). Runs the real script against temp fixtures with
 * PATH-stubbed {@code gh} and {@code git}, covering the full error taxonomy: confirmed-missing /
 * closed-unmerged / merged-but-still-quarantined are ERRORS; transient gh failure, unowned entries,
 * base-not-updated, preview-keeps-annotation, and file-missing-from-preview are ADVISORY only.
 */
class CheckQuarantineOwnersScriptTest extends AbstractQuarantineScriptTest {

    private Path stubBin;

    @BeforeEach
    void setupFixture() throws IOException {
        stubBin = fixture.resolve("stub-bin");
        Files.createDirectories(stubBin);
        writeStub("gh", "#!/usr/bin/env bash\n" +
                "case \"$*\" in *'auth status'*) exit 0;; esac\n" +
                "state=\"${GH_STUB_STATE:-OPEN}\"\n" +
                "if [ \"$state\" = TRANSIENT ]; then echo 'HTTP 502 bad gateway' >&2; exit 1; fi\n" +
                "if [ \"$state\" = MISSING ]; then echo 'Could not resolve to a PullRequest' >&2; exit 1; fi\n" +
                "case \"$*\" in\n" +
                "  *state*) echo \"$state\";;\n" +
                "  *baseRefName*) echo main;;\n" +
                "esac\n");
        writeStub("git", "#!/usr/bin/env bash\n" +
                "case \"$1\" in\n" +
                "  remote) echo 'https://github.com/stub/repo.git';;\n" +
                "  fetch)\n" +
                "    case \"$*\" in\n" +
                "      *pull/*) echo merge > \"$STUB_PHASE_FILE\"; exit \"${GIT_STUB_MERGE_FETCH_EXIT:-0}\";;\n" +
                "      *) echo base > \"$STUB_PHASE_FILE\"; exit \"${GIT_STUB_BASE_FETCH_EXIT:-0}\";;\n" +
                "    esac;;\n" +
                "  cat-file) exit \"${GIT_STUB_CATFILE_EXIT:-0}\";;\n" +
                "  show)\n" +
                "    phase=$(cat \"$STUB_PHASE_FILE\" 2>/dev/null || echo base)\n" +
                "    if [ \"$phase\" = base ]; then flag=\"${GIT_STUB_BASE_ANNOTATED:-1}\"; else flag=\"${GIT_STUB_MERGE_ANNOTATED:-1}\"; fi\n" +
                "    if [ \"$flag\" = 1 ]; then printf '    @Quarantined(\\n'; fi;;\n" +
                "esac\n");
        // the quarantined class file the script locates for relpath + fixedBy cross-check
        Path src = fixture.resolve("module/src/test-integration/java");
        Files.createDirectories(src);
        Files.write(src.resolve("SomeQuarantinedIT.java"),
                ("class SomeQuarantinedIT {\n" +
                        "    @Quarantined(reason = \"d\", tracking = \"t\", fixedBy = \"PR #80\")\n" +
                        "    void someMethod() {}\n}\n").getBytes(StandardCharsets.UTF_8));
        writeRegistry("- [ ] `SomeQuarantinedIT.someMethod` - diagnosed. **Owner: PR #80**\n");
    }

    @Test
    void missingOwnerPrIsAnError() throws Exception {
        Result r = run("GH_STUB_STATE", "MISSING");
        assertThat(r.exitCode).isEqualTo(1);
        assertThat(r.output).contains("does not exist");
    }

    @Test
    void closedUnmergedOwnerIsAnError() throws Exception {
        Result r = run("GH_STUB_STATE", "CLOSED");
        assertThat(r.exitCode).isEqualTo(1);
        assertThat(r.output).contains("orphaned");
    }

    @Test
    void mergedOwnerWithTestStillQuarantinedIsAnError() throws Exception {
        Result r = run("GH_STUB_STATE", "MERGED");
        assertThat(r.exitCode).isEqualTo(1);
        assertThat(r.output).contains("OVERDUE");
    }

    @Test
    void transientGhFailureIsAdvisoryNotError() throws Exception {
        // ce-review P1: infra weather must not red the audit ("red here is real")
        Result r = run("GH_STUB_STATE", "TRANSIENT");
        assertWithMessage("transient gh failure wrongly treated as error - output: %s", r.output)
                .that(r.exitCode).isEqualTo(0);
        assertThat(r.output).contains("gh unavailable after retries");
    }

    @Test
    void openOwnerWithQuarantineNotOnBaseIsAdvisory() throws Exception {
        Result r = run("GIT_STUB_BASE_ANNOTATED", "0");
        assertThat(r.exitCode).isEqualTo(0);
        assertThat(r.output).contains("not yet on its base");
    }

    @Test
    void openOwnerWhosePreviewRemovesTheAnnotationClosesTheLoop() throws Exception {
        Result r = run("GIT_STUB_MERGE_ANNOTATED", "0");
        assertThat(r.exitCode).isEqualTo(0);
        assertThat(r.output).contains("loop closed");
    }

    @Test
    void openOwnerWhosePreviewKeepsTheAnnotationIsAdvisory() throws Exception {
        Result r = run("GIT_STUB_MERGE_ANNOTATED", "1");
        assertThat(r.exitCode).isEqualTo(0);
        assertThat(r.output).contains("does NOT yet remove");
    }

    @Test
    void fileMissingFromMergePreviewIsAdvisory() throws Exception {
        // ce-review: renamed file must not produce a false "loop closed"
        Result r = run("GIT_STUB_CATFILE_EXIT", "1");
        assertThat(r.exitCode).isEqualTo(0);
        assertThat(r.output).contains("renamed/moved");
    }

    @Test
    void unownedEntryIsAdvisory() throws Exception {
        writeRegistry("- [ ] `SomeQuarantinedIT.someMethod` - diagnosed, hunting an owner.\n");
        Result r = run();
        assertThat(r.exitCode).isEqualTo(0);
        assertThat(r.output).contains("no owning PR");
    }

    @Test
    void fixedByDisagreeingWithRegistryOwnerIsFlagged() throws Exception {
        writeRegistry("- [ ] `SomeQuarantinedIT.someMethod` - diagnosed. **Owner: PR #123**\n");
        Result r = run("GIT_STUB_BASE_ANNOTATED", "0");
        assertThat(r.exitCode).isEqualTo(0);
        assertThat(r.output).contains("fixedBy PR #80");
        assertThat(r.output).contains("Owner line says PR #123");
    }

    // ---- helpers ----

    private void writeStub(String name, String body) throws IOException {
        Path stub = stubBin.resolve(name);
        Files.write(stub, body.getBytes(StandardCharsets.UTF_8));
        Files.setPosixFilePermissions(stub, PosixFilePermissions.fromString("rwxr-xr-x"));
    }

    @Override
    protected void customizeEnvironment(Map<String, String> env) {
        env.put("PATH", stubBin + ":" + env.get("PATH"));
        env.put("STUB_PHASE_FILE", fixture.resolve("stub-phase").toString());
    }

    private Result run(String... envPairs) throws IOException, InterruptedException {
        return runScript("bin/check-quarantine-owners.sh", envPairs);
    }
}
