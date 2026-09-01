package bz.stub.parallelconsumer;
/*-
 * Copyright (C) 2020-2026 Antony Stubbs and contributors
 */

import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.google.common.truth.Truth.assertWithMessage;

/**
 * Shared harness for the quarantine shell-script behavioural tests ({@link QuarantineRegistryScriptTest},
 * {@link CheckQuarantineOwnersScriptTest}): temp fixture root, registry writing, and the
 * run-script-capture-output plumbing, so the script tests contain only their fixtures and assertions.
 */
@DisabledOnOs(OS.WINDOWS) // bash scripts under test; CI and dev machines are POSIX
abstract class AbstractQuarantineScriptTest {

    @TempDir
    protected Path fixture;

    /** Runs a repo script (path relative to repo root) against the fixture root; envPairs = key,value,... */
    protected Result runScript(String scriptRepoRelativePath, String... envPairs)
            throws IOException, InterruptedException {
        Path script = RepoRoot.find().resolve(scriptRepoRelativePath);
        ProcessBuilder pb = new ProcessBuilder("bash", script.toString());
        pb.environment().put("QUARANTINE_CHECK_ROOT", fixture.toString());
        for (int i = 0; i < envPairs.length; i += 2) {
            pb.environment().put(envPairs[i], envPairs[i + 1]);
        }
        customizeEnvironment(pb.environment());
        pb.redirectErrorStream(true);
        Process process = pb.start();
        String output = readFully(process.getInputStream());
        assertWithMessage("script under test hung - output so far: %s", output)
                .that(process.waitFor(60, TimeUnit.SECONDS)).isTrue();
        return new Result(process.exitValue(), output);
    }

    /** Hook for subclasses to add environment (e.g. PATH-stubbed executables). */
    protected void customizeEnvironment(Map<String, String> env) {
    }

    protected void writeRegistry(String entries) throws IOException {
        Path docs = fixture.resolve("docs");
        Files.createDirectories(docs);
        String content = "# Quarantined tests - live registry\n\n## Currently quarantined\n\n" + entries;
        Files.write(docs.resolve("quarantined-tests.md"), content.getBytes(StandardCharsets.UTF_8));
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

    protected static final class Result {
        final int exitCode;
        final String output;

        Result(int exitCode, String output) {
            this.exitCode = exitCode;
            this.output = output;
        }
    }
}
