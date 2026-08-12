package bz.stub.parallelconsumer.connect;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Modifier;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static bz.stub.parallelconsumer.connect.TestEnvironment.requiredProperty;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Mechanical bounds and failure-path tests for the generated-source harness. */
class PatchHarnessTest {

    private static final String WORKER_SINK_TASK_PATH =
            "org/apache/kafka/connect/runtime/WorkerSinkTask.java";
    private static final String BRIDGE_IMPORT =
            "+import bz.stub.parallelconsumer.connect.PcConnectDispatchBridge;";
    private static final String BRIDGE_FIELD =
            "+    private static final boolean PC_CONNECT_DISPATCH_ENABLED = PcConnectDispatchBridge.enabled();";

    @TempDir
    Path temporaryDirectory;

    @Test
    void trackedPatchChangesOnlyTheNamedDisabledBridgeLinkage() throws IOException {
        List<String> lines = Files.readAllLines(trackedPatch(), StandardCharsets.UTF_8);
        Set<String> oldFiles = patchPaths(lines, "--- ");
        Set<String> newFiles = patchPaths(lines, "+++ ");
        List<String> additions = lines.stream()
                .filter(line -> line.startsWith("+") && !line.startsWith("+++ "))
                .collect(Collectors.toList());
        List<String> removals = lines.stream()
                .filter(line -> line.startsWith("-") && !line.startsWith("--- "))
                .collect(Collectors.toList());
        long hunks = lines.stream().filter(line -> line.startsWith("@@ ")).count();

        assertThat(oldFiles).containsExactly("a/" + WORKER_SINK_TASK_PATH);
        assertThat(newFiles).containsExactly("b/" + WORKER_SINK_TASK_PATH);
        assertThat(additions).containsExactlyInAnyOrder(BRIDGE_IMPORT, BRIDGE_FIELD);
        assertThat(removals).isEmpty();
        assertThat(hunks).isEqualTo(2);
        assertThat(additions).noneMatch(line -> line.matches(
                ".*(poll|convertMessages|deliverMessages|commitOffsets|preCommit|rebalance).*"));
    }

    @Test
    void bridgeHasOneHardDisabledReadOnlyEntryPoint() {
        assertThat(PcConnectDispatchBridge.enabled()).isFalse();
        assertThat(Arrays.stream(PcConnectDispatchBridge.class.getDeclaredFields())
                .filter(field -> !field.isSynthetic())
                .collect(Collectors.toList())).isEmpty();
        assertThat(Arrays.stream(PcConnectDispatchBridge.class.getDeclaredMethods())
                .filter(method -> !method.isSynthetic())
                .map(MethodSummary::of)
                .collect(Collectors.toList()))
                .containsExactly(new MethodSummary("enabled", true, 0, boolean.class));
    }

    @Test
    void trackedPatchAppliesCleanlyToAFreshReleasedSourceCopy() throws Exception {
        Path freshTree = temporaryDirectory.resolve("fresh");
        Path source = freshTree.resolve(WORKER_SINK_TASK_PATH);
        Files.createDirectories(source.getParent());
        Files.copy(pristineWorkerSinkTask(), source, StandardCopyOption.REPLACE_EXISTING);

        ProcessResult result = run(sharedApplyPatch(), freshTree.toString(), trackedPatch().toString());

        assertThat(result.exitCode).as(result.output).isZero();
        assertThat(result.output).contains("apply-patch: applied 2 hunk(s)");
        assertThat(Files.readAllLines(source, StandardCharsets.UTF_8))
                .contains(BRIDGE_IMPORT.substring(1), BRIDGE_FIELD.substring(1));
    }

    @Test
    void malformedPatchFailsBeforeChangingTheSource() throws Exception {
        Path target = temporaryDirectory.resolve("malformed-target");
        Path source = target.resolve(WORKER_SINK_TASK_PATH);
        Files.createDirectories(source.getParent());
        Files.copy(pristineWorkerSinkTask(), source, StandardCopyOption.REPLACE_EXISTING);
        List<String> before = Files.readAllLines(source, StandardCharsets.UTF_8);
        Path malformed = temporaryDirectory.resolve("malformed.patch");
        Files.write(malformed, Arrays.asList(
                "--- a/" + WORKER_SINK_TASK_PATH,
                "+++ b/" + WORKER_SINK_TASK_PATH,
                "@@ -1,1 +1,1 @@",
                "-this context does not exist in WorkerSinkTask",
                "+nor does this replacement"), StandardCharsets.UTF_8);

        ProcessResult result = run(sharedApplyPatch(), target.toString(), malformed.toString());

        assertThat(result.exitCode).isNotZero();
        assertThat(result.output)
                .contains("apply-patch: FAILED")
                .contains("does not apply cleanly to the unpacked sources");
        assertThat(Files.readAllLines(source, StandardCharsets.UTF_8)).isEqualTo(before);
    }

    @Test
    void sharedRegenScriptCanTargetThisModuleWithoutBeingCopied() throws Exception {
        Path module = temporaryDirectory.resolve("other-module");
        Path pristine = module.resolve("target/connect-pristine/example.txt");
        Path patched = module.resolve("target/connect-patched/example.txt");
        Files.createDirectories(pristine.getParent());
        Files.createDirectories(patched.getParent());
        Files.createDirectories(module.resolve("src/main/patch"));
        Files.write(pristine, Collections.singletonList("released"), StandardCharsets.UTF_8);
        Files.write(patched, Collections.singletonList("patched"), StandardCharsets.UTF_8);

        ProcessResult result = run(sharedRegenPatch(), "connect-pristine", "connect-patched",
                "src/main/patch/test.patch", module.toString());

        assertThat(result.exitCode).as(result.output).isZero();
        assertThat(result.output).contains("regen-patch: wrote");
        assertThat(Files.readAllLines(module.resolve("src/main/patch/test.patch"), StandardCharsets.UTF_8))
                .anyMatch(line -> line.startsWith("--- a/example.txt"))
                .anyMatch(line -> line.startsWith("+++ b/example.txt"));
    }

    @Test
    void reportVerifierRejectsEmptyTestDiscovery() throws IOException {
        Path module = Paths.get(requiredProperty("pc.connect.module.dir"));
        Path stock = temporaryDirectory.resolve("empty-stock-reports");
        Path patched = temporaryDirectory.resolve("empty-patched-reports");
        Files.createDirectories(stock);
        Files.createDirectories(patched);

        assertThatThrownBy(() -> WorkerSinkTaskRegressionReportsVerifier.verify(
                module.resolve("src/test/resources/worker-sink-task-stock-baseline-tests.txt"), stock, patched))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("stock reports discovered no WorkerSinkTaskTest cases");
    }

    private static Set<String> patchPaths(List<String> lines, String prefix) {
        Set<String> paths = new LinkedHashSet<>();
        for (String line : lines) {
            if (!line.startsWith(prefix)) {
                continue;
            }
            String pathAndTimestamp = line.substring(prefix.length());
            int tab = pathAndTimestamp.indexOf('\t');
            paths.add(tab >= 0 ? pathAndTimestamp.substring(0, tab) : pathAndTimestamp);
        }
        return paths;
    }

    private static Path trackedPatch() {
        return Paths.get(requiredProperty("pc.connect.module.dir"), "src/main/patch/pcconnect.patch");
    }

    private static Path pristineWorkerSinkTask() {
        return Paths.get(requiredProperty("pc.connect.pristine.dir"), WORKER_SINK_TASK_PATH);
    }

    private static String sharedApplyPatch() {
        return requiredProperty("pc.connect.apply.patch.script");
    }

    private static String sharedRegenPatch() {
        return requiredProperty("pc.connect.regen.patch.script");
    }

    private static ProcessResult run(String... command) throws Exception {
        Process process = new ProcessBuilder(command).redirectErrorStream(true).start();
        List<String> output = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                output.add(line);
            }
        }
        return new ProcessResult(process.waitFor(), String.join("\n", output));
    }

    private static final class ProcessResult {
        private final int exitCode;
        private final String output;

        private ProcessResult(int exitCode, String output) {
            this.exitCode = exitCode;
            this.output = output;
        }
    }

    private static final class MethodSummary {
        private final String name;
        private final boolean isStatic;
        private final int parameterCount;
        private final Class<?> returnType;

        private MethodSummary(String name, boolean isStatic, int parameterCount, Class<?> returnType) {
            this.name = name;
            this.isStatic = isStatic;
            this.parameterCount = parameterCount;
            this.returnType = returnType;
        }

        private static MethodSummary of(java.lang.reflect.Method method) {
            return new MethodSummary(method.getName(), Modifier.isStatic(method.getModifiers()),
                    method.getParameterCount(), method.getReturnType());
        }

        @Override
        public boolean equals(Object other) {
            if (!(other instanceof MethodSummary)) {
                return false;
            }
            MethodSummary that = (MethodSummary) other;
            return name.equals(that.name) && isStatic == that.isStatic && parameterCount == that.parameterCount
                    && returnType.equals(that.returnType);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(new Object[]{name, isStatic, parameterCount, returnType});
        }

        @Override
        public String toString() {
            return name + "(static=" + isStatic + ", parameters=" + parameterCount + ", returns="
                    + returnType.getName() + ")";
        }
    }
}
