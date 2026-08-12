package bz.stub.parallelconsumer.connect;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.apache.kafka.connect.runtime.WorkerConfig;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;

import static bz.stub.parallelconsumer.connect.TestEnvironment.codeSourceOf;
import static bz.stub.parallelconsumer.connect.TestEnvironment.requiredProperty;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Execution-specific guards for the isolated stock, patched-disabled, and report-verifier forks. */
class WorkerSinkTaskRegressionTest {

    private static final String MODE_PROPERTY = "pc.connect.regression.mode";
    private static final String WORKER_SINK_TASK = "org.apache.kafka.connect.runtime.WorkerSinkTask";
    private static final String BRIDGE = "bz.stub.parallelconsumer.connect.PcConnectDispatchBridge";
    private static final String BRIDGE_FIELD = "PC_CONNECT_DISPATCH_ENABLED";

    @Test
    void regressionArmLoadsItsIntendedIsolatedClasspath() throws Exception {
        String mode = requiredProperty(MODE_PROPERTY);
        if ("verify".equals(mode)) {
            return;
        }

        Class<?> workerSinkTask = Class.forName(WORKER_SINK_TASK);
        URL workerLocation = codeSourceOf(workerSinkTask);
        assertThat(workerSinkTask.getClassLoader()).isSameAs(WorkerConfig.class.getClassLoader());
        assertThat(workerSinkTask.getPackage().getName()).isEqualTo(WorkerConfig.class.getPackage().getName());

        if ("stock".equals(mode)) {
            assertThat(workerLocation.toString()).contains("connect-runtime").endsWith(".jar");
            assertThatThrownBy(() -> workerSinkTask.getDeclaredField(BRIDGE_FIELD))
                    .isInstanceOf(NoSuchFieldException.class);
            assertThatThrownBy(() -> Class.forName(BRIDGE)).isInstanceOf(ClassNotFoundException.class);
            return;
        }

        if (!"patched".equals(mode)) {
            throw new IllegalStateException("unknown " + MODE_PROPERTY + " value: " + mode);
        }

        assertThat(workerLocation.toString())
                .doesNotContain(".jar")
                .contains("/parallel-consumer-connect/target/classes/");
        Field bridgeField = workerSinkTask.getDeclaredField(BRIDGE_FIELD);
        bridgeField.setAccessible(true);
        assertThat(Modifier.isPrivate(bridgeField.getModifiers())).isTrue();
        assertThat(Modifier.isStatic(bridgeField.getModifiers())).isTrue();
        assertThat(Modifier.isFinal(bridgeField.getModifiers())).isTrue();
        assertThat(bridgeField.getBoolean(null)).isFalse();

        Class<?> bridge = Class.forName(BRIDGE);
        Method enabled = bridge.getDeclaredMethod("enabled");
        assertThat(codeSourceOf(bridge)).isEqualTo(workerLocation);
        assertThat(enabled.invoke(null)).isEqualTo(false);
    }

    @Test
    void regressionReportsMatchTheCheckedExactManifest() throws Exception {
        String mode = requiredProperty(MODE_PROPERTY);
        if (!"verify".equals(mode)) {
            return;
        }

        Path module = Paths.get(requiredProperty("pc.connect.module.dir"));
        WorkerSinkTaskRegressionReportsVerifier.verify(
                module.resolve("src/test/resources/worker-sink-task-stock-baseline-tests.txt"),
                Paths.get(requiredProperty("pc.connect.stock.reports.dir")),
                Paths.get(requiredProperty("pc.connect.patched.reports.dir")));
    }

}
