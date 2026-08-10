package io.confluent.parallelconsumer.connect;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.apache.kafka.connect.runtime.WorkerConfig;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.net.URL;

import static org.assertj.core.api.Assertions.assertThat;

/** Proves that the one generated Connect class wins without splitting its runtime package. */
class ShadowedClassLoadingTest {

    private static final String WORKER_SINK_TASK = "org.apache.kafka.connect.runtime.WorkerSinkTask";

    @Test
    void generatedWorkerSinkTaskWinsOverTheJar() throws ClassNotFoundException {
        Class<?> generated = Class.forName(WORKER_SINK_TASK);

        assertThat(codeSourceOf(generated).toString())
                .as("the package-private WorkerSinkTask must load from this module's output")
                .doesNotContain(".jar")
                .contains("/classes/");
    }

    @Test
    void unpatchedSiblingStillComesFromConnectRuntimeJar() {
        assertThat(codeSourceOf(WorkerConfig.class).toString())
                .as("unpatched Connect runtime siblings must continue to come from the released jar")
                .contains("connect-runtime")
                .endsWith(".jar");
    }

    @Test
    void generatedAndJarResidentClassesShareOneRuntimePackage() throws ClassNotFoundException {
        Class<?> generated = Class.forName(WORKER_SINK_TASK);

        assertThat(generated.getPackage().getName()).isEqualTo(WorkerConfig.class.getPackage().getName());
        assertThat(generated.getClassLoader()).isSameAs(WorkerConfig.class.getClassLoader());
    }

    @Test
    void generatedWorkerSinkTaskLinksOnlyTheHardDisabledBridge() throws Exception {
        Class<?> generated = Class.forName(WORKER_SINK_TASK);
        Field field = generated.getDeclaredField("PC_CONNECT_DISPATCH_ENABLED");
        field.setAccessible(true);

        assertThat(Modifier.isPrivate(field.getModifiers())).isTrue();
        assertThat(Modifier.isStatic(field.getModifiers())).isTrue();
        assertThat(Modifier.isFinal(field.getModifiers())).isTrue();
        assertThat(field.getBoolean(null)).isFalse();
        assertThat(PcConnectDispatchBridge.enabled()).isFalse();
        assertThat(codeSourceOf(PcConnectDispatchBridge.class)).isEqualTo(codeSourceOf(generated));
    }

    private static URL codeSourceOf(Class<?> type) {
        assertThat(type.getProtectionDomain()).as("no protection domain for %s", type.getName()).isNotNull();
        assertThat(type.getProtectionDomain().getCodeSource())
                .as("no code source for %s", type.getName())
                .isNotNull();
        return type.getProtectionDomain().getCodeSource().getLocation();
    }
}
