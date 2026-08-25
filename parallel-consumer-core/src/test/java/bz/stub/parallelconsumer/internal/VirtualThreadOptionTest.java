package bz.stub.parallelconsumer.internal;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import static com.google.common.truth.Truth.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The half of the virtual-threads contract that holds on <b>every</b> JDK: the option's default, and what happens
 * when it is enabled on a runtime that cannot serve it.
 * <p>
 * Deliberately a separate class from {@link VirtualThreadExecutionModeTest}, which
 * {@code bin/check-execution-mode.sh} uses as its evidence that the mode was exercised at all. That guard counts
 * executed tests in the marker class, so a test living there that passes <em>without</em> virtual threads would
 * inflate the count and let a JDK 17 run report the mode as exercised - the guard committing the exact defect it
 * exists to catch. Found by pointing the guard at a real JDK 17 report and reading what it claimed.
 *
 * @see ParallelConsumerOptions#isUseVirtualThreads()
 */
class VirtualThreadOptionTest {

    private static final String MODE_PROPERTY = "pc.virtualThreads";

    private static boolean virtualThreadsAvailable() {
        try {
            Class.forName("java.lang.Thread").getMethod("ofVirtual");
            Executors.class.getMethod("newThreadPerTaskExecutor", ThreadFactory.class);
            return true;
        } catch (ReflectiveOperationException e) {
            return false;
        }
    }

    private static ParallelConsumerOptions.ParallelConsumerOptionsBuilder<String, String> options() {
        return ParallelConsumerOptions.<String, String>builder()
                .consumer(new MockConsumer<>(OffsetResetStrategy.EARLIEST));
    }

    /**
     * The negative half of the contract. On a JDK without virtual threads, asking for them must fail at
     * construction with a message that names the option and a cause worth reading - not at the first poll, and not
     * silently on the platform path.
     */
    @Test
    void enablingTheModeOnAJvmWithoutVirtualThreadsFailsAtConstructionWithACause() {
        Assumptions.assumeFalse(virtualThreadsAvailable(),
                "this JVM has virtual threads, so the unsupported path cannot be provoked here - "
                        + "VirtualThreadExecutionModeTest covers the complementary case");

        assertThatThrownBy(() -> options().useVirtualThreads(true).build().validate())
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("useVirtualThreads")
                .hasMessageContaining("JDK 21")
                .hasCauseInstanceOf(ReflectiveOperationException.class);
    }

    /**
     * The default must stay platform threads whatever else changes here.
     */
    @Test
    void theOptionIsOffByDefault() {
        Assumptions.assumeFalse(Boolean.getBoolean(MODE_PROPERTY),
                "the execution-mode axis has selected virtual threads for this run");

        assertThat(options().build().isUseVirtualThreads()).isFalse();
    }

    /**
     * The system property is the seam the benchmark harness and CI's matrix both use, so an explicit builder call
     * has to win over it - otherwise a test cannot pin itself to the platform path inside a virtual-thread lane,
     * which several now do.
     */
    @Test
    void anExplicitBuilderValueBeatsTheSystemProperty() {
        assertThat(options().useVirtualThreads(false).build().isUseVirtualThreads()).isFalse();
    }
}
