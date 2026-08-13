package bz.stub.parallelconsumer.examples.metrics.integrationTests;

/*-
 * Copyright (C) 2020-2023 Confluent, Inc.
 * Modifications Copyright (C) 2026 Antony Stubbs and contributors
 */

import com.fasterxml.jackson.databind.ObjectMapper;
import bz.stub.parallelconsumer.internal.utils.LongPollingMockConsumer;
import bz.stub.parallelconsumer.examples.metrics.CoreApp;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import pl.tlinkowski.unij.api.UniLists;

import java.io.BufferedInputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.time.Duration;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

@Slf4j
@Testcontainers
public class CoreAppMetricsIntegrationTest {

    @Container
    private final static PrometheusContainer PROMETHEUS_CONTAINER = new PrometheusContainer();

    @Test
    @SneakyThrows
    void testMetrics() {
        org.testcontainers.Testcontainers.exposeHostPorts(7001);

        var mockConsumer = Mockito.spy(new LongPollingMockConsumer<String, String>(OffsetResetStrategy.EARLIEST));
        when(mockConsumer.groupMetadata()).thenReturn(new ConsumerGroupMetadata("groupid")); // todo fix AK mock consumer
        CoreAppUnderTest coreApp = new CoreAppUnderTest(mockConsumer);

        final var expectedMetrics =
                UniLists.of("pc_status", "pc_partitions_number", "pc_incomplete_offsets_total", "pc_user_function_processing_time_seconds");

        coreApp.run();

        coreApp.mockConsumer.addRecord(new ConsumerRecord(coreApp.getInputTopic(), 0, 0, "a key 1", "a value"));
        coreApp.mockConsumer.addRecord(new ConsumerRecord(coreApp.getInputTopic(), 0, 1, "a key 2", "a value"));
        coreApp.mockConsumer.addRecord(new ConsumerRecord(coreApp.getInputTopic(), 0, 2, "a key 3", "a value"));

        Awaitility.await().pollDelay(Duration.ofSeconds(1)).untilAsserted(() -> {
            final var metrics = getPrometheusMetrics();
            assertThat(metrics).containsAll(expectedMetrics);
        });

        coreApp.close();
    }

    @SneakyThrows
    private Set<String> getPrometheusMetrics() {
        ObjectMapper mapper = new ObjectMapper();

        final var url = new URL(String.format("%s/api/v1/metadata", PROMETHEUS_CONTAINER.getPrometheusEndpoint()));
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        assertThat(conn.getResponseCode()).as("Prometheus metadata endpoint response code").isEqualTo(200);

        final Map<String, Object> jsonBody = mapper.readValue(new BufferedInputStream(conn.getInputStream()), Map.class);
        return ((Map) jsonBody.get("data")).keySet();
    }

    /**
     * Overrides only the lifecycle hook needed to drive the mock consumer's rebalance/assignment (a
     * MockConsumer-specific simulation, not production wiring). The consumer itself is now injected via the
     * constructor ({@code super(mockConsumer)}) instead of by overriding a package-private getter - which is
     * what let the test move out of the surefire suite into this {@code integrationTests} package.
     */
    static class CoreAppUnderTest extends CoreApp {
        final LongPollingMockConsumer<String, String> mockConsumer;

        CoreAppUnderTest(LongPollingMockConsumer<String, String> mockConsumer) {
            super(mockConsumer);
            this.mockConsumer = mockConsumer;
        }

        @Override
        protected void postSetup() {
            super.postSetup();
            mockConsumer.subscribeWithRebalanceAndAssignment(UniLists.of(getInputTopic()), 1);
        }
    }
}
