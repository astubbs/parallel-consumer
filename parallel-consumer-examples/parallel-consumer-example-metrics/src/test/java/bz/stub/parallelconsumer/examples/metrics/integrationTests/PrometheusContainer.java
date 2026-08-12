package io.confluent.parallelconsumer.examples.metrics.integrationTests;

/*-
 * Copyright (C) 2020-2023 Confluent, Inc.
 * Modifications Copyright (C) 2026 Antony Stubbs and contributors
 */

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.dockerjava.api.command.InspectContainerResponse;
import lombok.SneakyThrows;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Map;

public class PrometheusContainer extends GenericContainer<PrometheusContainer> {
    private static final DockerImageName DEFAULT_IMAGE_NAME = DockerImageName.parse("prom/prometheus");
    private static final String DEFAULT_TAG = "v2.40.6";
    public static final int PROMETHEUS_PORT = 9090;

    /** @deprecated */
    @Deprecated
    public PrometheusContainer() {
        this(DEFAULT_IMAGE_NAME.withTag(DEFAULT_TAG));
    }

    /** @deprecated */
    @Deprecated
    public PrometheusContainer(String prometheusVersion) {
        this(DEFAULT_IMAGE_NAME.withTag(prometheusVersion));
    }

    public PrometheusContainer(DockerImageName dockerImageName) {
        super(dockerImageName);
        dockerImageName.assertCompatibleWith(new DockerImageName[]{DEFAULT_IMAGE_NAME});
        this.withExposedPorts(new Integer[]{PROMETHEUS_PORT});
        this.withAccessToHost(true);
        this.withCopyFileToContainer(
                MountableFile.forClasspathResource("prometheus.yml"),"/etc/prometheus/");
        this.withReuse(true);
    }

    public String getPrometheusEndpoint() {
        return String.format("http://%s:%s", this.getHost(), this.getMappedPort(PROMETHEUS_PORT));
    }

    /**
     * GETs one of Prometheus' HTTP API endpoints and returns the parsed {@code data} member - the part
     * every {@code /api/v1/*} response wraps its answer in.
     * <p>
     * Real Jackson on purpose: the copy Testcontainers bundles under {@code org.testcontainers.shaded.*}
     * is off limits (enforced by {@code TestConventionRules}), which is why this module carries an
     * explicit {@code jackson-databind} test dependency.
     *
     * @param apiPath the path below the endpoint root, e.g. {@code /api/v1/targets}
     * @param dataType what {@code data} deserialises to for that endpoint - a {@code List} for
     *                 {@code /api/v1/label/__name__/values}, a {@code Map} for {@code /api/v1/targets}
     * @return the response's {@code data} member
     */
    @SneakyThrows
    public <T> T queryApi(String apiPath, Class<T> dataType) {
        URL url = new URL(getPrometheusEndpoint() + apiPath);
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        int responseCode = connection.getResponseCode();
        if (responseCode != 200) {
            throw new IllegalStateException("Prometheus " + apiPath + " answered HTTP " + responseCode);
        }
        try (BufferedInputStream body = new BufferedInputStream(connection.getInputStream())) {
            Map<?, ?> envelope = new ObjectMapper().readValue(body, Map.class);
            return dataType.cast(envelope.get("data"));
        }
    }

    @SneakyThrows
    protected void containerIsStarted(InspectContainerResponse containerInfo) {
        try {
            ExecResult result = this.execInContainer(new String[]{"nc", "-z", "localhost", "9090"});
            if (result.getExitCode() != 0) {
                throw new IllegalStateException(result.toString());
            }
        } catch (IOException|InterruptedException e) {
            throw e;
        }
    }
}
