package io.confluent.parallelconsumer.dashboard.server;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import io.confluent.parallelconsumer.dashboard.DashboardOptions;
import io.confluent.parallelconsumer.dashboard.DashboardServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The {@code Host} allowlist and the cross-origin rejection - the two cheap checks that stop a loopback-bound
 * dashboard being read by a web page that has no business reading it.
 * <p>
 * These are written against specific inputs rather than against the shape of the code, because both are cases where
 * a plausible-looking implementation passes casual inspection and fails a particular string: an IPv6 literal, a
 * trailing dot, a port that is present on one side and not the other.
 */
class HostAllowlistTest {

    private DashboardServer server;

    @AfterEach
    void stopServer() {
        if (server != null) {
            server.close();
            server = null;
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"localhost", "127.0.0.1", "[::1]", "0:0:0:0:0:0:0:1", "LOCALHOST", "localhost."})
    void loopbackNamesAreAccepted(String host) {
        assertThat(new HostAllowlist(DashboardOptions.defaults()).isAllowedHost(host)).isTrue();
    }

    /**
     * The port is stripped before matching - and the bracketed IPv6 form has to survive it, which is where a naive
     * "split on the last colon" gets it wrong.
     */
    @ParameterizedTest
    @ValueSource(strings = {"localhost:8080", "127.0.0.1:8080", "[::1]:8080", "localhost.:8080", "LOCALHOST:65535"})
    void loopbackNamesAreAcceptedWithAPortAttached(String host) {
        assertThat(new HostAllowlist(DashboardOptions.defaults()).isAllowedHost(host)).isTrue();
    }

    @ParameterizedTest
    @ValueSource(strings = {"evil.example.com", "evil.example.com:8080", "127.0.0.1.evil.example.com", ""})
    void anythingElseIsRejected(String host) {
        HostAllowlist allowlist = new HostAllowlist(DashboardOptions.defaults());

        assertThat(allowlist.isAllowedHost(host)).isFalse();
    }

    @Test
    void anAbsentHostHeaderIsRejectedRatherThanWavedThrough() {
        assertThat(new HostAllowlist(DashboardOptions.defaults()).isAllowedHost(null)).isFalse();
    }

    @Test
    void aUserConfiguredExtraHostIsAccepted() {
        DashboardOptions options = DashboardOptions.builder()
                .extraAllowedHosts(Collections.singleton("my-forwarded-box:9000"))
                .build();
        HostAllowlist allowlist = new HostAllowlist(options);

        assertThat(allowlist.isAllowedHost("my-forwarded-box:9000")).isTrue();
        assertThat(allowlist.isAllowedHost("my-forwarded-box")).isTrue();
        assertThat(allowlist.isAllowedHost("another-box:9000")).isFalse();
    }

    @Test
    void aCrossOriginRequestIsNotSameOrigin() {
        HostAllowlist allowlist = new HostAllowlist(DashboardOptions.defaults());

        assertThat(allowlist.isSameOrigin("http://127.0.0.1:8080", "127.0.0.1:8080")).isTrue();
        assertThat(allowlist.isSameOrigin("http://evil.example.com", "127.0.0.1:8080")).isFalse();
        assertThat(allowlist.isSameOrigin("https://127.0.0.1:8080", "127.0.0.1:8080"))
                .as("a scheme change is a different origin, even to the same host and port")
                .isFalse();
        assertThat(allowlist.isSameOrigin("http://127.0.0.1:9999", "127.0.0.1:8080"))
                .as("a port change is a different origin")
                .isFalse();
    }

    @Test
    void anOpaqueOriginIsRejectedRatherThanTreatedAsAbsent() {
        HostAllowlist allowlist = new HostAllowlist(DashboardOptions.defaults());

        assertThat(allowlist.isSameOrigin("null", "127.0.0.1:8080"))
                .as("Origin: null is what a sandboxed iframe sends; treating it as absent would let it through")
                .isFalse();
    }

    @Test
    void aRequestWithAForeignHostHeaderIsRefusedOverHttp() throws IOException {
        startServer();

        RawHttp.Response response = request("GET", "/api/state.json", headers("Host", "evil.example.com"));

        assertThat(response.statusCode).isEqualTo(403);
        assertThat(response.body).contains("evil.example.com").contains("extraAllowedHosts");
    }

    @Test
    void aRequestWithAForeignOriginIsRefusedOverHttp() throws IOException {
        startServer();

        RawHttp.Response response = request("GET", "/api/state.json",
                headers("Origin", "http://evil.example.com"));

        assertThat(response.statusCode).isEqualTo(403);
        assertThat(response.body).contains("cross-origin");
    }

    @Test
    void noResponseEverCarriesACorsHeader() throws IOException {
        startServer();

        for (String path : server.getProbePaths()) {
            RawHttp.Response rejected = request("GET", path, headers("Host", "evil.example.com"));
            assertThat(rejected.hasHeader("Access-Control-Allow-Origin"))
                    .as("no CORS header on a rejected %s", path).isFalse();

            if (DashboardServer.STREAM_PATH.equals(path)) {
                // an accepted GET here is a stream that stays open by design, so only its headers can be read
                try (RawHttp.SseStream stream = RawHttp.openSse(server.getPort(), path)) {
                    assertThat(stream.head.headers.keySet()).noneMatch(name -> name.startsWith("access-control-"));
                }
                continue;
            }

            RawHttp.Response allowed = RawHttp.get(server.getPort(), path);
            assertThat(allowed.hasHeader("Access-Control-Allow-Origin"))
                    .as("no CORS header on an accepted %s", path).isFalse();
            assertThat(allowed.headers.keySet()).noneMatch(name -> name.startsWith("access-control-"));
        }
    }

    @Test
    void theHostCheckRunsOnEveryRouteIncludingTheDiagnosticPage() throws IOException {
        startServer();

        for (String path : server.getProbePaths()) {
            RawHttp.Response response = request("GET", path, headers("Host", "evil.example.com"));

            assertThat(response.statusCode).as("path %s", path).isEqualTo(403);
        }
    }

    private void startServer() {
        server = new DashboardServer(DashboardTestSupport.populatedPublisher(), null,
                DashboardTestSupport.testOptions().build()).start();
    }

    private RawHttp.Response request(String method, String path, Map<String, String> headers) throws IOException {
        return RawHttp.request(method, server.getPort(), path, headers);
    }

    private static Map<String, String> headers(String name, String value) {
        Map<String, String> headers = new LinkedHashMap<>();
        headers.put(name, value);
        return headers;
    }
}
