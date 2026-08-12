package io.confluent.parallelconsumer.dashboard.server;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import io.confluent.parallelconsumer.dashboard.DashboardServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Static asset serving: the assets that must be reachable, and the paths that must not be.
 * <p>
 * Written before the handlers, per the plan's execution note - path traversal is the canonical case where an
 * implementation that looks obviously correct is defeated by one specific input.
 */
class StaticAssetTest {

    private DashboardServer server;

    @BeforeEach
    void start() {
        server = new DashboardServer(DashboardTestSupport.populatedPublisher(), null,
                DashboardTestSupport.testOptions().build()).start();
    }

    @AfterEach
    void stop() {
        server.close();
    }

    @Test
    void theChartLibraryIsServedStraightOutOfItsWebJar() throws IOException {
        RawHttp.Response response = RawHttp.get(server.getPort(),
                DashboardServer.VENDOR_PREFIX + "/uplot/uPlot.iife.min.js");

        assertThat(response.statusCode).isEqualTo(200);
        assertThat(response.body).isNotEmpty();
        assertThat(response.header("content-type")).contains("javascript");
    }

    @Test
    void aNestedAssetBeneathTheMountIsServed() throws IOException {
        RawHttp.Response response = RawHttp.get(server.getPort(),
                DashboardServer.VENDOR_PREFIX + "/uplot/uPlot.min.css");

        assertThat(response.statusCode).isEqualTo(200);
        assertThat(response.body).contains(".uplot");
    }

    @Test
    void theRootServesThePageShell() throws IOException {
        RawHttp.Response response = RawHttp.get(server.getPort(), "/");

        assertThat(response.statusCode).isEqualTo(200);
        assertThat(response.header("content-type")).startsWith("text/html");
        assertThat(response.body).contains("Parallel Consumer dashboard");
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "/vendor/uplot/../../../etc/passwd",
            "/assets/../../../etc/passwd",
            "/vendor/uplot/%2e%2e/%2e%2e/%2e%2e/etc/passwd",
            "/vendor/uplot/..%2f..%2f..%2fetc%2fpasswd",
            "/assets/%252e%252e/%252e%252e/etc/passwd",
            "/vendor/uplot/..\\..\\..\\windows\\win.ini"
    })
    void traversalIsRejected(String path) throws IOException {
        RawHttp.Response response = RawHttp.request("GET", server.getPort(), path,
                new java.util.LinkedHashMap<>());

        assertThat(response.statusCode).as("path %s", path).isEqualTo(400);
        assertThat(response.body).doesNotContain("root:").doesNotContain("[fonts]");
    }

    @Test
    void anAbsolutePathOutsideTheMountResolvesToNothing() throws IOException {
        RawHttp.Response response = RawHttp.get(server.getPort(), "/etc/passwd");

        assertThat(response.statusCode).isEqualTo(404);
        assertThat(response.body).doesNotContain("root:");
    }

    @Test
    void anAbsolutePathSmuggledUnderTheMountResolvesToNothing() throws IOException {
        RawHttp.Response response = RawHttp.get(server.getPort(),
                DashboardServer.VENDOR_PREFIX + "/uplot//etc/passwd");

        assertThat(response.statusCode).isBetween(400, 499);
        assertThat(response.body).doesNotContain("root:");
    }

    @Test
    void theWebJarsNonDistFilesAreNotReachable() throws IOException {
        // the WebJar also ships a README, a package.json and TypeScript definitions; only dist is mounted
        RawHttp.Response response = RawHttp.get(server.getPort(),
                DashboardServer.VENDOR_PREFIX + "/uplot/package.json");

        assertThat(response.statusCode).isEqualTo(404);
    }

    @Test
    void aDirectoryUnderTheMountIsNotListed() throws IOException {
        RawHttp.Response response = RawHttp.get(server.getPort(), DashboardServer.VENDOR_PREFIX + "/uplot/");

        assertThat(response.statusCode).isNotEqualTo(200);
        assertThat(response.body).doesNotContain("uPlot.iife.min.js");
    }

    /**
     * The page's assets are mounted from a PACKAGE-QUALIFIED classpath root, so a file that some other jar - or a
     * {@code ./web} directory beside the process - contributes at the unqualified {@code web/} cannot be reached
     * through this module's public {@code /assets/*} mount.
     * <p>
     * A classpath is a flat, shared, merged namespace and this module is a library inside somebody else's
     * application. Mounted at {@code web}, whichever jar came first on the classpath would decide what
     * {@code /assets/app.js} returns - a broken dashboard in one direction, and this module serving a stranger's file
     * in the other. The file is planted for real rather than argued about, because the whole failure mode is that it
     * looks fine from the source.
     */
    @Test
    void aFileAtTheUnqualifiedWebRootIsNotServedThroughTheAssetMount() throws IOException {
        Path planted = Paths.get("target", "test-classes", "web", "planted.txt");
        Files.createDirectories(planted.getParent());
        Files.write(planted, "shadowed-by-another-jar".getBytes(StandardCharsets.UTF_8));
        try {
            RawHttp.Response response = RawHttp.get(server.getPort(), DashboardServer.ASSETS_PREFIX + "/planted.txt");

            assertThat(response.statusCode)
                    .as("a file at the unqualified web/ root must not be reachable through /assets")
                    .isEqualTo(404);
            assertThat(response.body).doesNotContain("shadowed-by-another-jar");
        } finally {
            Files.deleteIfExists(planted);
        }
    }

    /**
     * The other half: the mount root really is the package-qualified one, so the test above is not passing merely
     * because static serving is broken altogether.
     */
    @Test
    void thePageAssetsAreServedFromThePackageQualifiedRoot() throws IOException {
        assertThat(DashboardServer.PAGE_CLASSPATH_ROOT)
                .as("an unqualified root shares a namespace with every other jar on the classpath")
                .isEqualTo("io/confluent/parallelconsumer/dashboard/web");

        RawHttp.Response response = RawHttp.get(server.getPort(), DashboardServer.ASSETS_PREFIX + "/app.js");

        assertThat(response.statusCode).isEqualTo(200);
        assertThat(response.body).isNotEmpty();
    }
}
