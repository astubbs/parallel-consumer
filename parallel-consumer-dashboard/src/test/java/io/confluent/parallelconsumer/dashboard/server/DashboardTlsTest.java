package io.confluent.parallelconsumer.dashboard.server;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import io.confluent.parallelconsumer.dashboard.DashboardOptions;
import io.confluent.parallelconsumer.dashboard.DashboardServer;
import io.vertx.core.net.SelfSignedCertificate;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.LinkedHashMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * TLS: off by default so the zero-setup path is not interrupted by a certificate interstitial, on with one flag, and
 * honest in the log about which mode is in force.
 */
class DashboardTlsTest {

    @Test
    void isOffByDefaultAndGeneratesNoCertificate() throws IOException {
        try (DashboardServer server = new DashboardServer(DashboardTestSupport.populatedPublisher(), null,
                DashboardTestSupport.testOptions().build()).start()) {

            assertThat(server.getOptions().isTlsEnabled()).isFalse();
            assertThat(server.isUsingGeneratedCertificate())
                    .as("nothing should be generated on the default path - a certificate nobody asked for is a "
                            + "browser warning nobody asked for")
                    .isFalse();
            assertThat(server.getUrl()).startsWith("http://");
            assertThat(RawHttp.get(server.getPort(), DashboardServer.STATE_PATH).statusCode).isEqualTo(200);
        }
    }

    @Test
    void turningItOnWithNoCertificateGeneratesASelfSignedOneAndSaysWhatThatMeans() throws IOException {
        try (DashboardTestSupport.LogCapture logs = DashboardTestSupport.captureLogs(DashboardServer.class);
             DashboardServer server = new DashboardServer(DashboardTestSupport.populatedPublisher(), null,
                     DashboardTestSupport.testOptions().tlsEnabled(true).build()).start()) {

            assertThat(server.isUsingGeneratedCertificate()).isTrue();
            assertThat(server.getUrl()).startsWith("https://");

            RawHttp.Response response = RawHttp.request("GET", "127.0.0.1", server.getPort(),
                    DashboardServer.STATE_PATH, new LinkedHashMap<>(), true);
            assertThat(response.statusCode).isEqualTo(200);
            assertThat(response.body).contains("schemaVersion");

            assertThat(logs.formattedMessages())
                    .anySatisfy(message -> assertThat(message)
                            .contains("SELF-SIGNED")
                            .contains("browser WILL")
                            .contains("tlsCertificatePath"));
        }
    }

    @Test
    void aSuppliedCertificateAndKeyAreUsedInsteadOfGeneratingOne() throws IOException {
        SelfSignedCertificate supplied = SelfSignedCertificate.create();
        try (DashboardTestSupport.LogCapture logs = DashboardTestSupport.captureLogs(DashboardServer.class);
             DashboardServer server = new DashboardServer(DashboardTestSupport.populatedPublisher(), null,
                     DashboardTestSupport.testOptions()
                             .tlsEnabled(true)
                             .tlsCertificatePath(supplied.certificatePath())
                             .tlsPrivateKeyPath(supplied.privateKeyPath())
                             .build()).start()) {

            assertThat(server.isUsingGeneratedCertificate())
                    .as("a supplied certificate must be used, not silently replaced by a generated one")
                    .isFalse();

            RawHttp.Response response = RawHttp.request("GET", "127.0.0.1", server.getPort(),
                    DashboardServer.STATE_PATH, new LinkedHashMap<>(), true);
            assertThat(response.statusCode).isEqualTo(200);

            assertThat(logs.formattedMessages())
                    .anySatisfy(message -> assertThat(message).contains(supplied.certificatePath()));
        } finally {
            supplied.delete();
        }
    }

    @Test
    void halfATlsConfigurationIsRefusedAtBuildTimeRatherThanQuietlySelfSigning() {
        assertThatThrownBy(() -> DashboardOptions.builder()
                .tlsEnabled(true)
                .tlsCertificatePath("/tmp/cert.pem")
                .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("tlsPrivateKeyPath");

        assertThatThrownBy(() -> DashboardOptions.builder()
                .tlsCertificatePath("/tmp/cert.pem")
                .tlsPrivateKeyPath("/tmp/key.pem")
                .build())
                .as("paths supplied without enabling TLS would be silently ignored")
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("tlsEnabled");
    }

    @Test
    void aBrokenCertificatePathFailsFastRatherThanWalkingAHundredPorts() {
        DashboardServer server = new DashboardServer(DashboardTestSupport.populatedPublisher(), null,
                DashboardTestSupport.testOptions()
                        .tlsEnabled(true)
                        .tlsCertificatePath("/definitely/not/a/certificate.pem")
                        .tlsPrivateKeyPath("/definitely/not/a/key.pem")
                        .build());
        try {
            assertThatThrownBy(server::start)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("not a busy port");
        } finally {
            server.close();
        }
    }
}
