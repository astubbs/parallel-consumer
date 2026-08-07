package io.confluent.parallelconsumer.dashboard.server;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import io.confluent.parallelconsumer.dashboard.DashboardOptions;
import io.confluent.parallelconsumer.dashboard.DashboardServer;
import io.confluent.parallelconsumer.dashboard.snapshot.PcMeterFixture;
import io.confluent.parallelconsumer.dashboard.snapshot.SnapshotPublisher;
import io.micrometer.core.instrument.MeterRegistry;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The self-diagnostic page: it has to be right about the three states it exists for, and it has to render in all of
 * them - including the ones where the registry, the snapshot and the assets are all missing, which is precisely when
 * somebody is looking at it.
 */
class StatusRouteTest {

    private static final DashboardOptions OPTIONS = DashboardOptions.defaults();

    @Test
    void reportsAMissingRegistryAndDoesNotThenBlameTheMetersForIt() {
        StatusRoute route = new StatusRoute(DashboardTestSupport.populatedPublisher(), null, OPTIONS);

        List<StatusRoute.Check> checks = route.runChecks(System.currentTimeMillis());

        assertThat(outcome(checks, "Meter registry supplied")).isEqualTo(StatusRoute.Outcome.FAILED);
        assertThat(remedy(checks, "Meter registry supplied")).contains("same MeterRegistry");
        assertThat(outcome(checks, "Parallel Consumer meters are present"))
                .as("blaming the meters when no registry was supplied points the reader at the wrong end")
                .isEqualTo(StatusRoute.Outcome.SKIPPED);
        assertThat(detail(checks, "Parallel Consumer meters are present"))
                .contains("Not run").contains("Meter registry supplied");
    }

    @Test
    void reportsThatTheCallbackHasNeverFired() {
        MeterRegistry registry = PcMeterFixture.fullyPopulated().getRegistry();
        StatusRoute route = new StatusRoute(DashboardTestSupport.populatedPublisher(), registry, OPTIONS);

        List<StatusRoute.Check> checks = route.runChecks(System.currentTimeMillis());

        assertThat(outcome(checks, "Meter registry supplied")).isEqualTo(StatusRoute.Outcome.PASSED);
        assertThat(outcome(checks, "Control loop has sampled at least once")).isEqualTo(StatusRoute.Outcome.FAILED);
        assertThat(remedy(checks, "Control loop has sampled at least once")).contains("registerWith");
        assertThat(outcome(checks, "Most recent sample is fresh")).isEqualTo(StatusRoute.Outcome.SKIPPED);
    }

    @Test
    void reportsARegistryThatIsNotTheOneTheConsumerWritesInto() {
        SnapshotPublisher publisher = DashboardTestSupport.emptyPublisher();
        publisher.sampleOnce();
        StatusRoute route = new StatusRoute(publisher, new PcMeterFixture().getRegistry(), OPTIONS);

        List<StatusRoute.Check> checks = route.runChecks(System.currentTimeMillis());

        assertThat(outcome(checks, "Control loop has sampled at least once")).isEqualTo(StatusRoute.Outcome.PASSED);
        assertThat(outcome(checks, "Parallel Consumer meters are present")).isEqualTo(StatusRoute.Outcome.FAILED);
        assertThat(remedy(checks, "Parallel Consumer meters are present")).contains("Two registries");
    }

    @Test
    void reportsAStaleSample() {
        SnapshotPublisher publisher = DashboardTestSupport.populatedPublisher();
        publisher.sampleOnce();
        StatusRoute route = new StatusRoute(publisher, PcMeterFixture.fullyPopulated().getRegistry(), OPTIONS);

        List<StatusRoute.Check> fresh = route.runChecks(System.currentTimeMillis());
        assertThat(outcome(fresh, "Most recent sample is fresh")).isEqualTo(StatusRoute.Outcome.PASSED);

        List<StatusRoute.Check> stale = route.runChecks(System.currentTimeMillis() + 60_000);
        assertThat(outcome(stale, "Most recent sample is fresh")).isEqualTo(StatusRoute.Outcome.FAILED);
        assertThat(remedy(stale, "Most recent sample is fresh")).contains("control loop has stopped");
    }

    @Test
    void reportsAMissingStaticAssetWithoutDependingOnAssetsToSaySo() {
        StatusRoute route = new StatusRoute(DashboardTestSupport.populatedPublisher(), null, OPTIONS,
                Arrays.asList("web/index.html", "web/definitely-not-packaged.js"),
                StatusRouteTest.class.getClassLoader());

        List<StatusRoute.Check> checks = route.runChecks(System.currentTimeMillis());

        assertThat(outcome(checks, "Static assets resolve on the classpath")).isEqualTo(StatusRoute.Outcome.FAILED);
        assertThat(detail(checks, "Static assets resolve on the classpath"))
                .contains("web/definitely-not-packaged.js")
                .doesNotContain("web/index.html");

        // and it still renders - the page that reports missing assets cannot need assets
        String html = route.renderHtml(System.currentTimeMillis());
        assertThat(html).contains("definitely-not-packaged").contains("<!doctype html>");
    }

    @Test
    void theRealAssetListResolvesInThisBuild() {
        StatusRoute route = new StatusRoute(DashboardTestSupport.populatedPublisher(), null, OPTIONS);

        assertThat(outcome(route.runChecks(System.currentTimeMillis()), "Static assets resolve on the classpath"))
                .as("the page shell and the chart library must actually be packaged: %s",
                        StatusRoute.DEFAULT_REQUIRED_ASSETS)
                .isEqualTo(StatusRoute.Outcome.PASSED);
    }

    @Test
    void rendersWithNoPublisherAtAllAndSaysSo() {
        StatusRoute route = new StatusRoute(null, null, OPTIONS);

        List<StatusRoute.Check> checks = route.runChecks(System.currentTimeMillis());
        assertThat(outcome(checks, "Snapshot publisher wired")).isEqualTo(StatusRoute.Outcome.FAILED);
        assertThat(outcome(checks, "Sampling is not failing")).isEqualTo(StatusRoute.Outcome.SKIPPED);
        assertThat(outcome(checks, "Static assets resolve on the classpath"))
                .as("the asset check depends on nothing else, so it must still run")
                .isEqualTo(StatusRoute.Outcome.PASSED);

        assertThat(route.renderHtml(System.currentTimeMillis())).contains("self diagnostic");
    }

    @Test
    void everyFailureCarriesARemedy() {
        StatusRoute route = new StatusRoute(null, null, OPTIONS,
                Collections.singletonList("web/nope.js"), StatusRouteTest.class.getClassLoader());

        for (StatusRoute.Check check : route.runChecks(System.currentTimeMillis())) {
            if (check.getOutcome() == StatusRoute.Outcome.FAILED) {
                assertThat(check.getRemedy()).as("remedy for '%s'", check.getName()).isNotBlank();
            }
        }
    }

    @Test
    void thePageCarriesTheAccuracyNoticeAndNoJavaScript() {
        String html = new StatusRoute(DashboardTestSupport.populatedPublisher(), null, OPTIONS)
                .renderHtml(System.currentTimeMillis());

        assertThat(html).contains("not a measurement platform");
        assertThat(html)
                .as("a diagnostic that needs the page's JavaScript is blank in exactly the cases it exists for")
                .doesNotContain("<script");
    }

    @Test
    void userSuppliedTextIsEscapedRatherThanInterpolated() {
        assertThat(StatusRoute.escape("<img src=x onerror=alert(1)>&\"'"))
                .isEqualTo("&lt;img src=x onerror=alert(1)&gt;&amp;&quot;&#39;");
    }

    @Test
    void isServedOverHttpEvenWithNothingWiredUp() throws IOException {
        try (DashboardServer server = new DashboardServer(null, null,
                DashboardTestSupport.testOptions().build()).start()) {
            RawHttp.Response response = RawHttp.get(server.getPort(), DashboardServer.STATUS_PATH);

            assertThat(response.statusCode).isEqualTo(200);
            assertThat(response.header("content-type")).startsWith("text/html");
            assertThat(response.body).contains("Snapshot publisher wired").contains("FAILED");
        }
    }

    private static StatusRoute.Outcome outcome(List<StatusRoute.Check> checks, String name) {
        return find(checks, name).getOutcome();
    }

    private static String detail(List<StatusRoute.Check> checks, String name) {
        return find(checks, name).getDetail();
    }

    private static String remedy(List<StatusRoute.Check> checks, String name) {
        return find(checks, name).getRemedy();
    }

    private static StatusRoute.Check find(List<StatusRoute.Check> checks, String name) {
        return checks.stream()
                .filter(check -> check.getName().equals(name))
                .findFirst()
                .orElseThrow(() -> new AssertionError("No check named '" + name + "' in " + checks));
    }
}
