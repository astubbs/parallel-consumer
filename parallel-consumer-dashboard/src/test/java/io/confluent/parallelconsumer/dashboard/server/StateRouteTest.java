package io.confluent.parallelconsumer.dashboard.server;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import io.confluent.parallelconsumer.dashboard.DashboardServer;
import io.confluent.parallelconsumer.dashboard.snapshot.SnapshotPublisher;
import io.vertx.core.json.JsonObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The state document endpoint: valid JSON, conditional requests, and the caching that keeps a page polling faster
 * than the sample rate from re-serialising the same document every time.
 */
class StateRouteTest {

    private SnapshotPublisher publisher;

    private DashboardServer server;

    @BeforeEach
    void start() {
        publisher = DashboardTestSupport.populatedPublisher();
        publisher.sampleOnce();
        server = new DashboardServer(publisher, null, DashboardTestSupport.testOptions().build()).start();
    }

    @AfterEach
    void stop() {
        server.close();
    }

    @Test
    void returnsValidJsonCarryingTheSampledState() throws IOException {
        RawHttp.Response response = RawHttp.get(server.getPort(), DashboardServer.STATE_PATH);

        assertThat(response.statusCode).isEqualTo(200);
        assertThat(response.header("content-type")).isEqualTo(StateRoute.CONTENT_TYPE);

        JsonObject document = new JsonObject(response.body);
        assertThat(document.getInteger("schemaVersion")).isEqualTo(1);
        assertThat(document.getString("notice")).contains("not a measurement platform");
        assertThat(document.getBoolean("registryPopulated")).isTrue();
        assertThat(document.getJsonArray("partitions")).hasSize(3);
        // offsets travel as strings - a page that got a number here would silently round past 2^53
        assertThat(document.getJsonArray("partitions").getJsonObject(0).getString("highestSeenOffset"))
                .isEqualTo("150");
    }

    @Test
    void servesAValidDocumentBeforeAnySampleHasBeenTaken() throws IOException {
        try (DashboardServer fresh = new DashboardServer(DashboardTestSupport.populatedPublisher(), null,
                DashboardTestSupport.testOptions().build()).start()) {
            RawHttp.Response response = RawHttp.get(fresh.getPort(), DashboardServer.STATE_PATH);

            assertThat(response.statusCode).isEqualTo(200);
            JsonObject document = new JsonObject(response.body);
            assertThat(document.getBoolean("empty")).isTrue();
            assertThat(document.getValue("captureEpochMillis")).isNull();
        }
    }

    @Test
    void advertisesTheUpdateIntervalSoTheClientNeedNotGuess() throws IOException {
        RawHttp.Response response = RawHttp.get(server.getPort(), DashboardServer.STATE_PATH);

        assertThat(response.header(StateRoute.UPDATE_INTERVAL_HEADER))
                .isEqualTo(String.valueOf(server.getOptions().getUpdateInterval().toMillis()));
    }

    @Test
    void aRepeatWithIfNoneMatchGetsThreeOhFourAndNoBody() throws IOException {
        RawHttp.Response first = RawHttp.get(server.getPort(), DashboardServer.STATE_PATH);
        String etag = first.header("etag");
        assertThat(etag).startsWith("W/\"");

        Map<String, String> conditional = new LinkedHashMap<>();
        conditional.put("If-None-Match", etag);
        RawHttp.Response second = RawHttp.request("GET", server.getPort(), DashboardServer.STATE_PATH, conditional);

        assertThat(second.statusCode).isEqualTo(304);
        assertThat(second.body).isEmpty();
        assertThat(second.header("etag")).isEqualTo(etag);
    }

    @Test
    void theTagChangesWhenANewSampleIsPublished() throws IOException {
        String before = RawHttp.get(server.getPort(), DashboardServer.STATE_PATH).header("etag");

        publisher.sampleOnce();

        RawHttp.Response after = RawHttp.get(server.getPort(), DashboardServer.STATE_PATH);
        assertThat(after.statusCode).isEqualTo(200);
        assertThat(after.header("etag")).isNotEqualTo(before);

        Map<String, String> stale = new LinkedHashMap<>();
        stale.put("If-None-Match", before);
        assertThat(RawHttp.request("GET", server.getPort(), DashboardServer.STATE_PATH, stale).statusCode)
                .as("a stale tag must not be answered 304 - that is a dashboard frozen on an old reading")
                .isEqualTo(200);
    }

    @Test
    void ifNoneMatchIsComparedWeaklyAndAcrossAList() {
        assertThat(StateRoute.matches("W/\"abc\"", "W/\"abc\"")).isTrue();
        assertThat(StateRoute.matches("\"abc\"", "W/\"abc\"")).as("weak comparison ignores the W/ prefix").isTrue();
        assertThat(StateRoute.matches("W/\"zzz\", W/\"abc\"", "W/\"abc\"")).isTrue();
        assertThat(StateRoute.matches("*", "W/\"abc\"")).isTrue();
        assertThat(StateRoute.matches("W/\"zzz\"", "W/\"abc\"")).isFalse();
        assertThat(StateRoute.matches(null, "W/\"abc\"")).isFalse();
    }

    @Test
    void renderingIsReusedWhileTheSnapshotHasNotBeenReplaced() {
        StateRoute route = new StateRoute(publisher, server.getOptions());

        StateRoute.Rendered first = route.render();
        assertThat(route.render()).isSameAs(first);

        publisher.sampleOnce();
        StateRoute.Rendered second = route.render();
        assertThat(second).isNotSameAs(first);
        assertThat(second.etag).isNotEqualTo(first.etag);
        assertThat(second.bodyAsString()).contains("\"sampleSequence\":2");
    }

    @Test
    void distinctBodiesGetDistinctTags() {
        assertThat(StateRoute.weakETag("a".getBytes()))
                .isNotEqualTo(StateRoute.weakETag("b".getBytes()))
                .startsWith("W/\"");
    }
}
