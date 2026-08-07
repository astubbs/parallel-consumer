package io.confluent.parallelconsumer.dashboard.server;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import io.confluent.parallelconsumer.dashboard.DashboardOptions;
import io.confluent.parallelconsumer.dashboard.DashboardServer;
import io.confluent.parallelconsumer.dashboard.snapshot.SnapshotPublisher;
import io.vertx.core.json.JsonObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * The server-sent-events endpoint: delivery, deregistration, the concurrency cap, and the reaper.
 * <p>
 * The interval is turned down hard here so the tests are quick without sleeping - the same code path runs, just
 * faster.
 */
class StreamRouteTest {

    private static final Duration FAST_TICK = Duration.ofMillis(30);

    private SnapshotPublisher publisher;

    private DashboardServer server;

    @BeforeEach
    void start() {
        publisher = DashboardTestSupport.populatedPublisher();
        server = new DashboardServer(publisher, null, DashboardTestSupport.testOptions()
                .updateInterval(FAST_TICK)
                .maxConcurrentStreams(2)
                .build()).start();
    }

    @AfterEach
    void stop() {
        server.close();
    }

    @Test
    void aClientReceivesOneEventPerPublishedSnapshot() throws IOException {
        try (RawHttp.SseStream stream = RawHttp.openSse(server.getPort(), DashboardServer.STREAM_PATH)) {
            assertThat(stream.head.statusCode).isEqualTo(200);
            assertThat(stream.head.header("content-type")).isEqualTo(StreamRoute.CONTENT_TYPE);

            // the state on connect, before anything has been sampled
            RawHttp.SseEvent onConnect = stream.readEvent();
            assertThat(onConnect.event).isEqualTo(StreamRoute.EVENT_NAME);
            assertThat(onConnect.id).isEqualTo("0");
            assertThat(new JsonObject(onConnect.data).getBoolean("empty")).isTrue();

            // then exactly one event per sample, in order, with no repeats
            List<String> ids = new ArrayList<>();
            for (int sample = 1; sample <= 3; sample++) {
                publisher.sampleOnce();
                RawHttp.SseEvent event = stream.readEvent();
                ids.add(event.id);
                assertThat(new JsonObject(event.data).getLong("sampleSequence")).isEqualTo((long) sample);
            }
            assertThat(ids).containsExactly("1", "2", "3");
        }
    }

    /**
     * The difference between push and a poll wearing this protocol's clothes, made into an assertion.
     * <p>
     * The ceiling is set far longer than this test's patience. Under the timer this route used to run, an event
     * could not reach the client until the next tick, so this sample would sit unsent for ten seconds. Pushed, it
     * goes out at the moment it is published.
     */
    @Test
    void aSampleIsSentWhenItIsPublishedRatherThanOnSomeLaterTick() throws IOException {
        Duration ceilingLongerThanThisTestWillWait = Duration.ofSeconds(10);
        try (DashboardServer pushed = new DashboardServer(publisher, null, DashboardTestSupport.testOptions()
                .updateInterval(ceilingLongerThanThisTestWillWait)
                .build()).start();
             RawHttp.SseStream stream = RawHttp.openSse(pushed.getPort(), DashboardServer.STREAM_PATH)) {
            stream.readEvent();

            long startedAt = System.nanoTime();
            publisher.sampleOnce();
            RawHttp.SseEvent event = stream.readEvent();
            long waitedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedAt);

            assertThat(event.id).isEqualTo("1");
            assertThat(waitedMillis)
                    .as("pushed on publication; a %s tick would have made this wait", ceilingLongerThanThisTestWillWait)
                    .isLessThan(2000);
        }
    }

    /**
     * The control loop samples thousands of times a minute and no screen can use that, so the ceiling drops
     * intermediate samples rather than queueing them - and, critically, the newest sample still arrives. A queue
     * would instead deliver every one of them late, and drift further behind the longer the page stayed open.
     */
    @Test
    void samplesArrivingFasterThanTheCeilingAreCoalescedAndTheNewestStillArrives() throws IOException {
        int burst = 200;
        Duration ceiling = Duration.ofMillis(300);
        try (DashboardServer capped = new DashboardServer(publisher, null, DashboardTestSupport.testOptions()
                .updateInterval(ceiling)
                .build()).start();
             RawHttp.SseStream stream = RawHttp.openSse(capped.getPort(), DashboardServer.STREAM_PATH)) {
            stream.readEvent();

            for (int i = 0; i < burst; i++) {
                publisher.sampleOnce();
            }

            List<String> received = new ArrayList<>();
            while (!received.contains(Long.toString(burst))) {
                received.add(stream.readEvent().id);
            }

            assertThat(received)
                    .as("%d samples inside a %s ceiling must not become %d events", burst, ceiling, burst)
                    .hasSizeLessThan(10);
            assertThat(received.get(received.size() - 1))
                    .as("coalescing drops what is superseded; it must never drop what is newest")
                    .isEqualTo(Long.toString(burst));
        }
    }

    /**
     * The publisher outlives the server that subscribed to it, so a listener left registered is not untidiness - it
     * is a dead object being called on every control loop iteration for the rest of the consumer's life.
     */
    @Test
    void closingTheServerDeregistersItsListenerFromTheControlLoop() {
        SnapshotPublisher itsOwnPublisher = DashboardTestSupport.populatedPublisher();
        DashboardServer own = new DashboardServer(itsOwnPublisher, null,
                DashboardTestSupport.testOptions().build()).start();
        assertThat(itsOwnPublisher.getListenerCount()).isEqualTo(1);

        own.close();

        assertThat(itsOwnPublisher.getListenerCount()).isZero();
    }

    @Test
    void closingTheClientReleasesTheRegistrationRatherThanLeakingIt() throws IOException {
        RawHttp.SseStream stream = RawHttp.openSse(server.getPort(), DashboardServer.STREAM_PATH);
        stream.readEvent();
        await().atMost(5, TimeUnit.SECONDS).until(() -> streamRoute().getActiveStreamCount() == 1);

        stream.close();

        await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
            assertThat(streamRoute().getActiveStreamCount()).isZero();
            assertThat(streamRoute().getSubscriptions()).isEmpty();
        });
    }

    @Test
    void pastTheCapTheServerAnswers503WithRetryAfterAndLeavesOpenStreamsAlone() throws IOException {
        try (RawHttp.SseStream first = RawHttp.openSse(server.getPort(), DashboardServer.STREAM_PATH);
             RawHttp.SseStream second = RawHttp.openSse(server.getPort(), DashboardServer.STREAM_PATH)) {
            first.readEvent();
            second.readEvent();

            RawHttp.Response rejected = RawHttp.get(server.getPort(), DashboardServer.STREAM_PATH);

            assertThat(rejected.statusCode).isEqualTo(503);
            assertThat(rejected.header("retry-after")).isNotNull();
            assertThat(Integer.parseInt(rejected.header("retry-after"))).isGreaterThanOrEqualTo(1);
            assertThat(rejected.body).contains("/api/state.json").contains("maxConcurrentStreams");

            // the point of the cap is that it protects the streams already open, so prove they still work
            publisher.sampleOnce();
            assertThat(first.readEvent().id).isEqualTo("1");
            assertThat(second.readEvent().id).isEqualTo("1");
        }
    }

    @Test
    void aStreamWhoseClientStoppedReadingIsReapedAfterTheIdleTimeout() {
        StreamRoute route = new StreamRoute(publisher, DashboardOptions.builder()
                .updateInterval(FAST_TICK)
                .streamIdleTimeout(Duration.ofMillis(100))
                .build());
        FakeSink vanished = new FakeSink();
        FakeSink healthy = new FakeSink();
        long start = 1_000_000L;
        assertThat(route.subscribe(vanished, start)).isNotNull();
        assertThat(route.subscribe(healthy, start)).isNotNull();

        // the vanished client's socket buffer filled and nothing is draining it
        vanished.writeQueueFull = true;

        route.reap(start + 50);
        assertThat(route.getActiveStreamCount()).as("not yet past the timeout").isEqualTo(2);

        route.reap(start + 500);

        assertThat(vanished.closed).isTrue();
        assertThat(healthy.closed).as("a client that is keeping up is not collateral").isFalse();
        assertThat(route.getActiveStreamCount()).isEqualTo(1);
        assertThat(route.getSubscriptions()).hasSize(1);
    }

    @Test
    void aStreamThatSimplyHasNothingToSendIsNotReaped() {
        StreamRoute route = new StreamRoute(publisher, DashboardOptions.builder()
                .streamIdleTimeout(Duration.ofMillis(10))
                .build());
        FakeSink idleButAlive = new FakeSink();
        long start = 1_000_000L;
        route.subscribe(idleButAlive, start);

        // hours pass with no sample published at all - which is what watching an idle consumer looks like
        route.reap(start + TimeUnit.HOURS.toMillis(3));

        assertThat(idleButAlive.closed)
                .as("time alone is not the signal; a client that is still draining is still there")
                .isFalse();
        assertThat(route.getActiveStreamCount()).isEqualTo(1);
    }

    @Test
    void aSinkThatIsAlreadyClosedIsDroppedOnTheNextSweep() {
        StreamRoute route = new StreamRoute(publisher, DashboardOptions.builder().build());
        FakeSink gone = new FakeSink();
        route.subscribe(gone, 0);
        gone.closed = true;

        route.reap(1);

        assertThat(route.getActiveStreamCount()).isZero();
    }

    @Test
    void aTickSendsNothingWhenTheSampleHasNotMoved() {
        StreamRoute route = new StreamRoute(publisher, DashboardOptions.builder().build());
        FakeSink sink = new FakeSink();
        route.subscribe(sink, 0);
        publisher.sampleOnce();

        route.broadcastIfNew(0);
        route.broadcastIfNew(1);
        route.broadcastIfNew(2);

        assertThat(sink.writes).as("one sample, one event - repeated ticks must not repeat it").hasSize(1);
        assertThat(sink.writes.get(0)).startsWith("id: 1\nevent: snapshot\ndata: {");
    }

    private StreamRoute streamRoute() {
        // the route instance the server wired in; reached through the server so the test observes the real one
        return server.getStreamRoute();
    }

    /**
     * An {@link StreamRoute.EventSink} whose progress and closure a test controls directly.
     * <p>
     * A vanished client cannot be reproduced with a real socket in a unit test: a peer that stops reading stays
     * invisible until kernel and Netty buffers fill, which is minutes and megabytes away. A test that waited for that
     * would be testing the kernel's buffer sizing, not the reaper.
     */
    private static final class FakeSink implements StreamRoute.EventSink {

        private final List<String> writes = new ArrayList<>();

        private boolean closed;

        private boolean writeQueueFull;

        @Override
        public boolean isClosed() {
            return closed;
        }

        @Override
        public boolean isWriteQueueFull() {
            return writeQueueFull;
        }

        @Override
        public void write(String chunk) {
            writes.add(chunk);
        }

        @Override
        public void close() {
            closed = true;
        }
    }
}
