package io.confluent.parallelconsumer.dashboard.server;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import io.confluent.parallelconsumer.dashboard.DashboardOptions;
import io.confluent.parallelconsumer.dashboard.json.SnapshotJson;
import io.confluent.parallelconsumer.dashboard.snapshot.PcSnapshot;
import io.confluent.parallelconsumer.dashboard.snapshot.SnapshotPublisher;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.RoutingContext;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.annotation.InterfaceStability;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Server-sent events at {@code /api/stream}: one event per published snapshot, pushed rather than polled.
 *
 * <h2>Why push, and why not WebSocket</h2>
 * <p>
 * Polling costs a request per tick whether or not anything changed, and forces the client to keep interval and
 * in-flight bookkeeping. Server-sent events cost neither. WebSocket is deliberately not used: it buys nothing over
 * SSE for a one-way feed, and a bidirectional channel into a strictly read-only dashboard is attack surface with no
 * purpose (plan R24).
 * <p>
 * Vert.x is event-loop based, so an idle stream costs a registration rather than a parked thread. The cap below is
 * therefore not a resource constraint - it is a bound on an accidental scrape loop, and past it the server answers
 * {@code 503} with {@code Retry-After} so the client degrades to polling instead of treating the dashboard as broken
 * (KTD6). Existing streams are untouched by a rejection.
 *
 * <h2>How a snapshot reaches the wire, and the one honest caveat</h2>
 * <p>
 * {@link SnapshotPublisher} offers a volatile value, not a callback, so this class re-reads it on a timer at
 * {@link DashboardOptions#getUpdateInterval()} and emits when {@link PcSnapshot#getSampleSequence()} has moved. One
 * timer serves every subscriber, so N open tabs cost one read rather than N.
 * <p>
 * The caveat, stated rather than hidden: if Parallel Consumer's control loop publishes faster than the update
 * interval, intermediate samples are <em>coalesced</em> - the client sees the latest, not every one. That is the
 * correct behaviour for a sampled operational view (plan R42) and it is what keeps client-side smoothing from ever
 * raising the sampling rate against the running instance (plan R19). It is not a guarantee of every sample.
 * <p>
 * A newly-connected client is sent the current document immediately, before the next tick, so a tab opened between
 * ticks renders at once instead of showing an empty page for up to an interval.
 *
 * <h2>Reaping</h2>
 * <p>
 * A client that closes cleanly deregisters through the response's close handler. A client that <em>vanishes</em> -
 * killed tab, closed lid, severed network - does not, and its write queue fills and stays full because nothing is
 * draining it. That is the signal: a subscription whose sink has been unable to make progress for longer than
 * {@link DashboardOptions#getStreamIdleTimeout()} is closed and dropped. Time alone is not the signal, because a
 * healthy client watching an idle consumer also receives nothing for a long time - it just keeps draining.
 * <p>
 * Experimental: the dashboard module is opt-in and its API may change without notice.
 */
@InterfaceStability.Unstable
@Slf4j
public final class StreamRoute implements Handler<RoutingContext>, AutoCloseable {

    public static final String CONTENT_TYPE = "text/event-stream; charset=utf-8";

    /**
     * Named so a client can select on it rather than matching every message on the connection.
     */
    public static final String EVENT_NAME = "snapshot";

    /**
     * Sentinel meaning "this subscription has been sent nothing", chosen below the publisher's 1-based sequence and
     * below the 0 that stands for "no sample taken", so the first thing a subscription is offered always reaches it.
     */
    static final long NOTHING_SENT_YET = -1L;

    private final SnapshotPublisher publisher;

    private final long updateIntervalMillis;

    private final long idleTimeoutMillis;

    private final int maxConcurrentStreams;

    private final List<Subscription> subscriptions = new CopyOnWriteArrayList<>();

    private final AtomicInteger activeStreams = new AtomicInteger();

    private final AtomicBoolean closed = new AtomicBoolean();

    private Vertx vertx;

    private long broadcastTimerId = -1;

    private long reaperTimerId = -1;

    public StreamRoute(SnapshotPublisher publisher, DashboardOptions options) {
        this.publisher = publisher;
        this.updateIntervalMillis = Math.max(1, options.getUpdateInterval().toMillis());
        this.idleTimeoutMillis = Math.max(1, options.getStreamIdleTimeout().toMillis());
        this.maxConcurrentStreams = options.getMaxConcurrentStreams();
    }

    /**
     * Starts the broadcast and reaper timers. Separate from construction so the route can be wired into a router
     * before anything begins ticking.
     */
    public void start(Vertx vertx) {
        this.vertx = vertx;
        this.broadcastTimerId = vertx.setPeriodic(updateIntervalMillis, id -> broadcastIfNew(System.currentTimeMillis()));
        // often enough that the timeout means roughly what it says, never so often that it is its own load
        long reaperPeriod = Math.max(100, Math.min(updateIntervalMillis, idleTimeoutMillis / 4));
        this.reaperTimerId = vertx.setPeriodic(reaperPeriod, id -> reap(System.currentTimeMillis()));
    }

    @Override
    public void handle(RoutingContext ctx) {
        if (!tryAcquireSlot()) {
            long retryAfterSeconds = Math.max(1, (updateIntervalMillis + 999) / 1000);
            ctx.response()
                    .setStatusCode(503)
                    .putHeader("Retry-After", Long.toString(retryAfterSeconds))
                    .putHeader("Content-Type", "text/plain; charset=utf-8")
                    .putHeader("X-Content-Type-Options", "nosniff")
                    .end("503 Service Unavailable. This dashboard already has " + maxConcurrentStreams
                            + " concurrent event streams open, which is its configured maximum. Poll "
                            + "/api/state.json instead, or raise DashboardOptions.maxConcurrentStreams. Streams "
                            + "already open are unaffected.");
            return;
        }

        HttpServerResponse response = ctx.response();
        response.setChunked(true);
        response.putHeader("Content-Type", CONTENT_TYPE);
        response.putHeader("Cache-Control", "no-cache, no-transform");
        response.putHeader("X-Content-Type-Options", "nosniff");
        // tells a reverse proxy not to buffer, which would otherwise hold every event until the buffer filled and
        // make a live dashboard look frozen
        response.putHeader("X-Accel-Buffering", "no");

        Subscription subscription = new Subscription(new ResponseEventSink(response), System.currentTimeMillis());
        subscriptions.add(subscription);
        response.closeHandler(v -> release(subscription));
        response.endHandler(v -> release(subscription));

        long now = System.currentTimeMillis();
        // the reconnect hint the EventSource API honours natively, so the client needs no reconnect bookkeeping
        deliver(subscription, "retry: " + updateIntervalMillis + "\n\n", now);
        // and the current state at once, so a tab opened between ticks is not blank until the next one
        sendCurrent(subscription, now);
    }

    /**
     * Re-reads the published snapshot and sends it to every subscription that has not already had that sample.
     * <p>
     * The dedupe is <em>per subscription</em>, not global, and that is the point: a global "last broadcast sequence"
     * would either send a joining client the current sample twice (once on join, once on the next tick) or skip a
     * client that joined after the tick. Per-subscription bookkeeping makes "exactly one event per sample per
     * client" true by construction rather than by timing.
     * <p>
     * Package-visible so a test can drive ticks deterministically instead of sleeping through timers.
     */
    void broadcastIfNew(long nowMillis) {
        if (subscriptions.isEmpty()) {
            return;
        }
        PcSnapshot snapshot = currentSnapshot();
        long sequence = snapshot == null ? 0 : snapshot.getSampleSequence();
        String frame = null;
        for (Subscription subscription : subscriptions) {
            if (subscription.lastSentSequence == sequence) {
                continue;
            }
            if (frame == null) {
                // rendered once for the whole fan-out, not once per client
                frame = frame(snapshot);
            }
            subscription.lastSentSequence = sequence;
            deliver(subscription, frame, nowMillis);
        }
    }

    private void sendCurrent(Subscription subscription, long nowMillis) {
        PcSnapshot snapshot = currentSnapshot();
        subscription.lastSentSequence = snapshot == null ? 0 : snapshot.getSampleSequence();
        deliver(subscription, frame(snapshot), nowMillis);
    }

    /**
     * Closes and drops subscriptions whose client has gone: closed connections, and connections that have been
     * unable to accept a write for longer than the idle timeout.
     * <p>
     * Package-visible so a test can drive the reaper against a fake sink, which is the only way to reproduce a
     * vanished client deterministically - a real TCP peer that stops reading takes minutes of kernel buffering to
     * become observable, so a test that waited for it would be a test that was really testing the kernel.
     */
    void reap(long nowMillis) {
        for (Subscription subscription : subscriptions) {
            if (subscription.sink.isClosed()) {
                release(subscription);
                continue;
            }
            if (!subscription.sink.isWriteQueueFull()) {
                subscription.lastProgressMillis = nowMillis;
                continue;
            }
            if (nowMillis - subscription.lastProgressMillis > idleTimeoutMillis) {
                log.debug("Dashboard event stream reaped after {}ms without progress - the client stopped reading.",
                        nowMillis - subscription.lastProgressMillis);
                closeQuietly(subscription);
                release(subscription);
            }
        }
    }

    /**
     * How many event streams are open right now. What the cap is counted against, and what a diagnostic reports.
     */
    public int getActiveStreamCount() {
        return activeStreams.get();
    }

    /**
     * Registers an arbitrary sink, honouring the cap, and returns it - or null if the cap is reached. This is what
     * {@link #handle(RoutingContext)} does underneath, exposed so the cap and the reaper can be tested without a
     * socket.
     */
    Subscription subscribe(EventSink sink, long nowMillis) {
        if (!tryAcquireSlot()) {
            return null;
        }
        Subscription subscription = new Subscription(sink, nowMillis);
        subscriptions.add(subscription);
        return subscription;
    }

    /**
     * The subscriptions currently registered. Package-visible for assertions about leaks - a stream that was closed
     * but never deregistered is exactly the bug this exposes.
     */
    Collection<Subscription> getSubscriptions() {
        return subscriptions;
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        if (vertx != null) {
            if (broadcastTimerId >= 0) {
                vertx.cancelTimer(broadcastTimerId);
            }
            if (reaperTimerId >= 0) {
                vertx.cancelTimer(reaperTimerId);
            }
        }
        for (Subscription subscription : subscriptions) {
            closeQuietly(subscription);
            release(subscription);
        }
    }

    private PcSnapshot currentSnapshot() {
        return publisher == null ? null : publisher.getSnapshots().getCurrent();
    }

    /**
     * One SSE frame. {@code id} is the sample sequence, so a reconnecting client's {@code Last-Event-ID} says which
     * sample it last saw - and a gap in the ids is a visible, diagnosable coalescing rather than a silent one.
     */
    static String frame(PcSnapshot snapshot) {
        long sequence = snapshot == null ? 0 : snapshot.getSampleSequence();
        // the document is compact JSON with no newline in it, so it needs exactly one data: line - SSE would
        // otherwise require every embedded newline to be re-prefixed
        return "id: " + sequence + "\nevent: " + EVENT_NAME + "\ndata: " + SnapshotJson.encode(snapshot) + "\n\n";
    }

    private void deliver(Subscription subscription, String frame, long nowMillis) {
        try {
            subscription.sink.write(frame);
            if (!subscription.sink.isWriteQueueFull()) {
                subscription.lastProgressMillis = nowMillis;
            }
        } catch (RuntimeException e) {
            // a write failing means the peer has already gone; that is routine, so it is not a warning
            log.debug("Dashboard event stream write failed; dropping the subscription.", e);
            closeQuietly(subscription);
            release(subscription);
        }
    }

    private boolean tryAcquireSlot() {
        while (true) {
            int current = activeStreams.get();
            if (current >= maxConcurrentStreams) {
                return false;
            }
            if (activeStreams.compareAndSet(current, current + 1)) {
                return true;
            }
        }
    }

    /**
     * Deregisters a subscription and returns its slot. Idempotent, because Vert.x can fire both the close and the end
     * handler for one connection and a double release would let the cap drift downward until the server refused
     * every stream.
     */
    private void release(Subscription subscription) {
        if (!subscription.released.compareAndSet(false, true)) {
            return;
        }
        subscriptions.remove(subscription);
        activeStreams.decrementAndGet();
    }

    private static void closeQuietly(Subscription subscription) {
        try {
            subscription.sink.close();
        } catch (RuntimeException e) {
            log.debug("Closing a dashboard event stream failed; it is being dropped anyway.", e);
        }
    }

    /**
     * Where an event goes. An interface rather than {@link HttpServerResponse} directly so the cap, the reaper and
     * the leak behaviour are testable without a socket - see {@link #reap(long)} for why that matters.
     * <p>
     * Experimental: the dashboard module is opt-in and its API may change without notice.
     */
    @InterfaceStability.Unstable
    public interface EventSink {

        boolean isClosed();

        /**
         * Whether the sink cannot currently accept more data. Sustained truth means the peer has stopped reading.
         */
        boolean isWriteQueueFull();

        void write(String chunk);

        void close();
    }

    /**
     * One registered client.
     */
    static final class Subscription {

        final EventSink sink;

        final AtomicBoolean released = new AtomicBoolean();

        /**
         * The last time this sink was able to accept a write. Only ever touched from the event loop.
         */
        volatile long lastProgressMillis;

        /**
         * The sample sequence this client has already been sent, so a tick cannot repeat it. See
         * {@link #broadcastIfNew(long)} for why this is per subscription rather than global.
         */
        volatile long lastSentSequence = NOTHING_SENT_YET;

        Subscription(EventSink sink, long nowMillis) {
            this.sink = sink;
            this.lastProgressMillis = nowMillis;
        }
    }

    /**
     * The production sink: a chunked HTTP response.
     */
    private static final class ResponseEventSink implements EventSink {

        private final HttpServerResponse response;

        private ResponseEventSink(HttpServerResponse response) {
            this.response = response;
        }

        @Override
        public boolean isClosed() {
            return response.closed() || response.ended();
        }

        @Override
        public boolean isWriteQueueFull() {
            return response.writeQueueFull();
        }

        @Override
        public void write(String chunk) {
            response.write(chunk);
        }

        @Override
        public void close() {
            if (!response.closed() && !response.ended()) {
                response.end();
            }
        }
    }
}
