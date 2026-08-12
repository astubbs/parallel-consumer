package io.confluent.parallelconsumer.dashboard.server;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import io.confluent.parallelconsumer.dashboard.DashboardOptions;
import io.confluent.parallelconsumer.dashboard.json.SnapshotJson;
import io.confluent.parallelconsumer.dashboard.snapshot.PcSnapshot;
import io.confluent.parallelconsumer.dashboard.snapshot.SnapshotPublisher;
import io.vertx.core.Context;
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
 * <h2>How a snapshot reaches the wire</h2>
 * <p>
 * This route <strong>subscribes</strong> to {@link SnapshotPublisher} through
 * {@link SnapshotPublisher#addListener(SnapshotPublisher.SnapshotListener) addListener} and sends when a snapshot is
 * published. It does not re-read the published value on a timer of its own.
 * <p>
 * That distinction is the whole point of the endpoint. A timer re-reading a volatile is a poll wearing this
 * protocol's clothes: it delivers a sample late by up to its own period, it wakes up when nothing has happened, and
 * its period silently becomes the floor on how fresh the page can ever be. Turning that timer up would only produce
 * a faster poll. So the timer is gone, and the only remaining time constant is a <em>ceiling</em> on the send rate -
 * see below.
 *
 * <h2>Two threads, and the handoff between them</h2>
 * <p>
 * {@link #onSnapshotPublished()} runs on Parallel Consumer's control thread. It does one compare-and-set and, at
 * most once per outstanding flush, one non-blocking task handoff to the event loop. It renders nothing, touches no
 * socket, takes no lock and reads no snapshot. Everything expensive - reading the published pair, serialising it,
 * writing it to N sockets - happens on the event loop in {@link #flush()}.
 * <p>
 * That split is not tidiness. confluentinc/parallel-consumer#618 is a scrape thread that evaluated a Parallel
 * Consumer gauge off the control thread and got {@code ConcurrentModificationException: KafkaConsumer is not safe
 * for multi-threaded access}; the snapshot design exists so that the HTTP layer can never be that thread. A
 * subscriber that did its work inline would additionally put JSON encoding and socket writes on the critical path of
 * every control loop iteration, which is a throughput regression in somebody else's consumer.
 *
 * <h2>The ceiling, and what it coalesces</h2>
 * <p>
 * Sampling happens on every control loop pass, which can be thousands of times a second, and nothing on a screen
 * refreshing 60 times a second can use that. So {@link DashboardOptions#getUpdateInterval()} is a <em>maximum send
 * rate</em>: at most one event per interval per client. It is a ceiling, not a period - when the instance is quiet,
 * nothing is sent and nothing wakes up; when a sample arrives after a long silence it goes out immediately rather
 * than waiting for a tick.
 * <p>
 * Inside a ceiling window, intermediate samples are <em>dropped, not queued</em>: the deferred flush sends whatever
 * is newest when it runs. Queueing them would build an ever-growing backlog of stale frames on a fast instance,
 * which is the failure mode where a live dashboard drifts further behind the longer you watch it. Coalescing is the
 * correct behaviour for a sampled operational view (plan R42), and it is what keeps client-side smoothing from ever
 * raising the sampling rate against the running instance (plan R19).
 * <p>
 * A newly-connected client is sent the current document immediately, so a tab opened between samples renders at once
 * instead of showing an empty page.
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
public final class StreamRoute implements Handler<RoutingContext>, SnapshotPublisher.SnapshotListener, AutoCloseable {

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

    /**
     * The floor on the reconnect delay this server suggests to {@code EventSource}. The send ceiling can be tens of
     * milliseconds, and suggesting that as a reconnect delay would turn a server that has just died into a target
     * for a client reconnecting ten times a second - the browser honours this number literally.
     */
    private static final long MINIMUM_RETRY_HINT_MILLIS = 1000;

    /**
     * How often the reaper sweeps, bounded at both ends: often enough that the idle timeout means roughly what it
     * says, never more than once a second. Deliberately independent of the send ceiling - the two answer different
     * questions, and deriving the sweep from the ceiling made a faster page also mean a hotter reaper.
     */
    private static final long MINIMUM_REAPER_PERIOD_MILLIS = 250;

    private static final long MAXIMUM_REAPER_PERIOD_MILLIS = 1000;

    private final SnapshotPublisher publisher;

    /**
     * The coalescing ceiling: the shortest gap allowed between two events on one stream. See the class javadoc -
     * this is a maximum rate, not a period, and nothing ticks at it.
     */
    private final long minSendIntervalMillis;

    private final long idleTimeoutMillis;

    private final int maxConcurrentStreams;

    private final List<Subscription> subscriptions = new CopyOnWriteArrayList<>();

    private final AtomicInteger activeStreams = new AtomicInteger();

    private final AtomicBoolean closed = new AtomicBoolean();

    /**
     * Set by the control thread to claim the single outstanding flush, cleared by the event loop when that flush
     * actually sends. While it is set, further publications cost the control thread one failed compare-and-set and
     * nothing else - which is what bounds this route's cost on the control loop no matter how fast the loop runs.
     */
    private final AtomicBoolean flushClaimed = new AtomicBoolean();

    private Vertx vertx;

    /**
     * The one event-loop context every flush runs on - the first one and every deferred one. Captured once so that
     * they are serialised against each other on a single thread, which is what lets {@link #lastSendMillis} be a
     * plain field and what stops two flushes writing to one response concurrently.
     * <p>
     * Volatile because it is written by whichever thread called {@link #start(Vertx)} and read by Parallel
     * Consumer's control thread in {@link #onSnapshotPublished()}.
     */
    private volatile Context context;

    /**
     * When the last event was written, for measuring the ceiling against. Event loop only.
     */
    private long lastSendMillis;

    private long reaperTimerId = -1;

    public StreamRoute(SnapshotPublisher publisher, DashboardOptions options) {
        this.publisher = publisher;
        this.minSendIntervalMillis = Math.max(1, options.getUpdateInterval().toMillis());
        this.idleTimeoutMillis = Math.max(1, options.getStreamIdleTimeout().toMillis());
        this.maxConcurrentStreams = options.getMaxConcurrentStreams();
    }

    /**
     * Subscribes to the publisher and starts the reaper sweep. Separate from construction so the route can be wired
     * into a router before it starts receiving anything.
     */
    public void start(Vertx vertx) {
        this.vertx = vertx;
        this.context = vertx.getOrCreateContext();
        // backdated by a full ceiling so the very first sample after startup goes out at once rather than being
        // deferred by an interval that has not conceptually started yet
        this.lastSendMillis = System.currentTimeMillis() - minSendIntervalMillis;
        // the reaper needs no particular context - it touches only concurrent structures - so it stays a plain
        // periodic timer, unlike the flush path
        this.reaperTimerId = vertx.setPeriodic(reaperPeriodMillis(), id -> reap(System.currentTimeMillis()));
        if (publisher != null) {
            publisher.addListener(this);
        }
    }

    private long reaperPeriodMillis() {
        return Math.max(MINIMUM_REAPER_PERIOD_MILLIS, Math.min(MAXIMUM_REAPER_PERIOD_MILLIS, idleTimeoutMillis / 4));
    }

    /**
     * <strong>Runs on Parallel Consumer's control thread.</strong> Claims the single outstanding flush and hands it
     * to the event loop; does nothing else, ever. See the class javadoc for why this method's cost is a hard
     * constraint rather than a preference.
     */
    @Override
    public void onSnapshotPublished() {
        Context loop = this.context;
        if (loop == null || closed.get()) {
            return;
        }
        if (flushClaimed.compareAndSet(false, true)) {
            loop.runOnContext(v -> flush());
        }
    }

    /**
     * <strong>Runs on the event loop.</strong> Sends the newest published snapshot, or - if the ceiling has not
     * elapsed - arranges to do so when it has.
     * <p>
     * The claim is deliberately held across the deferral rather than released and re-taken. Holding it is what makes
     * the wait <em>coalescing</em>: every sample published while the ceiling is still running finds the claim taken,
     * costs the control thread one failed compare-and-set, and queues nothing. Exactly one deferred flush is ever
     * outstanding, and when it runs it sends whatever is newest at that moment.
     */
    private void flush() {
        if (closed.get()) {
            flushClaimed.set(false);
            return;
        }
        long now = System.currentTimeMillis();
        long sinceLastSend = now - lastSendMillis;
        if (sinceLastSend < minSendIntervalMillis) {
            try {
                // setTimer inside a context handler binds to that context, so the deferred flush lands back on
                // this same thread
                vertx.setTimer(minSendIntervalMillis - sinceLastSend, id -> flush());
            } catch (RuntimeException | Error e) {
                // if the timer was never armed, nothing will ever release the claim, and this route stops sending
                // for the life of the process. That is currently unreachable only because close() sets `closed`
                // before closing Vert.x - an invariant that lives in another class's call ordering, which is not
                // where a silent permanent stall should have its only guard
                flushClaimed.set(false);
                throw e;
            }
            return;
        }
        // released BEFORE the snapshot is read, so a sample published during this send re-arms rather than being
        // lost. The cost of getting that order wrong is a stream that silently stops on the last sample of a burst;
        // the cost of this order is at worst one extra flush that broadcastIfNew finds nothing to do in.
        flushClaimed.set(false);
        lastSendMillis = now;
        broadcastIfNew(now);
    }

    @Override
    public void handle(RoutingContext ctx) {
        if (!tryAcquireSlot()) {
            long retryAfterSeconds = Math.max(1, (minSendIntervalMillis + 999) / 1000);
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
        response.closeHandler(v -> release(subscription));
        response.endHandler(v -> release(subscription));

        // REGISTRATION AND THE OPENING FRAMES RUN ON THE FLUSH CONTEXT, NOT THIS REQUEST'S.
        //
        // This handler runs on the connection's own event loop, which is not the context flush() runs on. Registering
        // here and then writing here opened a window between the add and the first write in which a published sample
        // could reach flush(), find this brand-new subscription with lastSentSequence == NOTHING_SENT_YET, and write
        // to the same response from the other loop - so the retry hint, the opening state and a live sample could be
        // emitted in any order, and the sequence bookkeeping could double-send. Everything that touches
        // `subscriptions` or writes an event now happens on one thread, which is what the rest of this class already
        // assumes.
        onFlushContext(() -> {
            if (closed.get()) {
                // the route was closed between the slot acquisition and this task, so this subscription would never
                // be torn down by close()'s sweep - it must not be registered at all
                closeQuietly(subscription);
                release(subscription);
                return;
            }
            subscriptions.add(subscription);
            long now = System.currentTimeMillis();
            // the reconnect hint the EventSource API honours natively, so the client needs no reconnect bookkeeping.
            // Floored: this is how hard a client hammers a server that has GONE, which is not the same question as
            // how fast a healthy server may send.
            deliver(subscription, "retry: " + Math.max(MINIMUM_RETRY_HINT_MILLIS, minSendIntervalMillis) + "\n\n",
                    now);
            // and the current state at once, so a tab opened between ticks is not blank until the next one
            sendCurrent(subscription, now);
        });
    }

    /**
     * Runs {@code action} on the same context every flush runs on, so everything that touches a subscription or
     * writes an event is serialised onto one thread.
     * <p>
     * Falls back to running inline when {@link #start(Vertx)} has not been called: with no context there is no flush
     * that could be running, so there is nothing to serialise against - and that is the shape the unit tests drive.
     */
    private void onFlushContext(Runnable action) {
        Context loop = this.context;
        if (loop == null) {
            action.run();
            return;
        }
        loop.runOnContext(v -> action.run());
    }

    /**
     * Reads the published snapshot and sends it to every subscription that has not already had that sample.
     * <p>
     * The per-subscription sequence check is also what makes "do not resend an unchanged sample" true: a flush that
     * finds the publisher has not moved since the last one writes nothing at all, rather than re-sending bytes the
     * client already has.
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
        // FIRST, before anything else is torn down: the publisher outlives this server, so a listener left
        // registered would keep calling into a closed route on every control loop iteration for the life of the
        // consumer
        if (publisher != null) {
            publisher.removeListener(this);
        }
        if (vertx != null && reaperTimerId >= 0) {
            vertx.cancelTimer(reaperTimerId);
        }
        // a deferred flush may already be armed; it has no id to cancel because at most one exists, and it checks
        // `closed` on entry and returns without touching a subscription
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
        if (subscription.sink.isWriteQueueFull()) {
            // DROPPED, NOT QUEUED - which is this class's stated contract for an intermediate sample, applied to the
            // one case that was violating it. Testing fullness only AFTER the write meant every broadcast in the
            // window between a client going quiet and the reaper's idle timeout expiring buffered another whole
            // document on the heap of the user's consumer, for a peer that had already stopped reading.
            return;
        }
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
