package bz.stub.parallelconsumer.dashboard.server;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import com.google.common.hash.Hashing;
import bz.stub.parallelconsumer.dashboard.DashboardOptions;
import bz.stub.parallelconsumer.dashboard.json.SnapshotJson;
import bz.stub.parallelconsumer.dashboard.snapshot.PcSnapshot;
import bz.stub.parallelconsumer.dashboard.snapshot.SnapshotPublisher;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.web.RoutingContext;
import org.apache.kafka.common.annotation.InterfaceStability;

import java.nio.charset.StandardCharsets;
import java.util.Locale;

/**
 * Serves the machine-readable state document (plan R13) at {@code /api/state.json}.
 *
 * <h2>This handler never touches Parallel Consumer</h2>
 * <p>
 * It reads {@link SnapshotPublisher}'s volatile reference and renders the immutable value it finds there. It
 * evaluates no Micrometer gauge, holds no lock, and reaches no live PC object. That is not fastidiousness:
 * confluentinc/parallel-consumer#618 is a Prometheus scrape thread that evaluated a PC gauge and got
 * {@code ConcurrentModificationException: KafkaConsumer is not safe for multi-threaded access}. An HTTP thread doing
 * the same thing would reproduce it exactly, and this whole snapshot design exists to make that impossible rather
 * than merely unlikely.
 *
 * <h2>Conditional requests</h2>
 * <p>
 * The document changes once per control-loop sample, and a page polling faster than that would otherwise re-download
 * an identical body every time. So each response carries a <em>weak</em> {@code ETag} and a matching
 * {@code If-None-Match} gets {@code 304} with no body. Weak rather than strong because the guarantee being made is
 * semantic equivalence, not octet-for-octet identity - the tag is derived from the rendered bytes today, but a future
 * change to key ordering or number formatting must not be a correctness problem for a cached client.
 * <p>
 * Rendering is cached against the snapshot's <em>identity</em>, so N concurrent pollers between two samples cost one
 * serialisation rather than N. Identity comparison is sound here precisely because a snapshot is immutable and is
 * replaced - never mutated - on publication.
 * <p>
 * Experimental: the dashboard module is opt-in and its API may change without notice.
 */
@InterfaceStability.Unstable
public final class StateRoute implements Handler<RoutingContext> {

    /**
     * The advertised sampling cadence, so a client can choose a poll interval that matches the server instead of
     * guessing one. Carried as a header because the document body's schema belongs to {@code SnapshotJson} and this
     * is a property of the server, not of the sample.
     */
    public static final String UPDATE_INTERVAL_HEADER = "X-Pc-Dashboard-Update-Interval-Millis";

    public static final String CONTENT_TYPE = "application/json; charset=utf-8";

    private final SnapshotPublisher publisher;

    private final String updateIntervalMillis;

    /**
     * Last rendering, reused while the snapshot behind it has not been replaced. Volatile reference to an immutable
     * value, for the same reason the publisher uses one: a reader sees a whole rendering or the previous whole
     * rendering, never a half-built one. A duplicated render under a race is harmless - both produce the same bytes.
     */
    private volatile Rendered rendered;

    public StateRoute(SnapshotPublisher publisher, DashboardOptions options) {
        this.publisher = publisher;
        this.updateIntervalMillis = Long.toString(options.getUpdateInterval().toMillis());
    }

    @Override
    public void handle(RoutingContext ctx) {
        Rendered current = render();
        ctx.response()
                .putHeader("ETag", current.etag)
                // must revalidate every time: the document is a live reading, and a cached copy served without
                // asking would show a stalled consumer as a healthy one
                .putHeader("Cache-Control", "no-cache")
                .putHeader("X-Content-Type-Options", "nosniff")
                .putHeader(UPDATE_INTERVAL_HEADER, updateIntervalMillis);

        if (matches(ctx.request().getHeader("If-None-Match"), current.etag)) {
            // 304 carries no body and no Content-Type - the client already has both
            ctx.response().setStatusCode(304).end();
            return;
        }
        ctx.response()
                .putHeader("Content-Type", CONTENT_TYPE)
                .end(Buffer.buffer(current.body));
    }

    /**
     * The current document and its tag, rendering only if the published snapshot has been replaced since last time.
     */
    Rendered render() {
        PcSnapshot snapshot = publisher == null ? null : publisher.getSnapshots().getCurrent();
        Rendered cached = this.rendered;
        if (cached != null && cached.source == snapshot) {
            return cached;
        }
        // document(null) is a valid, fully-keyed "no sample yet" document, so there is exactly one rendering path
        // here rather than a normal path plus an error path that only runs in the first second of a process's life
        byte[] body = SnapshotJson.encodeUtf8(snapshot);
        Rendered fresh = new Rendered(snapshot, body, weakETag(body));
        this.rendered = fresh;
        return fresh;
    }

    /**
     * A weak entity tag for a body. 128-bit murmur3 rather than {@code Arrays.hashCode}: a 32-bit tag over a document
     * that changes once a second collides often enough to matter, and a collision here does not look like a bug - it
     * looks like a dashboard that has stopped updating, which is the single failure this dashboard exists to
     * diagnose.
     */
    @SuppressWarnings("UnstableApiUsage") // Guava's Hashing has carried @Beta since 2011 and is used across this repo
    public static String weakETag(byte[] body) {
        return "W/\"" + Hashing.murmur3_128().hashBytes(body).toString() + "\"";
    }

    /**
     * Whether an {@code If-None-Match} header covers {@code etag}. The header is a comma-separated list and may be
     * {@code *}; a client that sends the tag back as strong ({@code "..."}) when it was issued weak
     * ({@code W/"..."}) still means the same entity, and weak comparison - which is what {@code If-None-Match}
     * mandates for anything other than a range request - says so.
     */
    static boolean matches(String ifNoneMatch, String etag) {
        if (ifNoneMatch == null) {
            return false;
        }
        String header = ifNoneMatch.trim();
        if ("*".equals(header)) {
            return true;
        }
        String opaque = opaqueTag(etag);
        for (String candidate : header.split(",")) {
            if (opaque.equals(opaqueTag(candidate.trim()))) {
                return true;
            }
        }
        return false;
    }

    private static String opaqueTag(String tag) {
        String value = tag.trim();
        if (value.regionMatches(true, 0, "W/", 0, 2)) {
            value = value.substring(2).trim();
        }
        return value.toLowerCase(Locale.ROOT);
    }

    /**
     * One rendering of one snapshot: the bytes on the wire and the tag that identifies them, kept together so they
     * can never disagree.
     */
    static final class Rendered {

        /**
         * The snapshot these bytes came from, compared by identity. Null is a legitimate value - it is the
         * "no sample taken yet" document, which is cacheable like any other.
         */
        final PcSnapshot source;

        final byte[] body;

        final String etag;

        Rendered(PcSnapshot source, byte[] body, String etag) {
            this.source = source;
            this.body = body;
            this.etag = etag;
        }

        String bodyAsString() {
            return new String(body, StandardCharsets.UTF_8);
        }
    }
}
