package io.confluent.parallelconsumer.dashboard.json;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import io.confluent.parallelconsumer.dashboard.snapshot.EncodingSnapshot;
import io.confluent.parallelconsumer.dashboard.snapshot.LifecycleSnapshot;
import io.confluent.parallelconsumer.dashboard.snapshot.PartitionSnapshot;
import io.confluent.parallelconsumer.dashboard.snapshot.PcSnapshot;
import io.confluent.parallelconsumer.dashboard.snapshot.WorkSnapshot;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.apache.kafka.common.annotation.InterfaceStability;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * Renders a {@link PcSnapshot} as the dashboard's state document.
 * <p>
 * Written with the JSON layer {@code vertx-web} already brings ({@link JsonObject} / {@link JsonArray}) rather than a
 * hand-rolled writer: string escaping, surrogate pairs and control characters are a well-known source of subtle bugs
 * and there is nothing to gain by re-deriving them here.
 *
 * <h2>Which fields are strings and which are numbers - the rule, and why</h2>
 * <p>
 * <strong>Every exact offset is a JSON string. Everything else is a JSON number.</strong> This is the one rule the
 * library will not enforce, and the one whose violation is silent.
 * <p>
 * Kafka offsets are {@code long}. RFC 7493 (I-JSON) says a receiver cannot be expected to handle integers outside
 * &plusmn;(2<sup>53</sup>&minus;1) exactly, and a browser is exactly such a receiver: JavaScript's {@code JSON.parse}
 * silently rounds {@code 9223372036854775807} to {@code 9223372036854775808} with no error, no warning and no way for
 * the page to tell it happened. An offset that is off by one is not a cosmetic defect on a dashboard whose whole
 * subject is which offsets are complete - so offsets travel as strings and the page parses them with {@code BigInt}.
 * <p>
 * Counts, ratios, timestamps and anything destined for a chart series stay numbers: they are consumed as numbers,
 * they are nowhere near 2<sup>53</sup>, and stringifying them would only force the page to parse them back.
 *
 * <h3>What the string encoding does and does not buy, today</h3>
 * <p>
 * <strong>The offsets in this document are exact up to 2<sup>53</sup>, not up to 2<sup>63</sup>.</strong> They reach
 * this class through Micrometer gauges, and {@code Gauge.value()} returns a {@code double} - so
 * {@code MeterSource#gaugePerPartition} narrows back to {@code long} from a value that has already been through 53
 * bits of mantissa. Nothing downstream can restore what was lost there, and no amount of string encoding on this side
 * of it can either.
 * <p>
 * The string encoding is still the right rule and still load-bearing: it removes the <em>second</em> lossy step, the
 * one in the browser, which is the one nothing would have reported. Closing the remaining gap means reading the
 * offsets from {@code DirectStateSource} - straight off Parallel Consumer's own {@code long} state on the control
 * thread - instead of from the registry. That is the follow-on; it is not done, and this note exists so the guarantee
 * above is not read as stronger than it is.
 * <table>
 *   <caption>Field encoding by kind</caption>
 *   <tr><th>Kind</th><th>Encoding</th><th>Fields</th></tr>
 *   <tr>
 *     <td>Exact offset positions</td>
 *     <td><strong>string</strong></td>
 *     <td>{@code partitions[].highestSeenOffset}, {@code .highestCompletedOffset},
 *         {@code .highestSequentialSucceededOffset}, {@code .lastCommittedOffset}</td>
 *   </tr>
 *   <tr>
 *     <td>Counts and cardinalities</td>
 *     <td>number</td>
 *     <td>{@code partitions[].incompleteOffsets}, {@code .assignmentEpoch}, {@code .partition}, everything under
 *         {@code work}, the counts under {@code encoding}</td>
 *   </tr>
 *   <tr>
 *     <td>Rates, ratios and durations</td>
 *     <td>number</td>
 *     <td>{@code partitions[].processedRecords}, {@code .failedRecords}, {@code .slowRecords},
 *         {@code work.dynamicLoadFactor}, the millisecond and ratio fields under {@code encoding}</td>
 *   </tr>
 *   <tr>
 *     <td>Timestamps (epoch millis)</td>
 *     <td>number</td>
 *     <td>{@code captureEpochMillis} - safely inside 2<sup>53</sup> until the year 287396</td>
 *   </tr>
 *   <tr>
 *     <td>Identity and state names</td>
 *     <td>string</td>
 *     <td>{@code partitions[].topic}, {@code .key}, {@code lifecycle.state}, {@code lifecycle.pollerState},
 *         {@code lifecycle.instanceId}</td>
 *   </tr>
 * </table>
 *
 * <h2>Absent is not zero</h2>
 * <p>
 * A {@code null} in the snapshot means "no meter supplied this", and it renders as JSON {@code null} - never as
 * {@code 0}, and never by omitting the key. The key is always present so the page can tell "absent" from "a field
 * this build does not know about", and a measured zero renders as {@code 0}. The two are different facts and the
 * document keeps them different.
 *
 * <h2>NaN and infinity render as null - measured, not assumed</h2>
 * <p>
 * RFC 8259 has no representation for {@code NaN} or {@code Infinity}, and ratio fields produce both readily from a
 * zero denominator (Micrometer also reports {@code NaN} for a gauge whose weakly-referenced target has been
 * collected). Encoders disagree about what to do:
 * <ul>
 *   <li>Some emit a bare {@code NaN} token - invalid JSON that a strict parser rejects outright.</li>
 *   <li>Vert.x 4.5's {@code JacksonCodec}, which is what this module actually runs on, emits a <em>quoted</em>
 *       {@code "NaN"} / {@code "Infinity"} / {@code "-Infinity"} (jackson-core's {@code QUOTE_NON_NUMERIC_NUMBERS} is
 *       on by default). That is syntactically valid JSON, which makes it the worse failure of the two: the field
 *       silently changes type from number to string, every strict-parser test still passes, and the page's chart
 *       gets a string where it expected a number.</li>
 * </ul>
 * So this class maps both to {@code null} <em>before</em> handing anything to the encoder, and the tests assert it
 * against the rendered characters rather than a re-parse - a re-parse by the same lenient library is exactly what
 * would hide it.
 *
 * <h2>Sources</h2>
 * <p>
 * Rendered from the published snapshot and nothing else. There is no path from here to live Parallel Consumer state,
 * which is what keeps an HTTP thread off the control thread's data structures.
 * <p>
 * Experimental: the dashboard module is opt-in and its API may change without notice.
 */
@InterfaceStability.Unstable
public final class SnapshotJson {

    /**
     * Bumped whenever the document's shape changes incompatibly, so a cached page can tell it is talking to a server
     * it does not understand instead of silently rendering blanks.
     */
    public static final int SCHEMA_VERSION = 1;

    /**
     * The accuracy disclaimer, carried on every document rather than only in the page chrome - {@code curl} and any
     * other consumer deserve the same warning the page's footer gives (plan R42).
     */
    public static final String NOTICE =
            "Sampled operational view of one Parallel Consumer instance, not a measurement platform. Values are "
                    + "point-in-time samples taken on the control loop, may be stale, and are null where no meter "
                    + "supplied them. Offsets are strings because they are 64-bit and JSON numbers are not. Use "
                    + "Micrometer for anything that has to be accurate.";

    /**
     * The document's character encoding. UTF-8 with no byte-order mark - topic names, group ids and client ids are
     * arbitrary Unicode, and a BOM is not permitted in a JSON body (RFC 8259 s8.1). Java's UTF-8 encoder never
     * prepends one, so "no BOM" needs no code, only this note and the test that pins it.
     */
    public static final Charset CHARSET = StandardCharsets.UTF_8;

    private SnapshotJson() {
        // static factory only
    }

    /**
     * The state document for one snapshot.
     *
     * @param snapshot the published snapshot, or {@code null} when no sample has been taken yet - which yields a
     *                 valid, fully-keyed document reporting exactly that, rather than an error the page has to have a
     *                 second code path for
     */
    public static JsonObject document(PcSnapshot snapshot) {
        JsonObject document = new JsonObject();
        document.put("schemaVersion", SCHEMA_VERSION);
        document.put("notice", NOTICE);

        // Capture timestamp and the confidence signals go on EVERY document, including the no-sample-yet one, so
        // staleness and confidence are read from the document by every consumer instead of each deriving them
        // privately and disagreeing.
        if (snapshot == null) {
            document.putNull("captureEpochMillis");
            document.put("sampleSequence", 0L);
            document.put("registryPopulated", false);
            document.put("empty", true);
        } else {
            document.put("captureEpochMillis", snapshot.getCaptureEpochMillis());
            document.put("sampleSequence", snapshot.getSampleSequence());
            document.put("registryPopulated", snapshot.isRegistryPopulated());
            document.put("empty", snapshot.isEmpty());
        }

        document.put("lifecycle", lifecycle(snapshot == null ? null : snapshot.getLifecycle()));
        document.put("work", work(snapshot == null ? null : snapshot.getWork()));
        document.put("encoding", encoding(snapshot == null ? null : snapshot.getEncoding()));
        document.put("partitions", partitions(snapshot));
        return document;
    }

    /**
     * The state document as a compact JSON string.
     */
    public static String encode(PcSnapshot snapshot) {
        return document(snapshot).encode();
    }

    /**
     * The state document as the bytes to put on the wire: UTF-8, no BOM. This is what an HTTP handler writes.
     */
    public static byte[] encodeUtf8(PcSnapshot snapshot) {
        return encode(snapshot).getBytes(CHARSET);
    }

    private static JsonObject lifecycle(LifecycleSnapshot lifecycle) {
        JsonObject json = new JsonObject();
        putString(json, "state", lifecycle == null ? null : lifecycle.getState());
        putNumber(json, "stateValue", lifecycle == null ? null : lifecycle.getStateValue());
        putString(json, "pollerState", lifecycle == null ? null : lifecycle.getPollerState());
        putNumber(json, "pollerStateValue", lifecycle == null ? null : lifecycle.getPollerStateValue());
        putBoolean(json, "closedOrFailed", lifecycle == null ? null : lifecycle.getClosedOrFailed());
        putString(json, "instanceId", lifecycle == null ? null : lifecycle.getInstanceId());
        return json;
    }

    private static JsonObject work(WorkSnapshot work) {
        JsonObject json = new JsonObject();
        putNumber(json, "inflightRecords", work == null ? null : work.getInflightRecords());
        putNumber(json, "waitingRecords", work == null ? null : work.getWaitingRecords());
        putNumber(json, "shards", work == null ? null : work.getShards());
        putNumber(json, "shardsSize", work == null ? null : work.getShardsSize());
        putNumber(json, "incompleteOffsetsTotal", work == null ? null : work.getIncompleteOffsetsTotal());
        putNumber(json, "pausedPartitions", work == null ? null : work.getPausedPartitions());
        putNumber(json, "numberOfPartitions", work == null ? null : work.getNumberOfPartitions());
        putDouble(json, "dynamicLoadFactor", work == null ? null : work.getDynamicLoadFactor());
        return json;
    }

    private static JsonObject encoding(EncodingSnapshot encoding) {
        JsonObject json = new JsonObject();
        putNumber(json, "encodingCount", encoding == null ? null : encoding.getEncodingCount());
        putDouble(json, "encodingTotalTimeMillis", encoding == null ? null : encoding.getEncodingTotalTimeMillis());
        putDouble(json, "encodingMaxTimeMillis", encoding == null ? null : encoding.getEncodingMaxTimeMillis());
        json.put("usageByEncoding", usageByEncoding(encoding));
        putNumber(json, "metadataSpaceUsedCount", encoding == null ? null : encoding.getMetadataSpaceUsedCount());
        putDouble(json, "metadataSpaceUsedMean", encoding == null ? null : encoding.getMetadataSpaceUsedMean());
        putDouble(json, "metadataSpaceUsedMax", encoding == null ? null : encoding.getMetadataSpaceUsedMax());
        putNumber(json, "payloadRatioUsedCount", encoding == null ? null : encoding.getPayloadRatioUsedCount());
        putDouble(json, "payloadRatioUsedMean", encoding == null ? null : encoding.getPayloadRatioUsedMean());
        putDouble(json, "payloadRatioUsedMax", encoding == null ? null : encoding.getPayloadRatioUsedMax());
        return json;
    }

    private static JsonObject usageByEncoding(EncodingSnapshot encoding) {
        JsonObject json = new JsonObject();
        if (encoding == null) {
            return json;
        }
        // insertion-ordered in the snapshot, and kept that way here so the document does not reshuffle between ticks
        for (Map.Entry<String, Double> entry : encoding.getUsageByEncoding().entrySet()) {
            putDouble(json, entry.getKey(), entry.getValue());
        }
        return json;
    }

    private static JsonArray partitions(PcSnapshot snapshot) {
        JsonArray array = new JsonArray();
        if (snapshot == null) {
            return array;
        }
        for (PartitionSnapshot partition : snapshot.getPartitions()) {
            array.add(partition(partition));
        }
        return array;
    }

    private static JsonObject partition(PartitionSnapshot partition) {
        JsonObject json = new JsonObject();
        putString(json, "topic", partition.getTopic());
        json.put("partition", partition.getPartition());
        putString(json, "key", partition.getKey());

        // the four ribbon markers - strings, per the rule at the top of this class
        putOffset(json, "highestSeenOffset", partition.getHighestSeenOffset());
        putOffset(json, "highestCompletedOffset", partition.getHighestCompletedOffset());
        putOffset(json, "highestSequentialSucceededOffset", partition.getHighestSequentialSucceededOffset());
        putOffset(json, "lastCommittedOffset", partition.getLastCommittedOffset());

        // counts, not positions - numbers
        putNumber(json, "incompleteOffsets", partition.getIncompleteOffsets());
        putNumber(json, "assignmentEpoch", partition.getAssignmentEpoch());
        putDouble(json, "processedRecords", partition.getProcessedRecords());
        putDouble(json, "failedRecords", partition.getFailedRecords());
        putDouble(json, "slowRecords", partition.getSlowRecords());

        // carried rather than recomputed on the page: the ribbon is only meaningful if the markers are ordered, and
        // the snapshot is the thing that knows whether they were
        json.put("offsetOrderingConsistent", partition.isOffsetOrderingConsistent());
        return json;
    }

    /**
     * An exact offset, as a string. See the class javadoc for why this is not a number.
     */
    private static void putOffset(JsonObject json, String key, Long offset) {
        if (offset == null) {
            json.putNull(key);
        } else {
            json.put(key, offset.toString());
        }
    }

    private static void putNumber(JsonObject json, String key, Number value) {
        if (value == null) {
            json.putNull(key);
        } else if (value instanceof Double || value instanceof Float) {
            // integral callers only, by design - but a floating-point value arriving here must still not be able to
            // slip past the NaN/infinity guard, so it is routed rather than trusted
            putDouble(json, key, value.doubleValue());
        } else {
            json.put(key, value);
        }
    }

    /**
     * A floating-point value, with the two things JSON cannot represent mapped to {@code null} first. See the class
     * javadoc for what Vert.x does otherwise, and why it is worse than an outright failure.
     */
    private static void putDouble(JsonObject json, String key, Double value) {
        if (value == null || value.isNaN() || value.isInfinite()) {
            json.putNull(key);
        } else {
            json.put(key, value);
        }
    }

    private static void putString(JsonObject json, String key, String value) {
        if (value == null) {
            json.putNull(key);
        } else {
            json.put(key, value);
        }
    }

    private static void putBoolean(JsonObject json, String key, Boolean value) {
        if (value == null) {
            json.putNull(key);
        } else {
            json.put(key, value);
        }
    }
}
