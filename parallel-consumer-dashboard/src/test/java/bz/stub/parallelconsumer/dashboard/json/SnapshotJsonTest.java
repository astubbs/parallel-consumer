package io.confluent.parallelconsumer.dashboard.json;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import io.confluent.parallelconsumer.dashboard.snapshot.EncodingSnapshot;
import io.confluent.parallelconsumer.dashboard.snapshot.LifecycleSnapshot;
import io.confluent.parallelconsumer.dashboard.snapshot.PartitionSnapshot;
import io.confluent.parallelconsumer.dashboard.snapshot.PcMeterFixture;
import io.confluent.parallelconsumer.dashboard.snapshot.PcSnapshot;
import io.confluent.parallelconsumer.dashboard.snapshot.StateSampler;
import io.confluent.parallelconsumer.dashboard.snapshot.WorkSnapshot;
import io.vertx.core.json.JsonObject;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Pins the state document's two silent failure modes and its shape.
 * <p>
 * The two failures worth testing here are the ones that produce a wrong page rather than a broken one: a 64-bit offset
 * quietly rounded by the browser's {@code JSON.parse}, and a {@code NaN} that the encoder turns into something other
 * than {@code null}. Neither throws anywhere; both are only visible in the rendered characters, which is why the
 * assertions below are against the rendered string and not against a re-parse - a re-parse by the same lenient
 * library is exactly what would hide them.
 * <p>
 * The strict parser here is jackson-core's streaming parser at its defaults ({@code ALLOW_NON_NUMERIC_NUMBERS} off,
 * so a bare {@code NaN} token is rejected). It needs no new dependency: jackson-core is already on this module's
 * classpath as a compile-scope transitive of {@code vertx-core}, which is also what Vert.x itself encodes with.
 * {@link #strictParserIsActuallyStrict()} proves the parser is not silently lenient, so the parse tests cannot go
 * vacuous if a default ever flips.
 */
class SnapshotJsonTest {

    /**
     * A fixed instant so the fixture comparison is deterministic. 2026-08-06T10:13:20Z.
     */
    private static final long FIXED_CAPTURE_MILLIS = 1_754_475_200_000L;

    /**
     * 2<sup>53</sup>+1 - the smallest positive integer a JavaScript number cannot represent. Sitting an offset here
     * makes the rounding visible with one digit rather than requiring the reader to count to nineteen.
     */
    private static final long FIRST_UNREPRESENTABLE_IN_JS = 9007199254740993L;

    // ----------------------------------------------------------------------------------------------------
    // The offset-precision trap
    // ----------------------------------------------------------------------------------------------------

    /**
     * The whole reason offsets are strings. If this ever renders as a JSON number, every strict parser still accepts
     * the document, every test that re-parses it in Java still passes, and the browser silently draws the wrong
     * offset - so the assertion has to be on the exact digits, quoted, in the rendered text.
     */
    @Test
    void longMaxOffsetSurvivesAsExactDigits() {
        String rendered = SnapshotJson.encode(fixtureSnapshot());

        assertThat(rendered)
                .as("Long.MAX_VALUE must appear quoted, i.e. as a JSON string, digit for digit")
                .contains("\"highestSeenOffset\":\"9223372036854775807\"")
                .as("and must NOT appear as a bare JSON number, which is what the browser would corrupt")
                .doesNotContain("\"highestSeenOffset\":9223372036854775807");

        assertThat(rendered).contains("\"highestSequentialSucceededOffset\":\"9007199254740993\"");

        // The control arm: what a JSON number would have become. A browser's JSON.parse produces an IEEE-754 double,
        // and these are the values it would hand the page - off by one and off by 2^53 respectively, with no error.
        assertThat(new BigDecimal((double) Long.MAX_VALUE).toBigIntegerExact())
                .as("a double round trip does NOT preserve Long.MAX_VALUE")
                .isNotEqualTo(BigInteger.valueOf(Long.MAX_VALUE));
        assertThat((long) (double) FIRST_UNREPRESENTABLE_IN_JS)
                .as("a double round trip does NOT preserve 2^53+1")
                .isNotEqualTo(FIRST_UNREPRESENTABLE_IN_JS)
                .isEqualTo(9007199254740992L);
    }

    /**
     * The browser side of the same trap: the page parses these with {@code BigInt}, whose Java equivalent is
     * {@link BigInteger}. Round-tripping through it must be exact.
     */
    @Test
    void offsetsRoundTripExactlyThroughArbitraryPrecision() {
        JsonObject document = SnapshotJson.document(fixtureSnapshot());
        JsonObject partition = document.getJsonArray("partitions").getJsonObject(0);

        assertThat(new BigInteger(partition.getString("highestSeenOffset")))
                .isEqualTo(BigInteger.valueOf(Long.MAX_VALUE));
        assertThat(new BigInteger(partition.getString("highestSequentialSucceededOffset")))
                .isEqualTo(BigInteger.valueOf(FIRST_UNREPRESENTABLE_IN_JS));
    }

    // ----------------------------------------------------------------------------------------------------
    // NaN and infinity
    // ----------------------------------------------------------------------------------------------------

    /**
     * Asserted against the rendered characters, not a re-parse. RFC 8259 has no {@code NaN} and no {@code Infinity},
     * so both have to become {@code null} before the encoder ever sees them.
     */
    @Test
    void nonFiniteValuesRenderAsNullInTheRenderedCharacters() {
        String rendered = SnapshotJson.encode(fixtureSnapshot());

        assertThat(rendered)
                .as("no NaN or Infinity token, quoted or bare, anywhere in the document")
                .doesNotContain("NaN")
                .doesNotContain("Infinity");
        assertThat(rendered)
                .contains("\"payloadRatioUsedMean\":null")
                .contains("\"payloadRatioUsedMax\":null");
    }

    /**
     * The control arm for the test above, and the finding that made the guard necessary.
     * <p>
     * Vert.x 4.5's {@code JacksonCodec} does <strong>not</strong> emit a bare {@code NaN} token - it emits a
     * <em>quoted</em> {@code "NaN"}, because jackson-core's {@code QUOTE_NON_NUMERIC_NUMBERS} is on by default. That
     * is syntactically valid JSON, so a strict parser accepts it and every parse-based test stays green while the
     * field silently changes type from number to string under the page's chart.
     * <p>
     * This test asserts the library's raw behaviour rather than the guarded behaviour, so if a Vert.x or jackson
     * upgrade changes it, this fails and says so instead of the guard quietly becoming dead code.
     */
    @Test
    void vertxItselfEncodesNonFiniteNumbersAsQuotedStrings() {
        JsonObject raw = new JsonObject()
                .put("nan", Double.NaN)
                .put("positiveInfinity", Double.POSITIVE_INFINITY)
                .put("negativeInfinity", Double.NEGATIVE_INFINITY);

        assertThat(raw.encode())
                .isEqualTo("{\"nan\":\"NaN\",\"positiveInfinity\":\"Infinity\",\"negativeInfinity\":\"-Infinity\"}");

        // ...and because it is a valid JSON string, a strict parser is perfectly happy with it. That is the point:
        // strict parsing cannot detect this, only an assertion on the characters can.
        assertStrictlyParseable(raw.encode());
    }

    // ----------------------------------------------------------------------------------------------------
    // Absent versus zero
    // ----------------------------------------------------------------------------------------------------

    /**
     * A meter that was not present must not become a zero on the page. Zero is a claim; absent is the absence of one.
     */
    @Test
    void absentAndZeroRenderDifferently() {
        String zeroed = SnapshotJson.encode(snapshotWithLoadFactor(0.0d));
        String absent = SnapshotJson.encode(snapshotWithLoadFactor(null));

        assertThat(zeroed).contains("\"dynamicLoadFactor\":0.0");
        assertThat(absent).contains("\"dynamicLoadFactor\":null");
        assertThat(zeroed).isNotEqualTo(absent);
    }

    /**
     * Absence is rendered as an explicit {@code null}, never by dropping the key - a missing key is indistinguishable
     * from a field this build of the page has never heard of.
     */
    @Test
    void absentValuesKeepTheirKey() {
        JsonObject work = SnapshotJson.document(snapshotWithLoadFactor(null)).getJsonObject("work");

        assertThat(work.containsKey("dynamicLoadFactor")).isTrue();
        assertThat(work.getValue("dynamicLoadFactor")).isNull();
    }

    // ----------------------------------------------------------------------------------------------------
    // Strict parseability
    // ----------------------------------------------------------------------------------------------------

    /**
     * The hostile document from the plan: a non-ASCII topic name, {@link Long#MAX_VALUE} as an offset, a
     * {@code NaN}, a {@code -Infinity}, and a partition with no incomplete offsets.
     */
    @Test
    void hostileDocumentParsesUnderAStrictParser() {
        assertStrictlyParseable(SnapshotJson.encode(fixtureSnapshot()));
    }

    /**
     * The other end of the range: no partitions at all, and no snapshot at all.
     */
    @Test
    void emptyDocumentsParseUnderAStrictParser() {
        assertStrictlyParseable(SnapshotJson.encode(emptySnapshot()));
        assertStrictlyParseable(SnapshotJson.encode(null));
    }

    /**
     * Guards the tests above from going vacuous. If jackson's defaults ever changed to accept a bare {@code NaN},
     * every "parses strictly" assertion here would keep passing while proving nothing.
     */
    @Test
    void strictParserIsActuallyStrict() {
        assertThatThrownBy(() -> parse("{\"x\":NaN}")).isInstanceOf(JsonParseException.class);
        assertThatThrownBy(() -> parse("{\"x\":-Infinity}")).isInstanceOf(JsonParseException.class);
        assertThatThrownBy(() -> parse("{'x':1}")).isInstanceOf(JsonParseException.class);
    }

    // ----------------------------------------------------------------------------------------------------
    // Timestamp, completeness and the notice
    // ----------------------------------------------------------------------------------------------------

    /**
     * Every document carries when it was captured, how confident it is, and the accuracy disclaimer (plan R42) - so
     * {@code curl} and the page read the same staleness and confidence signals rather than each deriving its own.
     */
    @Test
    void everyDocumentCarriesTimestampCompletenessAndNotice() {
        JsonObject populated = SnapshotJson.document(fixtureSnapshot());
        assertThat(populated.getLong("captureEpochMillis")).isEqualTo(FIXED_CAPTURE_MILLIS);
        assertThat(populated.getLong("sampleSequence")).isEqualTo(42L);
        assertThat(populated.getBoolean("registryPopulated")).isTrue();
        assertThat(populated.getBoolean("empty")).isFalse();
        assertThat(populated.getString("notice")).contains("not a measurement platform").contains("Micrometer");
        assertThat(populated.getInteger("schemaVersion")).isEqualTo(SnapshotJson.SCHEMA_VERSION);

        JsonObject empty = SnapshotJson.document(emptySnapshot());
        assertThat(empty.getLong("captureEpochMillis")).isEqualTo(FIXED_CAPTURE_MILLIS);
        assertThat(empty.getBoolean("registryPopulated")).isFalse();
        assertThat(empty.getBoolean("empty")).isTrue();
        assertThat(empty.getString("notice")).isEqualTo(SnapshotJson.NOTICE);
    }

    /**
     * Before the first control-loop sample there is no snapshot, and the page must still get a document it can render
     * - reporting "nothing known yet" - rather than an error needing a second code path.
     */
    @Test
    void noSampleYetStillProducesAKeyedDocument() {
        JsonObject document = SnapshotJson.document(null);

        assertThat(document.containsKey("captureEpochMillis")).isTrue();
        assertThat(document.getValue("captureEpochMillis")).isNull();
        assertThat(document.getLong("sampleSequence")).isEqualTo(0L);
        assertThat(document.getBoolean("empty")).isTrue();
        assertThat(document.getString("notice")).isEqualTo(SnapshotJson.NOTICE);
        assertThat(document.getJsonArray("partitions")).isEmpty();
        assertThat(document.getJsonObject("work").getValue("inflightRecords")).isNull();
    }

    // ----------------------------------------------------------------------------------------------------
    // Shape
    // ----------------------------------------------------------------------------------------------------

    /**
     * The whole rendered document for a fixed snapshot, so an accidental shape change - a renamed key, a dropped
     * field, an offset that stopped being a string - shows up as a diff in review rather than as a blank panel.
     */
    @Test
    void renderedDocumentMatchesFixture() {
        assertThat(SnapshotJson.encode(fixtureSnapshot())).isEqualTo(EXPECTED_FIXTURE_DOCUMENT);
    }

    /**
     * The document is what goes on the wire: UTF-8, and no byte-order mark - a BOM is not permitted in a JSON body
     * (RFC 8259 s8.1) and would make a strict parser reject the very first byte.
     */
    @Test
    void bytesAreUtf8WithNoByteOrderMark() {
        byte[] bytes = SnapshotJson.encodeUtf8(fixtureSnapshot());

        assertThat(bytes[0]).as("document must start with '{', not a BOM").isEqualTo((byte) '{');
        assertThat(new String(bytes, StandardCharsets.UTF_8))
                .isEqualTo(SnapshotJson.encode(fixtureSnapshot()))
                .contains("订单-über");
    }

    // ----------------------------------------------------------------------------------------------------
    // Against a realistic sampler
    // ----------------------------------------------------------------------------------------------------

    /**
     * Serialising a snapshot built by the real sampler from a realistically-populated registry, rather than only
     * hand-built ones - so a field the sampler starts producing, or stops, is exercised here too.
     */
    @Test
    void snapshotFromTheRealSamplerSerialisesAndParsesStrictly() {
        PcSnapshot sampled = new StateSampler(null, PcMeterFixture.fullyPopulated().getRegistry()).sample();

        String rendered = SnapshotJson.encode(sampled);

        assertStrictlyParseable(rendered);
        assertThat(rendered).doesNotContain("NaN").doesNotContain("Infinity");
        assertThat(SnapshotJson.document(sampled).getJsonArray("partitions")).hasSize(3);
        // offsets from a live registry are strings too - the rule is per-field, not per-magnitude
        assertThat(rendered).contains("\"lastCommittedOffset\":\"101\"");
    }

    // ----------------------------------------------------------------------------------------------------
    // Fixtures and helpers
    // ----------------------------------------------------------------------------------------------------

    private static PcSnapshot fixtureSnapshot() {
        Map<String, Double> usage = new LinkedHashMap<>();
        usage.put("RunLength", 4.0d);
        usage.put("BitSetV2Compressed", 2.0d);

        return PcSnapshot.builder()
                .captureEpochMillis(FIXED_CAPTURE_MILLIS)
                .sampleSequence(42L)
                .registryPopulated(true)
                .lifecycle(LifecycleSnapshot.builder()
                        .state("RUNNING")
                        .stateValue(1)
                        .pollerState("RUNNING")
                        .pollerStateValue(1)
                        .closedOrFailed(false)
                        .instanceId("pc-über-1")
                        .build())
                .work(WorkSnapshot.builder()
                        .inflightRecords(9L)
                        .waitingRecords(31L)
                        .shards(4L)
                        .shardsSize(40L)
                        .incompleteOffsetsTotal(16L)
                        // a measured zero next to an absent value, in the same object
                        .pausedPartitions(0L)
                        .numberOfPartitions(2L)
                        .dynamicLoadFactor(null)
                        .build())
                .encoding(EncodingSnapshot.builder()
                        .encodingCount(2L)
                        .encodingTotalTimeMillis(10.0d)
                        .encodingMaxTimeMillis(7.0d)
                        .usageByEncoding(usage)
                        .metadataSpaceUsedCount(2L)
                        .metadataSpaceUsedMean(0.5d)
                        .metadataSpaceUsedMax(0.75d)
                        // a ratio over a zero denominator, which is how these arrive in practice
                        .payloadRatioUsedCount(0L)
                        .payloadRatioUsedMean(Double.NaN)
                        .payloadRatioUsedMax(Double.NEGATIVE_INFINITY)
                        .build())
                .partitions(Arrays.asList(
                        PartitionSnapshot.builder()
                                // non-ASCII topic name: topics, group ids and client ids are arbitrary Unicode
                                .topic("订单-über")
                                .partition(0)
                                .highestSeenOffset(Long.MAX_VALUE)
                                .highestCompletedOffset(Long.MAX_VALUE - 5)
                                .highestSequentialSucceededOffset(FIRST_UNREPRESENTABLE_IN_JS)
                                .lastCommittedOffset(0L)
                                .incompleteOffsets(3L)
                                .assignmentEpoch(1L)
                                .processedRecords(100.0d)
                                .failedRecords(0.0d)
                                .slowRecords(null)
                                .build(),
                        // a partition with no incomplete offsets and mostly absent meters
                        PartitionSnapshot.builder()
                                .topic("payments")
                                .partition(7)
                                .lastCommittedOffset(0L)
                                .incompleteOffsets(0L)
                                .build()))
                .build();
    }

    /**
     * A snapshot with nothing in it but a capture time - the honest "nothing known yet" reading. Sub-snapshots are
     * left null deliberately: {@link PcSnapshot} substitutes all-absent instances, and the document must render those
     * rather than blow up.
     */
    private static PcSnapshot emptySnapshot() {
        return PcSnapshot.builder()
                .captureEpochMillis(FIXED_CAPTURE_MILLIS)
                .sampleSequence(1L)
                .registryPopulated(false)
                .build();
    }

    private static PcSnapshot snapshotWithLoadFactor(Double loadFactor) {
        return emptySnapshot().toBuilder()
                .work(WorkSnapshot.builder().dynamicLoadFactor(loadFactor).build())
                .build();
    }

    private static void assertStrictlyParseable(String json) {
        assertThatCode(() -> parse(json)).doesNotThrowAnyException();
    }

    /**
     * Walks every token with jackson-core at its defaults. Streaming rather than binding on purpose: this is a syntax
     * check, and binding would only add a shape the document does not have.
     */
    private static void parse(String json) throws IOException {
        JsonFactory factory = new JsonFactory();
        try (JsonParser parser = factory.createParser(json)) {
            while (parser.nextToken() != null) {
                // walking is the assertion - an invalid token throws
            }
        }
    }

    /**
     * The full rendered document for {@link #fixtureSnapshot()}. Written out rather than generated so a shape change
     * is a reviewable diff. Compact (not pretty-printed) because that is what goes on the wire, and because a
     * pretty-printed fixture would embed the platform's line separator.
     */
    private static final String EXPECTED_FIXTURE_DOCUMENT =
            "{\"schemaVersion\":1,"
                    + "\"notice\":\"Sampled operational view of one Parallel Consumer instance, not a measurement platform. "
                    + "Values are point-in-time samples taken on the control loop, may be stale, and are null where no meter "
                    + "supplied them. Offsets are strings because they are 64-bit and JSON numbers are not. Use Micrometer "
                    + "for anything that has to be accurate.\","
                    + "\"captureEpochMillis\":1754475200000,\"sampleSequence\":42,\"registryPopulated\":true,\"empty\":false,"
                    + "\"lifecycle\":{\"state\":\"RUNNING\",\"stateValue\":1,\"pollerState\":\"RUNNING\",\"pollerStateValue\":1,"
                    + "\"closedOrFailed\":false,\"instanceId\":\"pc-über-1\"},"
                    + "\"work\":{\"inflightRecords\":9,\"waitingRecords\":31,\"shards\":4,\"shardsSize\":40,"
                    + "\"incompleteOffsetsTotal\":16,\"pausedPartitions\":0,\"numberOfPartitions\":2,\"dynamicLoadFactor\":null},"
                    + "\"encoding\":{\"encodingCount\":2,\"encodingTotalTimeMillis\":10.0,\"encodingMaxTimeMillis\":7.0,"
                    + "\"usageByEncoding\":{\"RunLength\":4.0,\"BitSetV2Compressed\":2.0},"
                    + "\"metadataSpaceUsedCount\":2,\"metadataSpaceUsedMean\":0.5,\"metadataSpaceUsedMax\":0.75,"
                    + "\"payloadRatioUsedCount\":0,\"payloadRatioUsedMean\":null,\"payloadRatioUsedMax\":null},"
                    + "\"partitions\":["
                    + "{\"topic\":\"订单-über\",\"partition\":0,\"key\":\"订单-über-0\","
                    + "\"highestSeenOffset\":\"9223372036854775807\",\"highestCompletedOffset\":\"9223372036854775802\","
                    + "\"highestSequentialSucceededOffset\":\"9007199254740993\",\"lastCommittedOffset\":\"0\","
                    + "\"incompleteOffsets\":3,\"assignmentEpoch\":1,\"processedRecords\":100.0,\"failedRecords\":0.0,"
                    + "\"slowRecords\":null,\"offsetOrderingConsistent\":true},"
                    + "{\"topic\":\"payments\",\"partition\":7,\"key\":\"payments-7\","
                    + "\"highestSeenOffset\":null,\"highestCompletedOffset\":null,\"highestSequentialSucceededOffset\":null,"
                    + "\"lastCommittedOffset\":\"0\",\"incompleteOffsets\":0,\"assignmentEpoch\":null,\"processedRecords\":null,"
                    + "\"failedRecords\":null,\"slowRecords\":null,\"offsetOrderingConsistent\":true}"
                    + "]}";
}
