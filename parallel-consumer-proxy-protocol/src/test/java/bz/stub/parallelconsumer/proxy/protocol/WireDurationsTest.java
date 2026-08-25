package bz.stub.parallelconsumer.proxy.protocol;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import com.github.bsideup.jabel.Desugar;
import org.junit.jupiter.api.Test;

import java.util.List;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;

/**
 * The edge-case table that pinned {@link WireDurations} when the sidecar's and the Java client's hand-written
 * copies were folded into it: every row here was run against <em>both</em> old implementations first, and all
 * three agreed on every one, which is what made the unification behaviour-preserving rather than a guess.
 * <p>
 * It stays as a test because the table is the useful artifact, not the one-off run: these are the values on
 * which a future edit to the bridge would silently change what a byte sequence means.
 * <p>
 * The table is a plain list rather than a {@code @ParameterizedTest} because this module's test classpath
 * carries no {@code junit-jupiter-params}, and a bridge with two methods on it does not justify adding one.
 *
 * @author Antony Stubbs
 */
class WireDurationsTest {

    /** One row of the equivalence table: a name for the failure message, and the two wire fields. */
    @Desugar // Jabel requires the annotation on every record, even in this module where release=17 makes it a no-op
    private record Row(String name, long seconds, int nanos) {
    }

    private static final List<Row> TABLE = List.of(
            new Row("zero", 0L, 0),
            new Row("whole seconds", 5L, 0),
            new Row("one nano", 0L, 1),
            new Row("sub-second nanos", 0L, 500_000_000),
            new Row("seconds and nanos", 1L, 500_000_000),
            new Row("maximal nanos", 1L, 999_999_999),
            // protobuf's own documented Duration bound, and then the widest the two fields can carry at all
            new Row("protobuf maximum", 315_576_000_000L, 999_999_999),
            new Row("long maximum seconds", Long.MAX_VALUE, 999_999_999));

    @Test
    void everyRepresentableValueRoundTripsThroughTheWireAndBack() {
        for (Row row : TABLE) {
            var wire = wire(row);

            var backToWire = WireDurations.toWire(WireDurations.toJava(wire));

            assertWithMessage("%s: seconds survived the round trip", row.name())
                    .that(backToWire.getSeconds()).isEqualTo(row.seconds());
            assertWithMessage("%s: nanos survived the round trip", row.name())
                    .that(backToWire.getNanos()).isEqualTo(row.nanos());
        }
    }

    @Test
    void theJavaValueCarriesTheSecondsAndNanosItWasGiven() {
        for (Row row : TABLE) {
            var asJava = WireDurations.toJava(wire(row));

            assertWithMessage("%s: seconds", row.name()).that(asJava.getSeconds()).isEqualTo(row.seconds());
            assertWithMessage("%s: nanos", row.name()).that(asJava.getNano()).isEqualTo(row.nanos());
        }
    }

    /**
     * The bridge normalises what the wire does not have to. A sender is free to put more than a second's worth
     * of nanos in the nanos field, and both hand-written copies accepted that by construction, because
     * {@code Duration.ofSeconds(long, long)} carries the excess into seconds. Pinned so that a "tidier" rewrite
     * reading the two fields independently fails here rather than in some client's timeout.
     */
    @Test
    void nanosBeyondASecondCarryIntoSeconds() {
        var wire = com.google.protobuf.Duration.newBuilder().setSeconds(0).setNanos(1_500_000_000).build();

        assertThat(WireDurations.toJava(wire)).isEqualTo(java.time.Duration.ofMillis(1500));
    }

    /**
     * Negative durations, both encodings. The wire's canonical form shares a sign between the two fields; what
     * {@link WireDurations#toWire} emits is {@link java.time.Duration}'s floor-second form, which does not - and
     * the class javadoc explains why carrying the fields across unchanged is the right call on a frozen wire.
     * Both decode to the same value, which is the property that matters.
     */
    @Test
    void bothNegativeEncodingsDecodeToTheSameValue() {
        var canonical = com.google.protobuf.Duration.newBuilder()
                .setSeconds(-1).setNanos(-500_000_000).build();
        var floorSecond = com.google.protobuf.Duration.newBuilder()
                .setSeconds(-2).setNanos(500_000_000).build();

        assertThat(WireDurations.toJava(canonical)).isEqualTo(java.time.Duration.ofMillis(-1500));
        assertThat(WireDurations.toJava(floorSecond)).isEqualTo(java.time.Duration.ofMillis(-1500));
    }

    /** The encoding a negative value actually gets, recorded rather than left to be rediscovered. */
    @Test
    void aNegativeDurationEncodesInJavaTimesFloorSecondForm() {
        var wire = WireDurations.toWire(java.time.Duration.ofMillis(-1500));

        assertThat(wire.getSeconds()).isEqualTo(-2);
        assertThat(wire.getNanos()).isEqualTo(500_000_000);
        assertThat(WireDurations.toJava(wire)).isEqualTo(java.time.Duration.ofMillis(-1500));
    }

    private static com.google.protobuf.Duration wire(Row row) {
        return com.google.protobuf.Duration.newBuilder().setSeconds(row.seconds()).setNanos(row.nanos()).build();
    }
}
