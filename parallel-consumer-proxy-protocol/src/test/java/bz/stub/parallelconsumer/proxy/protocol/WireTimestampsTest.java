package bz.stub.parallelconsumer.proxy.protocol;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import com.github.bsideup.jabel.Desugar;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;

/**
 * The edge-case table that pinned {@link WireTimestamps} when the sidecar's encoder and the Java client's
 * decoder - which had never seen each other, living in different modules - were folded into it. Every row was
 * run against both existing bodies first, and all of them agreed, including a full instant-to-wire-to-instant
 * round trip across the two halves.
 * <p>
 * A plain list rather than a {@code @ParameterizedTest} for the reason {@link WireDurationsTest} gives.
 *
 * @author Antony Stubbs
 */
class WireTimestampsTest {

    /** One row of the equivalence table: a name for the failure message, and the two wire fields. */
    @Desugar // Jabel requires the annotation on every record, even in this module where release=17 makes it a no-op
    private record Row(String name, long seconds, int nanos) {
    }

    private static final List<Row> TABLE = List.of(
            new Row("epoch", 0L, 0),
            new Row("whole seconds", 1_700_000_000L, 0),
            new Row("one nano", 0L, 1),
            new Row("sub-second nanos", 1_700_000_000L, 500_000_000),
            new Row("maximal nanos", 1_700_000_000L, 999_999_999),
            // the bounds the protobuf Timestamp documentation states: 0001-01-01 to 9999-12-31
            new Row("protobuf maximum, 9999-12-31", 253_402_300_799L, 999_999_999),
            new Row("protobuf minimum, 0001-01-01", -62_135_596_800L, 0),
            // and the wider bounds java.time actually carries, which the two fields also survive
            new Row("Instant.MAX", 31_556_889_864_403_199L, 999_999_999),
            new Row("Instant.MIN", -31_557_014_167_219_200L, 0));

    @Test
    void everyRepresentableValueRoundTripsThroughTheWireAndBack() {
        for (Row row : TABLE) {
            var backToWire = WireTimestamps.toWire(WireTimestamps.toJava(wire(row)));

            assertWithMessage("%s: seconds survived the round trip", row.name())
                    .that(backToWire.getSeconds()).isEqualTo(row.seconds());
            assertWithMessage("%s: nanos survived the round trip", row.name())
                    .that(backToWire.getNanos()).isEqualTo(row.nanos());
        }
    }

    @Test
    void theInstantCarriesTheSecondsAndNanosItWasGiven() {
        for (Row row : TABLE) {
            var asJava = WireTimestamps.toJava(wire(row));

            assertWithMessage("%s: epoch second", row.name())
                    .that(asJava.getEpochSecond()).isEqualTo(row.seconds());
            assertWithMessage("%s: nanos", row.name()).that(asJava.getNano()).isEqualTo(row.nanos());
        }
    }

    /** Same normalising contract as the duration bridge: the wire may send more than a second's worth of nanos. */
    @Test
    void nanosBeyondASecondCarryIntoSeconds() {
        var wire = com.google.protobuf.Timestamp.newBuilder().setSeconds(0).setNanos(1_500_000_000).build();

        assertThat(WireTimestamps.toJava(wire)).isEqualTo(Instant.ofEpochMilli(1500));
    }

    /**
     * Pre-epoch instants, both encodings. The wire's canonical form shares a sign between the two fields;
     * {@link WireTimestamps#toWire} emits {@link Instant}'s non-negative-nano form, which does not. Both decode
     * to the same instant, which is the property that matters - see the class javadoc for why the encoding is
     * carried across unchanged rather than re-derived.
     */
    @Test
    void bothPreEpochEncodingsDecodeToTheSameInstant() {
        var canonical = com.google.protobuf.Timestamp.newBuilder()
                .setSeconds(-1).setNanos(-500_000_000).build();
        var nonNegativeNano = com.google.protobuf.Timestamp.newBuilder()
                .setSeconds(-2).setNanos(500_000_000).build();

        assertThat(WireTimestamps.toJava(canonical)).isEqualTo(Instant.ofEpochMilli(-1500));
        assertThat(WireTimestamps.toJava(nonNegativeNano)).isEqualTo(Instant.ofEpochMilli(-1500));
    }

    /** The encoding a pre-epoch instant actually gets, recorded rather than left to be rediscovered. */
    @Test
    void aPreEpochInstantEncodesInInstantsNonNegativeNanoForm() {
        var wire = WireTimestamps.toWire(Instant.ofEpochMilli(-1500));

        assertThat(wire.getSeconds()).isEqualTo(-2);
        assertThat(wire.getNanos()).isEqualTo(500_000_000);
        assertThat(WireTimestamps.toJava(wire)).isEqualTo(Instant.ofEpochMilli(-1500));
    }

    private static com.google.protobuf.Timestamp wire(Row row) {
        return com.google.protobuf.Timestamp.newBuilder().setSeconds(row.seconds()).setNanos(row.nanos()).build();
    }
}
