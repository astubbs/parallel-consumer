package bz.stub.parallelconsumer.proxy.protocol;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

/**
 * The one implementation of the {@code google.protobuf.Duration} ↔ {@link java.time.Duration} bridge, for every
 * JVM speaker of this protocol - the sidecar's {@code OptionsMapper} and the Java client's {@code WireMapping}.
 * <p>
 * <b>Why this is not a tidiness class.</b> The conversion is <em>wire semantics</em>, not a utility: it decides
 * what a byte sequence means. Two hand-written copies of it - which is what the sidecar and the client each had -
 * can drift on a nanos or negative edge case without either side noticing, and the result is a protocol bug that
 * the conformance suite catches only if some scenario happens to exercise the exact value that drifted. One
 * owner removes the possibility rather than testing for it.
 * <p>
 * <b>Why it is hand-written rather than protobuf-java-util's {@code Durations}.</b> Measured against the pinned
 * protobuf 3.25.5, not assumed:
 * <ul>
 *   <li>{@code Durations} carries <em>no</em> {@code java.time} bridge at that version - no
 *       {@code toJavaDuration}, no {@code fromJavaDuration}. The only route through it is
 *       {@code fromNanos}/{@code toNanos}, a {@code long} of nanoseconds, which overflows above roughly 292
 *       years and so cannot carry the range the two methods below already carry.</li>
 *   <li>{@code Durations.checkValid} rejects the mixed-sign encoding that {@link #toWire} legitimately produces
 *       for a negative duration (see below), so routing through it would change behaviour.</li>
 *   <li>The artifact downgrades gson from 2.11.0 to 2.8.9 and j2objc-annotations from 3.0.0 to 2.8 on this
 *       classpath, measured with {@code dependency:list} against the transport's real dependency set.</li>
 * </ul>
 * <p>
 * <b>Negative durations encode with mixed signs, and that is deliberate.</b> {@link java.time.Duration}
 * normalises to a floor second plus a non-negative nano remainder, so {@code -1.5s} is {@code seconds = -2,
 * nano = +500000000}, and {@link #toWire} carries those two fields across unchanged. The protobuf {@code Duration}
 * specification asks for both fields to share a sign, so that encoding is non-canonical - but it round-trips
 * exactly through {@link #toJava}, and the alternative is re-deriving the pair, which is a behaviour change on a
 * frozen wire. Negative values are not reachable through any option the protocol exposes today; this note exists
 * so that if one ever becomes reachable, the encoding is a recorded decision rather than a discovery.
 *
 * @author Antony Stubbs
 */
public final class WireDurations {

    private WireDurations() {
    }

    /** Reads a protocol {@code Duration} field as a {@link java.time.Duration}, normalising seconds and nanos. */
    public static java.time.Duration toJava(com.google.protobuf.Duration wire) {
        return java.time.Duration.ofSeconds(wire.getSeconds(), wire.getNanos());
    }

    /** Writes a {@link java.time.Duration} into a protocol {@code Duration} field, field for field. */
    public static com.google.protobuf.Duration toWire(java.time.Duration duration) {
        return com.google.protobuf.Duration.newBuilder()
                .setSeconds(duration.getSeconds())
                .setNanos(duration.getNano())
                .build();
    }
}
