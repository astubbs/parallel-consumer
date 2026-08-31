package bz.stub.parallelconsumer.proxy.protocol;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

/**
 * The one implementation of the {@code google.protobuf.Timestamp} ↔ {@link java.time.Instant} bridge, for every
 * JVM speaker of this protocol - the sidecar's {@code RecordCodec}, which encodes a record's last failure time,
 * and the Java client's {@code WireMapping}, which decodes it.
 * <p>
 * <b>The two halves lived in different modules, which is the drift risk in its sharpest form.</b> Neither side
 * could see the other, so nothing would have gone red if one had been changed alone - and the value they carry
 * is the one a user reads off {@code InboundRecord} to decide whether to retry. Now the encoder and the decoder
 * are the same two methods.
 * <p>
 * <b>A sibling of {@link WireDurations} rather than more methods on it</b>, mirroring the split protobuf itself
 * makes between {@code Durations} and {@code Timestamps}: the two types share an encoding shape but not their
 * edge-case story, and a reader who came for one should not have to read past the other's caveats.
 * <p>
 * <b>Why hand-written rather than protobuf-java-util's {@code Timestamps}.</b> The same measurement that ruled
 * it out for durations rules it out here, and it is worth stating for the family rather than per type: at the
 * pinned protobuf 3.25.5, {@code Timestamps} carries <em>no</em> {@code java.time} bridge - no
 * {@code fromInstant}, no {@code toInstant}. Its nearest offers are {@code fromMillis}/{@code toMillis}, which
 * discard sub-millisecond nanos outright, and {@code fromNanos}/{@code toNanos}, a {@code long} of nanoseconds
 * that cannot represent the range below. {@link WireDurations} carries the rest of the reasoning, including the
 * dependency measurement.
 * <p>
 * <b>Pre-epoch instants encode with mixed signs, exactly as durations do.</b> {@link java.time.Instant} keeps a
 * non-negative nano-of-second, so 1969-12-31T23:59:58.5Z is {@code seconds = -2, nano = +500000000}, which the
 * protobuf specification would call non-canonical. It round-trips exactly through {@link #toJava}, and both
 * encodings decode to the same instant. Not reachable today - the encoded value is a failure time taken from a
 * running clock - so this is a recorded decision rather than a live concern.
 *
 * @author Antony Stubbs
 */
public final class WireTimestamps {

    private WireTimestamps() {
    }

    /** Reads a protocol {@code Timestamp} field as an {@link java.time.Instant}, normalising seconds and nanos. */
    public static java.time.Instant toJava(com.google.protobuf.Timestamp wire) {
        return java.time.Instant.ofEpochSecond(wire.getSeconds(), wire.getNanos());
    }

    /** Writes an {@link java.time.Instant} into a protocol {@code Timestamp} field, field for field. */
    public static com.google.protobuf.Timestamp toWire(java.time.Instant instant) {
        return com.google.protobuf.Timestamp.newBuilder()
                .setSeconds(instant.getEpochSecond())
                .setNanos(instant.getNano())
                .build();
    }
}
