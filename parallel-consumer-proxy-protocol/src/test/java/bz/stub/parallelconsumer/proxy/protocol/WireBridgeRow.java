package bz.stub.parallelconsumer.proxy.protocol;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

/**
 * One row of the equivalence tables that pin {@link WireDurations} and {@link WireTimestamps}: a name for the
 * failure message, and the two wire fields both well-known types are built from. Shared by the two tests
 * rather than declared twice - they had byte-identical copies, which is a clone the duplication detectors are
 * right to flag and which would drift the first time one of them gained a field.
 * <p>
 * <b>A plain class, not a {@code record}, and that is not a style choice.</b> Jabel rewrites every record in
 * this build into a class (it rejects an un-annotated one outright, even here where {@code release.target} is
 * 17), and the {@code toString} it synthesises trips Error Prone's {@code UnnecessaryStringBuilder} check on
 * code with no source positions - which does not report a finding, it crashes the compiler plugin with
 * {@code invalid replacement: [0, -1)} and fails the module. {@code @SuppressWarnings} does not reach it:
 * there is no user-written node for the suppression to attach to (measured, as a control arm, before this
 * class was written). Switching the check off globally was the alternative, and it is the wrong trade - the
 * off-set in the root pom is documented to only ever shrink, and this would disable a real check everywhere
 * to accommodate one synthesised {@code toString}.
 *
 * @author Antony Stubbs
 */
final class WireBridgeRow {

    private final String name;

    private final long seconds;

    private final int nanos;

    WireBridgeRow(String name, long seconds, int nanos) {
        this.name = name;
        this.seconds = seconds;
        this.nanos = nanos;
    }

    String name() {
        return name;
    }

    long seconds() {
        return seconds;
    }

    int nanos() {
        return nanos;
    }
}
