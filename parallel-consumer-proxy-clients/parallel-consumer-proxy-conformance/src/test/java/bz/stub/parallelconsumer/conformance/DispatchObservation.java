package bz.stub.parallelconsumer.conformance;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */


import java.util.Optional;

/**
 * One thing that happened to one record, as the runner saw it: a line a runner prints, either when a record
 * is delivered or when its outcome is decided.
 * <p>
 * <b>It is an observation, never a verdict.</b> The runner reports what arrived; this suite decides what
 * it means. That distinction is what keeps the assertions in one language while the runners stay dumb -
 * and it is why this is a flat line rather than a results schema. Both lines carry the same four fields, so
 * one parser serves both:
 * <pre>{@code dispatch key=<key> offset=<n> attempt=<n> reason=<text>}
 * {@code settled key=<key> offset=<n> attempt=<n> reason=<text>}</pre>
 * with {@code reason} last because it is worker-supplied text that may contain spaces - on a dispatch it is
 * the history the record ARRIVED with, and on a settled line the failure this runner REPORTED, empty for a
 * success.
 * <p>
 * <b>The pair is what expresses overlap.</b> A dispatch opens a record's unresolved window and a settled
 * line closes it, so the running difference between the two counts, read in line order, is the number of
 * records outstanding at that instant - see {@link RunnerTranscript#peakUnresolved()}.
 *
 * @author Antony Stubbs
 * @see RunnerContract#DISPATCH_LINE_PREFIX
 * @see RunnerContract#SETTLED_LINE_PREFIX
 */
/* A plain class rather than a record - LanguageRunner's header says why, for the whole module. */
public final class DispatchObservation {

    private final Kind kind;

    private final String key;

    private final long offset;

    private final int attempt;

    private final String reason;

    public DispatchObservation(Kind kind, String key, long offset, int attempt, String reason) {
        this.kind = kind;
        this.key = key;
        this.offset = offset;
        this.attempt = attempt;
        this.reason = reason;
    }

    /** Which of a record's two moments this line reports. */
    public Kind kind() {
        return kind;
    }

    /** The record's key, as UTF-8 text. */
    public String key() {
        return key;
    }

    /** The record's offset in its partition. */
    public long offset() {
        return offset;
    }

    /** Which attempt this delivery is, counting from one. */
    public int attempt() {
        return attempt;
    }

    /** On a dispatch, the history the record arrived with; on a settlement, the failure reported. */
    public String reason() {
        return reason;
    }

    /**
     * The whole line, as a runner would have printed it - this is what a failure message shows when it
     * attaches the transcript, so it has to read like the contract rather than like a default toString.
     */
    @Override
    public String toString() {
        return (kind == Kind.DISPATCH
                ? RunnerContract.DISPATCH_LINE_PREFIX
                : RunnerContract.SETTLED_LINE_PREFIX)
                + "key=" + key + " offset=" + offset + " attempt=" + attempt + " reason=" + reason;
    }

    /** Which of a record's two moments this line reports. */
    public enum Kind {
        /** The record was handed to the client's user function, before the prescribed behaviour acted on it. */
        DISPATCH,
        /** The prescribed behaviour decided this record's outcome, so it stopped being unresolved. */
        SETTLED
    }

    /**
     * Parses one line of a runner's stdout, or returns empty if the line is not an observation - runners
     * may print whatever else they like, and a spawned sidecar's logging can reach the same stream.
     *
     * @throws IllegalArgumentException if the line CLAIMS to be an observation but does not parse. A
     *                                  malformed observation is a contract violation and must be loud;
     *                                  silently dropping it would let a runner pass a scenario by printing
     *                                  nothing the suite could read.
     */
    public static Optional<DispatchObservation> parse(String line) {
        Kind kind;
        if (line.startsWith(RunnerContract.DISPATCH_LINE_PREFIX)) {
            kind = Kind.DISPATCH;
        } else if (line.startsWith(RunnerContract.SETTLED_LINE_PREFIX)) {
            kind = Kind.SETTLED;
        } else {
            return Optional.empty();
        }
        var prefix = kind == Kind.DISPATCH
                ? RunnerContract.DISPATCH_LINE_PREFIX
                : RunnerContract.SETTLED_LINE_PREFIX;
        var rest = line.substring(prefix.length());
        // reason is last and takes the remainder of the line, so the split is bounded at four fields
        var fields = rest.split(" ", 4);
        if (fields.length != 4) {
            throw new IllegalArgumentException("malformed observation line, expected four fields: " + line);
        }
        return Optional.of(new DispatchObservation(kind,
                value(fields[0], "key", line),
                Long.parseLong(value(fields[1], "offset", line)),
                Integer.parseInt(value(fields[2], "attempt", line)),
                value(fields[3], "reason", line)));
    }

    private static String value(String field, String expectedName, String line) {
        var separator = field.indexOf('=');
        if (separator < 0 || !field.substring(0, separator).equals(expectedName)) {
            throw new IllegalArgumentException("expected field '" + expectedName + "=' in observation line: " + line);
        }
        return field.substring(separator + 1);
    }
}
