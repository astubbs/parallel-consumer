package bz.stub.parallelconsumer.conformance;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import com.github.bsideup.jabel.Desugar;

import java.util.Optional;

/**
 * One delivery, as the runner saw it: the single line a runner prints per record.
 * <p>
 * <b>It is an observation, never a verdict.</b> The runner reports what arrived; this suite decides what
 * it means. That distinction is what keeps the assertions in one language while the runners stay dumb -
 * and it is why this is a flat line rather than a results schema. The format is fixed:
 * <pre>{@code dispatch key=<key> offset=<n> attempt=<n> reason=<text>}</pre>
 * with {@code reason} last because it is worker-supplied text that may contain spaces.
 *
 * @author Antony Stubbs
 * @see RunnerContract#DISPATCH_LINE_PREFIX
 */
@Desugar // Jabel requires the annotation on every record, even in this module where release=17 makes it a no-op
public record DispatchObservation(String key, long offset, int attempt, String reason) {

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
        if (!line.startsWith(RunnerContract.DISPATCH_LINE_PREFIX)) {
            return Optional.empty();
        }
        var rest = line.substring(RunnerContract.DISPATCH_LINE_PREFIX.length());
        // reason is last and takes the remainder of the line, so the split is bounded at four fields
        var fields = rest.split(" ", 4);
        if (fields.length != 4) {
            throw new IllegalArgumentException("malformed observation line, expected four fields: " + line);
        }
        return Optional.of(new DispatchObservation(
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
