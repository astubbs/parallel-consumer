package io.confluent.parallelconsumer.integrationTests;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import java.time.Duration;

/**
 * How long a run at a given size is allowed to take, given the deadline its gating size has always
 * had.
 * <p>
 * This exists because a hard-coded deadline is how a volume knob quietly stops working. Every volume
 * alternative recovered on this branch had been parked as a comment beside a live value, and at each
 * site the deadline stayed fixed while the volume above it moved - so the higher setting could not
 * pass no matter how healthy the run was, and was eventually deleted as dead.
 * {@code MultiInstanceHighVolumeTest} says so in its own history: the volume was reduced because
 * "the 60s wait below cannot be met at higher volumes".
 * <p>
 * It lives here, apart from {@link BrokerIntegrationTest}, so it can be unit tested. That class
 * initialises a {@code KafkaContainer} in a static field, so naming it from the unit suite would
 * start a container to test three lines of arithmetic.
 */
public final class CompletionCeiling {

    private CompletionCeiling() {
    }

    /**
     * Returns exactly {@code ceilingAtGating} at the gating size, so a test's default deadline is
     * unchanged, and never returns less - raising the size never shortens the deadline.
     * <p>
     * It is a ceiling, not a throughput assertion. Proving a rate is what the performance suite is
     * for; this only has to stop the deadline being the thing that fails a healthy run.
     * <p>
     * The scaling is linear in size, which assumes cost per unit is roughly flat. Where it is not -
     * if batching amortises better at high volume - the ceiling is looser than it needs to be at the
     * top of a ladder. That is the safe direction for a ceiling, but it does mean a slow-down
     * regression has more room to hide at the highest rungs than at the gating one.
     *
     * @throws IllegalArgumentException if {@code gatingUnits} is not positive, or if the scaled
     *                                  deadline would overflow {@link Duration} - both of which mean
     *                                  a misconfigured knob rather than a slow run, and are worth
     *                                  saying so rather than surfacing as arithmetic from
     *                                  {@code java.time} internals.
     */
    public static Duration completionCeiling(long units, long gatingUnits, Duration ceilingAtGating) {
        if (gatingUnits <= 0) {
            throw new IllegalArgumentException("gatingUnits must be positive, was " + gatingUnits);
        }
        long safeUnits = Math.max(1, units);
        long nanosAtGating = ceilingAtGating.toNanos();
        if (safeUnits > Long.MAX_VALUE / nanosAtGating) {
            throw new IllegalArgumentException(
                    "size " + units + " is too large to scale a " + ceilingAtGating + " ceiling - check the knob");
        }
        Duration scaled = ceilingAtGating.multipliedBy(safeUnits).dividedBy(gatingUnits);
        return scaled.compareTo(ceilingAtGating) > 0 ? scaled : ceilingAtGating;
    }
}
