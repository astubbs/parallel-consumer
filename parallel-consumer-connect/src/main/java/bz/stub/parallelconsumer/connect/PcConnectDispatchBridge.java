package io.confluent.parallelconsumer.connect;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

/**
 * The deliberately inert linkage seam used by the generated {@code WorkerSinkTask}.
 *
 * <p>There is no property, environment variable, setter, or alternate implementation that can enable this
 * bridge. U1 proves only that a generated Connect class can resolve fork-original code. Live polling,
 * conversion, delivery, rebalance, task lifecycle, and offset commit remain outside this seam.</p>
 */
public final class PcConnectDispatchBridge {

    private PcConnectDispatchBridge() {
    }

    /** Returns the hard-coded disabled state; the call exists solely to force runtime linkage. */
    public static boolean enabled() {
        return false;
    }
}
