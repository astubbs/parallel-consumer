package bz.stub.parallelconsumer.ffi;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.graalvm.nativeimage.IsolateThread;
import org.graalvm.nativeimage.c.function.CEntryPoint;

/**
 * Probe 0 of the FFI ladder: is a --shared export of Parallel Consumer callable from Go?
 *
 * Deliberately minimal. It answers the entry-point-surface and isolate/thread-attach checks, and
 * proves PC's own classes are INSIDE the library - not merely that cgo can reach a Graal isolate.
 */
public final class PcFfi {

    /** The ABI floor: if this does not come back as 7, nothing else below means anything. */
    @CEntryPoint(name = "pc_sum")
    static int sum(IsolateThread thread, int a, int b) {
        return a + b;
    }

    /**
     * Proof that Parallel Consumer itself is linked in: resolves a real core class and reads a real
     * enum from it. A stub returning a constant would pass the ABI check while proving nothing, so
     * this deliberately touches the engine's own types.
     */
    @CEntryPoint(name = "pc_ordering_modes")
    static int orderingModes(IsolateThread thread) {
        try {
            Class<?> options = Class.forName("bz.stub.parallelconsumer.ParallelConsumerOptions");
            for (Class<?> nested : options.getDeclaredClasses()) {
                if (nested.getSimpleName().equals("ProcessingOrder")) {
                    return nested.getEnumConstants().length;
                }
            }
            return -1;
        } catch (Throwable t) {
            return -2;
        }
    }

    /** Same question without reflection, so the closed-world analysis must link the class statically. */
    @CEntryPoint(name = "pc_static_ordering_modes")
    static int staticOrderingModes(IsolateThread thread) {
        return bz.stub.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.values().length;
    }
}
