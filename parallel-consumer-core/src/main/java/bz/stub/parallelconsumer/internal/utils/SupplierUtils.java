package bz.stub.parallelconsumer.internal.utils;

/*-
 * Copyright (C) 2020-2023 Confluent, Inc.
 * Modifications Copyright (C) 2026 Antony Stubbs and contributors
 */

import lombok.experimental.UtilityClass;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

@UtilityClass
public class SupplierUtils {

    /**
     * A {@link ReentrantLock} rather than {@code synchronized} because the monitor is held across whatever the
     * delegate does, and one caller's delegate is
     * {@code AbstractParallelEoSStreamProcessor#setupWorkerPool(int)} - which performs a JNDI
     * {@code InitialContext.doLookup}, i.e. potentially remote I/O. Before JDK 24 (JEP 491) a virtual thread that
     * blocks inside {@code synchronized} pins its carrier, so this generic-looking utility was the least obvious
     * of the pinning sites. The double-checked read is unchanged.
     */
    public static <T> Supplier<T> memoize(Supplier<T> delegate) {
        Objects.requireNonNull(delegate);
        AtomicReference<T> value = new AtomicReference<>();
        ReentrantLock lock = new ReentrantLock();
        return () -> {
            T val = value.get();
            if (val == null) {
                lock.lock();
                try {
                    val = value.get();
                    if (val == null) {
                        val = Objects.requireNonNull(delegate.get());
                        value.set(val);
                    }
                } finally {
                    lock.unlock();
                }
            }
            return val;
        };
    }
}
