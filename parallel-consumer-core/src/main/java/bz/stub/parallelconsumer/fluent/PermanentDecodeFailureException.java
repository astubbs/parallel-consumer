package bz.stub.parallelconsumer.fluent;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerException;
import org.apache.kafka.common.annotation.InterfaceStability;

/**
 * A route's decode step declared this payload permanently unreadable (R12), so the record is parked at once without
 * consuming any of its attempts and the park observer fires with an attempt count of zero.
 * <p>
 * Only a deserialiser wrapped by {@link Formats#classifyDecodeFailures} throws this. Every other decode failure is
 * transient by default, because a stock deserialiser cannot tell a corrupt payload from a registry outage.
 */
@InterfaceStability.Unstable
public class PermanentDecodeFailureException extends ParallelConsumerException {

    /**
     * Fixed rather than computed: a {@link Throwable} is serialisable whether or not anything here intends to
     * serialise one, so the identity is pinned rather than left to the compiler.
     */
    private static final long serialVersionUID = 1L;

    /**
     * The cause is the exception the deserialiser threw, kept because it is the only account of what was wrong with
     * the payload - this class adds the verdict, not the diagnosis. Public because the type is part of what a
     * classifier's caller may catch and assert on, though in the library only the wrapper built by
     * {@link Formats#classifyDecodeFailures} throws it.
     */
    public PermanentDecodeFailureException(String message, Throwable cause) {
        super(message, cause);
    }
}
