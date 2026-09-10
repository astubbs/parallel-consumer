package bz.stub.parallelconsumer.fluent;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.apache.kafka.common.annotation.InterfaceStability;

/**
 * A route's decode step declared this payload permanently unreadable (R12), so the record is parked at once without
 * consuming any of its attempts and the park observer fires with an attempt count of zero.
 * <p>
 * Only a deserialiser wrapped by {@link Formats#classifyDecodeFailures} throws this. Every other decode failure is
 * transient by default, because a stock deserialiser cannot tell a corrupt payload from a registry outage.
 */
@InterfaceStability.Unstable
public class PermanentDecodeFailureException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public PermanentDecodeFailureException(String message, Throwable cause) {
        super(message, cause);
    }
}
