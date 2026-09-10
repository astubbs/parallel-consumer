package bz.stub.parallelconsumer.fluent;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.apache.kafka.common.annotation.InterfaceStability;

/**
 * The instance's control thread died, and this is what {@link ConsumerHandle#awaitShutdown()} throws to say so
 * (R17).
 * <p>
 * <b>It wraps rather than rethrows.</b> The cause came off the engine's own failure record, which is a checked
 * {@link Exception} the awaiting caller never declared - and wrapping is also what distinguishes it from a
 * definition fault, which is the facade's own and is rethrown as it was thrown (KTD3).
 */
@InterfaceStability.Unstable
public class InstanceFailedException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    InstanceFailedException(String message, Throwable cause) {
        super(message, cause);
    }
}
