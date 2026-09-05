package bz.stub.parallelconsumer.streams;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

/**
 * The host described something the engine will not build.
 *
 * <p>Every message names the offending thing - the handle, the method, the state. A foreign caller never sees this
 * stack trace, only the text that crosses the wire, so text that says only "invalid request" is text that cannot be
 * debugged from the other side.
 */
public class TopologyDescriptionException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public TopologyDescriptionException(String message) {
        super(message);
    }
}
