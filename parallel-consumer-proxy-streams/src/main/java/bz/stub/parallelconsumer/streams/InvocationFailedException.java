package bz.stub.parallelconsumer.streams;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

/**
 * A foreign function did not produce a value: it reported an error, or it did not answer in time.
 *
 * <p>Thrown rather than swallowed. A record that fails is visible; a record that quietly took a substitute value
 * into an aggregation is a wrong count nobody can trace.
 */
public class InvocationFailedException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public InvocationFailedException(String message) {
        super(message);
    }
}
