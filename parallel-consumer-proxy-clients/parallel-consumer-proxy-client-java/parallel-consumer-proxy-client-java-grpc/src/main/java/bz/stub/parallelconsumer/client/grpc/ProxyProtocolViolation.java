package bz.stub.parallelconsumer.client.grpc;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

/**
 * The proxy did something the frozen contract forbids. Not a load condition and not retryable - the session
 * is over.
 * <p>
 * It is a distinct type rather than a plain {@code IllegalStateException} because the two possible readings of
 * a failed session lead opposite ways: a transport or environment failure is worth reconnecting through, and a
 * contract breach is worth failing loudly on. Every wrapper over this transport inherits the distinction
 * rather than inventing its own.
 *
 * @author Antony Stubbs
 */
public class ProxyProtocolViolation extends IllegalStateException {

    private static final long serialVersionUID = 1L;

    public ProxyProtocolViolation(String message) {
        super(message);
    }
}
