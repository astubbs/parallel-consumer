package bz.stub.parallelconsumer.client;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

/**
 * The one definition of how a {@link RecordProcessor} invocation becomes an {@link Outcome} - shared by every
 * transport so the Java-convenience exception bridge behaves identically under both (KTD20: the transport is
 * the only variable). A throw becomes {@link Outcome#failure(String)} with the exception's message; a
 * {@code null} return is a processor bug reported as a failure rather than a wrapper crash, because one bad
 * record must not take down the client.
 *
 * @author Antony Stubbs
 */
public final class Outcomes {

    private Outcomes() {
    }

    /** Runs the processor on the record, translating a throw or a {@code null} return into a failure. */
    public static Outcome applyProcessor(RecordProcessor processor, InboundRecord record) {
        Outcome outcome;
        try {
            outcome = processor.process(record);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return Outcome.failure("processing was interrupted");
        } catch (Exception e) {
            return Outcome.failure(e.getMessage() != null ? e.getMessage() : e.toString());
        }
        if (outcome == null) {
            return Outcome.failure("the RecordProcessor returned null instead of an Outcome");
        }
        return outcome;
    }
}
