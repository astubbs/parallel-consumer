package bz.stub.parallelconsumer.client;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

/**
 * The user's function: one record in, one {@link Outcome} out. The wrapper calls it once per delivered record,
 * on a wrapper-owned thread; the outcome - not any side channel - is what advances, retries, and produces.
 * <p>
 * The outcome is a <em>return value</em> rather than an exception contract because languages without
 * exceptions must mirror this surface exactly (KTD1). As a Java convenience only, a throw is taken as
 * {@link Outcome#failure(String)} with the exception's message - the transports translate it, so a lambda can
 * use either idiom.
 *
 * @author Antony Stubbs
 */
@FunctionalInterface
public interface RecordProcessor {

    /**
     * Processes one record.
     *
     * @return the per-record outcome; never {@code null}
     * @throws Exception equivalent to returning {@link Outcome#failure(String)} with the exception's message
     */
    Outcome process(InboundRecord record) throws Exception;
}
