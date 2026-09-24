package bz.stub.parallelconsumer.internal;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

/**
 * What a producer replacement attempt did - the result the control loop acts on, returned by the manager's
 * {@code completeReplacement()}, which lands with the recovery rung above this one. Its own file, beside
 * {@link ProducerRecoveryPolicy}, because it holds no lock and no producer.
 */
@lombok.Value
public class ReplacementOutcome {
    public enum Kind {REPLACED, DEFERRED, TERMINAL}

    Kind kind;
    /** The failure to report, for a TERMINAL outcome; null otherwise. */
    ProducerInvalidatedException failure;

    public boolean isTerminal() {
        return kind == Kind.TERMINAL;
    }
}
