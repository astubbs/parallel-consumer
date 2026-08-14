package bz.stub.parallelconsumer.client;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * The per-record verdict a {@link RecordProcessor} returns: success (optionally carrying records for Parallel
 * Consumer to produce) or failure (carrying a reason). Success lets the record's offset advance; failure sends
 * it back to Parallel Consumer's own retry scheduling, and the reason travels with the redelivery as
 * {@link InboundRecord#lastFailureReason()}.
 * <p>
 * A closed two-armed value rather than a class hierarchy, so a language with no subtyping or no exceptions
 * mirrors it directly (KTD1). There is deliberately no third arm: a processor that cannot decide has not
 * finished processing.
 *
 * @author Antony Stubbs
 */
public final class Outcome {

    private static final Outcome BARE_SUCCESS = new Outcome(true, Collections.emptyList(), null);

    private final boolean success;
    private final List<OutboundRecord> produce;
    private final String failureReason;

    private Outcome(boolean success, List<OutboundRecord> produce, String failureReason) {
        this.success = success;
        this.produce = produce;
        this.failureReason = failureReason;
    }

    /** Success with nothing to produce: the record's offset advances, and that is all. */
    public static Outcome success() {
        return BARE_SUCCESS;
    }

    /**
     * Success with records for Parallel Consumer to produce (R6) - the only sanctioned route for the
     * processor's Kafka output. An empty list is equivalent to {@link #success()}.
     */
    public static Outcome success(List<OutboundRecord> produce) {
        if (produce == null || produce.isEmpty()) {
            return BARE_SUCCESS;
        }
        return new Outcome(true, Collections.unmodifiableList(new ArrayList<>(produce)), null);
    }

    /**
     * Failure: the record returns to Parallel Consumer's retry scheduling, and the reason accompanies the
     * redelivery. The reason may be {@code null} when there is nothing useful to say.
     */
    public static Outcome failure(String reason) {
        return new Outcome(false, Collections.emptyList(), reason);
    }

    public boolean isSuccess() {
        return success;
    }

    /** The records to produce on success; always empty for a failure. */
    public List<OutboundRecord> produce() {
        return produce;
    }

    /** The failure reason; always empty for a success. */
    public Optional<String> failureReason() {
        return Optional.ofNullable(failureReason);
    }

    @Override
    public String toString() {
        return success ? "Outcome.success(produce=" + produce.size() + ")" : "Outcome.failure";
    }
}
