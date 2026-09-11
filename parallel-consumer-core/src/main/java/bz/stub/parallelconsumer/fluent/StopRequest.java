package bz.stub.parallelconsumer.fluent;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.annotation.InterfaceStability;

import java.time.Instant;

/**
 * Why this instance stopped: the record that asked and the reason it gave (R24).
 * <p>
 * A stop is a request about the <em>instance</em>, not a terminal outcome of the record, so it is recorded here
 * rather than in the parked view. It is what lets a caller of {@link ConsumerHandle#awaitShutdown()} tell a stop
 * apart from an ordinary close - both return normally, and only this says which happened.
 * <p>
 * Recorded once. The first record to ask wins: the pause it triggers stops every record dispatched after it, so a
 * second request would name a record that was on its way out anyway.
 *
 * @see ConsumerHandle#stopRequest()
 */
@InterfaceStability.Unstable
public final class StopRequest {

    /**
     * The stopping record's topic, copied rather than kept as a reference to the record: the request outlives the
     * dispatch, and the record itself is handed straight back to the engine as the stop is raised.
     */
    private final String topic;

    /**
     * The stopping record's partition. Recorded alongside the topic and offset because together they are enough to
     * find the record again after a restart re-delivers it.
     */
    private final int partition;

    /**
     * The stopping record's offset. It was deliberately left uncommitted, so this is also where processing resumes.
     */
    private final long offset;

    /**
     * What the route said when it asked, kept verbatim: it is the only account of the stop that reaches an operator.
     */
    private final String reason;

    /**
     * When the request was made, not when the instance finished closing. The close that follows takes as long as the
     * declared {@link ClosePath} needs, and the two moments are worth telling apart.
     */
    private final Instant requestedAt;

    /**
     * Package-private: a stop request is minted only by dispatch, from the record whose route asked. The record's
     * types are wildcards because the facade's engine-facing side sees raw bytes (KTD2), and nothing here needs them.
     */
    StopRequest(ConsumerRecord<?, ?> record, String reason, Instant requestedAt) {
        this.topic = record.topic();
        this.partition = record.partition();
        this.offset = record.offset();
        this.reason = reason;
        this.requestedAt = requestedAt;
    }

    /**
     * The topic of the record that asked - which also names the route, since one topic has one function.
     */
    public String topic() {
        return topic;
    }

    /**
     * The partition of the record that asked. With {@link #topic()} and {@link #offset()} it locates the record.
     */
    public int partition() {
        return partition;
    }

    /**
     * The stopping record's offset. It was left incomplete and never committed, so a restart delivers it again -
     * and the function will stop again unless the definition's author breaks that loop (R24).
     */
    public long offset() {
        return offset;
    }

    /**
     * What the route said when it asked, either from {@link Outcome#stop(String)} or from a route whose
     * {@link AfterRetries#stop()} reaction fired when the record ran out of attempts.
     */
    public String reason() {
        return reason;
    }

    /**
     * When the request was made. Read against the moment {@link ConsumerHandle#awaitShutdown()} returned, it says how
     * long the declared {@link ClosePath} took.
     */
    public Instant requestedAt() {
        return requestedAt;
    }

    /**
     * One line naming the record, the moment and the reason - what a shutdown log wants, without a caller having to
     * assemble it from five accessors.
     */
    @Override
    public String toString() {
        return "StopRequest(" + topic + "-" + partition + "@" + offset + ", at=" + requestedAt + ", " + reason + ")";
    }
}
