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
 * Recorded once. The first record to ask wins: the stopping flag fences every record dispatched after it, so a
 * second request would name a record that was on its way out anyway.
 *
 * @see ConsumerHandle#stopRequest()
 */
@InterfaceStability.Unstable
public final class StopRequest {

    private final String topic;

    private final int partition;

    private final long offset;

    private final String reason;

    private final Instant requestedAt;

    StopRequest(ConsumerRecord<?, ?> record, String reason, Instant requestedAt) {
        this.topic = record.topic();
        this.partition = record.partition();
        this.offset = record.offset();
        this.reason = reason;
        this.requestedAt = requestedAt;
    }

    public String topic() {
        return topic;
    }

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

    public Instant requestedAt() {
        return requestedAt;
    }

    @Override
    public String toString() {
        return "StopRequest(" + topic + "-" + partition + "@" + offset + ", at=" + requestedAt + ", " + reason + ")";
    }
}
