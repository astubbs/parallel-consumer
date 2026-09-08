package bz.stub.parallelconsumer;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;

/**
 * The async commit callback never arrives at all: no success, no failure, nothing.
 * <p>
 * Distinct from its sibling {@link MockConsumerAsyncCommitCallbackFailsTest}, and the more demanding
 * of the two, because there is no event to hang a decision on. A callback is delivered on the next
 * {@code poll()} that reaches the coordinator's response, so anything that stops that response
 * arriving - a coordinator that never answers, a connection that dies with the request in flight,
 * a consumer closed before the round trip completes - leaves the request permanently unanswered.
 * Silence must therefore be the state that does <em>not</em> advance PC's bookkeeping: the offsets
 * stay dirty and are simply committed again on the next cycle, which is what the async mode's
 * absent retry budget leaves as the only recovery. If instead the send had advanced the state, this
 * scenario would have no observable at all - the offsets would be clean, nothing would be
 * re-committed, and the broker would never learn the position.
 * <p>
 * The scenario and its assertions live in {@link AsyncCommitAcknowledgementTestBase}; only what the
 * mock does with the withheld acknowledgement differs.
 */
@Slf4j
class MockConsumerAsyncCommitCallbackDroppedTest extends AsyncCommitAcknowledgementTestBase {

    @Override
    protected void withholdAcknowledgement(Map<TopicPartition, OffsetAndMetadata> offsets,
                                           OffsetCommitCallback callback) {
        // deliberately empty: the callback is dropped, which is the scenario
        log.info("Dropping the callback for {} - no acknowledgement will ever arrive for it", offsets);
    }
}
