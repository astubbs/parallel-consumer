package bz.stub.parallelconsumer;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.clients.consumer.RetriableCommitFailedException;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;

/**
 * The async commit callback arrives carrying an exception: the commit did not happen.
 * <p>
 * This is the failure {@code Consumer#commitAsync} is specified to report - a
 * {@link RetriableCommitFailedException} for a coordinator that was unavailable, timed out, or was
 * mid-rebalance - and it arrives <em>after</em> the call has already returned normally. PC logged it
 * and carried on, which was correct as far as it went; what was not correct was that the offsets had
 * already been marked clean when the request was sent, so "retriable" described a retry nothing was
 * left to perform.
 * <p>
 * The scenario and its assertions live in {@link AsyncCommitAcknowledgementTestBase}; only what the
 * mock does with the withheld acknowledgement differs.
 */
@Slf4j
class MockConsumerAsyncCommitCallbackFailsTest extends AsyncCommitAcknowledgementTestBase {

    @Override
    protected void withholdAcknowledgement(Map<TopicPartition, OffsetAndMetadata> offsets,
                                           OffsetCommitCallback callback) {
        // a fresh instance per call - these carry stack traces
        callback.onComplete(offsets, new RetriableCommitFailedException(
                "simulated: the group coordinator was not available"));
    }
}
