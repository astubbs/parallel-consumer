package bz.stub.parallelconsumer;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.apache.kafka.common.errors.RebalanceInProgressException;

/**
 * {@link RebalanceInProgressException} on commit must not be fatal.
 * <p>
 * Kafka throws it when a commit lands while the group is mid-rebalance, and documents the remedy as
 * "complete the rebalance by calling poll() and then retry" - i.e. it is a transient protocol
 * condition, not a failure. It used to escape the classification ladder entirely, with
 * disproportionate consequences:
 * <ol>
 *     <li>it propagates out of {@code BrokerPollSystem.controlLoop()}, killing the broker-poll
 *         thread permanently;</li>
 *     <li>that thread is the only producer of commit responses, so the control thread's
 *         {@code commitAndWait()} blocks until {@code offsetCommitTimeout} and then throws
 *         "Timeout waiting for commit response" - a misleading symptom pointing nowhere near the
 *         cause;</li>
 *     <li>the control thread dies too, taking the whole PC instance down.</li>
 * </ol>
 * One retriable blip therefore killed the consumer. This surfaced as the Chaos Pain Suite's W4
 * revoke-under-work scenario going RED (both assignors), diagnosed in
 * {@code docs/plans/2026-08-01-001-investigate-chaos-w4-red-report.md}; cooperative rebalancing makes
 * it far likelier, because members keep committing <i>during</i> rebalances by design.
 * <p>
 * Mirrors {@link MockConsumerTestWithCommitTimeoutException} and
 * {@link MockConsumerTestWithSaslAuthenticationException}, which pin the other rungs of the ladder.
 * The scenario itself lives in {@link CommitRejectionTestBase}, shared with
 * {@link MockConsumerCommitFailedTest}.
 */
class MockConsumerRebalanceInProgressTest extends CommitRejectionTestBase {

    @Override
    protected RuntimeException rejection() {
        return new RebalanceInProgressException("Offset commit cannot be completed since the "
                + "consumer is undergoing a rebalance for auto partition assignment (mocking)");
    }
}
