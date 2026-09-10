package bz.stub.parallelconsumer.sandbox;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

/**
 * One topic's supply of generated records, as {@link RecordGenerator} sees it.
 * <p>
 * The seam between pacing and content: the generator owns the rate, the bound and the thread and knows nothing
 * about types or encoding; a feed owns exactly one topic's records and knows nothing about when to publish them.
 * That is what lets the fluent path (which must encode with each route's serialisers, because the engine below
 * the facade reads raw bytes) and the classic path (which hands the mock consumer typed objects and encodes
 * nothing) share one generator rather than two.
 */
interface TopicFeed {

    String topic();

    /**
     * Publish the record at {@code index} of this topic's sequence. The index addresses the record, so the same
     * seed and index give the same record however the run is paced.
     *
     * @return false when the consumer has been closed under us and there is nothing left to publish into;
     * anything else that goes wrong throws
     */
    boolean publish(long index);
}
