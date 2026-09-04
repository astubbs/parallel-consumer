package bz.stub.parallelconsumer.state;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.internal.PCModuleTestEnv;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * The fixture every {@link RetryQueue} unit test needs: an empty queue, a module to build work containers against,
 * and one factory for a container at a given offset.
 * <p>
 * Shared rather than repeated because it was repeated - `RetryQueueIteratorConfinementTest` arrived carrying a
 * verbatim copy of `RetryQueueTest`'s constants and {@code workFor}, differing only in the topic literal, and the
 * duplication report caught it. That is the shape `AGENTS.md` names when it says to reuse test utilities rather
 * than write a parallel one: a drifted copy of topic-creation logic once became a flaky-CI source.
 * <p>
 * The topic is one constant for both, deliberately. Neither suite asserts on the topic name - it exists only so a
 * {@link ConsumerRecord} can be built - so two names bought nothing and were the part most likely to drift.
 *
 * @author Antony Stubbs
 */
abstract class RetryQueueTestBase {

    static final String TOPIC = "retry-queue-topic";

    static final int PARTITION = 0;

    static final long EPOCH = 0L;

    final PCModuleTestEnv module = new PCModuleTestEnv();

    final RetryQueue retryQueue = new RetryQueue();

    WorkContainer<String, String> workFor(long offset) {
        return new WorkContainer<>(EPOCH, new ConsumerRecord<>(TOPIC, PARTITION, offset, "key-" + offset, "value"), module);
    }
}
