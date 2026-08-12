package bz.stub.parallelconsumer.mutiny;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 * Modifications Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.ParallelEoSStreamProcessorTestBase;
import bz.stub.parallelconsumer.internal.AbstractParallelEoSStreamProcessor;

import static bz.stub.parallelconsumer.ParallelConsumerOptions.CommitMode.PERIODIC_CONSUMER_SYNC;

public class MutinyUnitTestBase extends ParallelEoSStreamProcessorTestBase {

    protected MutinyProcessor<String, String> mutinyPC;

    protected static final int MAX_CONCURRENCY = 1000;

    @Override
    protected AbstractParallelEoSStreamProcessor initAsyncConsumer(ParallelConsumerOptions parallelConsumerOptions) {
        var build = parallelConsumerOptions.toBuilder()
                .commitMode(PERIODIC_CONSUMER_SYNC)
                .maxConcurrency(MAX_CONCURRENCY)
                .build();

        mutinyPC = new MutinyProcessor<>(build);

        return mutinyPC;
    }
}