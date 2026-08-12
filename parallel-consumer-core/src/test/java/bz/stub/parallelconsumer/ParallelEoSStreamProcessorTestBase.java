package bz.stub.parallelconsumer;

/*-
 * Copyright (C) 2020-2023 Confluent, Inc.
 * Modifications Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.internal.AbstractParallelEoSStreamProcessor;
import bz.stub.parallelconsumer.internal.PCModule;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ParallelEoSStreamProcessorTestBase extends AbstractParallelEoSStreamProcessorTestBase {

    protected ParallelEoSStreamProcessor<String, String> parallelConsumer;
    @Getter
    private PCModule module;

    @Override
    protected AbstractParallelEoSStreamProcessor<String, String> initAsyncConsumer(ParallelConsumerOptions<String, String> parallelConsumerOptions) {
        return initPollingAsyncConsumer(parallelConsumerOptions);
    }

    protected ParallelEoSStreamProcessor<String, String> initPollingAsyncConsumer(ParallelConsumerOptions<String, String> parallelConsumerOptions) {
        module = new PCModule<>(parallelConsumerOptions);
        parallelConsumer = new ParallelEoSStreamProcessor<>(parallelConsumerOptions);
        super.parentParallelConsumer = parallelConsumer;
        return parallelConsumer;
    }

}
