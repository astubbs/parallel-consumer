package bz.stub.parallelconsumer.reactor;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 * Modifications Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.ParallelEoSStreamProcessorTestBase;
import bz.stub.parallelconsumer.internal.AbstractParallelEoSStreamProcessor;
import reactor.core.publisher.Flux;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.BaseStream;

import static bz.stub.parallelconsumer.ParallelConsumerOptions.CommitMode.PERIODIC_CONSUMER_SYNC;

public class ReactorUnitTestBase extends ParallelEoSStreamProcessorTestBase {

    protected ReactorProcessor<String, String> reactorPC;

    protected static final int MAX_CONCURRENCY = 1000;

    /**
     * The in-flight cap the engine under test runs with. High by default, so throughput-shaped tests are never
     * throttled by it - override to a small number when the point of the test is what happens once the cap
     * <em>binds</em> (a record that never completes holds a slot forever, so with a small cap the stall shows up as a
     * user function that stops being invoked).
     *
     * @see ReactorEmptyPublisherTest
     */
    protected int getMaxConcurrency() {
        return MAX_CONCURRENCY;
    }

    /**
     * The commit mode the engine under test runs with. Override to exercise a different one.
     */
    protected ParallelConsumerOptions.CommitMode getCommitMode() {
        return PERIODIC_CONSUMER_SYNC;
    }

    @Override
    protected AbstractParallelEoSStreamProcessor initAsyncConsumer(ParallelConsumerOptions parallelConsumerOptions) {
        var build = parallelConsumerOptions.toBuilder()
                .commitMode(getCommitMode())
                .maxConcurrency(getMaxConcurrency())
                .build();

        reactorPC = new ReactorProcessor<>(build);

        return reactorPC;
    }

    protected static Flux<String> fromPath(Path path) {
        return Flux.using(() -> Files.lines(path),
                Flux::fromStream,
                BaseStream::close
        );
    }

}
