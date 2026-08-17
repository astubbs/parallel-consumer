package bz.stub.parallelconsumer.reactor;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 * Modifications Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.internal.utils.KafkaTestUtils;
import bz.stub.parallelconsumer.BatchTestBase;
import bz.stub.parallelconsumer.BatchTestMethods;
import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.PollContext;
import bz.stub.parallelconsumer.internal.AbstractParallelEoSStreamProcessor;
import bz.stub.parallelconsumer.internal.RateLimiter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static bz.stub.parallelconsumer.internal.utils.StringUtils.msg;

@Slf4j
public class ReactorBatchTest extends ReactorUnitTestBase implements BatchTestBase {

    BatchTestMethods<Mono<String>> batchTestMethods;

    @BeforeEach
    void setup() {
        batchTestMethods = new BatchTestMethods<>(this) {

            @Override
            protected KafkaTestUtils getKtu() {
                return ktu;
            }

            @SneakyThrows
            @Override
            protected Mono<String> averageBatchSizeTestPollStep(PollContext<String, String> recordList) {
                return Mono.just(msg("Saw batch or records: {}", recordList.getOffsetsFlattened()))
                        .delayElement(Duration.ofMillis(30));
            }

            @Override
            protected void averageBatchSizeTestPoll(AtomicInteger numBatches, AtomicInteger numRecords, RateLimiter statusLogger) {
                reactorPC.react(recordList ->
                        averageBatchSizeTestPollInner(numBatches, numRecords, statusLogger, recordList)
                );
            }

            @Override
            protected AbstractParallelEoSStreamProcessor getPC() {
                return reactorPC;
            }

            @Override
            public void simpleBatchTestPoll(List<PollContext<String, String>> batchesReceived) {
                reactorPC.react(recordList -> {
                    String msg = msg("Saw batch or records: {}", recordList.getOffsetsFlattened());
                    log.debug(msg);
                    batchesReceived.add(recordList);
                    return Mono.just(msg);
                });
            }

            @Override
            protected void batchFailPoll(List<PollContext<String, String>> batchesReceived) {
                reactorPC.react(recordList -> {
                    batchFailPollInner(recordList);
                    batchesReceived.add(recordList);
                    return Mono.just(msg("Saw batch or records: {}", recordList.getOffsetsFlattened()));
                });
            }
        };
    }

    @Test
    public void averageBatchSizeTest() {
        batchTestMethods.averageBatchSizeTest(10000);
    }

    @ParameterizedTest
    @EnumSource
    @Override
    public void simpleBatchTest(ParallelConsumerOptions.ProcessingOrder order) {
        batchTestMethods.simpleBatchTest(order);
    }

    @ParameterizedTest
    @EnumSource
    @Override
    public void batchFailureTest(ParallelConsumerOptions.ProcessingOrder order) {
        batchTestMethods.batchFailureTest(order);
    }

}
