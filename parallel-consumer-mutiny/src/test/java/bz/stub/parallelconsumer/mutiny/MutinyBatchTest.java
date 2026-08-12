package bz.stub.parallelconsumer.mutiny;

/*-
 * Copyright (C) 2020-2025 Confluent, Inc.
 * Modifications Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.internal.utils.KafkaTestUtils;
import bz.stub.parallelconsumer.BatchTestBase;
import bz.stub.parallelconsumer.BatchTestMethods;
import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.PollContext;
import bz.stub.parallelconsumer.internal.AbstractParallelEoSStreamProcessor;
import bz.stub.parallelconsumer.internal.RateLimiter;
import io.smallrye.mutiny.Uni;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static bz.stub.parallelconsumer.internal.utils.StringUtils.msg;

@Slf4j
public class MutinyBatchTest extends MutinyUnitTestBase implements BatchTestBase {

    BatchTestMethods<Uni<String>> batchTestMethods;

    @BeforeEach
    void setup() {
        batchTestMethods = new BatchTestMethods<>(this) {

            @Override
            protected KafkaTestUtils getKtu() {
                return ktu;
            }

            @SneakyThrows
            @Override
            protected Uni<String> averageBatchSizeTestPollStep(PollContext<String, String> recordList) {
                return Uni.createFrom()
                        .item(msg("Saw batch or records: {}", recordList.getOffsetsFlattened()))
                        .onItem().delayIt().by(Duration.ofMillis(30));
            }

            @Override
            protected void averageBatchSizeTestPoll(AtomicInteger numBatches, AtomicInteger numRecords, RateLimiter statusLogger) {
                mutinyPC.onRecord(recordList ->
                        averageBatchSizeTestPollInner(numBatches, numRecords, statusLogger, recordList)
                );
            }

            @Override
            protected AbstractParallelEoSStreamProcessor getPC() {
                return mutinyPC;
            }

            @Override
            public void simpleBatchTestPoll(List<PollContext<String, String>> batchesReceived) {
                mutinyPC.onRecord(recordList -> {
                    String msg = msg("Saw batch or records: {}", recordList.getOffsetsFlattened());
                    log.debug(msg);
                    batchesReceived.add(recordList);
                    return Uni.createFrom().item(msg);
                });
            }

            @Override
            protected void batchFailPoll(List<PollContext<String, String>> batchesReceived) {
                mutinyPC.onRecord(recordList -> {
                    batchFailPollInner(recordList);
                    batchesReceived.add(recordList);
                    return Uni.createFrom().item(msg("Saw batch or records: {}", recordList.getOffsetsFlattened()));
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

