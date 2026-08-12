package bz.stub.parallelconsumer;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 * Modifications Copyright (C) 2026 Antony Stubbs and contributors
 */

public interface BatchTestBase {

    void averageBatchSizeTest();

    /**
     * Use:
     *
     * @ParameterizedTest
     * @EnumSource
     */
    void simpleBatchTest(ParallelConsumerOptions.ProcessingOrder order);

    /**
     * Use:
     *
     * @ParameterizedTest
     * @EnumSource
     */
    void batchFailureTest(ParallelConsumerOptions.ProcessingOrder order);
}
