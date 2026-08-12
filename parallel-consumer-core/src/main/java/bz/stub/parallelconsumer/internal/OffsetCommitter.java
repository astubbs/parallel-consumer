package bz.stub.parallelconsumer.internal;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 * Modifications Copyright (C) 2026 Antony Stubbs and contributors
 */

import java.util.concurrent.TimeoutException;

/**
 * Contract for committing offsets. As there are two ways to commit offsets - through the Consumer or Producer, and
 * several systems involved, we need a contract.
 *
 * @author Antony Stubbs
 */
public interface OffsetCommitter {
    void retrieveOffsetsAndCommit() throws TimeoutException, InterruptedException;
}
