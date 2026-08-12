package bz.stub.parallelconsumer.internal.utils;

/*-
 * Copyright (C) 2020-2021 Confluent, Inc.
 * Modifications Copyright (C) 2026 Antony Stubbs and contributors
 */

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

import static bz.stub.parallelconsumer.internal.utils.StringTestUtils.pretty;

@Slf4j
class JavaEnvTest {

    /**
     * Used to manually inspect the java environment at runtime - particularly useful for CI environments
     */
    @Test
    void checkJavaEnvironment() {
        log.error("Java all env: {}", pretty(System.getProperties().entrySet()));
    }
}
