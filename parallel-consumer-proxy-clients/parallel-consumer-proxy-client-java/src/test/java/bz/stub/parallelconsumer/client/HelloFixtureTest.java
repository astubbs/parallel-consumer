/*
 * Copyright (C) 2026 Antony Stubbs and contributors
 */
package bz.stub.parallelconsumer.client;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;

import static com.google.common.truth.Truth.assertThat;

/**
 * The Java module's half of the polyglot scaffolding assertion: the PROGRAM runs and its stdout is
 * the fixture, rather than the constant being compared with itself. Reading the field would pass
 * even if {@code main} printed nothing at all, which is the failure this test exists to be able to
 * see.
 */
class HelloFixtureTest {

    @Test
    void theProgramPrintsTheFixtureLine() throws Exception {
        ByteArrayOutputStream captured = new ByteArrayOutputStream();
        PrintStream original = System.out;
        try {
            System.setOut(new PrintStream(captured, true, StandardCharsets.UTF_8.name()));
            HelloFixture.main(new String[0]);
        } finally {
            System.setOut(original);
        }

        assertThat(captured.toString(StandardCharsets.UTF_8.name()))
                .isEqualTo("parallel-consumer-proxy-client hello fixture: java");
    }
}
