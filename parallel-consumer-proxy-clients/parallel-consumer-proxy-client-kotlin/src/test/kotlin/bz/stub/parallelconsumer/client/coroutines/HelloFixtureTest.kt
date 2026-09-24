// Copyright (C) 2026 Antony Stubbs and contributors

package bz.stub.parallelconsumer.client.coroutines

import java.io.ByteArrayOutputStream
import java.io.PrintStream
import kotlin.test.Test
import kotlin.test.assertEquals

/**
 * Runs the PROGRAM and reads its stdout, rather than comparing the constant with itself - the
 * latter would pass with a `main` that printed nothing.
 */
class HelloFixtureTest {

    @Test
    fun `the program prints the fixture line`() {
        val captured = ByteArrayOutputStream()
        val original = System.out
        try {
            System.setOut(PrintStream(captured, true, Charsets.UTF_8.name()))
            HelloFixture.main(emptyArray())
        } finally {
            System.setOut(original)
        }

        assertEquals("parallel-consumer-proxy-client hello fixture: kotlin", captured.toString(Charsets.UTF_8.name()))
    }
}
