// Copyright (C) 2026 Antony Stubbs and contributors

package bz.stub.parallelconsumer.client.scaladsl

import java.io.{ByteArrayOutputStream, PrintStream}
import java.nio.charset.StandardCharsets

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

/** Runs the PROGRAM and reads its stdout, rather than comparing the constant with itself - the
  * latter would pass with a `main` that printed nothing.
  */
class HelloFixtureTest {

  @Test
  def theProgramPrintsTheFixtureLine(): Unit = {
    val captured = new ByteArrayOutputStream()
    val original = System.out
    try {
      System.setOut(new PrintStream(captured, true, StandardCharsets.UTF_8.name()))
      HelloFixture.main(Array.empty)
    } finally {
      System.setOut(original)
    }

    assertEquals("parallel-consumer-proxy-client hello fixture: scala", captured.toString(StandardCharsets.UTF_8.name()))
  }
}
