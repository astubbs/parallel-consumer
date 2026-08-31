// Copyright (C) 2026 Antony Stubbs and contributors

package bz.stub.parallelconsumer.client.scaladsl

import java.net.{InetAddress, ServerSocket}
import java.nio.file.attribute.PosixFilePermissions
import java.nio.file.{Files, Path, Paths}

import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.control.NonFatal

import bz.stub.parallelconsumer.client.scaladsl.ScalaTruth.assertThat
import io.grpc.{Status, StatusRuntimeException}
import org.junit.jupiter.api.{DisplayName, Test}

/**
 * '''The Scala client's handshake, against the real sidecar rather than a stand-in.'''
 *
 * The sidecar spawned is `parallel-consumer-proxy`'s production entry point, launched as an ordinary
 * child process - so this exercises the whole lifecycle contract the specification describes and not
 * an in-process shortcut: launch directly, hold the stdin pipe, find the port line, connect to
 * loopback, put `Configure` on the wire, and turn what came back into a failed `Future`.
 *
 * '''What it can prove on this branch, and what it deliberately cannot.''' The sidecar here hosts no
 * Parallel Consumer engine and answers every session `UNIMPLEMENTED`, so there is no dispatch to
 * observe and none is faked. The dispatch scenarios - one record end to end, the in-flight ceiling,
 * the redelivery history - belong to the shared conformance suite and are deferred until an engine
 * exists to run them against.
 *
 * '''It runs only in the harness lane''' (`-Dpc.foreignClients`, which this module's CI row passes),
 * because the classpath it needs is what a permanent Maven edge to the sidecar module would cost -
 * see the `scala-sidecar-harness` profile. When the classpath file is missing it FAILS and names the
 * command; a test that quietly does not run is not a passing test.
 *
 * '''The status code is the assertion, not merely "it failed".''' A refusal from the authority
 * allowlist is `PERMISSION_DENIED` and one from the admission slot is `RESOURCE_EXHAUSTED`, both
 * raised by interceptors ''before'' the service method runs. Only `UNIMPLEMENTED` can have come from
 * the service itself, so the code is what separates "the connection was turned away" from "the
 * handshake was delivered and answered".
 */
class SidecarHandshakeTest {

  private implicit val ec: ExecutionContext = ExecutionContext.global

  @Test
  @DisplayName("the handshake reaches the session service and its refusal reaches the caller")
  def theHandshakeReachesTheSessionServiceAndItsRefusalReachesTheCaller(): Unit = {
    val refused = failureOf(
      // kafka_properties is empty deliberately: this sidecar reads none, and real credentials never
      // belong in a test
      ParallelConsumerClient.open(ClientOptions(topics = Seq("handshake-topic")), engineLessSidecar()))

    val status = statusOf(refused)
    assertThat(status.getCode.name)
      .isEqualTo(Status.Code.UNIMPLEMENTED.name)
    assertThat(status.getDescription).contains(NoEngineDescription)
  }

  /**
   * The control arm, and it is permanent rather than a one-off demonstration: pointed at a port
   * nothing is listening on, the same client fails in a way that is not the refusal above. Without
   * it, the test that matters could be passing on any failure at all - which is the shape of an
   * assertion that cannot fail for the reason it names.
   */
  @Test
  @DisplayName("a sidecar that is not listening fails differently from one that refuses")
  def aSidecarThatIsNotListeningFailsDifferentlyFromOneThatRefuses(): Unit = {
    val failed = failureOf(
      ParallelConsumerClient.open(
        ClientOptions(topics = Seq("handshake-topic")),
        SidecarCommand(writeAnnouncer(reserveThenReleaseAPort()), Seq.empty)))

    assertThat(statusCodeName(failed)).isNotEqualTo(Status.Code.UNIMPLEMENTED.name)
  }

  /** The failure a `Future` completed with, or an assertion error if it succeeded. */
  private def failureOf(future: Future[ParallelConsumerClient]): Throwable =
    try {
      val client = Await.result(future, HandshakeBudget)
      client.close()
      throw new AssertionError(
        "the sidecar hosts no engine, so open must fail rather than report a configured session")
    } catch {
      case NonFatal(failure) if !failure.isInstanceOf[AssertionError] => failure
    }

  /**
   * The sidecar command. `Main` is a classpath invocation, so "the sidecar binary" here is the JVM
   * launcher and the classpath is an argument - everything awkward about that lives in this one
   * method.
   *
   * NO ARGUMENTS beyond the classpath: the sidecar takes none and refuses to start when given one,
   * because everything is configured connect-time over the protocol.
   */
  private def engineLessSidecar(): SidecarCommand = {
    val classpathFile = Paths.get("target", "sidecar-classpath.txt").toAbsolutePath
    if (!Files.isRegularFile(classpathFile)) {
      throw new IllegalStateException(
        s"$classpathFile is missing - it is written by the scala-sidecar-harness profile: run " +
          "`./mvnw --batch-mode test -pl :parallel-consumer-proxy-client-scala -am -Dpc.foreignClients`")
    }
    val classpath = Files.readString(classpathFile).trim
    if (classpath.isEmpty) {
      throw new IllegalStateException(s"$classpathFile is empty")
    }

    // PATH lookup would be wrong in the library and is unnecessary here: this is the JVM the test
    // itself is running on
    val java = Paths.get(System.getProperty("java.home"), "bin", "java")
    SidecarCommand(java, Seq("-cp", classpath, SidecarMain))
  }

  /** A loopback port the OS has just handed out and nothing is listening on. */
  private def reserveThenReleaseAPort(): Int = {
    val reserved = new ServerSocket(0, 1, InetAddress.getLoopbackAddress)
    try reserved.getLocalPort
    finally reserved.close()
  }

  /**
   * A sidecar that announces a port and then holds its stdin, which is the spawning contract's whole
   * client-visible surface - so the control arm drives the library's REAL connect path at a dead
   * port rather than the different path a child that printed nothing would take. `printf` and `read`
   * are shell builtins, so it is one process holding its own lifeline.
   */
  private def writeAnnouncer(port: Int): Path = {
    val script = Files.createTempDirectory("pc-scala-announcer").resolve("announcer.sh")
    Files.writeString(
      script,
      s"""|#!/bin/sh
          |printf 'port: $port\\n'
          |while read -r _ignored; do :; done
          |exit 0
          |""".stripMargin)
    Files.setPosixFilePermissions(script, PosixFilePermissions.fromString("rwx------"))
    script
  }

  /**
   * The gRPC status behind a failure. Asserted to be present rather than tolerated as absent: a
   * non-gRPC failure here would mean the transport never reached the wire, which is a different
   * finding from either outcome these tests distinguish and must not be reported as one of them.
   */
  private def statusOf(thrown: Throwable): Status =
    grpcStatus(thrown).getOrElse(throw new AssertionError(
      "no gRPC status in the failure chain, so the transport never reached the wire", thrown))

  private def statusCodeName(thrown: Throwable): String =
    grpcStatus(thrown).map(_.getCode.name).getOrElse("no-grpc-status: " + thrown)

  private def grpcStatus(thrown: Throwable): Option[Status] = {
    var cause: Throwable = thrown
    while (cause != null) {
      cause match {
        case status: StatusRuntimeException => return Some(status.getStatus)
        case _ => cause = cause.getCause
      }
    }
    None
  }

  private val SidecarMain = "bz.stub.parallelconsumer.proxy.Main"

  /** What the sidecar's refusal must name, so a client author does not debug their own code. */
  private val NoEngineDescription = "hosts no Parallel Consumer engine"

  private val HandshakeBudget: FiniteDuration = 90.seconds
}
