// Copyright (C) 2026 Antony Stubbs and contributors

package bz.stub.parallelconsumer.client.scaladsl

import java.io.{BufferedReader, IOException, InputStream, InputStreamReader}
import java.nio.charset.StandardCharsets
import java.nio.file.Path
import java.util.concurrent.{LinkedBlockingQueue, TimeUnit}

import scala.collection.mutable
import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters._

import org.slf4j.LoggerFactory

/**
 * Where the sidecar binary is and how to invoke it - supplied by the application, always.
 *
 * The library never resolves the binary through `PATH`, a relative lookup, or any directory an
 * attacker could influence: this process hands that binary the user's Kafka credentials, so which
 * binary runs is security-relevant. An absolute path is therefore a precondition, not a preference
 * (guide §2).
 *
 * `arguments` are not configuration. Configuration is code and travels in `Configure`; these are for
 * the harness's own flags, and nothing credential-bearing may appear among them.
 */
final case class SidecarCommand(executable: Path, arguments: Seq[String] = Nil) {

  require(
    executable.isAbsolute,
    s"the sidecar must be named by an absolute path, got '$executable' - this process hands it your " +
      "Kafka credentials, so it is never resolved through PATH or a relative lookup")

  override def toString: String = s"SidecarCommand($executable, ${arguments.size} argument(s))"
}

/**
 * A running sidecar process and the loopback port it reported.
 *
 * Three lifecycle rules from the specification are implemented here and nowhere else: the process is
 * launched '''directly, never through a shell''' (a shell wrapper holds the stdin write end and
 * defeats the proxy's parent-death signal, leaking a JVM that still holds group membership); its
 * stdin pipe is held open and never written to, because EOF on it is what tells the proxy its parent
 * is gone; and closing that pipe is how the process is reaped.
 *
 * It is this module's own for now, and it should not be. Spawning belongs to the Java lifecycle unit
 * the plan owns, and until that lands every JVM client writes it again - Kotlin's `Sidecar.kt` is
 * the same file in a different language.
 */
private[scaladsl] final class Sidecar private (
    process: Process,
    val port: Int,
    stderrTail: mutable.Queue[String]) {

  /** The last few stderr lines, for an error message that names its own cause. */
  def diagnostics: String = stderrTail.synchronized(stderrTail.mkString("\n"))

  /**
   * Closes the lifecycle pipe and waits for the process to go. EOF on stdin is the parent-death
   * signal, so this is the reap: no kill, no signal, nothing the proxy has to interpret.
   */
  def reap(grace: FiniteDuration): Unit = {
    try process.getOutputStream.close()
    catch {
      case already: IOException => Sidecar.log.debug("The sidecar's stdin was already closed", already)
    }
    if (!process.waitFor(grace.toMillis, TimeUnit.MILLISECONDS)) {
      Sidecar.log.warn("The sidecar did not exit within {} of its stdin closing; forcing it", grace)
      val _ = process.destroyForcibly()
    }
  }
}

private[scaladsl] object Sidecar {

  private val log = LoggerFactory.getLogger("bz.stub.parallelconsumer.client.scaladsl.Sidecar")

  /** How many stderr lines are kept for diagnostics when a spawn goes wrong (guide §10.1). */
  private val StderrTailLines = 30

  /** `port: <n>` - the line the proxy prints when it has bound. */
  private val PortLine = """^port:\s*(\d+)$""".r

  /**
   * Launches the sidecar and waits for its port line. Blocking, and called from a future so no
   * caller's thread waits on it.
   *
   * The line is ''scanned for'' rather than read as the first line: the specification says first,
   * and the shipped sidecar honours that, but the test-mode harness logs before it - a harness
   * limitation every language's test absorbs. Scanning satisfies both, and this client never depends
   * on the difference.
   */
  def spawn(command: SidecarCommand, budget: FiniteDuration): Sidecar = {
    val process = new ProcessBuilder((command.executable.toString +: command.arguments).asJava)
      // no shell, and stdin stays an open pipe we never write to: its EOF is the parent-death
      // signal the proxy watches
      .redirectErrorStream(false)
      .start()

    val tail = mutable.Queue.empty[String]
    drainStderr(readerFor(process.getErrorStream), tail)
    val stdout = new LinkedBlockingQueue[Option[String]]()
    drainStdout(readerFor(process.getInputStream), stdout)

    awaitPortLine(stdout, budget) match {
      case Some(port) =>
        log.info("Sidecar listening on loopback port {}", port)
        new Sidecar(process, port, tail)
      case None =>
        val _ = process.destroyForcibly()
        throw new IllegalStateException(
          s"the sidecar did not report 'port: <n>' within $budget. Its last output was:\n" +
            tail.synchronized(tail.mkString("\n")))
    }
  }

  private def awaitPortLine(stdout: LinkedBlockingQueue[Option[String]], budget: FiniteDuration): Option[Int] = {
    val deadline = System.nanoTime() + budget.toNanos
    var answer = Option.empty[Int]
    var reading = true
    while (reading) {
      val remaining = deadline - System.nanoTime()
      if (remaining <= 0) {
        reading = false
      } else {
        stdout.poll(remaining, TimeUnit.NANOSECONDS) match {
          case null => reading = false // the budget ran out mid-wait
          case None =>
            reading = false
            throw new IllegalStateException(
              "the sidecar's stdout ended before it reported a port - the process died")
          case Some(line) =>
            PortLine.findFirstMatchIn(line.trim) match {
              case Some(matched) =>
                answer = Some(matched.group(1).toInt)
                reading = false
              // the sidecar's own stdout is NOT republished into the application's logs (guide
              // §10.1): it is the proxy's logging, in the proxy's format, at levels this library
              // cannot interpret. Draining it is mandatory; re-emitting it is not.
              case None => log.debug("sidecar stdout: {} bytes", line.length)
            }
        }
      }
    }
    answer
  }

  /**
   * Both drains run for the process's whole life, not just until the port arrives: a pipe nobody
   * reads fills up and blocks the writer, which would hang the sidecar mid-session rather than at a
   * moment anyone would connect to this cause (guide §10.1 - 64 KiB on Linux, seconds under load).
   */
  private def drainStdout(reader: BufferedReader, sink: LinkedBlockingQueue[Option[String]]): Unit =
    daemon("pc-sidecar-stdout") {
      try reader.lines().iterator().asScala.foreach(line => sink.put(Some(line)))
      finally {
        sink.put(None)
        reader.close()
      }
    }

  /**
   * stderr keeps a bounded tail for the spawn diagnostic ''and'' reaches this process's own stderr,
   * because a misconfigured broker becoming an unexplained hang is exactly what silencing a child's
   * diagnostics buys. Bounded, because an unbounded buffer of a chatty child is a leak of its own.
   */
  private def drainStderr(reader: BufferedReader, tail: mutable.Queue[String]): Unit =
    daemon("pc-sidecar-stderr") {
      try reader
        .lines()
        .iterator()
        .asScala
        .foreach { line =>
          tail.synchronized {
            tail.enqueue(line)
            if (tail.size > StderrTailLines) {
              val _ = tail.dequeue()
            }
          }
          System.err.println(line)
        }
      finally reader.close()
    }

  private def daemon(name: String)(work: => Unit): Unit = {
    val thread = new Thread(() => work, name)
    thread.setDaemon(true)
    thread.start()
  }

  private def readerFor(stream: InputStream): BufferedReader =
    new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))
}
