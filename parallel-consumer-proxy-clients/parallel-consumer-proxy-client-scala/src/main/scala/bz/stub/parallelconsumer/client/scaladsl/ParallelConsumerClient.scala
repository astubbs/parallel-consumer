// Copyright (C) 2026 Antony Stubbs and contributors

package bz.stub.parallelconsumer.client.scaladsl

import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionStage
import java.util.concurrent.atomic.AtomicBoolean

import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.concurrent.{ExecutionContext, Future, Promise, blocking}
import scala.jdk.FutureConverters._
import scala.util.control.NonFatal

import bz.stub.parallelconsumer.client.grpc.GrpcParallelConsumerClient
import bz.stub.parallelconsumer.client.{InboundRecord => JavaInboundRecord}
import bz.stub.parallelconsumer.client.{Outcome => JavaOutcome}
import org.slf4j.LoggerFactory

/**
 * A Parallel Consumer session, driven from Scala: key-ordered concurrent Kafka processing with the
 * engine running as a sidecar child process and the user's function running as an ordinary
 * `InboundRecord => Future[Outcome]`.
 *
 * {{{
 * implicit val ec: ExecutionContext = ExecutionContext.global
 *
 * for {
 *   client <- ParallelConsumerClient.open(
 *     ClientOptions(topics = Seq("orders"), kafkaProperties = Map("bootstrap.servers" -> "...")),
 *     SidecarCommand(Paths.get("/absolute/path/to/parallel-consumer-proxy")))
 *   _ <- client.poll(record => handle(record.value).map(_ => Outcome.succeeded))
 * } yield ()
 * }}}
 *
 * ==It wraps the Java client, and that is the design==
 *
 * The session itself - the one bidirectional stream, the handshake, the dispatch queue, the overflow
 * rule, the FIFO hand-out, the token echo - belongs to
 * `parallel-consumer-proxy-client-java-grpc`, and this class holds one of those and gives it a Scala
 * shape. Nothing here reads a protobuf message or touches a channel.
 *
 * '''The reason is arithmetic rather than taste.''' A second JVM session implementation means every
 * session defect is fixed twice, a third means three times; the Kotlin wave settled this and paid
 * for it, and this module is the third that would have existed. What is left here is what is
 * genuinely Scala: futures, `Option`, a sealed outcome, and an execution context the application
 * supplies.
 *
 * The visible consequence is that this client '''inherits''' the transport's behaviour, for good and
 * ill: the in-flight ceiling and the queue that enforces it, overflow as a protocol violation rather
 * than a load condition, hand-out order, the verbatim token echo, the shutdown order, and the
 * session end with its cause. What it adds is the Scala shape over all of that, and the one thing
 * Kotlin left open - [[poll]]'s future is the transport's own session end, so a stream that dies
 * mid-session completes it with the cause rather than leaving the caller believing it is consuming.
 *
 * The client is '''stateless per record'''. The fencing token never reaches this layer at all; it
 * rides from the dispatch queue to the report inside the transport. Fencing is the proxy's job.
 *
 * This wave implements exactly the `dispatch` capability: connect, configure, receive a wave, run
 * the function, report, shut down cleanly. Leases and heartbeats, the manifest reconnect,
 * worker-death reporting, terminal outcomes and the `Shutdown` drain are later waves, and this
 * client declares none of them - so the proxy does not expect them of it.
 */
final class ParallelConsumerClient private (
    sidecar: Sidecar,
    transport: GrpcParallelConsumerClient,
    /** The effective, negotiated session - what the proxy said it is running, not what was asked. */
    val session: Session)(implicit ec: ExecutionContext)
    extends AutoCloseable {

  import ParallelConsumerClient._

  private val polled = new AtomicBoolean(false)
  private val teardownStarted = new AtomicBoolean(false)

  /**
   * False once shutdown has begun. A record handed out after that point is ''not'' run: the client
   * never invents a verdict for work it did not do - see [[ParallelConsumerClient.startRecord]].
   */
  @volatile private var handingOut = true

  /**
   * The session's end, as the transport reports it: completed when the session ended cleanly, failed
   * with the cause when it did not - a broken stream, a refused handshake, a protocol violation.
   * Teardown hangs off it, so the sidecar is reaped however the session ended.
   *
   * '''This is what closes the gap the Kotlin client still carries.''' There, `poll` returns only on
   * `close` or cancellation because nothing joins the caller's end to the transport's; here they are
   * the same future by construction, so there is no wiring left to forget.
   */
  private val sessionEnded: Future[Unit] = transport
    .sessionEnd()
    .asScala
    .transform { ended =>
      // before teardown, so a record handed out during the drain is not started
      handingOut = false
      teardown()
      ended.map(_ => ())
    }

  /**
   * Runs the session, handing every delivered record to `process`.
   *
   * '''It returns a future for the session, and that is this client's answer to a question the
   * shared specification deliberately leaves open''' (guide §1: the shape is each language's own,
   * the property is not). It does not block, and it does not return a value once processing has
   * started: the future completes when the session has ended and the sidecar has been reaped, and it
   * ''fails with the cause'' when the session died rather than ended - a broken stream, a protocol
   * violation, a sidecar that went away. A caller learns both halves - that it ended, and why -
   * without closing the client to find out.
   *
   * Each record's verdict is the future the user's function returns. The transport holds no thread
   * while one is outstanding: it is handed a `CompletionStage` that future completes, so concurrency
   * is bounded by the engine's in-flight ceiling rather than by an executor count.
   *
   * May be called at most once per client.
   */
  def poll(process: RecordProcessor): Future[Unit] = {
    if (!polled.compareAndSet(false, true)) {
      throw new IllegalStateException("poll may be called at most once per client")
    }
    transport.pollAsync(record => startRecord(handingOut, process, record))
    log.info(
      "Polling: up to {} record(s) in flight, {} executor(s) under the transport",
      session.maxConcurrency(),
      session.executorCount())
    sessionEnded
  }

  /**
   * The session's end on its own, for a caller that has not polled or does not hold [[poll]]'s
   * future. It is an accessor rather than only a return value because a session can die before or
   * without a poll: a client that only connected still has an end to observe (guide §1).
   */
  def ended: Future[Unit] = sessionEnded

  /**
   * Ends the session: stops hand-out, lets executing records finish and report, half-closes the
   * stream, and reaps the sidecar. Idempotent, and safe to call from anywhere - the future from
   * [[poll]] completes once this has finished.
   */
  override def close(): Unit = {
    handingOut = false
    teardown()
  }

  private def teardown(): Unit =
    if (teardownStarted.compareAndSet(false, true)) {
      // the transport's own close is the frozen shutdown order: stop hand-out, let executing
      // records reach their verdict and report it, then half-close
      transport.close()
      // last, and only now: killing the sidecar with the stream open would turn a clean drain into
      // a reconnect-window recovery for the next group member
      blocking(sidecar.reap(ReapGrace))
    }
}

object ParallelConsumerClient {

  private val log = LoggerFactory.getLogger(classOf[ParallelConsumerClient])

  private val SpawnBudget: FiniteDuration = 30.seconds
  private val ReapGrace: FiniteDuration = 15.seconds

  /**
   * '''The one way to say "this client has no verdict for that record", and the only thing it is
   * for.'''
   *
   * A future that never completes is how the transport's `AsyncRecordProcessor` contract spells "no
   * verdict": the record is never reported, and the engine reclaims it when the session ends,
   * exactly as it does after a connection loss. Use it for a record a shutdown means you will not
   * run, and for nothing else - an ordinary record whose future never completes is a stall, and it
   * will look like one.
   *
   * '''A failed future is not this.''' The transport turns a failed future into a failure ''report
   * on the wire'', which is correct for a function that threw and wrong for a function that was
   * never asked - that is the whole reason this exists as its own thing rather than as an exception
   * anybody could reach for.
   */
  def noVerdict: Future[Outcome] = Promise[Outcome]().future

  /**
   * Spawns the sidecar, connects to the loopback port it reports, and completes the fresh-session
   * handshake. The returned client is open: [[Session]] carries what the proxy is actually running
   * with.
   *
   * A future rather than a blocking call, because every step of it waits on something - a process to
   * print a line, a handshake to come back - and a library that blocks a caller's thread to wait is
   * not one a Scala application can compose. The spawn's own wait happens on the supplied execution
   * context; the handshake is bridged from a `CompletionStage`, so not even one pooled thread is
   * parked on it.
   */
  def open(options: ClientOptions, sidecar: SidecarCommand)(
      implicit ec: ExecutionContext): Future[ParallelConsumerClient] =
    // `blocking` because the spawn waits for a child process to print a line: on a fork-join
    // context that is the difference between the pool compensating and the pool starving
    Future(blocking(Sidecar.spawn(sidecar, SpawnBudget))).flatMap { started =>
      val transport = GrpcParallelConsumerClient
        .builder()
        .port(started.port)
        .options(Bridge.toJava(options))
        .build()
      transport
        .connect()
        .asScala
        .map { session =>
          log.info("Connected: {}", session)
          new ParallelConsumerClient(started, transport, session)
        }
        .recoverWith {
          case NonFatal(failure) =>
            transport.close()
            // the sidecar's last words, read BEFORE it is reaped: a handshake that fails because
            // the child died has its whole explanation on that child's stderr, and a spawn failure
            // without it costs an afternoon (guide §10.1)
            val lastWords = started.diagnostics
            blocking(started.reap(ReapGrace))
            Future.failed(
              if (lastWords.isEmpty) failure
              else
                new IllegalStateException(
                  s"${failure.getMessage}. The sidecar's last output was:\n$lastWords",
                  failure))
        }
    }

  /**
   * One record's life, as the transport's asynchronous processor sees it: the user's function starts
   * and its future becomes the stage the transport reports from. Nothing waits on a thread anywhere
   * along it - which is the whole reason this client can be a wrapper rather than a second session
   * implementation.
   *
   * '''One of its two exits deliberately leaves the stage uncompleted, which the transport reads as
   * "this client has no verdict for that record".''' Hand-out has stopped - shutdown began before
   * this record started - so it was never run and no verdict is invented for it. `Released` is what
   * the wire has for "returned unrun", but it is gated by the `shutdown` capability, and sending a
   * message outside the negotiated set would be this client's own violation; off the session the
   * proxy reclaims the record as unheld, which is what the connection-loss rule already relies on.
   *
   * Everything else is a verdict, including a function that threw and a future that failed - both
   * become a failure report carrying the exception's message, once, inside the transport's
   * `Outcomes`. That is deliberate and it is the hazard worth stating: on this surface the ''only''
   * silence is a future that never completes.
   *
   * It is a top-level function rather than a method so the silent path can be tested directly. It is
   * the kind of rule that is invisible when it breaks - a fabricated verdict looks exactly like a
   * real one from every side.
   */
  private[scaladsl] def startRecord(handingOut: Boolean, process: RecordProcessor, record: JavaInboundRecord)(
      implicit ec: ExecutionContext): CompletionStage[JavaOutcome] =
    if (!handingOut) {
      log.debug("Hand-out has stopped; {} was not run and is reported as nothing", record)
      new CompletableFuture[JavaOutcome]()
    } else {
      process(Bridge.toScala(record)).map(Bridge.toJava).asJava
    }
}
