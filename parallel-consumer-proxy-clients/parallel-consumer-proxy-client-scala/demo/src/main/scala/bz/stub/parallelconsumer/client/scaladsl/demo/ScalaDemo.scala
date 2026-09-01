// Copyright (C) 2026 Antony Stubbs and contributors

package bz.stub.parallelconsumer.client.scaladsl.demo

import java.nio.charset.StandardCharsets
import java.nio.file.Paths
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{ConcurrentHashMap, CountDownLatch, Executors, ThreadFactory, TimeUnit}
import java.util.{Collections, Locale}

import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.jdk.CollectionConverters._

import bz.stub.parallelconsumer.client.ProcessingOrder
import bz.stub.parallelconsumer.client.scaladsl.{ClientOptions, Outcome, ParallelConsumerClient, SidecarCommand}
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.slf4j.LoggerFactory

/**
 * '''The Scala demo.''' The same records through Scala's own Kafka client and through Scala over the
 * sidecar - the two arms `parallel-consumer-proxy/demo/README.md` binds every language to, with the
 * flags, defaults, tables and fingerprint that contract fixes.
 *
 * ==Two arms, and the four the Java seed carries are deliberately absent==
 *
 *  - '''`AK core (KafkaConsumer)`''' - the Apache Kafka `KafkaConsumer` itself, driven from Scala,
 *    one record at a time. The client is named in the label because "AK core" is a category and the
 *    answer differs in every language. Always spelled "AK core", never bare "core", which reads as
 *    `parallel-consumer-core` (`CONCEPTS.md`).
 *  - '''`pc-scala-grpc (this client)`''' - this module's own `ParallelConsumerClient` over a sidecar the
 *    client library spawns as a child process. The application does no Kafka I/O on that path: the
 *    sidecar owns the consumer, the producer, the group membership and the offsets.
 *
 * Java also runs `pc-core`, `java-direct`, `java-grpc-uds` and `java-raw-grpc`, and this demo runs
 * none of them '''on purpose'''. Those arms exist because one JVM can hold all of them against one
 * broker, so each ''pair'' changes exactly one term. Scala is a JVM language and could technically
 * run them too - which is precisely the temptation the contract forecloses: a reader who has run one
 * language's demo has run them all, and a Scala table with six rows beside a Ruby table with two
 * would not be the same demo. '''Two arms is the contract everywhere except the seed.'''
 *
 * ==This arm goes through the client library, not the wire==
 *
 * The engine is reached through `ParallelConsumerClient` - the artifact a Scala user actually
 * touches. An earlier version of the Java demo spoke the protocol by hand and had to be rewritten,
 * because it proved the ''engine'' worked and said nothing about the ''client library''. Nothing here
 * names a protobuf message, a channel or a token.
 *
 * ==Run it==
 *
 * {{{parallel-consumer-proxy-clients/parallel-consumer-proxy-client-scala/demo/run.sh}}}
 */
object ScalaDemo {

  private val log = LoggerFactory.getLogger(getClass)

  /** No arm may take longer than this before the demo calls it stalled rather than slow. */
  private val ArmBudget: FiniteDuration = 10.minutes

  /**
   * '''The first thing printed, and it is contract rather than decoration.'''
   * `parallel-consumer-proxy/demo/README.md` fixes this text, identical in all eleven languages bar
   * the language's own name. It exists because someone watched a demo open with
   * `ruby-grpc: the proxy granted 100 executor threads` and had been told nothing about what they
   * were looking at - not the product's name, not what the run was about to show them.
   *
   * Printed rather than logged, deliberately: it is the demo's headline, not an event, and a
   * timestamp and logger name in front of the rule line would put a configuration line first again.
   */
  private val Banner: String =
    """================================================================
      |  PARALLEL CONSUMER  -  Scala demo
      |  The same records, twice: one at a time, then all at once.
      |================================================================""".stripMargin

  /**
   * '''"AK core" is a category, not a client''', so the arm carries both - the role and the library
   * that produced the number. A reader cannot judge a comparison without knowing what ran: the
   * answer is `rdkafka` in Ruby, `franz-go` in Go, `kafkajs` in TypeScript, and here it is the
   * Apache Kafka `KafkaConsumer` itself, driven from Scala.
   *
   * '''Still spelled "AK core" and never bare "core"''', which reads as `parallel-consumer-core`
   * (`CONCEPTS.md`).
   */
  private val AkCore = "AK core (KafkaConsumer)"

  /**
   * The sidecar arm, labelled with what drives it: this module's own client library. "this client"
   * is the contract's wording rather than the type's name, so that the label means the same thing in
   * a language whose client library is not a class.
   */
  private val ScalaGrpc = "pc-scala-grpc (this client)"

  /** The bucket a record with no key at all falls into, so "unique keys" stays a truthful count. */
  private val NoKey = "<no key>"

  /**
   * The context the client library's own plumbing runs on - the spawn's blocking wait, the handshake
   * bridge, and each record's `Future.map` into the transport's `CompletionStage`.
   *
   * '''Deliberately not the context the user function runs on.''' That one is a pool sized to the
   * in-flight ceiling whose every thread is asleep at peak, and running the library's continuations
   * there would leave the completions queued behind the work they exist to complete.
   */
  private implicit val plumbing: ExecutionContext = ExecutionContext.global

  def main(args: Array[String]): Unit = {
    // Before the argument parsing, so even a misspelled flag is answered by something that says what
    // this program is.
    println(Banner)
    if (DemoOptions.isHelpRequested(args.toSeq)) {
      log.info("\n" + DemoOptions.Usage)
    } else {
      parsed(args) match {
        case None => System.exit(2)
        case Some(options) =>
          val topic = options.topic.getOrElse(s"pc-demo-scala-${System.nanoTime()}")
          // Banner, then the effective configuration, then the run - the contract's order. Printed
          // BEFORE the broker is resolved rather than inside runFor, because resolving it may mean
          // pulling and starting a Kafka image, and a reader should not wait half a minute to find
          // out what settings the numbers they are about to see were produced with.
          announce(options, topic)
          val broker = DemoBroker.resolve(options.bootstrap)
          try {
            val _ = runFor(options, broker, topic)
          } finally broker.close()
      }
    }
  }

  /**
   * The effective-configuration fingerprint: a number without its settings is not reproducible.
   *
   * '''The bootstrap address is not in it, and that rule binds the whole run rather than this block'''
   * - own-cluster mode puts a user's real broker there. `DemoOptions.toString` is hand-written for
   * that reason, and `demo/logback.xml` holds `org.apache.kafka` at `WARN` for the same one: every
   * Kafka client dumps its full effective configuration at `INFO` when constructed, `bootstrap.servers`
   * included, which would leak from the client the demo's own printing is careful not to leak.
   */
  private def announce(options: DemoOptions, topic: String): Unit =
    log.info(s"\nEffective configuration:\n  $options\n  topic = $topic")

  /** `None` when the command line was not usable - a misspelled flag must never be reported as a run. */
  private def parsed(args: Array[String]): Option[DemoOptions] =
    try Some(DemoOptions.parse(args.toSeq, sys.env))
    catch {
      case bad: IllegalArgumentException =>
        log.error(bad.getMessage)
        log.info("\n" + DemoOptions.Usage)
        None
    }

  /**
   * Runs the whole demo and hands back every arm's result.
   *
   * Returns the results rather than only printing them, so a test can assert what the arms actually
   * did against the same code path the reader runs - a parallel path could pass while the real one is
   * broken.
   *
   * The fingerprint is `announce`'s, printed by `main` before the broker is resolved.
   */
  def runFor(options: DemoOptions, broker: DemoBroker, topic: String): Seq[ArmResult] = {
    broker.ensureTopic(topic, options.partitions)
    broker.seed(topic, 0, options.records)

    val small = Seq(
      akCore(options, broker, topic, options.records),
      sidecar(options, broker, topic, options.records))
    val baseline = small.find(_.arm == AkCore)
    report(
      s"Small replay - every arm over the same ${options.records} records (the comparison)",
      small,
      baseline,
      acrossReplays = false)

    if (!options.bigReplayWanted) {
      log.info(s"\nBig replay skipped (--replay-factor ${options.replayFactor}).")
      small
    } else {
      val total = options.bigReplayRecords
      broker.seed(topic, options.records, total)

      // AK core is excluded here because it does not go parallel: it would need total * delayMs
      // milliseconds to finish a backlog the sidecar clears in seconds, and a demo that makes a
      // reader wait that long to learn nothing new is not worth the wall clock. In Scala that leaves
      // exactly one row, which is the shape the contract asks for rather than a degenerate table -
      // the figure it carries is what the engine sustains once start-up stops dominating, and the
      // small replay above is where the comparison lives.
      val big = Seq(sidecar(options, broker, topic, total))
      // the unit is chosen so the figure is never zero - see the demo contract.
      val serialMillis = total.toLong * options.delayMs
      val serialCost = if (serialMillis >= 1000) s"${serialMillis / 1000}s" else s"${serialMillis}ms"
      report(
        s"Big replay - $total records, parallel arms only (AK core is serial and would take " +
          s"$serialCost+)",
        big,
        baseline,
        acrossReplays = true)
      small ++ big
    }
  }

  /** The serial arm: Scala's own Kafka client, one record at a time, the same sleep as the other arm. */
  private def akCore(options: DemoOptions, broker: DemoBroker, topic: String, target: Int): ArmResult = {
    log.info(s"\n=== $AkCore starting over $target records ===")
    val config = broker.consumerConfig(groupId("ak-core"))
    config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[ByteArrayDeserializer].getName)
    config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[ByteArrayDeserializer].getName)

    val consumer = new KafkaConsumer[Array[Byte], Array[Byte]](config)
    val keys = ConcurrentHashMap.newKeySet[String]()
    try {
      consumer.subscribe(Collections.singletonList(topic))
      // The clock starts AFTER the consumer is built and stops before it closes, because this arm is
      // the denominator of every ratio in both tables and the other arm charges itself for neither
      // client construction nor teardown.
      val startedAt = System.nanoTime()
      val deadline = startedAt + ArmBudget.toNanos
      var processed = 0
      while (processed < target) {
        // The arm that does not wait on a latch still needs the budget ArmBudget promises, or a
        // backlog shorter than the target spins here forever with no output.
        if (System.nanoTime() > deadline) {
          throw new IllegalStateException(s"$AkCore stalled at $processed of $target")
        }
        consumer.poll(java.time.Duration.ofMillis(500)).asScala.foreach { record =>
          simulateWork(options.delayMs)
          val _ = keys.add(keyOf(record.key()))
          processed += 1
        }
      }
      finished(AkCore, startedAt, processed, keys.size)
    } finally consumer.close()
  }

  /**
   * '''The arm the whole design exists for''': this module's client library, over a sidecar it spawns
   * itself.
   *
   * The application does no Kafka I/O here - it spawns a binary, receives records over a socket, runs
   * its own function on them, and reports outcomes back, while the sidecar owns the consumer, the
   * producer, the group membership and the offsets. That is a claim about the ''path'', not about
   * this process: the same JVM seeded the topic and ran the AK core arm with ordinary Kafka clients,
   * because a comparison needs both sides. A genuinely foreign Scala application carries no Kafka
   * client library at all, which is the property this arm stands in for.
   */
  private def sidecar(options: DemoOptions, broker: DemoBroker, topic: String, target: Int): ArmResult = {
    log.info(s"\n=== $ScalaGrpc starting over $target records ===")
    val processed = new AtomicInteger()
    val done = new CountDownLatch(1)
    // Concurrent, unlike the AK core arm's: this arm's user function runs on as many threads as the
    // engine hands out records, so a plain mutable set would lose keys and under-report the spread
    // the column exists to demonstrate.
    val keys = ConcurrentHashMap.newKeySet[String]()

    // A pool of exactly the in-flight ceiling, and this is where a blocking sleep as the user
    // function gets paid for. The contract permits one in Scala - it is Python's worker processes and
    // TypeScript's single event loop that cannot afford it - but "permitted" is not "free": a record
    // occupies a thread for delayMs, so the pool must hold as many records as the engine will hand
    // out at once. Sizing it from maxConcurrency rather than from a core count is what keeps this arm
    // measuring the engine instead of measuring this pool.
    val work = ExecutionContext.fromExecutorService(
      Executors.newFixedThreadPool(options.maxConcurrency, daemonThreads("pc-demo-work")))

    val client = Await.result(
      ParallelConsumerClient.open(
        ClientOptions(
          topics = Seq(topic),
          kafkaProperties = broker.consumerProperties(groupId("pc-scala-grpc")),
          maxConcurrency = Some(options.maxConcurrency),
          ordering = Some(ProcessingOrder.UNORDERED)),
        sidecarCommand()),
      ArmBudget)

    try {
      // Started only now: the spawn and the handshake are the client library's start-up, and the AK
      // core arm charges itself for neither its own construction nor its subscription.
      val startedAt = System.nanoTime()
      val session = client.poll { record =>
        Future {
          simulateWork(options.delayMs)
          val _ = keys.add(keyOf(record.key.orNull))
          if (processed.incrementAndGet() >= target) {
            done.countDown()
          }
          Outcome.succeeded
        }(work)
      }

      if (!done.await(ArmBudget.toMillis, TimeUnit.MILLISECONDS)) {
        throw new IllegalStateException(s"$ScalaGrpc stalled at ${processed.get()} of $target")
      }
      val result = finished(ScalaGrpc, startedAt, processed.get(), keys.size)

      client.close()
      // The session's own end, which this client makes the same future poll returns. Waiting on it is
      // how a stream that died mid-run becomes a failure here rather than a plausible row at a
      // plausible rate - which is the worst thing a demo ten languages copy could print.
      Await.result(session, ArmBudget)
      result
    } finally {
      client.close()
      val _ = work.shutdownNow()
    }
  }

  /**
   * Where the sidecar binary is.
   *
   * '''The awkwardness is the JVM's, not the client library's.''' `SidecarCommand` wants an absolute
   * path to an executable, and a sidecar that ships as a jar means this JVM's own `java` launcher
   * with a classpath argument - exactly what this module's end-to-end test does, and what the Java
   * seed's `SidecarProcess` does. `java.class.path` is the demo's own resolved classpath because
   * `run.sh` forks a real JVM rather than running under `mvn exec:java`, where the property would be
   * Maven's classpath and the child would come up without the engine on it.
   *
   * The class is named through `classOf` rather than as a string: a drifted literal would fail after
   * a broker has already started, several minutes into a run.
   */
  private def sidecarCommand(): SidecarCommand = SidecarCommand(
    Paths.get(System.getProperty("java.home"), "bin", "java"),
    Seq("-cp", System.getProperty("java.class.path"), classOf[bz.stub.parallelconsumer.proxy.Main].getName))

  /**
   * The simulated work, identical in both arms so they differ by transport and nothing else.
   *
   * The interrupt is restored rather than swallowed: an arm being torn down mid-record must not look
   * like an arm that finished the record.
   */
  private def simulateWork(delayMs: Int): Unit =
    if (delayMs > 0) {
      try Thread.sleep(delayMs.toLong)
      catch {
        case _: InterruptedException => Thread.currentThread().interrupt()
      }
    }

  /**
   * The identity a key counts as, for the "unique keys" column.
   *
   * '''The bytes are never logged, only counted''' - a key is untrusted payload, and the column is a
   * cardinality rather than a sample. Kafka's own `null` key is a distinct case rather than an
   * absent one, so it gets its own bucket instead of being dropped: an arm that saw only keyless
   * records should report one key, not zero.
   */
  private def keyOf(key: Array[Byte]): String =
    if (key == null) NoKey else new String(key, StandardCharsets.UTF_8)

  private def finished(arm: String, startedAt: Long, processed: Int, keys: Int): ArmResult = {
    val elapsed = FiniteDuration(System.nanoTime() - startedAt, TimeUnit.NANOSECONDS)
    log.info(s"=== $arm finished: $processed records over $keys keys in ${elapsed.toMillis}ms ===")
    ArmResult(arm, elapsed, processed, keys)
  }

  /** A fresh group per arm per replay, so every arm reads the same records from the beginning. */
  private def groupId(arm: String): String = s"pc-demo-$arm-${System.nanoTime()}"

  private def daemonThreads(prefix: String): ThreadFactory = {
    val counter = new AtomicInteger()
    (runnable: Runnable) => {
      val thread = new Thread(runnable, s"$prefix-${counter.incrementAndGet()}")
      thread.setDaemon(true)
      thread
    }
  }

  /**
   * One of the contract's two tables. Same columns, same order, in every language - and '''no latency
   * column''', because the backlog is pre-produced and a per-record timing would be flattered by
   * however far an arm had fallen behind.
   *
   * ==`records` and `keys` come first, before anything about speed==
   *
   * Throughput alone cannot show the work happened. `records` must equal the target - a short arm is
   * a failed arm, not a fast one - and `keys` is the distinct keys that arm actually saw, which is
   * what shows the backlog was spread rather than one key repeated. They are the only two figures
   * here that are '''deterministic''': every language over the same records reports the same pair,
   * which is what makes eleven demos comparable when elapsed and msg/s never can be.
   */
  private def report(
      title: String,
      results: Seq[ArmResult],
      baseline: Option[ArmResult],
      acrossReplays: Boolean): Unit = {
    val table = new StringBuilder("\n\n").append(title).append('\n')
    table.append(
      row("arm", "records", "keys", "elapsed", "msg/s", if (acrossReplays) "vs AK core*" else "vs AK core"))
    results.foreach { result =>
      val ratio = baseline.filter(_.ratePerSecond != 0d) match {
        case Some(against) =>
          String.format(Locale.ROOT, "%.1fx", java.lang.Double.valueOf(result.ratePerSecond / against.ratePerSecond))
        case None => "-"
      }
      table.append(
        row(
          result.arm,
          String.format(Locale.ROOT, "%,d", java.lang.Integer.valueOf(result.processed)),
          String.format(Locale.ROOT, "%,d", java.lang.Integer.valueOf(result.keys)),
          String.format(Locale.ROOT, "%.1fs", java.lang.Double.valueOf(result.elapsed.toMillis / 1000d)),
          String.format(Locale.ROOT, "%,d", java.lang.Integer.valueOf(result.ratePerSecond.toInt)),
          ratio))
    }
    if (acrossReplays) {
      table.append("\n  * against the SMALL replay's AK core arm. Across replays, so not like-for-like.\n")
    }
    log.info(table.toString)
  }

  /**
   * One line of a table, header or body - the same formatter for both, so a column can never be
   * added to one and forgotten in the other.
   */
  private def row(
      arm: String,
      records: String,
      keys: String,
      elapsed: String,
      rate: String,
      ratio: String): String =
    String.format(Locale.ROOT, "  %-29s %9s %9s %10s %14s %14s%n", arm, records, keys, elapsed, rate, ratio)
}
