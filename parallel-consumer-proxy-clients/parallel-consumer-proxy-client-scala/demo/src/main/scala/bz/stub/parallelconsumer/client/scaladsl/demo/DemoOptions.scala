// Copyright (C) 2026 Antony Stubbs and contributors

package bz.stub.parallelconsumer.client.scaladsl.demo

/**
 * The demo's dials, and '''the interface every per-language demo mirrors''' - the contract is
 * `parallel-consumer-proxy/demo/README.md`, and the Java reference is `DemoOptions.java`.
 *
 * Flags beat the environment beats the defaults. That precedence is the ordinary convention and it
 * is stated rather than implied, because a container passes configuration by environment while a
 * person at a terminal passes flags, and both must be able to override the other's layer.
 *
 * '''R39 does not govern a demo.''' R39 constrains how configuration reaches the ''proxy''; a demo
 * is an application, so `--records` is not a breach of it. Said here because without it someone
 * reads the flags as breaking the plan's own rule and deletes them.
 *
 * A case class rather than the reference's builder, for the reason `ClientOptions` is one: default
 * arguments say "unset" once, at the declaration. The parsing is still a fold over the argument
 * array, because the flags arrive as a `String` array however Scala would rather receive them.
 */
final case class DemoOptions(
    records: Int = 2000,
    delayMs: Int = 2,
    maxConcurrency: Int = 100,
    partitions: Int = 10,
    replayFactor: Int = 20,
    /** An existing broker, or `None` to start one. */
    bootstrap: Option[String] = None,
    /** An existing topic, or `None` to name a fresh one. */
    topic: Option[String] = None) {

  // Checked as a Long rather than trusted as an Int later: records * replayFactor overflows
  // silently, and a wrapped value turns the big replay into a tiny one that still prints a
  // confident throughput figure.
  require(
    records.toLong * math.max(1, replayFactor) <= Int.MaxValue,
    s"--records times --replay-factor is ${records.toLong * math.max(1, replayFactor)}, which is more " +
      "records than the demo can count; lower one of them")

  /** The records the big replay consumes in total, including the small replay's own. */
  def bigReplayRecords: Int = records * math.max(1, replayFactor)

  /** True when the big replay is worth running at all; a factor of 1 or less skips it. */
  def bigReplayWanted: Boolean = replayFactor > 1

  /**
   * The effective configuration, printed before the run.
   *
   * A number without its settings is not reproducible, so this is part of the contract rather than a
   * debugging aid. '''The bootstrap address is deliberately absent''': own-cluster mode puts a
   * user's real broker there, and the credential-hygiene rule that binds the client library binds a
   * demo too - nothing logged, nothing echoed. The generated `toString` a case class would have
   * given us prints every field it has, including that one, which is why this is hand-written.
   */
  override def toString: String =
    s"records = $records" +
      s"\n  delayMs = $delayMs" +
      s"\n  maxConcurrency = $maxConcurrency" +
      s"\n  partitions = $partitions" +
      s"\n  replayFactor = $replayFactor"
}

object DemoOptions {

  /** Prefix for every environment variable this demo reads, so a reader can grep one string. */
  val EnvPrefix = "PC_DEMO_"

  /** The usage text, shared by `--help` and by the message a bad flag earns. */
  val Usage: String =
    """usage: demo/run.sh [options]
      |  --records N        records in the comparison replay   (default 2000)
      |  --delay-ms N       simulated work per record, ms      (default 2)
      |  --concurrency N    max in-flight records              (default 100)
      |  --partitions N     partitions on the demo topic       (default 10)
      |  --replay-factor N  big replay = records x N; 1 skips  (default 20)
      |  --bootstrap ADDR   an existing broker; omit to start one
      |  --topic NAME       an existing topic; omit to create one
      |
      |Every flag has an environment variable: --delay-ms is PC_DEMO_DELAY_MS.
      |Flags beat the environment beats the defaults.""".stripMargin

  /**
   * Whether the caller asked for the usage text rather than a run.
   *
   * Handled here rather than only in `run.sh`, because the script is not the only way in:
   * `docker compose run demo --help` reaches this main directly, and answering that with "unknown
   * option: --help" would be a poor first impression of a demo ten languages copy.
   */
  def isHelpRequested(args: Seq[String]): Boolean = args.exists(arg => arg == "-h" || arg == "--help")

  /**
   * Parses the demo's command line, falling back to the environment and then to the defaults.
   *
   * @param args the process arguments, which may legitimately be empty - that is the double-click
   *             case, and it must work
   * @param env  the environment to read, passed in rather than taken from `sys.env` so this is
   *             testable without mutating the JVM's own environment
   * @throws IllegalArgumentException on an unknown flag, a missing value, or a value that is not a
   *                                  number in range - a demo that silently ignores a misspelled
   *                                  flag reports numbers for settings the user did not ask for
   */
  def parse(args: Seq[String], env: Map[String, String]): DemoOptions = {
    var options = fromEnvironment(env)
    var index = 0
    while (index < args.length) {
      val flag = args(index)
      def next(): String = {
        index += 1
        if (index >= args.length) throw new IllegalArgumentException(s"$flag needs a value")
        args(index)
      }
      options = flag match {
        case "--records"       => options.copy(records = positive(flag, next()))
        case "--delay-ms"      => options.copy(delayMs = nonNegative(flag, next()))
        case "--concurrency"   => options.copy(maxConcurrency = positive(flag, next()))
        case "--partitions"    => options.copy(partitions = positive(flag, next()))
        // 1 or less skips the big replay, so this one is allowed to be zero
        case "--replay-factor" => options.copy(replayFactor = nonNegative(flag, next()))
        case "--bootstrap"     => options.copy(bootstrap = Some(next()))
        case "--topic"         => options.copy(topic = Some(next()))
        case unknown           => throw new IllegalArgumentException(s"unknown option: $unknown")
      }
      index += 1
    }
    options
  }

  private def fromEnvironment(env: Map[String, String]): DemoOptions = {
    def read(suffix: String): Option[String] =
      env.get(EnvPrefix + suffix).map(_.trim).filter(_.nonEmpty)

    val defaults = DemoOptions()
    DemoOptions(
      records = read("RECORDS").fold(defaults.records)(positive(s"${EnvPrefix}RECORDS", _)),
      delayMs = read("DELAY_MS").fold(defaults.delayMs)(nonNegative(s"${EnvPrefix}DELAY_MS", _)),
      maxConcurrency =
        read("CONCURRENCY").fold(defaults.maxConcurrency)(positive(s"${EnvPrefix}CONCURRENCY", _)),
      partitions = read("PARTITIONS").fold(defaults.partitions)(positive(s"${EnvPrefix}PARTITIONS", _)),
      replayFactor =
        read("REPLAY_FACTOR").fold(defaults.replayFactor)(nonNegative(s"${EnvPrefix}REPLAY_FACTOR", _)),
      bootstrap = read("BOOTSTRAP"),
      topic = read("TOPIC"))
  }

  private def positive(flag: String, raw: String): Int = {
    val parsed = number(flag, raw)
    if (parsed < 1) throw new IllegalArgumentException(s"$flag must be at least 1, got $parsed")
    parsed
  }

  private def nonNegative(flag: String, raw: String): Int = {
    val parsed = number(flag, raw)
    if (parsed < 0) throw new IllegalArgumentException(s"$flag must not be negative, got $parsed")
    parsed
  }

  private def number(flag: String, raw: String): Int =
    try raw.trim.toInt
    catch {
      case notANumber: NumberFormatException =>
        throw new IllegalArgumentException(s"$flag needs a whole number, got '$raw'", notANumber)
    }
}
