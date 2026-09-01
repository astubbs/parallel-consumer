// Copyright (C) 2026 Antony Stubbs and contributors

package bz.stub.parallelconsumer.client.scaladsl.demo

import java.nio.charset.StandardCharsets
import java.util.concurrent.atomic.AtomicReference
import java.util.{Collections, Properties}

import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

import org.apache.kafka.clients.admin.{Admin, AdminClientConfig, NewTopic}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.errors.TopicExistsException
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.common.utils.AppInfoParser
import org.slf4j.LoggerFactory
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.utility.DockerImageName

/**
 * The broker the demo reads from, however the reader got here.
 *
 * ==Two ways in, and the second one is a rule rather than a convenience==
 *
 *  - '''Nothing supplied''' - the demo starts a real broker in a container, which is the honest
 *    default because it is what a user actually runs.
 *  - '''An address supplied''' - the demo uses it and starts nothing. This is how the demo runs
 *    ''inside'' its own container, and it is not optional there: '''a demo container is never
 *    granted the host Docker socket''', so it cannot start a broker even if it wanted to. It reaches
 *    a compose sibling on the demo's own network instead. A documented socket mount is
 *    root-equivalent host access taught as the normal way to run the product, which is why the rule
 *    exists rather than the shortcut.
 *
 * The same door serves own-cluster mode, where the address is the user's real cluster - so nothing
 * here logs or echoes it.
 */
private[demo] final class DemoBroker private (val bootstrap: String, container: Option[KafkaContainer])
    extends AutoCloseable {

  import DemoBroker._

  /** Creates the demo's topic, tolerating one a previous run already left behind. */
  def ensureTopic(topic: String, partitions: Int): Unit = {
    val config = new Properties()
    config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap)
    val admin = Admin.create(config)
    try {
      try {
        admin.createTopics(Collections.singletonList(new NewTopic(topic, partitions, 1.toShort))).all().get()
        log.info("Created topic {} with {} partitions", topic, partitions)
      } catch {
        case failed: java.util.concurrent.ExecutionException
            if failed.getCause.isInstanceOf[TopicExistsException] =>
          // Reusing a topic silently is fine; reusing one with a DIFFERENT partition count is not,
          // because the effective-configuration block would print a --partitions value that never
          // applied - and that block is the demo's whole reproducibility promise.
          val existing = admin
            .describeTopics(Collections.singletonList(topic))
            .allTopicNames()
            .get()
            .get(topic)
            .partitions()
            .size()
          if (existing != partitions) {
            throw new IllegalStateException(
              s"topic $topic already exists with $existing partitions, but this run asked for " +
                s"$partitions - pass --topic to name a fresh one, or --partitions $existing")
          }
          log.info("Topic {} already exists with the requested {} partitions, reusing it", topic, partitions)
      }
    } finally admin.close()
  }

  /**
   * Produces the backlog every arm then replays.
   *
   * Pre-produced rather than produced alongside the arms, and that is what makes the workload
   * closed-loop - which is in turn why '''no arm reports latency'''. A per-record timing here would
   * be flattered by however far an arm had fallen behind, so throughput is the only honest number
   * this shape can produce.
   */
  def seed(topic: String, from: Int, to: Int): Unit =
    if (to > from) {
      val config = new Properties()
      config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap)
      config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[ByteArraySerializer].getName)
      config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[ByteArraySerializer].getName)
      config.put(ProducerConfig.LINGER_MS_CONFIG, "20")

      log.info("Producing records {} to {}...", from, to)
      // flush() does not throw for a send that failed, and a discarded callback swallows the
      // reason, so without this the demo would report a full backlog, run every arm against a short
      // one, and print numbers for a workload that never existed.
      val firstFailure = new AtomicReference[Exception]()
      val producer = new KafkaProducer[Array[Byte], Array[Byte]](config)
      try {
        var i = from
        while (i < to) {
          val key = s"key-${i % KeySpace}".getBytes(StandardCharsets.UTF_8)
          val value = s"record-$i".getBytes(StandardCharsets.UTF_8)
          val _ = producer.send(
            new ProducerRecord[Array[Byte], Array[Byte]](topic, key, value),
            (_, exception) =>
              if (exception != null) {
                val _ = firstFailure.compareAndSet(null, exception)
              })
          i += 1
        }
        producer.flush()
      } finally producer.close()

      Option(firstFailure.get()).foreach { failure =>
        throw new IllegalStateException("the demo could not seed its backlog", failure)
      }
      log.info("Produced {} records", to - from)
    }

  /**
   * The Kafka properties every arm's consumer needs to reach this broker.
   *
   * `enable.auto.commit` is set false explicitly. Parallel Consumer owns offset commits and refuses
   * a consumer that does not; the sidecar forces the setting itself, so on the sidecar arm this line
   * changes nothing - it is here so the AK core arm and the sidecar arm are configured identically
   * and the comparison turns on the engine rather than on a commit policy.
   */
  def consumerProperties(groupId: String): Map[String, String] = Map(
    "bootstrap.servers" -> bootstrap,
    "group.id" -> groupId,
    "auto.offset.reset" -> "earliest",
    "enable.auto.commit" -> "false")

  /** The same properties as a `java.util.Properties`, for the AK core arm's own `KafkaConsumer`. */
  def consumerConfig(groupId: String): Properties = {
    val config = new Properties()
    config.putAll(consumerProperties(groupId).asJava)
    config
  }

  override def close(): Unit = container.foreach(_.stop())
}

private[demo] object DemoBroker {

  private val log = LoggerFactory.getLogger(classOf[DemoBroker])

  /**
   * The key space the seeded records spread over. Ordering is UNORDERED in both arms, so this
   * changes nothing today; it exists so a key-ordered lane added later has more than one key to
   * shard across, rather than needing the seeding rewritten first.
   */
  private val KeySpace = 1000

  private val FallbackImage = "confluentinc/cp-kafka:7.9.0"

  /**
   * Uses the supplied broker, or starts one when none was supplied.
   *
   * @param supplied the address from `--bootstrap` or the environment, or `None`
   */
  def resolve(supplied: Option[String]): DemoBroker =
    supplied.map(_.trim).filter(_.nonEmpty) match {
      case Some(address) =>
        // deliberately not logged: own-cluster mode puts a real address here
        log.info("Using the broker supplied by the caller.")
        new DemoBroker(address, None)
      case None =>
        val image = brokerImage()
        log.info("No broker supplied, starting one in a container: {}", image)
        val started = new KafkaContainer(DockerImageName.parse(image))
          .withEnv("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
          .withEnv("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
          .withEnv("KAFKA_TRANSACTION_STATE_LOG_NUM_PARTITIONS", "1")
          // the demo forms one consumer group per arm, one after another, and the default
          // three-second settling delay would be charged to every one of them
          .withEnv("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "500")
        started.start()
        new DemoBroker(started.getBootstrapServers, Some(started))
    }

  /**
   * The broker image, tracking the Kafka client this build carries - CP major = AK major + 4, so AK
   * 3.9 means CP 7.9. The compose file beside this module pins the same image as a literal, because
   * a compose file cannot derive anything; keep the two in step.
   */
  private def brokerImage(): String = {
    val akVersion = AppInfoParser.getVersion
    try {
      val parts = akVersion.split("-")(0).split("\\.")
      s"confluentinc/cp-kafka:${parts(0).toInt + 4}.${parts(1)}.0"
    } catch {
      case NonFatal(unreadable) =>
        log.warn(
          s"Could not read the Kafka version from '$akVersion', falling back to $FallbackImage",
          unreadable)
        FallbackImage
    }
  }
}
