// Copyright (C) 2026 Antony Stubbs and contributors

package bz.stub.parallelconsumer.client.coroutines.demo

import org.apache.kafka.clients.admin.Admin
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.errors.TopicExistsException
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.common.utils.AppInfoParser
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.utility.DockerImageName
import java.util.Properties
import java.util.concurrent.ExecutionException
import java.util.concurrent.atomic.AtomicReference

/**
 * The broker the demo reads from, however the reader got here.
 *
 * Two ways in, and the second one is a rule rather than a convenience:
 *
 * - **Nothing supplied** - the demo starts a real broker in a container, which is KTD40's honest
 *   default because it is what a user actually runs.
 * - **An address supplied** - the demo uses it and starts nothing. This is how the demo runs
 *   *inside* its own container, and it is not optional there: **a demo container is never granted
 *   the host Docker socket** (plan unit U35), so it could not start a broker even if it wanted to.
 *   It reaches a compose sibling on the demo's own network instead.
 *
 * The same door serves own-cluster mode, where the address is the user's real cluster - so nothing
 * here logs or echoes it.
 */
internal class DemoBroker private constructor(
    val bootstrap: String,
    private val container: KafkaContainer?,
) : AutoCloseable {

    /**
     * Creates the demo's topic, tolerating one a previous run left behind - but not one with a
     * different partition count, because the fingerprint would then print a `--partitions` value
     * that never applied, and that block is the demo's whole reproducibility promise.
     */
    fun ensureTopic(topic: String, partitions: Int) {
        adminProperties().let { config ->
            Admin.create(config).use { admin ->
                try {
                    admin.createTopics(listOf(NewTopic(topic, partitions, 1.toShort()))).all().get()
                    Console.say("Created topic $topic with $partitions partitions")
                } catch (e: ExecutionException) {
                    if (e.cause !is TopicExistsException) {
                        throw IllegalStateException("could not create the demo topic $topic", e.cause)
                    }
                    val existing = partitionsOf(admin, topic)
                    check(existing == partitions) {
                        "topic $topic already exists with $existing partitions, but this run asked for " +
                            "$partitions - pass --topic to name a fresh one, or --partitions $existing"
                    }
                    Console.say("Topic $topic already exists with the requested $partitions partitions, reusing it")
                }
            }
        }
    }

    /**
     * Produces the backlog every arm then replays.
     *
     * Pre-produced rather than produced alongside the arms, which is what makes the workload
     * closed-loop - and in turn why no arm reports latency. A per-record timing here would be
     * flattered by however far an arm had fallen behind, so throughput is the only honest number
     * this shape can produce.
     */
    fun seed(topic: String, from: Int, to: Int) {
        if (to <= from) {
            return
        }
        val config = Properties().apply {
            put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap)
            put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer::class.java.name)
            put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer::class.java.name)
            put(ProducerConfig.LINGER_MS_CONFIG, "20")
        }

        Console.say("Producing records $from to $to...")
        // flush() does not throw for a send that failed and a discarded Future swallows the reason,
        // so without this the demo would report a full backlog, run every arm against a short one,
        // and print numbers for a workload that never existed.
        val firstFailure = AtomicReference<Exception>()
        KafkaProducer<ByteArray, ByteArray>(config).use { producer ->
            for (i in from until to) {
                val key = "key-${i % KEY_SPACE}".toByteArray()
                val value = "record-$i".toByteArray()
                producer.send(ProducerRecord(topic, key, value)) { _, exception ->
                    if (exception != null) {
                        firstFailure.compareAndSet(null, exception)
                    }
                }
            }
            producer.flush()
        }
        firstFailure.get()?.let { throw IllegalStateException("the demo could not seed its backlog", it) }
        Console.say("Produced ${to - from} records")
    }

    /**
     * The Kafka properties every arm needs to reach this broker.
     *
     * `enable.auto.commit` is false because Parallel Consumer owns offset commits. The sidecar
     * forces it itself whatever this map says, so on the sidecar arm the line is belt and braces;
     * it is the AK core arm - an ordinary consumer with Kafka's own default of true - that would
     * otherwise commit behind the demo's back and make a re-run read a partial backlog.
     */
    fun consumerProperties(groupId: String): Map<String, String> = linkedMapOf(
        "bootstrap.servers" to bootstrap,
        "group.id" to groupId,
        "auto.offset.reset" to "earliest",
        "enable.auto.commit" to "false",
    )

    override fun close() {
        container?.stop()
    }

    private fun adminProperties() = Properties().apply {
        put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap)
    }

    private fun partitionsOf(admin: Admin, topic: String): Int =
        try {
            admin.describeTopics(listOf(topic)).allTopicNames().get()[topic]!!.partitions().size
        } catch (e: ExecutionException) {
            throw IllegalStateException("could not describe the existing topic $topic", e.cause)
        }

    companion object {

        /**
         * The key space the seeded records spread over. Every arm runs UNORDERED, so this changes
         * nothing today; it exists so a KEY-ordered lane added later has more than one key to shard
         * across rather than needing the seeding rewritten first.
         */
        private const val KEY_SPACE = 1_000

        private const val FALLBACK_IMAGE = "confluentinc/cp-kafka:7.9.0"

        /** Uses the supplied broker, or starts one when none was supplied. */
        fun resolve(supplied: String?): DemoBroker {
            val address = supplied?.trim()?.takeIf { it.isNotEmpty() }
            if (address != null) {
                // deliberately not echoed: own-cluster mode puts a real address here
                Console.say("Using the broker supplied by the caller.")
                return DemoBroker(address, null)
            }
            val image = brokerImage()
            Console.say("No broker supplied, starting one in a container: $image")
            val started = KafkaContainer(DockerImageName.parse(image))
                .withEnv("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
                .withEnv("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
                .withEnv("KAFKA_TRANSACTION_STATE_LOG_NUM_PARTITIONS", "1")
                // the demo forms one consumer group per arm per replay, and the default
                // three-second settling delay would be charged to every one of them
                .withEnv("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "500")
            started.start()
            return DemoBroker(started.bootstrapServers, started)
        }

        /**
         * The broker image, tracking the Kafka client this build carries: CP major = AK major + 4
         * (AK 3.9 -> CP 7.9), CP minor tracks AK minor. The compose file beside this demo pins the
         * same image as a literal, because a compose file cannot derive anything - keep them in
         * step.
         */
        private fun brokerImage(): String =
            try {
                val parts = AppInfoParser.getVersion().split("-")[0].split(".")
                "confluentinc/cp-kafka:${parts[0].toInt() + 4}.${parts[1]}.0"
            } catch (e: RuntimeException) {
                Console.say(
                    "Could not read the Kafka version from '${AppInfoParser.getVersion()}' ($e), " +
                        "falling back to $FALLBACK_IMAGE"
                )
                FALLBACK_IMAGE
            }
    }
}
