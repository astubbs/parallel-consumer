// Copyright (C) 2026 Antony Stubbs and contributors

package bz.stub.parallelconsumer.client.coroutines

import com.google.common.truth.Truth.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.time.Instant
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds
import bz.stub.parallelconsumer.client.InboundRecord as JavaInboundRecord

/**
 * The boundary between this client's spelling and the reference API's, which since this client
 * became a wrapper is the only translation it performs.
 *
 * What is NOT tested here, deliberately: anything protobuf-shaped. The wire mapping belongs to
 * `parallel-consumer-proxy-client-java-grpc` and is tested once, there, in `WireMappingTest` - the
 * predecessor of this file tested a second copy of it, which is precisely the duplication that made
 * wrapping the right answer. What is left is the set of respellings this module actually chose, and
 * each test below names the reason its respelling exists.
 */
class BridgeTest {

    @Test
    fun `unset options stay unset, so the engine's own defaults apply`() {
        val java = ClientOptions(topics = listOf("orders")).toJava()

        assertThat(java.topics()).containsExactly("orders")
        assertThat(java.maxConcurrency().isPresent).isFalse()
        assertThat(java.ordering().isPresent).isFalse()
        assertThat(java.commitInterval().isPresent).isFalse()
        assertThat(java.defaultMessageRetryDelay().isPresent).isFalse()
    }

    @Test
    fun `set options are carried unmodified, durations included`() {
        val java = ClientOptions(
            topics = listOf("orders"),
            kafkaProperties = mapOf("bootstrap.servers" to "localhost:9092"),
            maxConcurrency = 4,
            ordering = ProcessingOrder.PARTITION,
            commitInterval = 1500.milliseconds,
            defaultMessageRetryDelay = 250.milliseconds,
        ).toJava()

        assertThat(java.maxConcurrency().asInt).isEqualTo(4)
        assertThat(java.ordering().get()).isEqualTo(ProcessingOrder.PARTITION)
        // a sub-second Kotlin Duration must survive as a sub-second java.time one: truncating to
        // whole seconds here would silently commit every 1s instead of every 1.5s
        assertThat(java.commitInterval().get().toMillis()).isEqualTo(1500)
        assertThat(java.defaultMessageRetryDelay().get().toMillis()).isEqualTo(250)
        assertThat(java.kafkaProperties()).containsEntry("bootstrap.servers", "localhost:9092")
    }

    @Test
    fun `options refuse a subscription-free or ceiling-free configuration`() {
        assertThrows<IllegalArgumentException> { ClientOptions(topics = emptyList()) }
        assertThrows<IllegalArgumentException> { ClientOptions(topics = listOf("t"), maxConcurrency = 0) }
    }

    @Test
    fun `options never print their Kafka properties`() {
        val options = ClientOptions(
            topics = listOf("orders"),
            kafkaProperties = mapOf("sasl.jaas.config" to "password=hunter2"),
            commitInterval = 5.seconds,
        )

        assertThat(options.toString()).doesNotContain("hunter2")
        assertThat(options.toString()).doesNotContain("sasl.jaas.config")
    }

    @Test
    fun `an absent key or value stays null, because a tombstone is not an empty value`() {
        val record = JavaInboundRecord("orders", 0, 0, "k".toByteArray(), null, 1, null, null).toKotlin()

        assertThat(record.key).isEqualTo("k".toByteArray())
        assertThat(record.value).isNull()
        assertThat(record.attempt).isEqualTo(1)
        assertThat(record.previousFailure).isNull()
    }

    @Test
    fun `failure history arrives as one nullable value, not two`() {
        // the respelling that is not cosmetic: the reference surface's two Optionals can express a
        // reason with no time, which the wire cannot say and no engine ever means
        val failedAt = Instant.ofEpochSecond(1_700_000_000, 1)
        val java = JavaInboundRecord("orders", 0, 1, null, "v".toByteArray(), 2, failedAt, "worker exploded")

        val record = java.toKotlin()

        assertThat(record.attempt).isEqualTo(2)
        assertThat(record.previousFailure).isNotNull()
        assertThat(record.previousFailure?.at).isEqualTo(failedAt)
        assertThat(record.previousFailure?.reason).isEqualTo("worker exploded")
    }

    @Test
    fun `a failure time with no recorded reason is still a failure`() {
        val java = JavaInboundRecord("orders", 0, 1, null, null, 2, Instant.EPOCH, null)

        val record = java.toKotlin()

        assertThat(record.previousFailure).isNotNull()
        assertThat(record.previousFailure?.reason).isNull()
    }

    @Test
    fun `a success carries its produce list, and an empty one is still a success`() {
        val produce = Outcome.Success(listOf(OutboundRecord("out", value = "v".toByteArray()))).toJava()

        assertThat(produce.isSuccess).isTrue()
        assertThat(produce.produce()).hasSize(1)
        assertThat(produce.produce()[0].topic()).isEqualTo("out")
        assertThat(produce.produce()[0].key()).isNull()

        assertThat(Outcome.Success().toJava().isSuccess).isTrue()
        assertThat(Outcome.Success().toJava().produce()).isEmpty()
    }

    @Test
    fun `a failure carries its reason, and an absent reason stays absent`() {
        assertThat(Outcome.Failure("boom").toJava().failureReason().get()).isEqualTo("boom")
        assertThat(Outcome.Failure().toJava().failureReason().isPresent).isFalse()
        assertThat(Outcome.Failure().toJava().isSuccess).isFalse()
    }
}
