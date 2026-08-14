// Copyright (C) 2026 Antony Stubbs and contributors

package bz.stub.parallelconsumer.client.coroutines

import bz.stub.parallelconsumer.proxy.protocol.v1.Configured
import bz.stub.parallelconsumer.proxy.protocol.v1.DispatchRecord
import bz.stub.parallelconsumer.proxy.protocol.v1.Record
import bz.stub.parallelconsumer.proxy.protocol.v1.Token
import com.google.common.truth.Truth.assertThat
import com.google.protobuf.ByteString
import com.google.protobuf.Timestamp
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

/**
 * The wire mapping, tested without a connection - which is why everything protobuf-shaped funnels
 * through one object.
 */
class WireTest {

    @Test
    fun `Configure declares exactly the capabilities this client implements`() {
        val configure = Wire.toConfigure(ClientOptions(topics = listOf("orders")))

        // NOT an empty list: the specification reads empty as the whole v1 baseline, which would
        // claim heartbeats, manifests, worker-death reporting, terminal outcomes and the drain -
        // and a granted `heartbeat` this client never sends would expire every record it holds
        assertThat(configure.capabilitiesList).containsExactly("dispatch")
    }

    @Test
    fun `unset options are absent on the wire, so the engine's own defaults apply`() {
        val configure = Wire.toConfigure(ClientOptions(topics = listOf("orders")))

        assertThat(configure.topicsList).containsExactly("orders")
        assertThat(configure.hasMaxConcurrency()).isFalse()
        assertThat(configure.hasOrdering()).isFalse()
        assertThat(configure.hasCommitInterval()).isFalse()
        assertThat(configure.hasDefaultMessageRetryDelay()).isFalse()
    }

    @Test
    fun `set options are carried unmodified, durations included`() {
        val configure = Wire.toConfigure(
            ClientOptions(
                topics = listOf("orders"),
                kafkaProperties = mapOf("bootstrap.servers" to "localhost:9092"),
                maxConcurrency = 4,
                ordering = ProcessingOrder.PARTITION,
                commitInterval = 1500.milliseconds,
            )
        )

        assertThat(configure.maxConcurrency).isEqualTo(4)
        assertThat(configure.ordering.name).isEqualTo("PROCESSING_ORDER_PARTITION")
        assertThat(configure.commitInterval.seconds).isEqualTo(1)
        assertThat(configure.commitInterval.nanos).isEqualTo(500_000_000)
        assertThat(configure.kafkaPropertiesMap).containsEntry("bootstrap.servers", "localhost:9092")
    }

    @Test
    fun `an absent key or value is null, because a tombstone is not an empty value`() {
        val dispatch = DispatchRecord.newBuilder()
            .setToken(Token.newBuilder().setRecordId("orders/0/0").setEpoch(1))
            .setRecord(Record.newBuilder().setTopic("orders").setPartition(0).setOffset(0).setKey(key("k")))
            .build()

        val record = Wire.toInboundRecord(dispatch)

        assertThat(record.key).isEqualTo("k".toByteArray())
        assertThat(record.value).isNull()
        assertThat(record.attempt).isEqualTo(1)
        assertThat(record.previousFailure).isNull()
    }

    @Test
    fun `failure history arrives as one nullable value, not two`() {
        val dispatch = DispatchRecord.newBuilder()
            .setToken(Token.newBuilder().setRecordId("orders/0/1").setEpoch(EPOCH_BEYOND_INT32))
            .setRecord(Record.newBuilder().setTopic("orders").setValue(key("v")))
            .setAttempt(2)
            .setLastFailureAt(Timestamp.newBuilder().setSeconds(1_700_000_000).setNanos(1))
            .setLastFailureReason("worker exploded")
            .build()

        val record = Wire.toInboundRecord(dispatch)

        assertThat(record.attempt).isEqualTo(2)
        assertThat(record.previousFailure).isNotNull()
        assertThat(record.previousFailure?.reason).isEqualTo("worker exploded")
        assertThat(record.previousFailure?.at?.epochSecond).isEqualTo(1_700_000_000)
    }

    @Test
    fun `the token is echoed as the very message the dispatch carried`() {
        val token = Token.newBuilder().setRecordId("orders/0/0").setEpoch(EPOCH_BEYOND_INT32).build()

        val report = Wire.toReport(token, Outcome.Success(listOf(OutboundRecord("out", value = "v".toByteArray()))))

        assertThat(report.token).isSameInstanceAs(token)
        assertThat(report.token.epoch).isEqualTo(EPOCH_BEYOND_INT32)
        assertThat(report.hasSuccess()).isTrue()
        assertThat(report.success.produceList).hasSize(1)
        assertThat(report.success.produceList[0].topic).isEqualTo("out")
        assertThat(report.success.produceList[0].hasKey()).isFalse()
    }

    @Test
    fun `a failure carries its reason, and an absent reason stays absent`() {
        val token = Token.newBuilder().setRecordId("orders/0/0").setEpoch(1).build()

        assertThat(Wire.toReport(token, Outcome.Failure("boom")).failure.reason).isEqualTo("boom")
        assertThat(Wire.toReport(token, Outcome.Failure()).failure.hasReason()).isFalse()
    }

    @Test
    fun `a Configured without the ceiling is a violation, never an unlimited`() {
        val incomplete = Configured.newBuilder().setExecutorCount(2).addCapabilities("dispatch").build()

        val violation = assertThrows<ProxyProtocolViolation> { Wire.toSession(incomplete) }

        assertThat(violation).hasMessageThat().contains("max_concurrency")
    }

    @Test
    fun `the session reports what came back, not what was asked for`() {
        val effective = Configured.newBuilder()
            .setExecutorCount(2)
            .setMaxConcurrency(16)
            .addCapabilities("dispatch")
            .build()

        val session = Wire.toSession(effective)

        assertThat(session.executorCount).isEqualTo(2)
        assertThat(session.maxConcurrency).isEqualTo(16)
        assertThat(session.capabilities).containsExactly("dispatch")
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

    private fun key(text: String): ByteString = ByteString.copyFromUtf8(text)

    private companion object {
        /** Beyond int32, deliberately: a parser truncating the epoch fails the golden bytes. */
        const val EPOCH_BEYOND_INT32 = 5_000_000_000L
    }
}
