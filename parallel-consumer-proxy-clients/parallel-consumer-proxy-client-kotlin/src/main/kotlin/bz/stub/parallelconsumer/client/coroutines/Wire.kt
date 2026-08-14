// Copyright (C) 2026 Antony Stubbs and contributors

package bz.stub.parallelconsumer.client.coroutines

import bz.stub.parallelconsumer.proxy.protocol.v1.Configure
import bz.stub.parallelconsumer.proxy.protocol.v1.Configured
import bz.stub.parallelconsumer.proxy.protocol.v1.DispatchRecord
import bz.stub.parallelconsumer.proxy.protocol.v1.ProduceRecord
import bz.stub.parallelconsumer.proxy.protocol.v1.Report
import bz.stub.parallelconsumer.proxy.protocol.v1.Token
import com.google.protobuf.ByteString
import java.time.Instant
import kotlin.time.Duration
import bz.stub.parallelconsumer.proxy.protocol.v1.ProcessingOrder as WireProcessingOrder

/**
 * The wire boundary: this client's own types in, protocol messages out, and back. Everything
 * protobuf-shaped funnels through here, so the session code above it reads in the library's terms -
 * and so the mapping is testable without a connection.
 *
 * **The token is echoed verbatim.** [toReport] sets the report's token to the very message object
 * the dispatch carried: no rebuild, no field read, no interpretation. Nothing in this client stores
 * anything per record - the token rides from the dispatch queue to the report on the executing
 * coroutine's stack, and a client that holds no per-record state cannot have a per-record state bug.
 */
internal object Wire {

    /**
     * The capability tokens this client actually implements, declared rather than left empty.
     *
     * An empty `capabilities` list does not mean "nothing" on the wire - the specification reads it
     * as the complete v1 baseline, which would claim heartbeats, manifest reconciliation,
     * worker-death reporting, terminal outcomes and the shutdown drain. Wave one performs exactly
     * one of those. The claim is not harmless: once the proxy grants `heartbeat`, a client that
     * never heartbeats has every in-flight record returned at lease expiry and its later reports
     * fenced as superseded, so nothing commits. Grow this set as each duty is genuinely
     * implemented, never ahead of it.
     */
    const val DISPATCH_CAPABILITY: String = "dispatch"

    /** The connect-time `Configure`: the options carried to the proxy, unmodified. */
    fun toConfigure(options: ClientOptions): Configure {
        val configure = Configure.newBuilder()
            .addAllTopics(options.topics)
            .addCapabilities(DISPATCH_CAPABILITY)
            .putAllKafkaProperties(options.kafkaProperties)
        options.maxConcurrency?.let { configure.maxConcurrency = it }
        options.ordering?.let { configure.ordering = toWireOrdering(it) }
        options.commitInterval?.let { configure.commitInterval = toWireDuration(it) }
        options.defaultMessageRetryDelay?.let { configure.defaultMessageRetryDelay = toWireDuration(it) }
        return configure.build()
    }

    /**
     * The handshake reply as this client's [Session] - the effective values, which is what the
     * client obeys. A `Configured` without the ceiling or the executor count is a protocol
     * violation, never an "unlimited": absence is meaningless for both.
     */
    fun toSession(configured: Configured): Session {
        if (!configured.hasMaxConcurrency() || !configured.hasExecutorCount()) {
            throw ProxyProtocolViolation(
                "the proxy's Configured omitted max_concurrency or executor_count; both are always set, " +
                    "and absence never means unlimited"
            )
        }
        return Session(
            executorCount = configured.executorCount,
            maxConcurrency = configured.maxConcurrency,
            capabilities = configured.capabilitiesList.toSet(),
        )
    }

    /**
     * One dispatched record as the user's function sees it. Absent wire fields become `null`:
     * Kafka's tombstone distinction for key and value, and "has not failed before" for the failure
     * history.
     */
    fun toInboundRecord(dispatch: DispatchRecord): InboundRecord {
        val record = dispatch.record
        return InboundRecord(
            topic = record.topic,
            partition = record.partition,
            offset = record.offset,
            key = if (record.hasKey()) record.key.toByteArray() else null,
            value = if (record.hasValue()) record.value.toByteArray() else null,
            attempt = if (dispatch.hasAttempt()) dispatch.attempt else 1,
            previousFailure = toPreviousFailure(dispatch),
        )
    }

    /** The outcome as a report, keyed by the dispatch's token echoed byte-identically. */
    fun toReport(token: Token, outcome: Outcome): Report {
        val report = Report.newBuilder().setToken(token)
        when (outcome) {
            is Outcome.Success -> report.success = toWireSuccess(outcome)
            is Outcome.Failure -> report.failure = toWireFailure(outcome)
        }
        return report.build()
    }

    private fun toPreviousFailure(dispatch: DispatchRecord): PreviousFailure? {
        if (!dispatch.hasLastFailureAt()) {
            return null
        }
        val at = Instant.ofEpochSecond(dispatch.lastFailureAt.seconds, dispatch.lastFailureAt.nanos.toLong())
        return PreviousFailure(at, if (dispatch.hasLastFailureReason()) dispatch.lastFailureReason else null)
    }

    private fun toWireSuccess(outcome: Outcome.Success): Report.Success {
        val success = Report.Success.newBuilder()
        outcome.produce.forEach { success.addProduce(toProduceRecord(it)) }
        return success.build()
    }

    private fun toWireFailure(outcome: Outcome.Failure): Report.Failure {
        val failure = Report.Failure.newBuilder()
        outcome.reason?.let { failure.reason = it }
        return failure.build()
    }

    private fun toProduceRecord(outbound: OutboundRecord): ProduceRecord {
        val produce = ProduceRecord.newBuilder().setTopic(outbound.topic)
        outbound.key?.let { produce.key = ByteString.copyFrom(it) }
        outbound.value?.let { produce.value = ByteString.copyFrom(it) }
        return produce.build()
    }

    private fun toWireOrdering(ordering: ProcessingOrder): WireProcessingOrder = when (ordering) {
        ProcessingOrder.UNORDERED -> WireProcessingOrder.PROCESSING_ORDER_UNORDERED
        ProcessingOrder.PARTITION -> WireProcessingOrder.PROCESSING_ORDER_PARTITION
        ProcessingOrder.KEY -> WireProcessingOrder.PROCESSING_ORDER_KEY
    }

    /** Built by hand rather than with protobuf-java-util, which is not on this module's classpath. */
    private fun toWireDuration(duration: Duration): com.google.protobuf.Duration {
        val wholeSeconds = duration.inWholeSeconds
        val nanos = duration.inWholeNanoseconds - wholeSeconds * NANOS_PER_SECOND
        return com.google.protobuf.Duration.newBuilder()
            .setSeconds(wholeSeconds)
            .setNanos(nanos.toInt())
            .build()
    }

    private const val NANOS_PER_SECOND = 1_000_000_000L
}
