// Copyright (C) 2026 Antony Stubbs and contributors

package bz.stub.parallelconsumer.client.coroutines

import bz.stub.parallelconsumer.client.ClientOptions as JavaClientOptions
import bz.stub.parallelconsumer.client.InboundRecord as JavaInboundRecord
import bz.stub.parallelconsumer.client.Outcome as JavaOutcome
import bz.stub.parallelconsumer.client.OutboundRecord as JavaOutboundRecord

/**
 * **This file is the entire cost of being a Kotlin client rather than a Java one, and it is here in
 * one place so that cost stays visible.**
 *
 * Everything below translates between a type this client respells and the reference API's spelling
 * of the same thing. Nothing here decides anything: no session state, no protocol rule, no verdict.
 * Wave one carried a `Wire` object of the same size that mapped these types onto protobuf instead -
 * the difference is that the protobuf mapping had to be *right*, and this one only has to be
 * *faithful*, because the transport underneath is now the one every JVM client shares.
 *
 * The rule for adding to it: a respelling earns its place only when Kotlin genuinely says the thing
 * better - nullability instead of `Optional`, default arguments instead of a builder, a sealed
 * interface instead of a boolean flag, one nullable [PreviousFailure] instead of two independent
 * ones that can describe a state the wire cannot. Anything else is imported and used as it stands;
 * [ProcessingOrder] and [Session] are the two that already were.
 */
internal fun ClientOptions.toJava(): JavaClientOptions {
    val builder = JavaClientOptions.builder()
        .topics(topics)
        .kafkaProperties(kafkaProperties)
    maxConcurrency?.let(builder::maxConcurrency)
    ordering?.let(builder::ordering)
    // built with ofNanos rather than kotlin.time's own converter: the conversion is total, exact,
    // and needs no opt-in, so the surface stays free of an experimental annotation
    commitInterval?.let { builder.commitInterval(java.time.Duration.ofNanos(it.inWholeNanoseconds)) }
    defaultMessageRetryDelay?.let {
        builder.defaultMessageRetryDelay(java.time.Duration.ofNanos(it.inWholeNanoseconds))
    }
    return builder.build()
}

/**
 * One delivered record in this client's spelling.
 *
 * The two `Optional` fields collapse into one nullable [PreviousFailure], which is the one place
 * this translation is not merely cosmetic: separately, a time and a reason can express "a reason
 * with no time", which the wire cannot say and no engine ever means.
 */
internal fun JavaInboundRecord.toKotlin(): InboundRecord = InboundRecord(
    topic = topic(),
    partition = partition(),
    offset = offset(),
    key = key(),
    value = value(),
    attempt = attempt(),
    previousFailure = lastFailureAt()
        .map { at -> PreviousFailure(at, lastFailureReason().orElse(null)) }
        .orElse(null),
)

/** The sealed verdict as the reference API's two-armed value. */
internal fun Outcome.toJava(): JavaOutcome = when (this) {
    is Outcome.Success -> JavaOutcome.success(produce.map { it.toJava() })
    is Outcome.Failure -> JavaOutcome.failure(reason)
}

private fun OutboundRecord.toJava(): JavaOutboundRecord = JavaOutboundRecord.of(topic, key, value)
