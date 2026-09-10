package bz.stub.parallelconsumer.fluent;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ExceptionInUserFunctionException;
import bz.stub.parallelconsumer.PollContext;
import bz.stub.parallelconsumer.RecordContext;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import static bz.stub.parallelconsumer.internal.utils.StringUtils.msg;

/**
 * The one engine function: raw bytes in, a route table by topic, and each record decoded, run and mapped to its
 * outcome (KTD2).
 *
 * <h2>What happens to one record</h2>
 * <ol>
 *     <li>Its topic picks its route. A topic has exactly one route (R2), and the instance subscribes to exactly the
 *     union of the routes' topics, so a record with no route is an invariant break rather than a user error.</li>
 *     <li>The route's deserialisers decode it, with three possible results (R12): a value, a <em>permanent</em>
 *     failure, which parks the record at once without spending an attempt, or a <em>transient</em> one, which is a
 *     failed attempt like any other.</li>
 *     <li>The attempt ledger advances - immediately before the function runs, or when a decode failure was
 *     transient - and that count is what the route's retry limit is measured against (KTD4).</li>
 *     <li>The function runs and reports its outcome: succeeded and filtered complete the record and produce
 *     nothing, a produced outcome is serialised with the route's produced types and returned for the engine to
 *     send, and a throw is a retry until the ledger reaches the route's limit, after which the record parks
 *     (R7 to R10).</li>
 * </ol>
 *
 * <h2>Why a throw is the only way back</h2>
 * The engine has no "give this record back" call today, so every hand-back the facade makes - a park, a stop - is a
 * throw, and the retry-delay provider is where the wrapper says what it meant by it. Hence {@link RetryIntents}: the
 * intent goes into a thread-local <b>before</b> the throw, because the engine asks the provider synchronously inside
 * the failure path. Getting that order wrong does not fail; it silently turns a park into a one-second retry, which
 * is why {@code RetryIntentHookTest} carries a control arm for it.
 * <p>
 * Every throw the wrapper itself makes extends the engine's retriable exception, so it is logged at debug rather
 * than as an error: it is the outcome the definition asked for, not a fault. A throw from the <em>user's</em>
 * function is passed through unchanged, so the engine keeps classifying it as the classic API does - that
 * distinction is logging and nothing else (R9).
 */
@Slf4j
class RouteDispatcher {

    /**
     * The delay a parked record is handed back with: far enough away to be "until somebody does something", and
     * still representable.
     * <p>
     * <b>Not {@code ChronoUnit.FOREVER} or anything of that magnitude.</b> The engine adds this delay to the time
     * of the failure, and a sum that falls outside {@link java.time.Instant}'s range is caught and silently
     * replaced with the default one-second delay - so an unrepresentable park delay is a hot retry loop against
     * whatever was already failing, with no error anywhere (KTD4).
     */
    static final Duration PARKED_UNTIL_RESUMED = Duration.ofDays(36500);

    private final Map<String, RouteState> routesByTopic;

    /**
     * What the retry-delay provider answers for a record whose topic has no route - which the engine can ask about
     * a partition being revoked. Never null, so the provider cannot answer null.
     */
    private final Duration fallbackRetryDelay;

    /**
     * The deserialisers read off a pre-built consumer when it was supplied, for the message the raw-bytes fault
     * carries (KTD3). Null when the definition built its own consumer, in which case the fault cannot happen.
     */
    private final String preBuiltConsumerDescription;

    private final AttemptLedger ledger = new AttemptLedger();

    /**
     * The facade's count and the engine's, as they stood the last time the engine asked about each record.
     * <p>
     * They are two different numbers on purpose (see {@link AttemptLedger}), and in this milestone they agree for
     * every record whose hand-backs were all real failures - which is every record except one parked by a
     * permanent decode failure, where the facade spends no attempt and the engine counts the throw. Keeping the
     * pair is what lets a test assert that rather than assume it.
     */
    private final Map<String, int[]> attemptCountsAtLastHandBack = new ConcurrentHashMap<>();


    /**
     * Replaced at start with the handle. Until then, and in a test that drives the wrapper directly, a fault has
     * nowhere to go but the log.
     */
    private volatile InstanceControl instance = new LoggingOnlyInstanceControl();

    // ---------------------------------------------------------------- outcome counters
    //
    // Seam for the metrics unit: these are the counts R19 publishes, tagged by topic and outcome, registered
    // through PCMetricsDef by the module the facade builds (KTD8). They are counters here so that this unit's
    // tests can assert the outcome of a thousand records without a meter registry.

    private final AtomicLong succeeded = new AtomicLong();

    private final AtomicLong filtered = new AtomicLong();

    private final AtomicLong parked = new AtomicLong();

    private final AtomicLong producedRecords = new AtomicLong();

    RouteDispatcher(Map<String, RouteState> routesByTopic, Duration fallbackRetryDelay,
                    String preBuiltConsumerDescription) {
        this.routesByTopic = routesByTopic;
        this.fallbackRetryDelay = fallbackRetryDelay;
        this.preBuiltConsumerDescription = preBuiltConsumerDescription;
    }

    void instanceControl(InstanceControl instance) {
        this.instance = instance;
    }

    ConsumerRebalanceListener rebalanceListener(ConsumerRebalanceListener usersListener) {
        return new FacadeRebalanceListener(ledger, usersListener);
    }

    AttemptLedger ledger() {
        return ledger;
    }

    long succeededCount() {
        return succeeded.get();
    }

    long filteredCount() {
        return filtered.get();
    }

    long parkedCount() {
        return parked.get();
    }

    long producedRecordCount() {
        return producedRecords.get();
    }

    // ---------------------------------------------------------------- the engine function

    /**
     * The produce-many arm: whatever the routes returned is sent by the engine and committed with the record's
     * offset.
     */
    List<ProducerRecord<byte[], byte[]>> dispatch(PollContext<byte[], byte[]> poll) {
        // Bounds what an intent nobody read can cost - see RetryIntents' lifecycle note.
        RetryIntents.clearThread();
        List<ProducerRecord<byte[], byte[]>> produced = new ArrayList<>();
        for (ConsumerRecord<byte[], byte[]> record : poll.getConsumerRecordsFlattened()) {
            produced.addAll(dispatchOne(record));
        }
        return produced;
    }

    /**
     * The plain-poll arm, for a definition that opens no producer. Nothing on it can produce a record: a route that
     * declares produced types is exactly what makes the definition need a producer (R4), so the check below is an
     * invariant rather than a user-facing refusal.
     */
    void dispatchWithoutProducing(PollContext<byte[], byte[]> poll) {
        List<ProducerRecord<byte[], byte[]>> produced = dispatch(poll);
        if (!produced.isEmpty()) {
            throw new IllegalStateException(msg("A route returned {} records to produce on an instance that opened "
                    + "no producer. A route that declares produced types is what makes a definition need one, so "
                    + "this is a bug in the fluent API rather than in the definition.", produced.size()));
        }
    }

    private List<ProducerRecord<byte[], byte[]>> dispatchOne(ConsumerRecord<byte[], byte[]> record) {
        RouteState route = routesByTopic.get(record.topic());
        if (route == null) {
            throw new IllegalStateException(msg("No route claims topic {}, yet a record from it was dispatched. The "
                            + "instance subscribes to exactly the union of its routes' topics, so this is a bug in "
                            + "the fluent API. Routed topics: {}",
                    record.topic(), routesByTopic.keySet()));
        }

        Object key;
        Object value;
        try {
            byte[] rawKey = record.key();
            byte[] rawValue = record.value();
            key = decode(route.consumedKey(), record.topic(), record.headers(), rawKey);
            value = decode(route.consumedValue(), record.topic(), record.headers(), rawValue);
        } catch (PermanentDecodeFailureException permanent) {
            // No attempt is spent: the payload will never decode, so there is nothing to try again (R12).
            throw park(record, route, permanent, 0, "its payload can never be decoded");
        } catch (ClassCastException castFailed) {
            if (RawBytesConsumerFaultException.isRawBytesCastFailure(castFailed)) {
                throw rawBytesFault(castFailed);
            }
            throw afterAttempt(record, route, castFailed, ledger.advance(record));
        } catch (RuntimeException decodeFailed) {
            // A stock deserialiser cannot tell a corrupt payload from a registry outage, so this is transient by
            // default and costs an attempt (R12).
            throw afterAttempt(record, route, decodeFailed, ledger.advance(record));
        }

        int attempts = ledger.advance(record);
        Outcome<Object, Object> outcome;
        try {
            outcome = run(route, new ProcessContext<>(record, key, value));
        } catch (Exception userFunctionThrew) {
            throw afterAttempt(record, route, userFunctionThrew, attempts);
        }
        if (outcome == null) {
            throw new IllegalStateException(msg("The processing function for topic {} returned null. Return "
                    + "Outcome.succeeded() for a record that was processed, or throw to retry it.",
                    route.describeTopics()));
        }
        return apply(outcome, record, route, attempts);
    }

    private List<ProducerRecord<byte[], byte[]>> apply(Outcome<Object, Object> outcome,
                                                       ConsumerRecord<byte[], byte[]> record,
                                                       RouteState route,
                                                       int attempts) {
        switch (outcome.kind()) {
            case SUCCEEDED:
                forget(record);
                succeeded.incrementAndGet();
                return emptyProduce();
            case FILTERED:
                // Completes and commits exactly as a success does, and is counted apart from one (R8).
                forget(record);
                filtered.incrementAndGet();
                return emptyProduce();
            case PRODUCE:
                List<ProducerRecord<byte[], byte[]>> serialised = serialise(route, outcome.records());
                forget(record);
                succeeded.incrementAndGet();
                producedRecords.addAndGet(serialised.size());
                return serialised;
            case PARK:
                // The function already knows this record is hopeless, so its remaining attempts are skipped (R8).
                throw park(record, route, null, attempts, outcome.reason());
            case STOP:
                throw stop(record, outcome.reason());
            default:
                throw new IllegalStateException(msg("Unhandled outcome {} on topic {}", outcome.kind(),
                        route.describeTopics()));
        }
    }

    /**
     * This record reached an outcome that completes it, so nothing will ask about it again. Both per-record maps
     * drop it here - which is what keeps them bounded by the records currently in flight or parked, rather than by
     * everything the instance has ever seen.
     */
    private void forget(ConsumerRecord<byte[], byte[]> record) {
        ledger.forget(record);
        attemptCountsAtLastHandBack.remove(keyOf(record.topic(), record.partition(), record.offset()));
    }

    private static String keyOf(String topic, int partition, long offset) {
        return topic + '-' + partition + '@' + offset;
    }

    private List<ProducerRecord<byte[], byte[]>> emptyProduce() {
        // The engine reads an empty list as "nothing to send", completes the record and commits its offset - which
        // is what makes filtered and succeeded the same thing to the engine and different things to the counters.
        // Immutable and shared: this is the common case, once per successful record.
        return Collections.emptyList();
    }

    // ---------------------------------------------------------------- attempts, retry and park

    /**
     * One attempt at this record has failed. Either it retries, or it has run out and parks (R10).
     *
     * @return the exception to throw, so a caller reads as {@code throw afterAttempt(...)} and the compiler knows
     * the path ends
     */
    private RuntimeException afterAttempt(ConsumerRecord<byte[], byte[]> record, RouteState route,
                                          Throwable failure, int attempts) {
        if (isExhausted(route, attempts)) {
            return park(record, route, failure, attempts, "it ran out of attempts");
        }
        RetryIntents.retry(record, route.retryDelay());
        return asUnchecked(failure);
    }

    /**
     * The retry limit counts the attempts <em>after the first</em> (R10), so a limit of two allows three runs and
     * the fourth never happens.
     */
    private static boolean isExhausted(RouteState route, int attempts) {
        OptionalInt limit = route.retryLimit();
        return limit.isPresent() && attempts > limit.getAsInt();
    }

    /**
     * Park this record: it stays incomplete in the offset map, holds no worker, and is not attempted again until an
     * operator resumes it or a restart re-delivers it (R27).
     * <p>
     * <b>Seam for the park unit.</b> What this does today is the hand-back itself - the intent, the far-future
     * delay, the count. What it does not yet do is the bookkeeping around it: the parked view's entry for this
     * record (topic, partition, offset, key, attempts, last failure, parked-since, and under key ordering the
     * number of records held behind it), the once-per-record park observer (R16), and the parked gauges (R19).
     * They all attach here, on the one path every park goes through.
     * <p>
     * <b>Seam for the export unit.</b> Export is a re-dispatch, not a send from this failure path (KTD5): on a
     * later dispatch of an already-parked record the wrapper returns the export record instead of calling the
     * function. The definition refuses a dead-letter destination at start until that lands, so no definition
     * reaching here has one.
     */
    private RuntimeException park(ConsumerRecord<byte[], byte[]> record, RouteState route, Throwable failure,
                                  int attempts, String why) {
        RetryIntents.park(record, PARKED_UNTIL_RESUMED);
        parked.incrementAndGet();
        String message = msg("Parked {}-{}@{} after {} attempt(s): {}. It stays incomplete in the offset map and "
                        + "holds no worker; offsets past it still commit under key and unordered processing.",
                record.topic(), record.partition(), record.offset(), attempts, why);
        if (failure == null) {
            // Nothing failed: the function asked for this, so it is not a warning.
            log.info(message);
        } else {
            // WARN, with the failure attached. The final attempt's exception is NOT logged by the engine - it
            // becomes the cause of this retriable throw, and retriable throws are logged at debug - so this line
            // is the only place an operator sees why the record gave up. Once per record, never per attempt.
            log.warn(message, failure);
        }
        return failure == null ? new RecordParkedException(message) : new RecordParkedException(message, failure);
    }

    /**
     * @see InstanceControl#stopRequested the seam that makes this actually stop the instance
     */
    private RuntimeException stop(ConsumerRecord<byte[], byte[]> record, String reason) {
        // Far future, not the route's retry delay: a drain must not re-invoke this record in the window before the
        // instance closes (KTD6).
        RetryIntents.park(record, PARKED_UNTIL_RESUMED);
        instance.stopRequested(record, reason);
        return new StopRequestedException(msg("The route for {} asked the instance to stop at {}-{}@{}: {}. The "
                        + "record is left incomplete, so a restart delivers it again.",
                record.topic(), record.topic(), record.partition(), record.offset(), reason));
    }

    /**
     * A pre-built consumer that is not configured for raw bytes: a definition fault, never a retry (KTD3, R1).
     * <p>
     * <b>Seam for the lifecycle unit.</b> {@link InstanceControl#fatal} is what makes it fatal; today the handle
     * closes the instance and surfaces this exception to whoever is awaiting shutdown, and the lifecycle unit
     * routes it through the stop path instead. The throw itself matters either way: the record must not complete,
     * because it was never processed.
     */
    private RuntimeException rawBytesFault(ClassCastException castFailed) {
        RawBytesConsumerFaultException fault =
                RawBytesConsumerFaultException.from(castFailed, preBuiltConsumerDescription);
        instance.fatal(fault);
        return fault;
    }

    // ---------------------------------------------------------------- the retry-delay provider

    /**
     * The provider the engine actually calls. It is also the one place the facade's attempt count and the engine's
     * own meet, so it is where they are compared (KTD4).
     */
    Duration retryDelayFor(RecordContext<byte[], byte[]> context) {
        recordCrossCheck(context);
        return retryDelayFor(context.topic(), context.partition(), context.offset());
    }

    private void recordCrossCheck(RecordContext<byte[], byte[]> context) {
        int ours = ledger.attempts(context.topic(), context.partition(), context.offset());
        int theirs = context.getNumberOfFailedAttempts();
        attemptCountsAtLastHandBack.put(keyOf(context.topic(), context.partition(), context.offset()),
                new int[]{ours, theirs});
        if (log.isDebugEnabled() && ours != theirs) {
            log.debug("The facade has run {}-{}@{} {} time(s) and the engine has failed it {} time(s) - they differ "
                            + "when a hand-back was not an attempt at the user's work",
                    context.topic(), context.partition(), context.offset(), ours, theirs);
        }
    }

    /**
     * Visible for tests: {facade attempts, engine attempts} as they stood when the engine last asked about this
     * record, or null if it never has.
     */
    int[] attemptCountsAtLastHandBack(String topic, int partition, long offset) {
        return attemptCountsAtLastHandBack.get(keyOf(topic, partition, offset));
    }

    /**
     * What the engine's retry-delay provider answers for one record: the intent this thread recorded immediately
     * before it threw, or the route's ordinary retry delay when there is none (KTD4).
     * <p>
     * <b>It cannot throw, return null, return a negative delay, or return one that overflows.</b> The engine
     * replaces a provider that does any of those with its own one-second default and a rate-limited warning, so a
     * fault here would turn every park into a hot retry loop with nothing going red.
     * {@code EngineRetryDelayProviderContractTest} pins that engine behaviour so this claim keeps its teeth.
     */
    Duration retryDelayFor(String topic, int partition, long offset) {
        RetryIntent intent = RetryIntents.take(topic, partition, offset);
        if (intent != null) {
            return intent.delay();
        }
        RouteState route = routesByTopic.get(topic);
        return route == null ? fallbackRetryDelay : route.retryDelay();
    }

    // ---------------------------------------------------------------- decoding, running and serialising

    @SuppressWarnings("unchecked")
    private static Object decode(Format<?> format, String topic, Headers headers, byte[] bytes) {
        return ((Deserializer<Object>) format.deserializer()).deserialize(topic, headers, bytes);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static Outcome<Object, Object> run(RouteState route, ProcessContext<?, ?> context) throws Exception {
        return ((ProcessFunction) route.function()).process(context);
    }

    private List<ProducerRecord<byte[], byte[]>> serialise(RouteState route,
                                                           List<? extends ProducerRecord<?, ?>> produced) {
        List<ProducerRecord<byte[], byte[]>> serialised = new ArrayList<>(produced.size());
        for (ProducerRecord<?, ?> record : produced) {
            byte[] key = encode(route.producedKey(), record.topic(), record.headers(), record.key());
            byte[] value = encode(route.producedValue(), record.topic(), record.headers(), record.value());
            serialised.add(new ProducerRecord<>(record.topic(), record.partition(), record.timestamp(), key, value,
                    record.headers()));
        }
        return serialised;
    }

    @SuppressWarnings("unchecked")
    private static byte[] encode(Format<?> format, String topic, Headers headers, Object value) {
        return ((Serializer<Object>) format.serializer()).serialize(topic, headers, value);
    }

    /**
     * The user's function may throw a checked exception, deliberately, so the clients it calls need no wrapping.
     * A checked one is wrapped in the engine's own "user code threw" wrapper, which the engine treats as
     * transparent - so what classifies the failure for logging is the exception the user actually threw (R9).
     */
    private static RuntimeException asUnchecked(Throwable failure) {
        if (failure instanceof RuntimeException) {
            return (RuntimeException) failure;
        }
        if (failure instanceof Error) {
            throw (Error) failure;
        }
        return new ExceptionInUserFunctionException("Error occurred in code supplied by user", failure);
    }

    /**
     * Where a fault goes before the handle exists, or when the wrapper is driven directly by a test.
     */
    @Slf4j
    private static class LoggingOnlyInstanceControl implements InstanceControl {

        @Override
        public void fatal(Throwable definitionFault) {
            log.error("A definition fault reached the dispatch wrapper before it was wired to an instance",
                    definitionFault);
        }

        @Override
        public void stopRequested(ConsumerRecord<byte[], byte[]> record, String reason) {
            log.warn("A stop was requested at {}-{}@{} ({}) before the wrapper was wired to an instance",
                    record.topic(), record.partition(), record.offset(), reason);
        }
    }
}
