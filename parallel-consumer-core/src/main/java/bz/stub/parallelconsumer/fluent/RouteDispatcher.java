package bz.stub.parallelconsumer.fluent;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ExceptionInUserFunctionException;
import bz.stub.parallelconsumer.PCRetriableException;
import bz.stub.parallelconsumer.PollContext;
import bz.stub.parallelconsumer.RecordContext;
import bz.stub.parallelconsumer.internal.UserFunctions;
import bz.stub.parallelconsumer.state.WorkContainer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Supplier;

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
 *     <li>The function runs and reports its outcome: succeeded and filtered complete the record and produce
 *     nothing, a produced outcome is serialised with the route's produced types and returned for the engine to
 *     send, and a throw is a retry until the record's attempts reach the route's limit, after which it parks
 *     (R7 to R10).</li>
 * </ol>
 *
 * <h2>Why a throw is the only way back, and what a throw can say</h2>
 * The engine has no "give this record back" call, so every hand-back the facade makes - a park, a stop - is a
 * throw. What it <b>meant</b> by the throw rides on the exception itself: {@link PCRetriableException} carries an
 * optional retry delay, an optional "this was not an attempt", and an optional park, each read by the engine on the
 * failure path before it consults any retry-delay provider (KTD14). So there is one attempt count, the engine's,
 * and the route's retry limit is measured against it.
 * <p>
 * Every throw the wrapper itself makes extends that retriable exception, so it is logged at debug rather than as an
 * error: it is the outcome the definition asked for, not a fault. A throw from the <em>user's</em> function is
 * passed through unchanged, so the engine keeps classifying it as the classic API does - that distinction is
 * logging and nothing else (R9).
 *
 * <h2>Where the parked set lives</h2>
 * Nowhere here. A parked record is a failed record the engine will never make due again, so the engine's retry
 * queue already holds it, and {@link #parkedRecordsFrom} rebuilds the view from those containers rather than from a
 * second store this class would have to keep in step. What the engine cannot hold - the key as the route decoded
 * it, and how many park cycles the record spent - is derived here from the route and the attempt count.
 */
@Slf4j
class RouteDispatcher {

    /**
     * The route table, by the topic that selects a route (R2). It is the definition's own map rather than a copy,
     * and is only ever read here - a definition refuses a second start, so no route can be added behind the
     * wrapper's back once records are flowing.
     */
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

    /**
     * Whether the definition was handed a finished consumer at all - the condition without which the raw-bytes
     * fault is not merely unlikely but impossible, because a consumer this facade built itself is a byte-array
     * consumer by construction.
     * <p>
     * Separate from {@link #preBuiltConsumerDescription}, which is null both when no consumer was supplied and when
     * one was supplied that the probe could not read: a best-effort diagnostic cannot also be the flag that decides
     * whether an instance is stopped.
     */
    private final boolean preBuiltConsumerSupplied;

    /**
     * Replaced at start with the instance. Until then, and in a test that drives the wrapper directly, a fault has
     * nowhere to go but the log.
     */
    private volatile InstanceControl instance = new LoggingOnlyInstanceControl();

    /**
     * Replaced at start with the meters the definition registered. Until then, counting nothing.
     */
    private volatile FluentMeters meters = FluentMeters.none();

    /**
     * Where the parked set is read from: the engine's retry queue, through the instance. Empty until it wires
     * it, because a wrapper with no engine behind it has nothing parked.
     */
    private volatile Supplier<List<WorkContainer<?, ?>>> parkedContainers = Collections::emptyList;

    // ---------------------------------------------------------------- outcome counters
    //
    // Instance-wide totals, beside the per-topic meters of R19 that FluentMeters publishes: these are plain
    // counters so a test can assert the outcome of a thousand records without standing up a meter registry, and
    // they are what the wrapper's own tests read.

    /**
     * Records whose function reported success, a produce outcome included (R7) - counted where the outcome is
     * mapped, which for a produce outcome is before the engine has sent anything.
     * <p>
     * <b>A known over-report, and what closing it needs.</b> On a producing route a send that fails after
     * serialisation succeeded leaves the source record incomplete and retried, while this has already counted it -
     * so repeated send failures count one record several times. The cross-model review on
     * astubbs/parallel-consumer#502 asked for this to be driven from the engine's successful-work callback instead,
     * and the seams that exist do not carry it: the produce callback ({@link #produceAcknowledged}) is per produced
     * record with the batch context attached, never per source record, and
     * {@code WorkManager.addSuccessfulWorkListener} is per record but cannot tell a success from a filtered record,
     * because the engine completes both identically - which is the whole reason this facade counts them apart at
     * all (R8). Driving it from there needs the facade to remember each record's reported outcome until the engine
     * completes it, which is per-record facade state this package deliberately does not keep (KTD14). Left as the
     * count of reported successes until that is decided; the produced-record total beside it is exact.
     */
    private final LongAdder succeeded = new LongAdder();

    /**
     * Records completed by a filtered outcome. The engine cannot tell one from a success - both complete and
     * commit - so counting them apart here is the only place the distinction survives (R8).
     */
    private final LongAdder filtered = new LongAdder();

    /**
     * Records that parked, counted on the one path every park goes through. A park for a partition this instance
     * no longer owns is deliberately not counted: it belongs to whoever owns the partition now.
     */
    private final LongAdder parked = new LongAdder();

    /**
     * Produced records the broker has acknowledged, counted one at a time as the engine's produce callback reports
     * each send - records, not outcomes, since one outcome may carry several.
     * <p>
     * It used to be summed at dispatch, off the list handed back, which counted records the engine had not sent yet
     * and would go on counting them again on every retry of a record whose send kept failing. The engine's callback
     * runs after the send's own future has returned its metadata, so what this counts now is acknowledged sends.
     */
    private final LongAdder producedRecords = new LongAdder();

    /**
     * Built by the definition before the engine exists, because the retry-delay provider the options carry is one
     * of this object's own methods.
     *
     * @param routesByTopic               the definition's route table, read here and never written
     * @param fallbackRetryDelay          what {@link #retryDelayFor} answers for a topic no route claims
     * @param preBuiltConsumerDescription the deserialisers read off a supplied consumer, or null when the
     *                                    definition built its own or the probe could not read them
     * @param preBuiltConsumerSupplied    whether a finished consumer was supplied - what makes the raw-bytes fault
     *                                    possible at all
     */
    RouteDispatcher(Map<String, RouteState> routesByTopic, Duration fallbackRetryDelay,
                    String preBuiltConsumerDescription, boolean preBuiltConsumerSupplied) {
        this.routesByTopic = routesByTopic;
        this.fallbackRetryDelay = fallbackRetryDelay;
        this.preBuiltConsumerDescription = preBuiltConsumerDescription;
        this.preBuiltConsumerSupplied = preBuiltConsumerSupplied;
    }

    /**
     * Replace the logging-only stand-in with the instance this wrapper runs in, so a fatal fault or a stop request
     * reaches something that can act on it. Wired by the instance before anything polls, which is what makes it true
     * that the first record already has somewhere to report to.
     */
    void instanceControl(InstanceControl instance) {
        this.instance = instance;
    }

    /**
     * Hand over the meters the definition registered, so per-topic outcomes are published (R19). Until this is
     * called the wrapper still counts into its own totals, so a test needs no registry to see what happened.
     */
    void meters(FluentMeters meters) {
        this.meters = meters;
    }

    /**
     * Point the parked view at the engine's retry queue. A supplier rather than the queue, because all this class
     * needs to know is that something can list what the engine is holding parked - which is what keeps the parked
     * set out of this class entirely (KTD14).
     */
    void parkedContainers(Supplier<List<WorkContainer<?, ?>>> parkedContainers) {
        this.parkedContainers = parkedContainers;
    }

    // ---------------------------------------------------------------- the parked view

    /**
     * One route's parked records, across every partition (R28). A route declared over a set of topics answers for
     * all of them, since they share one function and one policy.
     *
     * @param topic any topic of the route
     */
    List<ParkedRecord> parkedForRoute(String topic) {
        return parkedFor(routeFor(topic).topics());
    }

    /**
     * Every parked record on this instance, whichever route it belongs to - the roll-up that is named apart so the
     * per-route accessor is never overloaded (R28).
     */
    List<ParkedRecord> parkedAcrossAllRoutes() {
        return parkedRecordsFrom(parkedContainers.get(), null);
    }

    /**
     * The parked records of a named set of topics, filtered <b>before</b> anything is built rather than after.
     * <p>
     * It matters because building one entry decodes the record's key: answering a single route's query by
     * materialising every route's parked records and discarding the rest decoded the whole instance's parked set to
     * report on one topic of it.
     *
     * @param topics the topics to report on; null for every routed topic
     */
    List<ParkedRecord> parkedFor(Set<String> topics) {
        return parkedRecordsFrom(parkedContainers.get(), topics);
    }

    /**
     * This instance's route for a topic.
     *
     * @throws IllegalArgumentException naming the routed topics, when nothing routes this one
     */
    private RouteState routeFor(String topic) {
        RouteState route = routesByTopic.get(topic);
        if (route == null) {
            throw noRouteClaims(topic, routesByTopic.keySet());
        }
        return route;
    }

    /**
     * The one wording of "you asked about a topic this instance does not route", so the instance and the wrapper
     * cannot answer the same mistake in two different sentences. A misspelled topic answered with an empty parked
     * set would read as good news, which is why it is a refusal at all.
     */
    static IllegalArgumentException noRouteClaims(String topic, Set<String> routedTopics) {
        return new IllegalArgumentException(msg("No route claims topic {} - this instance routes {}", topic,
                routedTopics));
    }

    /**
     * Rebuild the parked view from the containers the engine is holding parked (KTD14).
     * <p>
     * Everything on a {@link ParkedRecord} comes from the container or from the route, and nothing from a store:
     * the raw record, the failure and the moment it parked are the container's failure history; the reason is the
     * verdict the engine recorded when the throw said {@code park}; the key is decoded from the raw bytes as
     * the view is built; and the park cycles are arithmetic over the attempt count (see {@link #cyclesUsed}).
     * <p>
     * A container for a topic this instance does not route is skipped rather than reported: the engine's queue is
     * the whole instance's, and a definition only ever answers for its own routes.
     *
     * @param wanted the topics to report on, or null for every routed topic. Applied here rather than by the
     *               caller because everything below it costs a key deserialisation.
     */
    private List<ParkedRecord> parkedRecordsFrom(List<WorkContainer<?, ?>> containers, Set<String> wanted) {
        List<ParkedRecord> view = new ArrayList<>(containers.size());
        for (WorkContainer<?, ?> container : containers) {
            RecordContext<byte[], byte[]> recordContext = contextOf(container);
            RouteState route = routesByTopic.get(recordContext.topic());
            if (route == null || (wanted != null && !wanted.contains(recordContext.topic()))) {
                continue;
            }
            view.add(new ParkedRecord(recordContext, decodeKeyQuietly(route, recordContext.getConsumerRecord()),
                    cyclesUsed(route, recordContext.getNumberOfFailedAttempts())));
        }
        return Collections.unmodifiableList(view);
    }

    /**
     * Every parked container reaches the view through here, so the one cast the engine's generic queue forces is
     * spelled in a single place rather than at each reader.
     */
    @SuppressWarnings("unchecked")
    private static RecordContext<byte[], byte[]> contextOf(WorkContainer<?, ?> container) {
        // The facade builds every instance on a byte[] consumer (KTD2), so the engine's containers carry byte[]
        // records - the wildcards are the engine's queue being generic over every instance, not a real unknown.
        return new RecordContext<>((WorkContainer<byte[], byte[]>) container);
    }

    /**
     * How many park cycles this record spent, <b>derived</b> rather than counted (R27).
     * <p>
     * A park cycle is one further attempt granted after exhaustion, and the engine counts every attempt - so the
     * cycles a record has spent are exactly the attempts it has beyond its limit's last ordinary run, capped by
     * the policy. Keeping a second map of cycle counts said the same thing in a shape that needed clearing on
     * every rebalance.
     */
    private static int cyclesUsed(RouteState route, int attempts) {
        int cycles = route.resolvedAfterRetries().parkCycles();
        OptionalInt limit = route.retryLimit();
        if (cycles == 0 || !limit.isPresent() || attempts <= limit.getAsInt()) {
            return 0;
        }
        return Math.min(attempts - limit.getAsInt() - 1, cycles);
    }

    /**
     * The key as the route reads it, for the parked view (R28). A key that will not decode is reported as absent
     * rather than as a failure: the record parked for its own reason, and the raw bytes are on the entry either
     * way.
     */
    private static Object decodeKeyQuietly(RouteState route, ConsumerRecord<byte[], byte[]> raw) {
        try {
            return decode(route.consumedKey(), raw.topic(), raw.headers(), raw.key());
        } catch (RuntimeException keyWillNotDecode) {
            log.debug("Could not decode the key of parked record {}-{}@{} for its parked view entry",
                    raw.topic(), raw.partition(), raw.offset(), keyWillNotDecode);
            return null;
        }
    }

    /**
     * How many records completed as successes. A sum over the adder's cells, so it is a reading taken while work
     * is running rather than a snapshot of a moment - which is all a total of finished work can be.
     */
    long succeededCount() {
        return succeeded.sum();
    }

    /**
     * How many records the routes filtered. Reading this beside {@link #succeededCount()} is the only way to see
     * what a route rejected, since the engine completed both the same way (R8).
     */
    long filteredCount() {
        return filtered.sum();
    }

    /**
     * How many records parked, which is also how many are still holding their partition's committed offset where
     * it is until somebody acts (R27).
     */
    long parkedCount() {
        return parked.sum();
    }

    /**
     * How many records the routes handed back to be sent. It counts what was returned to the engine, not what the
     * broker acknowledged - the send is the engine's, and its outcome is the engine's to report.
     */
    long producedRecordCount() {
        return producedRecords.sum();
    }

    /**
     * One produced record the broker has acknowledged, from the engine's produce callback - the seam that fires
     * after the send's future has returned its metadata, and not at all when the send failed (R7).
     * <p>
     * <b>Only the produced-record total is driven from here, not the success count.</b> This callback fires once per
     * produced RECORD and carries the whole poll context rather than the one source record that produced it, so it
     * cannot say "this source record completed": a function returning three records fires it three times, and one
     * returning none never fires it at all. What the succeeded counter still cannot see is recorded on
     * {@link #succeeded}.
     */
    void produceAcknowledged() {
        producedRecords.increment();
    }

    /**
     * Close every format any route holds, once each, after the engine has stopped.
     * <p>
     * <b>The facade owns these.</b> A route's formats are built and {@code configure}d by this package, and the
     * reflective Avro and Protobuf wrappers delegate {@code close()} precisely because their serialisers may hold an
     * HTTP client and a schema cache - so an instance that shut down without closing them leaked those, once per
     * definition started. Nothing else was going to: the engine closes the clients it built and knows nothing about
     * a route's typing.
     * <p>
     * <b>Once each, by identity</b>, because the route table holds a route under every topic it was declared over
     * (R5) and because two routes may legitimately share one format object - a schema-registry serde built once and
     * handed to both. A second {@code close()} on a serde is not contracted to be harmless, so being asked twice is
     * not something to rely on.
     * <p>
     * <b>Every failure is contained.</b> This runs during shutdown, where a format that throws on the way out must
     * not stop the formats after it from closing, and must not replace whatever the caller was already being told
     * about the shutdown.
     */
    void closeRouteFormats() {
        Set<Format<?>> alreadyClosed = Collections.newSetFromMap(new IdentityHashMap<Format<?>, Boolean>());
        for (RouteState route : routesByTopic.values()) {
            closeOnce(route.consumedKey(), alreadyClosed);
            closeOnce(route.consumedValue(), alreadyClosed);
            closeOnce(route.producedKey(), alreadyClosed);
            closeOnce(route.producedValue(), alreadyClosed);
        }
    }

    /**
     * One format, if it is there and has not been closed already by this pass.
     *
     * @param format        null on the produced side of a route that declares no produced types
     * @param alreadyClosed identity-keyed, because two distinct formats may compare equal and closing one of them
     *                      twice while never closing the other is the failure that would be invisible
     */
    private static void closeOnce(Format<?> format, Set<Format<?>> alreadyClosed) {
        if (format == null || !alreadyClosed.add(format)) {
            return;
        }
        try {
            format.close();
        } catch (RuntimeException closeFailed) {
            log.warn("The format {} threw while closing during shutdown; the remaining formats are closed anyway",
                    format, closeFailed);
        }
    }

    // ---------------------------------------------------------------- the engine function

    /**
     * The produce-many arm: whatever the routes returned is sent by the engine and committed with the record's
     * offset.
     */
    List<ProducerRecord<byte[], byte[]>> dispatch(PollContext<byte[], byte[]> poll) {
        List<ProducerRecord<byte[], byte[]>> produced = new ArrayList<>();
        for (RecordContext<byte[], byte[]> context : poll.getContextsFlattened()) {
            produced.addAll(dispatchOne(context));
        }
        return produced;
    }

    /**
     * The plain-poll arm, for a definition that opens no producer. Nothing on it can produce a record: a route that
     * declares produced types is exactly what makes the definition need a producer (R4), so the check below is an
     * invariant rather than a user-facing refusal.
     */
    void dispatchWithoutProducing(PollContext<byte[], byte[]> poll) {
        // Counted rather than collected: this arm runs for every record of a non-producing definition, and the
        // list it used to build existed only to be asserted empty.
        int produced = 0;
        for (RecordContext<byte[], byte[]> context : poll.getContextsFlattened()) {
            produced += dispatchOne(context).size();
        }
        if (produced != 0) {
            throw new IllegalStateException(msg("A route returned {} records to produce on an instance that opened "
                    + "no producer. A route that declares produced types is what makes a definition need one, and a "
                    + "route with none that returns records is refused by name in the outcome mapping - see "
                    + "producedWithoutProducedTypes - so reaching this is a bug in the fluent API rather than in "
                    + "the definition.", produced));
        }
    }

    /**
     * One record, end to end: pick its route, decode it, run its function, and map what came back. Both arms above
     * go through here, so the produce-many and plain-poll flows cannot come to differ about what a record's
     * outcome means.
     * <p>
     * Every failure leaves by a throw, because the engine has no other way to be handed a record back - so this
     * either returns the records to send, or does not return at all.
     *
     * @return the records the engine should send for this one, empty when the record completes with nothing
     */
    private List<ProducerRecord<byte[], byte[]>> dispatchOne(RecordContext<byte[], byte[]> recordContext) {
        ConsumerRecord<byte[], byte[]> record = recordContext.getConsumerRecord();
        RouteState route = routesByTopic.get(record.topic());
        if (route == null) {
            throw new IllegalStateException(msg("No route claims topic {}, yet a record from it was dispatched. The "
                            + "instance subscribes to exactly the union of its routes' topics, so this is a bug in "
                            + "the fluent API. Routed topics: {}",
                    record.topic(), routesByTopic.keySet()));
        }

        // The engine has failed this record `alreadyFailed` times; the run about to happen is the next attempt, and
        // that is the number the retry limit is measured against (R10, KTD14).
        int alreadyFailed = recordContext.getNumberOfFailedAttempts();
        int attempts = alreadyFailed + 1;

        // Whatever decoded before the failure, which is what the park observer is given: both sides
        // when the record decoded, the key alone when only the value failed, neither when nothing did (R13, R16).
        Object key = null;
        Object value = null;
        try {
            key = decode(route.consumedKey(), record.topic(), record.headers(), record.key());
            value = decode(route.consumedValue(), record.topic(), record.headers(), record.value());
        } catch (PermanentDecodeFailureException permanent) {
            // No attempt is spent: the payload will never decode, so there is nothing to try again (R12). And no
            // park cycle either - a wait cannot change a payload that can never be read (R27).
            throw park(new TypedRecordContext<>(recordContext, key, value), route, permanent, alreadyFailed,
                    "its payload can never be decoded", false);
        } catch (ClassCastException castFailed) {
            if (preBuiltConsumerSupplied && RawBytesConsumerFaultException.isRawBytesCastFailure(castFailed)) {
                throw rawBytesFault(castFailed);
            }
            throw afterAttempt(new TypedRecordContext<>(recordContext, key, value), route, castFailed, attempts);
        } catch (RuntimeException decodeFailed) {
            // A stock deserialiser cannot tell a corrupt payload from a registry outage, so this is transient by
            // default and costs an attempt (R12).
            throw afterAttempt(new TypedRecordContext<>(recordContext, key, value), route, decodeFailed, attempts);
        }

        TypedRecordContext<Object, Object> context = new TypedRecordContext<>(recordContext, key, value);
        Outcome<Object, Object> outcome;
        try {
            outcome = run(route, context);
        } catch (Exception userFunctionThrew) {
            throw afterAttempt(context, route, userFunctionThrew, attempts);
        }
        return applyWithinTheRetryLimit(outcome, context, route, attempts);
    }

    /**
     * Everything after the user's function returns, held inside the same retry limit as the function itself (R10).
     *
     * <h2>Why this exists</h2>
     * Mapping an outcome can fail, and those failures used to escape straight to the engine - which retries
     * forever, because the route's finite limit is applied here and nowhere else. A serialiser that consistently
     * rejects a produced value, a null record inside a producing outcome, or a function returning null therefore
     * retried without end: the route's after-retries reaction was never reached, so nothing ever parked and,
     * under key ordering, the key stayed blocked behind a record that would never finish. The limit now covers the
     * whole of what this wrapper does with a record, not just the part the user wrote.
     *
     * <h2>What is deliberately let through</h2>
     * A park and a stop leave {@link #apply} by a throw because a throw is the only way to hand a record back
     * (R8, R24) - they are what the mapping DID, not a failure of it. Both are recognised by type and rethrown
     * untouched, so a park still spends the attempt it declared and a stop still stops the instance. Sending either
     * through the exhaustion path would re-park an already-parked record and count it twice.
     */
    private List<ProducerRecord<byte[], byte[]>> applyWithinTheRetryLimit(Outcome<Object, Object> outcome,
                                                                         TypedRecordContext<Object, Object> context,
                                                                         RouteState route,
                                                                         int attempts) {
        try {
            if (outcome == null) {
                throw new IllegalStateException(msg("The processing function for topic {} returned null. Return "
                        + "Outcome.succeeded() for a record that was processed, or throw to retry it.",
                        route.describeTopics()));
            }
            return apply(outcome, context, route, attempts);
        } catch (RecordParkedException | StopRequestedException whatTheMappingDid) {
            // The park and the stop arms of apply(), which report an outcome rather than failing to map one.
            throw whatTheMappingDid;
        } catch (RuntimeException mappingTheOutcomeFailed) {
            throw afterAttempt(context, route, mappingTheOutcomeFailed, attempts);
        }
    }

    /**
     * The outcome the function reported, turned into what the engine is told - the one place the five kinds are
     * mapped, so the counters, the meters and the engine can never be told three different stories about the same
     * record.
     * <p>
     * Two of them leave by a throw rather than a return, which is what a park and a stop are (R8, R24).
     *
     * @param attempts the run just finished, counted from one - carried through so a park records the attempt it
     *                 parked on
     * @return the records for the engine to send, empty when the record completes with nothing
     */
    private List<ProducerRecord<byte[], byte[]>> apply(Outcome<Object, Object> outcome,
                                                       TypedRecordContext<Object, Object> context,
                                                       RouteState route,
                                                       int attempts) {
        ConsumerRecord<byte[], byte[]> record = context.raw();
        switch (outcome.kind()) {
            case SUCCEEDED:
                succeeded.increment();
                meters.recordOutcome(record.topic(), OutcomeTag.SUCCEEDED);
                return emptyProduce();
            case FILTERED:
                // Completes and commits exactly as a success does, and is counted apart from one (R8).
                filtered.increment();
                meters.recordOutcome(record.topic(), OutcomeTag.FILTERED);
                return emptyProduce();
            case PRODUCE:
                if (!route.producesRecords()) {
                    throw producedWithoutProducedTypes(route, record);
                }
                List<ProducerRecord<byte[], byte[]>> serialised = serialise(route, outcome.records());
                succeeded.increment();
                meters.recordOutcome(record.topic(), OutcomeTag.SUCCEEDED);
                // Not counted here: the engine has not sent these yet, and produceAcknowledged() is told when it
                // has.
                return serialised;
            case PARK:
                // The function already knows this record is hopeless, so its remaining attempts are skipped - and
                // so are its park cycles, which are more attempts by another name (R8, R27). The run that reported it
                // is still an attempt - the function ran - so it counts, and the parked entry shows it.
                throw park(context, route, null, attempts, outcome.reason(), true);
            case STOP:
                throw stop(record, outcome.reason());
            default:
                throw new IllegalStateException(msg("Unhandled outcome {} on topic {}", outcome.kind(),
                        route.describeTopics()));
        }
    }

    /**
     * "This record is finished and there is nothing to send", named once so the succeeded and filtered arms
     * cannot come to say it differently.
     */
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
    private RuntimeException afterAttempt(TypedRecordContext<Object, Object> context, RouteState route,
                                          Throwable failure, int attempts) {
        ConsumerRecord<byte[], byte[]> record = context.raw();
        if (isExhausted(route, attempts)) {
            AfterRetries policy = route.resolvedAfterRetries();
            int cycles = cyclesUsed(route, attempts);
            if (policy.parkCycles() > cycles) {
                return parkCycle(record, policy, failure, attempts, cycles + 1);
            }
            if (policy.reaction() == AfterRetries.Reaction.STOP) {
                // This route's author says a record that runs out of attempts here means the deployment is wrong,
                // not the record - so the instance stops instead of parking, and the parked count does not move
                // (R24, R27). It is the same stop path the outcome takes, entered from exhaustion.
                return stop(record, msg("{}-{}@{} ran out of attempts after {} attempt(s), and this route reacts to "
                                + "exhaustion by stopping the instance",
                        record.topic(), record.partition(), record.offset(), attempts));
            }
            return park(context, route, failure, attempts, "it ran out of attempts", true);
        }
        // An ordinary retry, at the route's own delay: carried on the throw so a route's delay needs no state
        // anywhere, and a route that declares none simply carries the instance default (R6, KTD14).
        return retriable(asUnchecked(failure), route.retryDelay());
    }

    /**
     * Spend one park cycle: the record waits the policy's delay, holding no worker, and is then attempted once
     * more. It is <b>not</b> parked meanwhile - it has an attempt coming, so it is not in the parked view and
     * nothing observes it. This is scheduled retry (astubbs#234) delivered as a park delay.
     */
    private RuntimeException parkCycle(ConsumerRecord<byte[], byte[]> record, AfterRetries policy, Throwable failure,
                                       int attempts, int cycle) {
        log.info("Park cycle {} of {} for {}-{}@{} after {} attempt(s): it waits {} and is then attempted once "
                        + "more; after the last cycle it parks until resumed.",
                cycle, policy.parkCycles(), record.topic(), record.partition(), record.offset(), attempts,
                policy.parkDelay());
        return retriable(asUnchecked(failure), policy.parkDelay());
    }

    /**
     * Attach a retry delay to the exception that is about to be thrown, without changing what the exception is.
     * <p>
     * A throw from the user's function keeps its own type and message - the engine classifies it, and the user
     * reads it - so the delay is added to the instance in flight rather than to a wrapper around it. Only a
     * {@link PCRetriableException} can carry one; anything else takes the configured delay, which for this facade
     * is what {@link #retryDelayFor} answers - and that method, not this one, is why a plain throw under a park
     * policy still waits the park cycle's delay rather than the route's ordinary one.
     */
    private static RuntimeException retriable(RuntimeException failure, Duration delay) {
        if (failure instanceof PCRetriableException && delay != null && !delay.isNegative()) {
            ((PCRetriableException) failure).retryAfter(delay);
        }
        return failure;
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
     * <b>The one path every park goes through</b>, whether the record ran out of attempts, spent its last park
     * cycle, could never be decoded, or was declared hopeless by the function itself. The throw carries the park to
     * the engine, which records it on the record itself - never due again, with the reason attached - so the parked
     * view is a read of the engine's own retry queue and there is nothing here to keep in step with it (KTD14).
     * <p>
     * <b>A park for a partition that is no longer ours records nothing and tells nobody.</b> A worker can finish
     * after its partition was revoked, and what it would report then belongs to whoever owns the partition now.
     * The engine already knows - it drops the container rather than holding it parked - so this asks it rather
     * than keeping an assignment view of its own.
     *
     * @param countsAsAttempt false for a park that spent no attempt at the user's work - a payload that can never
     *                        be decoded (R12)
     */
    private RuntimeException park(TypedRecordContext<Object, Object> context, RouteState route, Throwable failure,
                                  int attempts, String why, boolean countsAsAttempt) {
        ConsumerRecord<byte[], byte[]> record = context.raw();
        String message = msg("Parked {}-{}@{} after {} attempt(s): {}. It stays incomplete in the offset map and "
                        + "holds no worker; offsets past it still commit under key and unordered processing.",
                record.topic(), record.partition(), record.offset(), attempts, why);

        if (context.recordContext().isStale()) {
            // A worker can finish after its partition was revoked. The engine will drop this container rather than
            // hold it parked, so there is nothing to list - and reporting it would tell an observer, and an
            // operator, about a record that belongs to whoever owns the partition now. The record is still handed
            // back, because it must not complete under this instance either way.
            log.debug("{} (not recorded: this instance's claim on the partition was revoked while it ran)", message);
            return parkedThrow(message, failure, why, countsAsAttempt);
        }

        notifyObserver(route, context, failure, attempts);
        parked.increment();
        meters.recordOutcome(record.topic(), OutcomeTag.PARKED);
        if (failure == null) {
            // Nothing failed: the function asked for this, so it is not a warning.
            log.info(message);
        } else {
            // WARN, with the failure attached. The final attempt's exception is NOT logged by the engine - it
            // becomes the cause of this retriable throw, and retriable throws are logged at debug - so this line
            // is the only place an operator sees why the record gave up. Once per record, never per attempt.
            log.warn(message, failure);
        }

        return parkedThrow(message, failure, why, countsAsAttempt);
    }

    /**
     * The throw itself: a retriable exception carrying the park, so the engine marks the record never due, records
     * why, and keeps the whole thing out of the error log (KTD14).
     */
    private static RecordParkedException parkedThrow(String message, Throwable failure, String why,
                                                     boolean countsAsAttempt) {
        RecordParkedException parkedException = failure == null
                ? new RecordParkedException(message)
                : new RecordParkedException(message, failure);
        parkedException.park(why);
        if (!countsAsAttempt) {
            parkedException.notAnAttempt();
        }
        return parkedException;
    }

    /**
     * Tell the route's park observer, if it has one, and <b>let nothing it does matter</b> (R16).
     * <p>
     * It runs on the worker thread, inside the failure path, immediately before the throw that hands the record
     * back - which is what makes "after the last attempt and before the offset commits" true: the commit happens
     * later, on the control thread, once this throw has been processed.
     */
    private void notifyObserver(RouteState route, TypedRecordContext<Object, Object> context, Throwable failure,
                                int attempts) {
        ParkObserver<?, ?> observer = route.parkObserver();
        if (observer == null) {
            return;
        }
        try {
            observe(observer, context, failure, attempts);
        } catch (Exception observerThrew) {
            // Contained, and logged once - the record parks either way. An observer that could change the outcome
            // would make parking depend on the reporting, which is the one thing sugar may never do.
            log.warn("The park observer for topic {} threw for {}-{}@{}; the record parked anyway",
                    route.describeTopics(), context.topic(), context.partition(), context.offset(), observerThrew);
        }
    }

    /**
     * The one raw-typed call into a park observer. An observer is declared over its route's consumed types, which
     * are gone by the time the wrapper holds it, so the cast is isolated here rather than repeated - and the
     * values it is handed came from that same route's deserialisers, which is what makes it sound.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private static void observe(ParkObserver<?, ?> observer, TypedRecordContext<Object, Object> context,
                                Throwable failure, int attempts) {
        ((ParkObserver) observer).onParked(context, failure, attempts);
    }

    /**
     * A route asked the instance to stop, either from its function's {@link Outcome#stop(String)} or because one of
     * its records ran out of attempts on a route whose reaction is {@link AfterRetries#stop()} (R24, R27).
     *
     * <h2>Four steps, and the order is the design (KTD6)</h2>
     * <ol>
     *     <li><b>Mark</b> this record never-due, so a drain does not re-invoke it in the window before the instance
     *     closes, and spend no attempt on it: it asked to stop, it did not fail.</li>
     *     <li><b>Pause</b>, through the handle. The engine's pause stops the controller handing out new work
     *     <em>and</em> hands back the batches already queued in the worker pool, so nothing further reaches a
     *     route's function - that second half is the engine change this facade used to work around with a flag of
     *     its own (KTD14).</li>
     *     <li><b>Close</b>, also through the instance, on a thread of its own because this one is a worker and the
     *     close awaits the worker pool.</li>
     *     <li><b>Throw</b>, which is the only way to hand a record back, leaving it incomplete so a restart
     *     delivers it again.</li>
     * </ol>
     */
    private RuntimeException stop(ConsumerRecord<byte[], byte[]> record, String reason) {
        StopRequestedException stopping = new StopRequestedException(msg("The route for {} asked the instance to "
                        + "stop at {}-{}@{}: {}. The record is left incomplete, so a restart delivers it again.",
                record.topic(), record.topic(), record.partition(), record.offset(), reason));
        // Parked, with the stop as its reason: the record is held where it is until somebody acts, which is what
        // park means, and an operator looking at why an instance stopped wants to see exactly this record. No
        // attempt is spent - it reported, it did not fail.
        stopping.notAnAttempt().park("it asked the instance to stop");
        instance.stopRequested(record, reason);
        return stopping;
    }

    /**
     * A route with no produced types returned records to produce: a definition fault, never a retry - the same
     * shape as the raw-bytes consumer fault below, and raised the same way.
     * <p>
     * R3 makes producing from such a route a compile error by declaring its produced types as {@code Void}, and
     * that closes every case but one: {@code null} inhabits every reference type, so
     * {@code Outcome.produce(new ProducerRecord<>("out", null, null))} infers {@code ProducerRecord<Void, Void>}
     * and compiles. Kafka permits null keys and values, so that is a real record shape rather than an impossible
     * generic value, and no choice of type parameter can exclude it. What reached the route then was a
     * {@code NullPointerException} out of the produced formats that are not there; this says what actually
     * happened instead, and says it once rather than per record, because retrying it would fail identically for
     * every record on the topic forever.
     */
    private RuntimeException producedWithoutProducedTypes(RouteState route,
                                                          ConsumerRecord<byte[], byte[]> record) {
        IllegalStateException fault = new IllegalStateException(msg("The route for {} returned records to produce, "
                        + "and declares no produced types - so there is nothing to serialise them with. Declare "
                        + "them on the route with produced(...) to produce from it, or return "
                        + "Outcome.succeeded() instead (R3, R4). Raised at {}-{}@{}; this is a fault of the "
                        + "definition, so retrying it would fail identically for every record on this topic.",
                route.describeTopics(), record.topic(), record.partition(), record.offset()));
        instance.fatal(fault);
        return fault;
    }

    /**
     * A pre-built consumer that is not configured for raw bytes: a definition fault, never a retry (KTD3, R1).
     * <p>
     * <b>Only reachable when a consumer was actually supplied.</b> A consumer this facade built is a byte-array
     * consumer by construction, so a cast failure on a definition that built its own is somebody else's - and
     * classifying it here would stop the instance instead of taking R12's transient decode-failure retry path. The
     * other half of that narrowing is {@link RawBytesConsumerFaultException#isRawBytesCastFailure}, which requires
     * the throw to have come from this class's own cast.
     * <p>
     * {@link InstanceControl#fatal} is what makes it fatal; the instance closes itself and surfaces this
     * exception to whoever is awaiting shutdown. The throw itself matters either way: the record must not complete,
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
     * What the engine's retry-delay provider answers for one record: the route's ordinary delay, or its park
     * policy's delay when this failure is the one that spends a park cycle.
     * <p>
     * <b>It is a pure function of the topic and the record's attempt count</b>, which is what it became once a
     * throw could carry its own delay (KTD14). It used to read a thread-local the throw site had written a moment
     * earlier, because park and retry needed different answers for the same topic and this was the only channel;
     * the engine calls the provider synchronously inside the failure path, so getting that write order wrong did
     * not fail, it silently turned a park into a one-second retry.
     * <p>
     * <b>Why the park cycle's delay comes back through here rather than on the throw.</b>
     * {@link #parkCycle} attaches it to the exception, and {@link #retriable} can only attach a delay to a
     * {@link PCRetriableException} - so a route declaring {@code park().thenRetryAfter(30s).forCycles(3)} whose
     * function throws a plain {@code IllegalStateException} waited the route's ORDINARY delay instead. Wrapping
     * the user's throw to carry the delay is not available: the engine reads the hand-back and classifies the
     * failure for logging with the same {@code unwrapTransparentWrappers} walk, so any wrapper that carried a
     * delay would also demote the user's error to debug, which R9 says must not change. The provider is the other
     * channel the engine already offers, and it is consulted for exactly the failures that carried nothing.
     * <p>
     * The attempt count the engine passes has already been incremented for the failure being handled, so it is
     * the same {@code attempts} {@link #afterAttempt} decided on - the two therefore cannot disagree about which
     * failure spends a cycle.
     * <p>
     * <b>It cannot throw, return null, return a negative delay, or return one that overflows.</b> The engine
     * replaces a provider that does any of those with its own one-second default and a rate-limited warning.
     * {@code EngineRetryDelayProviderContractTest} pins that engine behaviour so this claim keeps its teeth.
     *
     * @param topic    the record's topic, which is what selects the route
     * @param attempts how many times this record has failed, INCLUDING the failure being handled
     */
    Duration retryDelayFor(String topic, int attempts) {
        RouteState route = routesByTopic.get(topic);
        if (route == null) {
            return fallbackRetryDelay;
        }
        if (spendsAParkCycle(route, attempts)) {
            return route.resolvedAfterRetries().parkDelay();
        }
        return route.retryDelay();
    }

    /**
     * Whether the failure at {@code attempts} is the one {@link #afterAttempt} answers with a park cycle - the
     * same two conditions, in the same order, read from the same two functions.
     */
    private static boolean spendsAParkCycle(RouteState route, int attempts) {
        return isExhausted(route, attempts)
                && route.resolvedAfterRetries().parkCycles() > cyclesUsed(route, attempts);
    }

    // ---------------------------------------------------------------- decoding, running and serialising

    /**
     * One field through a route's deserialiser. The topic and the headers go with it because a schema-aware
     * deserialiser needs both - the subject is derived from the topic, and the schema id may ride in a header
     * (KTD7).
     */
    @SuppressWarnings("unchecked")
    private static Object decode(Format<?> format, String topic, Headers headers, byte[] bytes) {
        return ((Deserializer<Object>) format.deserializer()).deserialize(topic, headers, bytes);
    }

    /**
     * Call the route's function, raw-typed in one place. A route's declared types are erased by the time the
     * wrapper holds its function, and the context it is handed carries exactly the values that route's own
     * deserialisers produced - which is the whole of why the cast holds.
     * <p>
     * The checked {@code Exception} is deliberate: a function may throw one, so the clients it calls need no
     * wrapping of their own (R9).
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private static Outcome<Object, Object> run(RouteState route, TypedRecordContext<?, ?> context) throws Exception {
        return ((ProcessFunction) route.function()).process(context);
    }

    /**
     * Encode a produce outcome with the <em>consuming</em> route's declared produced formats (R4), keeping the
     * topic, partition, timestamp and headers the function set. The record's own destination is left alone
     * because a route may produce to several topics, and none of them need be routed by this instance.
     *
     * @param produced the records as the function returned them, still in the route's produced types
     */
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

    /**
     * One field through a route's serialiser, the mirror of {@link #decode}: the topic and headers travel with it
     * for the same reason, and a schema-aware serialiser may write into those headers as it goes.
     */
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
        return new ExceptionInUserFunctionException(UserFunctions.MSG, failure);
    }

    /**
     * Where a fault goes before the instance exists, or when the wrapper is driven directly by a test.
     */
    @Slf4j
    private static class LoggingOnlyInstanceControl implements InstanceControl {

        /**
         * Satisfies {@link InstanceControl#fatal} with the only thing available when there is no instance: an
         * error log. Nothing here can stop the run, so the log is at ERROR rather than debug - a fault that
         * reached this stand-in in production is one nobody else is going to report.
         */
        @Override
        public void fatal(Throwable definitionFault) {
            log.error("A definition fault reached the dispatch wrapper before it was wired to an instance",
                    definitionFault);
        }

        /**
         * Satisfies {@link InstanceControl#stopRequested} when there is no instance to pause or close. Warned
         * rather than logged quietly, because a route asked for something that is not going to happen - and the
         * record is still handed back, so nothing completes on the strength of a stop that never ran.
         */
        @Override
        public void stopRequested(ConsumerRecord<byte[], byte[]> record, String reason) {
            log.warn("A stop was requested at {}-{}@{} ({}) before the wrapper was wired to an instance",
                    record.topic(), record.partition(), record.offset(), reason);
        }
    }
}
