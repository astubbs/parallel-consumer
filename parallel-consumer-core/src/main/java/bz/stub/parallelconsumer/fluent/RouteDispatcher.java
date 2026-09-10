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
import java.time.Instant;
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
 *     <li><b>The stopping flag is read first.</b> If a route has asked the instance to stop, the record is handed
 *     straight back without being run - see {@link #stop}, which owns why a flag is needed beside the engine's own
 *     pause (R24, KTD6).</li>
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
 *
 * <p>
 * One consequence of parking through that same hook is that the engine reports every parked record as slow work,
 * because a park is a retry with a far-future delay and the shard scan cannot tell the two apart. The package
 * javadoc owns that statement - what an operator sees, why the facade cannot suppress it, and the small-tier
 * engine change that removes it - and {@code ParkedRecordsAreSlowWorkForNowTest} pins it.
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
     * The facade's half of the parked view, and the park-cycle counts (R27, R28).
     */
    private final ParkedRecords parkedRecords = new ParkedRecords();

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

    /**
     * Replaced at start with the meters the definition registered. Until then, counting nothing.
     */
    private volatile FluentMeters meters = FluentMeters.none();

    /**
     * <b>The fence.</b> Raised by the first record to report a stop, and read before every function call (KTD6).
     * <p>
     * The engine's pause stops the control thread handing out <em>new</em> work, and that is all it stops: the
     * tasks already queued in the worker pool run regardless, and a record dispatched in that window would be
     * processed by an instance that is on its way down. So every dispatch checks this first and hands the record
     * straight back with the far-future marker instead of running it - incomplete, uncounted, and delivered again
     * after the restart.
     * <p>
     * Volatile and one-way: a worker thread raises it, every other worker reads it, and nothing ever lowers it. An
     * instance that has been asked to stop does not come back.
     */
    private volatile boolean stopping;

    // ---------------------------------------------------------------- outcome counters
    //
    // Instance-wide totals, beside the per-topic meters of R19 that FluentMeters publishes: these are plain
    // counters so a test can assert the outcome of a thousand records without standing up a meter registry, and
    // they are what the wrapper's own tests read.

    private final AtomicLong succeeded = new AtomicLong();

    private final AtomicLong filtered = new AtomicLong();

    private final AtomicLong parked = new AtomicLong();

    private final AtomicLong producedRecords = new AtomicLong();

    /**
     * Records handed straight back by the stopping fence. Not an outcome and not a meter: nothing happened to these
     * records, and they are delivered again after the restart.
     */
    private final AtomicLong fenced = new AtomicLong();

    RouteDispatcher(Map<String, RouteState> routesByTopic, Duration fallbackRetryDelay,
                    String preBuiltConsumerDescription) {
        this.routesByTopic = routesByTopic;
        this.fallbackRetryDelay = fallbackRetryDelay;
        this.preBuiltConsumerDescription = preBuiltConsumerDescription;
    }

    void instanceControl(InstanceControl instance) {
        this.instance = instance;
    }

    void meters(FluentMeters meters) {
        this.meters = meters;
    }

    /**
     * How many records have been fenced by the stopping flag: dispatched after the stop and handed straight back
     * without running. Visible so a test can prove the fence did the work rather than the timing.
     */
    long fencedCount() {
        return fenced.get();
    }

    ConsumerRebalanceListener rebalanceListener(ConsumerRebalanceListener usersListener) {
        return new FacadeRebalanceListener(ledger, parkedRecords, usersListener);
    }

    AttemptLedger ledger() {
        return ledger;
    }

    // ---------------------------------------------------------------- the parked view
    //
    // Seam for the lifecycle unit: these three are what the handle's per-route parked() and its instance roll-up
    // read (R28). What is missing from them is the reconciliation against the engine's incomplete offsets, which
    // has to be taken on the control thread, and the per-partition figures that need engine accessors that do not
    // exist yet - the payload fraction above all (KTD11).

    /**
     * The parked set as this wrapper holds it: entries, park cycles, and which partitions are still ours.
     */
    ParkedRecords parkedRecords() {
        return parkedRecords;
    }

    /**
     * One route's parked records, across every partition (R28). A route declared over a set of topics answers for
     * all of them, since they share one function and one policy.
     *
     * @param topic any topic of the route
     */
    List<ParkedRecord> parkedForRoute(String topic) {
        RouteState route = routesByTopic.get(topic);
        if (route == null) {
            throw new IllegalArgumentException(msg("No route claims topic {} - this instance routes {}", topic,
                    routesByTopic.keySet()));
        }
        return parkedRecords.forTopics(route.topics());
    }

    /**
     * Every parked record on this instance, whichever route it belongs to - the roll-up that is named apart so the
     * per-route accessor is never overloaded (R28).
     */
    List<ParkedRecord> parkedAcrossAllRoutes() {
        return parkedRecords.all();
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
        if (stopping) {
            throw fence(record);
        }
        RouteState route = routesByTopic.get(record.topic());
        if (route == null) {
            throw new IllegalStateException(msg("No route claims topic {}, yet a record from it was dispatched. The "
                            + "instance subscribes to exactly the union of its routes' topics, so this is a bug in "
                            + "the fluent API. Routed topics: {}",
                    record.topic(), routesByTopic.keySet()));
        }

        // Whatever decoded before the failure, which is what the park observer and an export are given: both sides
        // when the record decoded, the key alone when only the value failed, neither when nothing did (R13, R16).
        Object key = null;
        Object value = null;
        try {
            byte[] rawKey = record.key();
            byte[] rawValue = record.value();
            key = decode(route.consumedKey(), record.topic(), record.headers(), rawKey);
            value = decode(route.consumedValue(), record.topic(), record.headers(), rawValue);
        } catch (PermanentDecodeFailureException permanent) {
            // No attempt is spent: the payload will never decode, so there is nothing to try again (R12). And no
            // park cycle either - a wait cannot change a payload that can never be read (R27).
            throw park(new ProcessContext<>(record, key, value), route, permanent, 0,
                    "its payload can never be decoded");
        } catch (ClassCastException castFailed) {
            if (RawBytesConsumerFaultException.isRawBytesCastFailure(castFailed)) {
                throw rawBytesFault(castFailed);
            }
            throw afterAttempt(new ProcessContext<>(record, key, value), route, castFailed, ledger.advance(record));
        } catch (RuntimeException decodeFailed) {
            // A stock deserialiser cannot tell a corrupt payload from a registry outage, so this is transient by
            // default and costs an attempt (R12).
            throw afterAttempt(new ProcessContext<>(record, key, value), route, decodeFailed,
                    ledger.advance(record));
        }

        ProcessContext<Object, Object> context = new ProcessContext<>(record, key, value);
        int attempts = ledger.advance(record);
        Outcome<Object, Object> outcome;
        try {
            outcome = run(route, context);
        } catch (Exception userFunctionThrew) {
            throw afterAttempt(context, route, userFunctionThrew, attempts);
        }
        if (outcome == null) {
            throw new IllegalStateException(msg("The processing function for topic {} returned null. Return "
                    + "Outcome.succeeded() for a record that was processed, or throw to retry it.",
                    route.describeTopics()));
        }
        return apply(outcome, context, route, attempts);
    }

    private List<ProducerRecord<byte[], byte[]>> apply(Outcome<Object, Object> outcome,
                                                       ProcessContext<Object, Object> context,
                                                       RouteState route,
                                                       int attempts) {
        ConsumerRecord<byte[], byte[]> record = context.raw();
        switch (outcome.kind()) {
            case SUCCEEDED:
                forget(record);
                succeeded.incrementAndGet();
                meters.recordOutcome(record.topic(), FluentMeters.SUCCEEDED);
                return emptyProduce();
            case FILTERED:
                // Completes and commits exactly as a success does, and is counted apart from one (R8).
                forget(record);
                filtered.incrementAndGet();
                meters.recordOutcome(record.topic(), FluentMeters.FILTERED);
                return emptyProduce();
            case PRODUCE:
                List<ProducerRecord<byte[], byte[]>> serialised = serialise(route, outcome.records());
                forget(record);
                succeeded.incrementAndGet();
                meters.recordOutcome(record.topic(), FluentMeters.SUCCEEDED);
                producedRecords.addAndGet(serialised.size());
                return serialised;
            case PARK:
                // The function already knows this record is hopeless, so its remaining attempts are skipped - and
                // so are its park cycles, which are more attempts by another name (R8, R27).
                throw park(context, route, null, attempts, outcome.reason());
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
        // A record that completes here may have been parked and then resumed, so its parked entry and its park
        // cycles go with it - the parked view lists what an operator can still act on, not what once parked.
        parkedRecords.forget(record);
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
    private RuntimeException afterAttempt(ProcessContext<Object, Object> context, RouteState route,
                                          Throwable failure, int attempts) {
        ConsumerRecord<byte[], byte[]> record = context.raw();
        if (isExhausted(route, attempts)) {
            AfterRetries policy = route.afterRetries();
            if (parkCycleRemains(policy, record)) {
                return parkCycle(record, route, policy, failure, attempts);
            }
            if (policy.reaction() == AfterRetries.Reaction.STOP) {
                // This route's author says a record that runs out of attempts here means the deployment is wrong,
                // not the record - so the instance stops instead of parking, and the parked count does not move
                // (R24, R27). It is the same stop path the outcome takes, entered from exhaustion.
                return stop(record, msg("{}-{}@{} ran out of attempts after {} attempt(s), and this route reacts to "
                                + "exhaustion by stopping the instance",
                        record.topic(), record.partition(), record.offset(), attempts));
            }
            return park(context, route, failure, attempts, "it ran out of attempts");
        }
        RetryIntents.retry(record, route.retryDelay());
        return asUnchecked(failure);
    }

    /**
     * Whether this record still has a park cycle to spend: an exhausted record whose policy declared a delay and a
     * number of cycles gets one more attempt per cycle, and parks for good after the last of them (R27).
     */
    private boolean parkCycleRemains(AfterRetries policy, ConsumerRecord<byte[], byte[]> record) {
        return policy.parkCycles() > 0 && parkedRecords.cyclesUsed(record) < policy.parkCycles();
    }

    /**
     * Spend one park cycle: the record waits the policy's delay, holding no worker, and is then attempted once
     * more. It is <b>not</b> parked meanwhile - it has an attempt coming, so it is not in the parked view and
     * nothing observes it. This is scheduled retry (astubbs#234) delivered as a park delay.
     */
    private RuntimeException parkCycle(ConsumerRecord<byte[], byte[]> record, RouteState route, AfterRetries policy,
                                       Throwable failure, int attempts) {
        int cycle = parkedRecords.spendCycle(record);
        RetryIntents.park(record, policy.parkDelay());
        log.info("Park cycle {} of {} for {}-{}@{} after {} attempt(s): it waits {} and is then attempted once "
                        + "more; after the last cycle it parks until resumed.",
                cycle, policy.parkCycles(), record.topic(), record.partition(), record.offset(), attempts,
                policy.parkDelay());
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
     * <b>The one path every park goes through</b>, whether the record ran out of attempts, spent its last park
     * cycle, could never be decoded, or was declared hopeless by the function itself. Four things happen here and
     * they are deliberately in this order: the far-future intent, so the engine's provider has it before the throw
     * (KTD4); the parked entry, which is what the parked view lists and what decides whether this park is the
     * first; the observer, told once and unable to change anything (R16); and the outcome counter (R19).
     * <p>
     * <b>A park for a partition that is no longer ours records nothing and tells nobody.</b> A worker can finish
     * after its partition was revoked, and an entry written then belongs to whoever owns the partition now -
     * {@link ParkedRecords} owns that judgement. The record is still handed back incomplete, because it must not
     * complete under this instance either way.
     * <p>
     * <b>Seam for the export unit.</b> Export is a re-dispatch, not a send from this failure path (KTD5): on a
     * later dispatch of an already-parked record the wrapper returns the export record instead of calling the
     * function, reading the entry recorded here for the provenance headers. The definition refuses a dead-letter
     * destination at start until that lands, so no definition reaching here has one.
     */
    private RuntimeException park(ProcessContext<Object, Object> context, RouteState route, Throwable failure,
                                  int attempts, String why) {
        ConsumerRecord<byte[], byte[]> record = context.raw();
        RetryIntents.park(record, PARKED_UNTIL_RESUMED);

        int cycles = parkedRecords.cyclesUsed(record);
        boolean firstParkOfThisAssignment = parkedRecords.park(new ParkedRecord(record, context.key(), attempts,
                cycles, failure, why, Instant.now()));

        String message = msg("Parked {}-{}@{} after {} attempt(s){}: {}. It stays incomplete in the offset map and "
                        + "holds no worker; offsets past it still commit under key and unordered processing.",
                record.topic(), record.partition(), record.offset(), attempts,
                cycles == 0 ? "" : msg(" and {} park cycle(s)", cycles), why);
        if (!firstParkOfThisAssignment) {
            // Either a partition we no longer own, or a record already parked in this assignment. Neither is worth
            // an operator's attention, and neither may fire the observer or move the counter a second time.
            log.debug("{} (not recorded: this instance has no live claim on the record)", message);
            return parkedException(message, failure);
        }

        notifyObserver(route, context, failure, attempts);
        parked.incrementAndGet();
        meters.recordOutcome(record.topic(), FluentMeters.PARKED);
        if (failure == null) {
            // Nothing failed: the function asked for this, so it is not a warning.
            log.info(message);
        } else {
            // WARN, with the failure attached. The final attempt's exception is NOT logged by the engine - it
            // becomes the cause of this retriable throw, and retriable throws are logged at debug - so this line
            // is the only place an operator sees why the record gave up. Once per record, never per attempt.
            log.warn(message, failure);
        }
        return parkedException(message, failure);
    }

    private static RecordParkedException parkedException(String message, Throwable failure) {
        return failure == null ? new RecordParkedException(message) : new RecordParkedException(message, failure);
    }

    /**
     * Tell the route's park observer, if it has one, and <b>let nothing it does matter</b> (R16).
     * <p>
     * It runs on the worker thread, inside the failure path, immediately before the throw that hands the record
     * back - which is what makes "after the last attempt and before the offset commits" true: the commit happens
     * later, on the control thread, once this throw has been processed.
     */
    private void notifyObserver(RouteState route, ProcessContext<Object, Object> context, Throwable failure,
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

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static void observe(ParkObserver<?, ?> observer, ProcessContext<Object, Object> context,
                                Throwable failure, int attempts) {
        ((ParkObserver) observer).onParked(context, failure, attempts);
    }

    /**
     * A route asked the instance to stop, either from its function's {@link Outcome#stop(String)} or because one of
     * its records ran out of attempts on a route whose reaction is {@link AfterRetries#stop()} (R24, R27).
     *
     * <h2>Five steps, and the order is the design (KTD6)</h2>
     * <ol>
     *     <li><b>Mark</b> this record with the far-future delay, so a drain does not re-invoke it in the window
     *     before the instance closes. The intent has to be recorded before the throw, because the engine asks for
     *     the delay synchronously inside the failure path.</li>
     *     <li><b>Fence</b>: raise the stopping flag, so any record already queued in the worker pool is handed back
     *     rather than run. The pause below cannot do this - it stops the control thread giving out work, not the
     *     pool running what it already has.</li>
     *     <li><b>Pause and close</b>, both through the handle: pausing is non-blocking and immediate, closing
     *     happens on a thread of its own because this one is a worker and the close awaits the worker pool.</li>
     *     <li><b>Throw</b>, which is the only way to hand a record back, leaving it incomplete so a restart
     *     delivers it again.</li>
     * </ol>
     * The fence is raised <em>before</em> the handle is told, not after: between those two calls the instance is
     * still running at full speed, and a record dispatched there is exactly what the fence exists to catch.
     */
    private RuntimeException stop(ConsumerRecord<byte[], byte[]> record, String reason) {
        RetryIntents.park(record, PARKED_UNTIL_RESUMED);
        stopping = true;
        instance.stopRequested(record, reason);
        return new StopRequestedException(msg("The route for {} asked the instance to stop at {}-{}@{}: {}. The "
                        + "record is left incomplete, so a restart delivers it again.",
                record.topic(), record.topic(), record.partition(), record.offset(), reason));
    }

    /**
     * This record was dispatched after the instance was asked to stop, so it is handed straight back without being
     * run (R24, KTD6).
     * <p>
     * No attempt is spent, no park is recorded and nothing is counted: nothing happened to this record. It stays
     * incomplete and is delivered again after the restart, alongside the record that asked for the stop.
     */
    private RuntimeException fence(ConsumerRecord<byte[], byte[]> record) {
        RetryIntents.park(record, PARKED_UNTIL_RESUMED);
        long fencedSoFar = fenced.incrementAndGet();
        log.debug("Not running {}-{}@{}: this instance has been asked to stop, so the record is handed back "
                        + "incomplete ({} fenced so far)",
                record.topic(), record.partition(), record.offset(), fencedSoFar);
        return new StopRequestedException(msg("{}-{}@{} was not run: this instance has been asked to stop. The "
                        + "record is left incomplete, so a restart delivers it again.",
                record.topic(), record.partition(), record.offset()));
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
