package bz.stub.parallelconsumer.state;

/*-
 * Copyright (C) 2020-2024 Confluent, Inc.
 * Modifications Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.PollContextInternal;
import bz.stub.parallelconsumer.RecordContext;
import bz.stub.parallelconsumer.internal.PCModule;
import bz.stub.parallelconsumer.internal.ProducerManager;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.Temporal;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;

import static bz.stub.parallelconsumer.internal.utils.KafkaUtils.toTopicPartition;
import static java.util.Optional.of;

/**
 * Context object for a given {@link ConsumerRecord}, carrying completion status, various time stamps, retry data etc..
 *
 * @author Antony Stubbs
 */
@Slf4j
public class WorkContainer<K, V> implements Comparable<WorkContainer<K, V>> {

    static final String DEFAULT_TYPE = "DEFAULT";

    /**
     * Where a record is in its execution lifecycle, as one value.
     * <p>
     * <pre>
     *     AVAILABLE  --claim-->  IN_FLIGHT  --verdict-->  IN_FLIGHT_SUCCEEDED  --land-->  SUCCEEDED (terminal)
     *          ^                     |                    IN_FLIGHT_FAILED     --land-->  FAILED
     *          |                     +--no verdict------------------------------land-->  AVAILABLE (abandoned)
     *          +----------------------------------------claim, once the retry delay passes-----------+
     * </pre>
     * <p>
     * <b>Two dimensions, one value.</b> A record is either out at a worker or it is not, and it either carries a
     * verdict or it does not; those two questions used to be answered by two fields, and a claim that re-validated
     * only one of them is what let an already-succeeded record be delivered again. These six states are that pair
     * of questions crossed, and every one of them is a situation the code already reaches - in particular the two
     * {@code IN_FLIGHT_*} states, which is where a record sits between the worker recording its verdict and the
     * controller returning it. Collapsing those would make an outstanding record look parked to the revocation
     * sweep.
     * <p>
     * <b>The retry delay is deliberately NOT a state.</b> A delay expires because a clock passed a point, and
     * nothing fires a transition when that happens - so {@link #FAILED} covers both "waiting" and "due", and
     * {@link WorkContainer#isDelayPassed()} separates them at the moment of asking. That term is safe to read
     * outside the atomic: only the holder of a claim can write the retry deadline, a {@link #FAILED} record has no
     * holder, and time only ever moves one way.
     *
     * @author Antony Stubbs
     * @see WorkContainer#onQueueingForExecution()
     */
    public enum ExecutionState {

        /** Free, carrying no verdict: a fresh record, or one returned without a verdict at all. Claimable. */
        AVAILABLE(false, null),

        /** Out at a worker, which has not reported yet. */
        IN_FLIGHT(true, null),

        /** The user function succeeded; the controller has not taken the record back yet. */
        IN_FLIGHT_SUCCEEDED(true, Boolean.TRUE),

        /** The user function failed; the controller has not taken the record back yet. */
        IN_FLIGHT_FAILED(true, Boolean.FALSE),

        /** Succeeded and returned. <b>Terminal</b> - a claim from here is always refused. */
        SUCCEEDED(false, Boolean.TRUE),

        /** Failed and returned. Claimable again, but only once its retry delay has passed. */
        FAILED(false, Boolean.FALSE);

        private final boolean inFlight;

        private final Optional<Boolean> verdict;

        ExecutionState(boolean inFlight, Boolean verdict) {
            this.inFlight = inFlight;
            this.verdict = Optional.ofNullable(verdict);
        }

        /** @return true while the record is out at a worker and has not been returned to the controller */
        public boolean isInFlight() {
            return inFlight;
        }

        /**
         * @return true, false, or empty when the user function has not reported on this delivery
         */
        public Optional<Boolean> getVerdict() {
            return verdict;
        }

        /**
         * @return true when a claim may be attempted from this state - the state half of the decision; the retry
         *         delay is the other half
         */
        boolean isClaimable() {
            return this == AVAILABLE || this == FAILED;
        }

        /**
         * @return the state this delivery leaves behind when it is returned to the controller, keeping whatever
         *         verdict it carries. Only meaningful while {@link #isInFlight()}.
         */
        ExecutionState afterFlightEnds() {
            switch (this) {
                case IN_FLIGHT:
                    return AVAILABLE;
                case IN_FLIGHT_SUCCEEDED:
                    return SUCCEEDED;
                case IN_FLIGHT_FAILED:
                    return FAILED;
                default:
                    return this;
            }
        }

        /**
         * @return this state with a verdict attached, preserving whether the record is in flight. Total, because a
         *         verdict may legitimately be recorded against a record that was never claimed - retry-delay
         *         arithmetic is built that way in tests, and the failure history it drives is independent of the
         *         claim.
         */
        ExecutionState withVerdict(boolean succeeded) {
            if (inFlight) {
                return succeeded ? IN_FLIGHT_SUCCEEDED : IN_FLIGHT_FAILED;
            }
            return succeeded ? SUCCEEDED : FAILED;
        }
    }

    /**
     * Instance reference to otherwise static state, for access to the instance type parameters of WorkContainer as
     * static fields cannot access them.
     */
    @NonNull
    private final PCModule<K, V> module;

    /**
     * Assignment generation this record comes from. Used for fencing messages after partition loss, for work lingering
     * in the system of in flight.
     */
    @Getter
    private final long epoch;

    /**
     * Simple way to differentiate treatment based on type
     */
    @Getter
    @Setter
    // todo change to enum, remove setter - confluentinc#241
    private String workType;

    @Getter
    private final ConsumerRecord<K, V> cr;

    @Getter
    private int numberOfFailedAttempts = 0;

    @Getter
    private Optional<Instant> lastFailedAt = Optional.empty();

    @Getter
    private Optional<Instant> succeededAt = Optional.empty();

    @Getter
    private Optional<Throwable> lastFailureReason;

    /**
     * Where this record is in its execution lifecycle: whether it is out at a worker, and what verdict it carries.
     * <p>
     * <b>One field, because two were the bug.</b> This used to be an {@code AtomicBoolean inFlight} plus a separate
     * {@code Optional<Boolean> maybeUserFunctionSucceeded}, and selection read both but re-validated only the
     * boolean when it claimed. A puller whose availability decision predated another puller's completion could
     * therefore win the boolean compare-and-set on an already-succeeded record, and the claim then cleared the
     * verdict - erasing the term that should have refused it. The record was delivered, and its offset committed,
     * twice. Diagnosis, reproduction and the refuted predictions:
     * {@code docs/inflight/bug-direct-pull-claim-is-check-then-act.md}.
     * <p>
     * Atomic because the direct-pull engine lets every worker select work straight from the shards, so the
     * "is it free? then take it" pair has to be one indivisible step. Under the default engine only the control
     * loop selects work and a plain field would do; making it atomic for both keeps one code path, and the cost is
     * one uncontended compare-and-set per delivery - the same cost the boolean carried.
     *
     * @see ExecutionState
     * @see #onQueueingForExecution()
     */
    private final AtomicReference<ExecutionState> state = new AtomicReference<>(ExecutionState.AVAILABLE);

    /**
     * The shard holding this record, told when a delivery is claimed and when that delivery lands - so the two
     * are the halves of one state transition and cannot drift apart. Null only for a container that never entered
     * a shard, which in production cannot happen and in tests is common.
     * <p>
     * Written once, by {@link ProcessingShard#addWorkContainer}, before the container is published into the shard's
     * entry map - so every thread that can reach this container through the map has already seen the write.
     * <p>
     * <b>This pair of calls is what makes departure-on-take possible under {@code UNORDERED}</b>: the shard's
     * in-flight count and its membership of the offerable set are both derived from exactly this transition, so
     * there is no removal site whose condition can be got wrong.
     *
     * @see ProcessingShard#onFlightBegan
     * @see ProcessingShard#onFlightEnded
     */
    private ProcessingShard<K, V> shard;

    /**
     * Counts deliveries of this record. Incremented every time it is queued for execution, so each delivery has
     * an identity a return can be matched against.
     * <p>
     * This exists because an abandoned record is immediately re-selectable, and the control loop drains returns
     * and re-selects work in the same iteration. Without a delivery identity, a return arriving late for
     * delivery <em>n</em> is indistinguishable from a return for the live delivery <em>n+1</em>, and acting on
     * it ends a flight that is still running and decrements the in-flight counter twice.
     */
    @Getter
    private long deliveryCount = 0;

    /**
     * The delivery this record was abandoned on, or {@code -1} if it has never been abandoned. Deliberately
     * <em>not</em> cleared on redelivery: a late return still has to be recognisable as belonging to a delivery
     * that has already ended.
     *
     * @see #markAbandoned(long)
     */
    private long abandonedAtDelivery = -1;

    @Getter
    @Setter(AccessLevel.PUBLIC)
    private Future<List<?>> future;

    private Optional<Long> timeTakenAsWorkMs = Optional.empty();

    private Optional<Instant> retryDueAt = Optional.empty();

    private Comparator<WorkContainer<?, ?>> comparator = Comparator
            .comparing((WorkContainer<?, ?> workContainer) -> {
                // TopicPartition does not implement comparable
                TopicPartition tp = workContainer.getTopicPartition();
                return tp.topic() + tp.partition();
            })
            .thenComparing(WorkContainer::offset);

    public WorkContainer(long epoch, ConsumerRecord<K, V> cr, @NonNull PCModule<K, V> module, @NonNull String workType) {
        this.epoch = epoch;
        this.cr = cr;
        this.workType = workType;
        this.module = module;
    }

    public WorkContainer(long epoch, ConsumerRecord<K, V> cr, PCModule<K, V> module) {
        this(epoch, cr, module, DEFAULT_TYPE);
    }

    /**
     * This delivery has been handed back to the controller: the record leaves flight, keeping whatever verdict it
     * came back with, and releases its shard's in-flight charge.
     * <p>
     * A no-op when the record is not in flight. That is not a legal transition, but it has always been silently
     * idempotent here and the callers rely on it - {@link WorkManager#handleFutureResult} reaches this from the
     * revoked-partition branch, which does not know how far the delivery got.
     */
    public void endFlight() {
        while (true) {
            ExecutionState current = state.get();
            if (!current.isInFlight()) {
                log.trace("Flight already ended, nothing to release {}", this);
                return;
            }
            if (state.compareAndSet(current, current.afterFlightEnds())) {
                log.trace("Ending flight {}", this);
                releaseShardInFlightCharge();
                return;
            }
            // the worker's verdict landed between the read and the write - re-read and end the flight it left
        }
    }

    public boolean isDelayPassed() {
        if (!hasPreviouslyFailed()) {
            // if never failed, there is no artificial delay, so "delay" has always passed
            return true;
        }
        Duration delay = getDelayUntilRetryDue();
        boolean negative = delay.isNegative() || delay.isZero(); // for debug
        return negative;
    }

    /**
     * @return time until it should be retried
     */
    public Duration getDelayUntilRetryDue() {
        Instant now = module.clock().instant();
        Temporal nextAttemptAt = getRetryDueAt();
        return Duration.between(now, nextAttemptAt);
    }

    /**
     * @return The point in time at which the record should ideally be retried.
     */
    public Instant getRetryDueAt() {
        return retryDueAt.orElse(Instant.MIN); // use a constant for stable comparison
    }

    /**
     * @return the delay between retries e.g. retry after 1 second
     */
    public Duration getRetryDelayConfig() {
        var options = module.options();
        var retryDelayProvider = options.getRetryDelayProvider();
        if (retryDelayProvider != null) {
            return retryDelayProvider.apply(new RecordContext<>(this));
        } else {
            return options.getDefaultMessageRetryDelay();
        }
    }

    @Override
    public int compareTo(WorkContainer o) {
        return comparator.compare(this, o);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        WorkContainer<?, ?> that = (WorkContainer<?, ?>) o;
        String thisTopic = getTopicPartition().topic();
        String thatTopic = that.getTopicPartition().topic();
        if (!thisTopic.equals(thatTopic)) {
            return false;
        }
        int thisPartition = getTopicPartition().partition();
        int thatPartition = that.getTopicPartition().partition();
        if (thisPartition != thatPartition) {
            return false;
        }
        long thisOffset = getCr().offset();
        long thatOffset = that.getCr().offset();
        return thisOffset == thatOffset;
    }

    @Override
    public int hashCode() {
        return Objects.hash(getTopicPartition().topic(), getTopicPartition().partition(), cr.offset());
    }

    public boolean isNotInFlight() {
        return !isInFlight();
    }

    public boolean isInFlight() {
        return state.get().isInFlight();
    }

    /**
     * @return where this record currently is in its lifecycle
     */
    public ExecutionState getExecutionState() {
        return state.get();
    }

    /**
     * Claims this record for execution. <b>One compare-and-set: the check IS the act.</b>
     * <p>
     * The whole decision - not in flight, no success verdict, retry delay passed - is evaluated against a single
     * observed {@link ExecutionState}, and the claim then compares against <em>that exact state</em>. Anything that
     * moved the record in between makes the compare fail, so there is no window in which a decision can outlive the
     * facts it was made on. That window is what let an already-completed record be claimed and delivered a second
     * time, and it is why callers must NOT pre-filter with {@link #isAvailableToTakeAsWork()} and then call this:
     * the two-step form is the defect, restated.
     * <p>
     * A won claim starts a new delivery, and the new delivery carries no verdict - not because anything is cleared,
     * but because {@link ExecutionState#IN_FLIGHT} has none. That matters for a record that failed, was
     * redelivered and was then abandoned: under the old field pair it would still have been carrying
     * {@code succeeded == false} from the earlier attempt and would have taken the failure path, earning a retry
     * delay the abandonment never earned. The abandon marker is deliberately NOT reset - it is keyed by delivery,
     * so a stale one identifies itself.
     *
     * @return {@code true} if this caller won the claim; {@code false} if the record was not claimable, or another
     *         caller moved it first, in which case this caller must not process it. Only ever {@code false} under
     *         the direct-pull engine, where two workers can scan the same shard at the same time.
     */
    public boolean onQueueingForExecution() {
        ExecutionState observed = state.get();
        if (!isClaimableFrom(observed)) {
            log.trace("Not claimable from {}: {}", observed, this);
            return false;
        }
        if (!state.compareAndSet(observed, ExecutionState.IN_FLIGHT)) {
            log.trace("Lost the race to claim {}", this);
            return false;
        }
        log.trace("Queueing for execution: {}", this);
        deliveryCount++;
        timeTakenAsWorkMs = of(System.currentTimeMillis());
        chargeShardInFlight();
        return true;
    }

    /**
     * The claim decision, over one observed state. Read the state FIRST and the delay second: the state read is
     * the volatile one, so its acquire semantics make the retry deadline written by the previous holder visible.
     * Doing it the other way round would open a second, independent hole.
     */
    private boolean isClaimableFrom(ExecutionState observed) {
        return observed.isClaimable() && isDelayPassed();
    }

    private void chargeShardInFlight() {
        ProcessingShard<K, V> owner = shard;
        if (owner != null) {
            owner.onFlightBegan(this);
        }
    }

    private void releaseShardInFlightCharge() {
        ProcessingShard<K, V> owner = shard;
        if (owner != null) {
            owner.onFlightEnded(this);
        }
    }

    /**
     * Called by the shard as it takes ownership of this record, before the container is published into the shard's
     * entry map.
     */
    void onAdmittedToShard(ProcessingShard<K, V> shard) {
        this.shard = shard;
    }

    /**
     * Marks the given delivery of this work as returned without a verdict, so
     * {@link WorkManager#handleFutureResult} returns it to scheduling rather than throwing. Does not touch
     * {@link #numberOfFailedAttempts} or the retry delay - the record is redelivered as the same attempt it
     * already was.
     *
     * @param delivery the {@link #getDeliveryCount()} value observed when the record was handed out. Callers
     *                 must capture it at dispatch, not read it at return time: by then the record may already
     *                 have been redelivered, and passing the current value would make a stale return look live.
     */
    public void markAbandoned(long delivery) {
        log.trace("Abandoning delivery {} without verdict {}", delivery, this);
        this.abandonedAtDelivery = delivery;
    }

    /**
     * @return true when this record was abandoned on the delivery that is currently outstanding
     */
    public boolean isAbandonedForCurrentDelivery() {
        return abandonedAtDelivery == deliveryCount;
    }

    /**
     * @return true when a return carries no verdict and its abandon marker belongs to a delivery that has
     *         already ended - a late duplicate, which must be ignored rather than acted on
     */
    public boolean isReturnForSupersededDelivery() {
        // not isEmpty() - core compiles to Java 8 bytecode, where that Optional method does not exist
        return !getMaybeUserFunctionSucceeded().isPresent()
                && abandonedAtDelivery >= 0
                && abandonedAtDelivery != deliveryCount;
    }

    public TopicPartition getTopicPartition() {
        return toTopicPartition(getCr());
    }

    public void onUserFunctionSuccess() {
        this.succeededAt = of(module.clock().instant());
        recordVerdict(true);
    }

    public void onUserFunctionFailure(Throwable cause) {
        log.trace("Failing {}", this);

        updateFailureHistory(cause);

        recordVerdict(false);
    }

    /**
     * Attaches the user function's verdict to the current state, without ending the flight - the worker that ran
     * the function still holds the record until the controller takes it back through
     * {@link WorkManager#handleFutureResult}.
     * <p>
     * The failure history behind {@link #getDelayUntilRetryDue()} is written before this, so the state write is
     * what publishes it: a thread that observes {@link ExecutionState#FAILED} has, by the same happens-before
     * edge, observed the retry deadline that goes with it.
     */
    private void recordVerdict(boolean succeeded) {
        while (true) {
            ExecutionState current = state.get();
            ExecutionState next = current.withVerdict(succeeded);
            if (current == next || state.compareAndSet(current, next)) {
                return;
            }
        }
    }

    private void updateFailureHistory(Throwable cause) {
        numberOfFailedAttempts++;
        lastFailedAt = of(Instant.now(module.clock()));
        lastFailureReason = Optional.ofNullable(cause);
        Duration retryDelay = getRetryDelayConfig();
        retryDueAt = of(lastFailedAt.get().plus(retryDelay));
    }

    /**
     * The user function's verdict on the current delivery, or empty if it has not reported yet.
     * <p>
     * Derived from {@link #getExecutionState()} rather than stored beside it. Keeping it as its own field is what
     * made a claim able to contradict it.
     */
    public Optional<Boolean> getMaybeUserFunctionSucceeded() {
        return state.get().getVerdict();
    }

    public boolean isUserFunctionComplete() {
        return this.getMaybeUserFunctionSucceeded().isPresent();
    }

    public boolean isUserFunctionSucceeded() {
        Optional<Boolean> userFunctionSucceeded = this.getMaybeUserFunctionSucceeded();
        return userFunctionSucceeded.orElse(false);
    }

    @Override
    public String toString() {
        return "WorkContainer(tp:" + toTopicPartition(cr) + ":o:" + cr.offset() + ":k:" + cr.key() + ")";
    }

    public Duration getTimeInFlight() {
        if (!timeTakenAsWorkMs.isPresent()) {
            return Duration.ZERO;
        }
        long millis = System.currentTimeMillis() - timeTakenAsWorkMs.get();
        return Duration.ofMillis(millis);
    }

    public long offset() {
        return getCr().offset();
    }

    public boolean hasPreviouslyFailed() {
        return getNumberOfFailedAttempts() > 0;
    }

    /**
     * Whether a claim would be accepted <em>at the instant this is asked</em> - the record is not in flight, has no
     * success verdict, and its retry delay has passed.
     * <p>
     * <b>Answering true here does not reserve anything, and callers that intend to take the record must NOT use
     * this as a pre-filter.</b> Call {@link #onQueueingForExecution()} directly: it evaluates exactly this
     * predicate and claims from the state it evaluated, in one step. Testing here and claiming afterwards is
     * precisely the check-then-act that delivered records twice. This survives for the callers that only want to
     * know - metrics, diagnostics, and tests.
     * <p>
     * Checking that there's no back pressure for the partition it belongs to is covered by
     * {@link PartitionStateManager#isAllowedMoreRecords(WorkContainer)}.
     */
    public boolean isAvailableToTakeAsWork() {
        return isClaimableFrom(state.get());
    }

    /**
     * Only unlock our producing lock, when we've had the {@link WorkContainer} state safely returned to the controllers
     * inbound queue, so we know it'll be included properly before the next commit as a succeeded offset. As in order
     * for the controller to perform the transaction commit, it will be blocked from acquiring its commit lock until all
     * produce locks have been returned, inbound queue processed, and thus their representative offsets placed into the
     * commit payload (offset map).
     */
    public void onPostAddToMailBox(PollContextInternal<K, V> context, Optional<ProducerManager<K, V>> producerManager) {
        producerManager.ifPresent(pm -> {
            var producingLock = context.getProducingLock();
            producingLock.ifPresent(pm::finishProducing);
        });
    }
}
