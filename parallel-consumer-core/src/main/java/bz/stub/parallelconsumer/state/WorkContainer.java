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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

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
     * One occupancy of an {@link ExecutionState} - the state, plus the attempt it belongs to.
     * <p>
     * <b>Why the state alone is not enough to compare-and-set against.</b> {@link ExecutionState} values are enum
     * singletons, so a record that leaves {@link ExecutionState#FAILED} and comes all the way back to it is
     * {@code ==} to where it started: an ABA. A selector that read {@code FAILED}, found the retry delay passed,
     * and stalled would then compare-and-set successfully against a {@code FAILED} that is <em>a different
     * failure</em> - one whose retry deadline has been pushed out - and redeliver immediately, ignoring the new
     * delay. Reported by Codex on astubbs#335.
     * <p>
     * Every transition mints a new instance, so the {@link AtomicReference} compare - which is reference identity -
     * cannot match across an intervening cycle. That closes the whole ABA class in one place rather than
     * re-validating each derived term (the retry deadline today, whatever is added tomorrow) at every claim site.
     * The deadline is therefore <b>not</b> re-read after the claim: it does not need to be, because the claim
     * refuses any state the deadline could have been rewritten under.
     * <p>
     * {@link #sequence} carries no correctness weight - identity already does the work, so there is nothing to
     * wrap around - it is there so a log line or a test can say <em>which</em> attempt, not merely which state.
     */
    static final class Execution {

        private final ExecutionState state;

        private final long sequence;

        private Execution(ExecutionState state, long sequence) {
            this.state = state;
            this.sequence = sequence;
        }

        static Execution initial() {
            return new Execution(ExecutionState.AVAILABLE, 0);
        }

        ExecutionState state() {
            return state;
        }

        long sequence() {
            return sequence;
        }

        /**
         * @return a fresh occupancy of {@code to}, one attempt on from this one. Always a new instance, even when
         *         {@code to} equals the current state, because a same-state transition still publishes writes made
         *         under the claim (the retry deadline) that a stale observer must not claim across.
         */
        Execution transitionTo(ExecutionState to) {
            return new Execution(to, sequence + 1);
        }

        @Override
        public String toString() {
            return state + "#" + sequence;
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

    /**
     * How many times the user function has failed on this record.
     * <p>
     * Atomic for publication, not for mutual exclusion: only the holder of the claim writes it, but
     * {@link #getNumberOfFailedAttempts()} is public (through {@code RecordContext}) and
     * {@link #isDelayPassed()} reads it from any selector thread, so a plain field made the increment a
     * read-modify-write on shared state - which is what SpotBugs' {@code AT_NONATOMIC_OPERATIONS_ON_SHARED_VARIABLE}
     * names on astubbs#335.
     * <p>
     * <b>This does not make the (attempts, deadline) pair atomic, and it is not what makes reading them safe.</b>
     * That is {@code isClaimableFrom(Execution)} reading the state first: the state write is the release that
     * publishes both, so an observer that saw the state saw the pair the holder left behind. See that method.
     */
    private final AtomicInteger numberOfFailedAttempts = new AtomicInteger(0);

    @Getter
    private Optional<Instant> lastFailedAt = Optional.empty();

    @Getter
    private Optional<Instant> succeededAt = Optional.empty();

    @Getter
    private Optional<Throwable> lastFailureReason;

    /**
     * Where this record is in its execution lifecycle: whether it is out at a worker, and what verdict it carries.
     * <p>
     * <b>One field, because two were the bug.</b> This used to be a plain {@code boolean inFlight} plus a separate
     * {@code Optional<Boolean> maybeUserFunctionSucceeded}: selection read both, and then claimed in a separate
     * step that re-validated neither. Under a single selector the gap is unreachable; give the engine two
     * concurrent selectors and a claim whose availability decision predated another worker's completion could
     * still win on an already-succeeded record - and the claim then cleared the verdict, erasing the term that
     * should have refused it. The record would be delivered, and its offset committed, twice. Diagnosis and the
     * state machine that closes it: the commit and PR that introduced {@link ExecutionState}, and
     * {@code docs/inflight/core-a-lost-claim-means-two-different-things.md}.
     * <p>
     * Atomic because the direct-pull engine (in development) lets every worker select work straight from the
     * shards, so the "is it free? then take it" pair has to be one indivisible step. Under the shipped engine only
     * the control loop selects work and a plain field would do; making it atomic for both keeps one code path, and
     * the cost is one uncontended compare-and-set per delivery.
     *
     * @see ExecutionState
     * @see #onQueueingForExecution()
     */
    private final AtomicReference<Execution> state = new AtomicReference<>(Execution.initial());

    /**
     * How many times this record has been handed to a worker. Incremented only by a WON claim, so a refused
     * claim leaves it untouched - which is one of the properties {@code WorkClaimStateMachineTest} pins.
     * <p>
     * Written only by the claim winner, read anywhere - and the increment happens <em>after</em> the claim's
     * compare-and-set, so that write publishes nothing. An earlier revision of this comment claimed it did; it
     * did not, which is the {@code AT_NONATOMIC_OPERATIONS_ON_SHARED_VARIABLE} SpotBugs reports here. The counter
     * carries its own publication instead.
     */
    private final AtomicLong deliveryCount = new AtomicLong(0);

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
        Execution before = state.getAndUpdate(WorkContainer::endFlightOf);
        if (before.state().isInFlight()) {
            log.trace("Ending flight {}", this);
        } else {
            log.trace("Flight already ended, nothing to release {}", this);
        }
    }

    /**
     * The transition {@link #endFlight()} applies, as a function of the state it observes - which is what makes
     * that call <b>terminate</b>.
     * <p>
     * {@link AtomicReference#getAndUpdate} retries only when another thread wrote in between, and it re-applies
     * <em>this</em> function to whatever it now finds. So the terminal condition is a property of the function
     * rather than of a counter: it is <b>total</b> (defined on all six states) and <b>a fixed point on its own
     * output</b> - no state it produces is in flight, so the next round would return the argument unchanged.
     * A caller therefore cannot be made to go round more than once by any transition another thread can perform;
     * a stall would need an unbounded supply of <em>new</em> flights, and only a won claim starts one, which
     * cannot happen while the record is in flight.
     * <p>
     * That is the whole argument, and it is why this is not a {@code while (true)} loop with the reasoning left
     * to the reader (astubbs#335 review).
     */
    private static Execution endFlightOf(Execution observed) {
        if (!observed.state().isInFlight()) {
            // not a transition: leave the identity alone, so a concurrent claim is not invalidated for nothing
            return observed;
        }
        return observed.transitionTo(observed.state().afterFlightEnds());
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
        return state.get().state().isInFlight();
    }

    /**
     * @return where this record currently is in its lifecycle
     */
    public ExecutionState getExecutionState() {
        return state.get().state();
    }

    /**
     * @return the current occupancy, state and attempt together - what a selector's claim decision is made over.
     *         Package-private: {@link Execution} is an implementation detail of the atomic, and the only callers
     *         that need it are the claim itself and the test that drives the interleaving - see
     *         {@link #takeClaimOn(Execution)}.
     */
    Execution observeExecution() {
        return state.get();
    }

    /**
     * Claims this record for execution. <b>One compare-and-set: the check IS the act.</b>
     * <p>
     * The whole decision - not in flight, no success verdict, retry delay passed - is evaluated against a single
     * observed {@link Execution}, and the claim then compares against <em>that exact observation</em> - not
     * against its state, which is an enum singleton a record can return to. Anything that moved the record in
     * between makes the compare fail, so there is no window in which a decision can outlive the facts it was
     * made on, and no ABA in which it appears not to have moved at all. That window is what could let an
     * already-completed record be claimed and delivered a second time, and it is why callers must NOT pre-filter
     * with {@link #isAvailableToTakeAsWork()} and then call this: the two-step form is the defect, restated.
     * <p>
     * A won claim starts a new delivery, and the new delivery carries no verdict - not because anything is
     * cleared, but because {@link ExecutionState#IN_FLIGHT} has none.
     *
     * @return {@code true} if this caller won the claim; {@code false} if the record was not claimable, or another
     *         caller moved it first, in which case this caller must not process it. Never expected under the
     *         shipped engine, where the control loop is the only selector; an engine with concurrent selectors
     *         (the direct-pull engine, in development) loses claims routinely and relies on the refusal.
     */
    public boolean onQueueingForExecution() {
        return claimFrom(state.get());
    }

    /**
     * The whole claim - decide over one observation, then act on that same observation.
     */
    private boolean claimFrom(Execution observed) {
        if (!isClaimableFrom(observed)) {
            log.trace("Not claimable from {}: {}", observed, this);
            return false;
        }
        return takeClaimOn(observed);
    }

    /**
     * The act half of the claim, on its own - <b>the seam that makes the interleaving testable without
     * threads</b> (see {@code WorkClaimStateMachineTest}).
     * <p>
     * A caller reaches here having already decided, over {@code observed}, that the record was claimable. At
     * runtime the gap between that decision and this call is a handful of instructions; a descheduled selector
     * can widen it to anything, and what happens inside it is the ABA this method exists to refuse - the record
     * cycling away and back to a state that is {@code ==} to what the caller saw, with the retry deadline the
     * caller checked rewritten under it (astubbs#335, reported by Codex).
     * <p>
     * <b>Splitting the act out is safe here in a way the defect's own two-step form was not</b>, and the
     * difference is the whole point of {@link Execution}: the caller's observation is a <em>value</em>, and this
     * compares against that exact value, so a stale caller is refused rather than obeyed. The old form threw its
     * observation away and re-read nothing, so a stale decision won. The state check is kept as well, so this
     * cannot be misused to claim a current-but-unclaimable record.
     *
     * @param observed the exact occupancy the claim decision was made over
     */
    boolean takeClaimOn(Execution observed) {
        if (!observed.state().isClaimable()) {
            log.trace("Not claimable from {}: {}", observed, this);
            return false;
        }
        // Compares the exact occupancy, not merely its state, so a record that cycled away and back to the same
        // state since the decision refuses this claim rather than honouring a decision made against a superseded
        // attempt - and with it, against a retry deadline that has since been renewed. See Execution.
        if (!state.compareAndSet(observed, observed.transitionTo(ExecutionState.IN_FLIGHT))) {
            log.trace("Lost the race to claim {}", this);
            return false;
        }
        log.trace("Queueing for execution: {}", this);
        deliveryCount.incrementAndGet();
        timeTakenAsWorkMs = of(System.currentTimeMillis());
        return true;
    }

    /**
     * The claim decision, over one observed occupancy. Read the state FIRST and the delay second: the state read
     * is the volatile one, so its acquire semantics make the failure count and retry deadline written by the
     * previous holder visible together, as the pair that holder left behind. Doing it the other way round would
     * open a second, independent hole - a half-written pair reads as "never failed, so no delay", which is an
     * immediate retry.
     * <p>
     * The deadline is a derived term, not part of the atomic; what stops a claim from acting on a <em>stale</em>
     * one is that {@link #onQueueingForExecution()} compares against the exact {@link Execution} this was
     * evaluated over, and anything that could rewrite the deadline transitions the record. See {@link Execution}.
     */
    private boolean isClaimableFrom(Execution observed) {
        return observed.state().isClaimable() && isDelayPassed();
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
        // Terminates for the same reason endFlightOf() does, and the argument is stated there: the update
        // function is total, and applying it to its own output is idempotent in the state it names, so no
        // sequence of transitions another thread can perform keeps this caller going round.
        state.updateAndGet(observed -> observed.transitionTo(observed.state().withVerdict(succeeded)));
    }

    private void updateFailureHistory(Throwable cause) {
        numberOfFailedAttempts.incrementAndGet();
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
        return state.get().state().getVerdict();
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
     * @return how many times the user function has failed on this record. Hand-written rather than a Lombok
     *         {@code @Getter} because the field is an {@link AtomicInteger} and this is public API, reached from
     *         a user's retry-delay function through {@code RecordContext}: the accessor's {@code int} is the
     *         contract, the field's type is not.
     */
    public int getNumberOfFailedAttempts() {
        return numberOfFailedAttempts.get();
    }

    /**
     * @return how many times this record has been handed to a worker. Hand-written for the same reason as
     *         {@link #getNumberOfFailedAttempts()}.
     */
    public long getDeliveryCount() {
        return deliveryCount.get();
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
