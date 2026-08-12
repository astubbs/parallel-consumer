package io.confluent.parallelconsumer.state;

/*-
 * Copyright (C) 2020-2024 Confluent, Inc.
 * Modifications Copyright (C) 2026 Antony Stubbs and contributors
 */

import io.confluent.parallelconsumer.PollContextInternal;
import io.confluent.parallelconsumer.RecordContext;
import io.confluent.parallelconsumer.internal.PCModule;
import io.confluent.parallelconsumer.internal.ProducerManager;
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

import static io.confluent.csid.utils.KafkaUtils.toTopicPartition;
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

    private boolean inFlight = false;

    @Getter
    private Optional<Boolean> maybeUserFunctionSucceeded = Optional.empty();

    /**
     * Set when work is returned to scheduling without any verdict at all - the record was neither processed
     * successfully nor seen to fail. An external engine whose worker disconnects mid-record has no verdict to
     * report, but the record must not be treated as a failure either, because a dropped connection is not a
     * processing attempt and must not consume a retry.
     * <p>
     * Deliberately distinct from {@link #maybeUserFunctionSucceeded}: an empty verdict with this flag unset is
     * still the bug {@code WorkManager.handleFutureResult} throws on.
     *
     * @see #markAbandoned()
     */
    @Getter
    private boolean abandoned = false;

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

    public void endFlight() {
        log.trace("Ending flight {}", this);
        inFlight = false;
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
        return inFlight;
    }

    public void onQueueingForExecution() {
        log.trace("Queueing for execution: {}", this);
        inFlight = true;
        // A redelivery is a fresh attempt and carries no verdict yet. Both of these must be cleared, or state
        // from the previous delivery decides the outcome of this one: a record that failed, was redelivered and
        // then abandoned would still be carrying `maybeUserFunctionSucceeded == false` and would be routed down
        // the failure path, earning a retry delay the abandonment never earned.
        abandoned = false;
        maybeUserFunctionSucceeded = Optional.empty();
        timeTakenAsWorkMs = of(System.currentTimeMillis());
    }

    /**
     * Marks this work as returned without a verdict, so {@code WorkManager.handleFutureResult} returns it to
     * scheduling rather than throwing. Does not touch {@link #numberOfFailedAttempts} or the retry delay - the
     * record is redelivered as the same attempt it already was.
     */
    public void markAbandoned() {
        log.trace("Abandoning without verdict {}", this);
        this.abandoned = true;
    }

    public TopicPartition getTopicPartition() {
        return toTopicPartition(getCr());
    }

    public void onUserFunctionSuccess() {
        this.succeededAt = of(module.clock().instant());
        this.maybeUserFunctionSucceeded = of(true);
    }

    public void onUserFunctionFailure(Throwable cause) {
        log.trace("Failing {}", this);

        updateFailureHistory(cause);

        this.maybeUserFunctionSucceeded = of(false);
    }

    private void updateFailureHistory(Throwable cause) {
        numberOfFailedAttempts++;
        lastFailedAt = of(Instant.now(module.clock()));
        lastFailureReason = Optional.ofNullable(cause);
        Duration retryDelay = getRetryDelayConfig();
        retryDueAt = of(lastFailedAt.get().plus(retryDelay));
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
     * Checks the work is not already in flight, it's retry delay has passed and that it's not already been succeeded.
     * <p>
     * Checking that there's no back pressure for the partition it belongs to is covered by
     * {@link PartitionStateManager#isAllowedMoreRecords(WorkContainer)}.
     */
    public boolean isAvailableToTakeAsWork() {
        return isNotInFlight() && !isUserFunctionSucceeded() && isDelayPassed();
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
