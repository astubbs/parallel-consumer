package bz.stub.parallelconsumer.internal;

/*-
 * Copyright (C) 2020-2024 Confluent, Inc.
 * Modifications Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.OffsetCommitBudgetExceededException;
import bz.stub.parallelconsumer.ParallelConsumerOptions;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.WakeupException;
import pl.tlinkowski.unij.api.UniMaps;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;

import static bz.stub.parallelconsumer.internal.utils.StringUtils.msg;

/**
 * Delegate for {@link KafkaConsumer}
 */
@Slf4j
@RequiredArgsConstructor
public class ConsumerManager<K, V> {

    private final Consumer<K, V> consumer;

    private final Duration offsetCommitTimeout;

    private final Duration saslAuthenticationRetryTimeout;

    private final Duration saslAuthenticationRetryBackOff;

    private final AtomicBoolean pollingBroker = new AtomicBoolean(false);

    /**
     * Reports whether the broker poll system has begun its final close ({@code CLOSING}/{@code CLOSED}) -
     * the point at which in-progress poll and commit retry loops should abort promptly instead of
     * retrying. Wired by {@link BrokerPollSystem} at construction; defaults to "never" so a standalone
     * ConsumerManager behaves as running.
     * <p>
     * The poll system's {@code runState} is the single source of truth: this class deliberately holds NO
     * lifecycle state of its own. A duplicated private shutdown flag here previously desynced from the
     * lifecycle (it was set at <i>drain</i> time instead of close time), which stopped
     * {@code consumer.poll()} being invoked during {@code DRAINING} - busy-spinning the poll loop and
     * leaving a rebalance-unresponsive "zombie" group member holding its partitions.
     * See {@code docs/solutions/test-flakiness/pc-silent-stall-under-contention-2026-07-29.md}.
     */
    @Setter
    private BooleanSupplier closeInProgressSignal = () -> false;

    private final AtomicLong pendingRequests = new AtomicLong(0L);
    /**
     * Since Kakfa 2.7, multi-threaded access to consumer group metadata was blocked, so before and after polling, save
     * a copy of the metadata.
     *
     * @since 2.7.0
     */
    private ConsumerGroupMetadata metaCache;

    private volatile int pausedPartitionSizeCache = 0;

    private int erroneousWakups = 0;
    private int correctPollWakeups = 0;
    private int noWakeups = 0;
    private boolean commitRequested;

    ConsumerRecords<K, V> poll(Duration requestedLongPollTimeout) {
        Duration timeoutToUse = requestedLongPollTimeout;
        ConsumerRecords<K, V> records = null;
        try {
            if (commitRequested) {
                log.debug("Commit requested, so will not long poll as need to perform the commit");
                timeoutToUse = Duration.ofMillis(1);// disable long poll, as commit needs performing
                commitRequested = false;
            }
            pollingBroker.set(true);
            updateCache();
            log.debug("Poll starting with timeout: {}", timeoutToUse);
            Instant pollStarted = Instant.now();
            long tryCount = 0;
            boolean polledSuccessfully = false;
            try {
                pendingRequests.addAndGet(1L);
                while (!closeInProgressSignal.getAsBoolean()) {
                    tryCount++;
                    try {
                        records = consumer.poll(timeoutToUse);
                        polledSuccessfully = true;
                        break;
                    } catch (SaslAuthenticationException authenticationException) {
                        Instant now = Instant.now();
                        Duration elapsed = Duration.between(pollStarted, now);
                        boolean shouldRetry = elapsed.toMillis() < saslAuthenticationRetryTimeout.toMillis();
                        if (shouldRetry) {
                            log.warn("Poll error: SaslAuthenticationException. Retrying ({})", tryCount);
                            try {
                                retryBackOff(this.saslAuthenticationRetryBackOff.toMillis()); // no need to check return value here as next loop will check
                            } catch (InterruptedException ex) {
                                throw new RuntimeException("Poll interrupted", ex);
                            }
                        } else {
                            // no more retries allowed
                            log.error("Poll error: SaslAuthenticationException. {} tries attempted, since {}", tryCount, pollStarted, authenticationException);
                            throw authenticationException;
                        }
                    }
                }
            } finally {
                if (polledSuccessfully) {
                    log.debug("Poll completed normally (after timeout of {} on try {}) and returned {}...", timeoutToUse, tryCount, records.count());
                } else {
                    log.debug("Poll did not completed (after timeout of {} and tries {}), closeInProgress {}", timeoutToUse, tryCount, closeInProgressSignal.getAsBoolean());
                }
                pendingRequests.addAndGet(-1L);
            }
            updateCache();
        } catch (WakeupException w) {
            correctPollWakeups++;
            log.debug("Awoken from broker poll");
            log.trace("Wakeup caller is:", w);
            records = new ConsumerRecords<>(UniMaps.of());
        } finally {
            pollingBroker.set(false);
        }
        return records != null ? records : new ConsumerRecords<>(UniMaps.of());
    }

    protected void updateCache() {
        metaCache = consumer.groupMetadata();
        pausedPartitionSizeCache = consumer.paused().size();
    }

    /**
     * Wakes up the consumer, but only if it's polling.
     * <p>
     * Otherwise, we can interrupt other operations like {@link KafkaConsumer#commitSync()}.
     */
    public void wakeup() {
        // boolean reduces the chances of a mis-timed call to wakeup, but doesn't prevent all spurious wake up calls to other methods like #commit
        // if the call to wakeup happens /after/ the check for a wake up state inside #poll, then the next call will through the wake up exception (i.e. #commit)
        if (pollingBroker.get()) {
            log.debug("Waking up consumer");
            consumer.wakeup();
        }
    }

    /**
     * Commits, retrying the transient failures for as long as the configured budget allows.
     * <p>
     * {@code startedTime} is captured <b>once, for the whole call</b> - matching
     * {@link #poll(Duration)}'s {@code pollStarted} - because the budgets it feeds
     * ({@code offsetCommitTimeout}, {@code saslAuthenticationRetryTimeout}) are budgets for
     * committing these offsets, not for one attempt at it. Capturing it inside the retry loop reset
     * the budget on every attempt, so whenever an attempt failed faster than the budget the
     * comparison could never become false and this retried forever with no backoff - see
     * {@code ConsumerManagerCommitRetryBudgetTest}. That is reachable with ordinary settings: it needs
     * only {@code default.api.timeout.ms} below {@code offsetCommitTimeout}, and it strands the
     * broker-poll thread inside this method, which surfaces to the user as the unrelated-looking
     * "Timeout waiting for commit response" (astubbs#177, confluentinc#833).
     */
    public void commitSync(final Map<TopicPartition, OffsetAndMetadata> offsetsToSend) {
        // we don't want to be woken up during a commit, only polls
        boolean inProgress = true;
        noWakeups++;
        Instant startedTime = Instant.now();
        while (inProgress) {
            try {
                pendingRequests.addAndGet(1L);
                long tryCount = 0;
                boolean committed = false;
                // SASL's budget is a DIFFERENT option, so it gets a different clock, started at its own
                // first failure. Sharing startedTime would charge saslAuthenticationRetryTimeout for time
                // spent retrying unrelated commit timeouts, so an LDAP flap arriving late in a slow commit
                // would find its budget already spent by something that is not LDAP.
                Instant saslFirstFailure = null;
                //allow to try to commit at least once during close / shutdown regardless of the signal.
                while (tryCount == 0 || !closeInProgressSignal.getAsBoolean()) {
                    tryCount++;
                    try {
                        consumer.commitSync(offsetsToSend);
                        committed = true;
                        // break when offset commit is okay. Do not throw exception to main threads
                        break;
                    } catch(TimeoutException timeoutException) {
                        // offset commit times out after 1 minute.
                        // We should honor the user configured timeout offsetCommitTimeout here.
                        Instant now = Instant.now();
                        Duration elapsed = Duration.between(startedTime, now);
                        boolean shouldRetry = elapsed.toMillis() <= offsetCommitTimeout.toMillis();
                        if(shouldRetry) {
                            log.warn("Encountered timeout while committing offset. Retrying ({})", tryCount);
                            // The timeout is already after 1 minute. There is no need to sleep in between retries
                        } else {
                            log.error("Offset commit took too long due to TimeoutException (tried {} times)", tryCount);
                            throw new OffsetCommitBudgetExceededException(msg(
                                    "Offset commit gave up after {} attempt(s) and {}, having spent its whole " +
                                            "offsetCommitTimeout of {}.{} To allow longer, raise offsetCommitTimeout. " +
                                            "What happens next is the configured commitFailureHandler's decision " +
                                            "(astubbs/parallel-consumer#317) - the default policy shuts PC down " +
                                            "(fail fast), and CommitFailurePolicies has canned alternatives.",
                                    tryCount, elapsed, offsetCommitTimeout, retriesWereReachable(tryCount)),
                                    timeoutException, tryCount, elapsed, offsetsToSend);
                        }
                    } catch(SaslAuthenticationException authenticationException) {
                        // We should honor the user configured SaslAuthenticationException timeout here.
                        // to allow the program to sustain temporary LDAP failures
                        if (saslFirstFailure == null) {
                            saslFirstFailure = Instant.now();
                        }
                        Duration elapsed = Duration.between(saslFirstFailure, Instant.now());
                        // '<' not '<=', matching #poll's identical branch. The two had drifted, and with the
                        // shipped default of PT0S the difference is the whole behaviour: '<=' makes elapsed==0
                        // satisfy a zero budget, so "do not retry SASL" retried anyway.
                        boolean shouldRetry = elapsed.toMillis() < saslAuthenticationRetryTimeout.toMillis();
                        if(shouldRetry) {
                            log.warn("Encountered SaslAuthenticationException while committing offset. Retrying ({})", tryCount);
                            // Since authentication exception may happen immediately, it is good to sleep a few seconds before trying again
                            try {
                                retryBackOff(saslAuthenticationRetryBackOff.toMillis()); // no need to check return value
                            } catch(InterruptedException ex) {
                                // don't swallow the interrupted exception
                                log.warn("Offset Commit was interrupted", ex);
                                throw new RuntimeException("Offset Commit was interrupted");
                            }
                        } else {
                            log.error("Offset commit failed due to SaslAuthenticationException (tried {} times)", tryCount);
                            throw new OffsetCommitBudgetExceededException(msg(
                                    "Offset commit gave up after {} attempt(s) of SASL authentication failure and {}, " +
                                            "having spent its whole saslAuthenticationRetryTimeout of {} (retries are " +
                                            "spaced by saslAuthenticationExceptionRetryBackoff, currently {}). To ride " +
                                            "out longer authentication outages, raise saslAuthenticationRetryTimeout. " +
                                            "What happens next is the configured commitFailureHandler's decision " +
                                            "(astubbs/parallel-consumer#317) - the default policy shuts PC down " +
                                            "(fail fast), and CommitFailurePolicies has canned alternatives.",
                                    tryCount, elapsed, saslAuthenticationRetryTimeout, saslAuthenticationRetryBackOff),
                                    authenticationException, tryCount, elapsed, offsetsToSend);
                        }
                    }
                }
                if (!committed) {
                    // The loop above ends without committing when a retry was due but close had begun -
                    // the condition's job is to stop RETRYING during shutdown, not to claim the commit
                    // happened. Returning normally here would do the latter: the caller,
                    // AbstractOffsetCommitter#retrieveOffsetsAndCommit, calls onOffsetCommitSuccess as
                    // soon as this returns, marking offsets the broker never received as committed and
                    // leaving nothing to retry them. That is exactly the "swallow" option
                    // ConsumerOffsetCommitter#commitDeferringOnRebalance rejects, one layer down and on
                    // the close path, where the final commit matters most. Fail instead: the close
                    // sequence logs it and shuts down, which is true rather than quietly wrong.
                    // TODO(refactor): a user-facing failure wants a PC-named type - see
                    // docs/inflight/core-exception-hierarchy-cleanup.md
                    throw new InternalRuntimeException(
                            "Offset commit abandoned after {} attempt(s) because close began - these offsets were NOT " +
                                    "committed, so they must not be recorded as successful", null, tryCount);
                }
                inProgress = false;
            } catch (WakeupException w) {
                log.debug("Got woken up, retry. errors: " + erroneousWakups + " none: " + noWakeups + " correct:" + correctPollWakeups, w);
                erroneousWakups++;
            } finally {
                pendingRequests.addAndGet(-1L);
            }
        }
    }

    /**
     * The check Kafka would make at construction, made here instead because it cannot be made there.
     * <p>
     * Kafka validates the relationship between its own two-level timeouts up front - a producer refuses
     * a {@code delivery.timeout.ms} below {@code request.timeout.ms + linger.ms} - precisely so nobody
     * ships a total budget smaller than one attempt. PC's {@code offsetCommitTimeout} is the same kind
     * of total, layered over the consumer's own per-call {@code default.api.timeout.ms}, but PC cannot
     * read that: {@link org.apache.kafka.clients.consumer.Consumer} exposes no configuration, and the
     * one place PC does reach for consumer config it does so by reflection, which its own javadoc calls
     * brittle.
     * <p>
     * So the relationship is reported when it actually bites rather than guessed at start-up. Giving up
     * after a single attempt means that attempt outlived the whole budget, so no retry was ever
     * reachable - which is silent today: a user who set {@code offsetCommitTimeout} expecting retries
     * gets exactly one try and no indication why.
     */
    private String retriesWereReachable(long tryCount) {
        if (tryCount > 1) {
            return "";
        }
        return " Only ONE attempt was made, so no retry was reachable: a single commit attempt outlived the" +
                " whole budget. That is what happens when offsetCommitTimeout is below the consumer's own" +
                " default.api.timeout.ms (60s by default), which bounds each individual attempt - raise" +
                " offsetCommitTimeout above it for retries to be possible at all.";
    }

    // Return true if backoff is finished successfully
    // Return false if it ended before the timeout
    // Throws InterruptedException if interrupted
    private boolean retryBackOff(long backOffTimeMs) throws InterruptedException {
        int interval = 100; // sleep in 100ms interval
        long started = System.currentTimeMillis();
        long deadLine = started + backOffTimeMs;
        while(System.currentTimeMillis() < deadLine) {
            Thread.sleep(interval);
            if(closeInProgressSignal.getAsBoolean()) {
                return false;
            }
        }
        return true;
    }
    public void commitAsync(Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback) {
        // we dont' want to be woken up during a commit, only polls
        boolean inProgress = true;
        noWakeups++;
        while (inProgress) {
            try {
                consumer.commitAsync(offsets, callback);
                inProgress = false;
            } catch (WakeupException w) {
                log.debug("Got woken up, retry. errors: " + erroneousWakups + " none: " + noWakeups + " correct:" + correctPollWakeups, w);
                erroneousWakups++;
            }
        }
    }

    public ConsumerGroupMetadata groupMetadata() {
        return metaCache;
    }

    public void close(final Duration defaultTimeout) {
        long deadline = System.currentTimeMillis() + defaultTimeout.toMillis();
        log.debug("Consumer Manager Closing...");
        // no stop flag to raise: by the time the poll system calls this, its runState is already
        // CLOSING/CLOSED, so closeInProgressSignal reports true to any in-flight retry loops
        log.debug("ConsumerManager close waiting for max of {} for pending requests to complete", defaultTimeout);
        while(pendingRequests.get() > 0L && System.currentTimeMillis() < deadline) {
            try {
                Thread.sleep(100);
            } catch(InterruptedException ex) {
                throw new RuntimeException("Wait interrupted");
            }
        }
        log.debug("ConsumerManager close wait completed.");
        consumer.close(defaultTimeout);
        log.debug("ConsumerManager closed");
    }

    public Set<TopicPartition> assignment() {
        return consumer.assignment();
    }

    public void pause(final Set<TopicPartition> assignment) {
        consumer.pause(assignment);
    }

    public Set<TopicPartition> paused() {
        return consumer.paused();
    }

    public int getPausedPartitionSize() {
        return pausedPartitionSizeCache;
    }

    public void resume(final Set<TopicPartition> pausedTopics) {
        consumer.resume(pausedTopics);
    }

    public void onCommitRequested() {
        this.commitRequested = true;
    }
}
