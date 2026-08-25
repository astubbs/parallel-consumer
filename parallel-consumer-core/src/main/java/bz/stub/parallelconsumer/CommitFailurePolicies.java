package bz.stub.parallelconsumer;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.CommitFailureHandler.CommitFailureDecision;
import org.apache.kafka.common.annotation.InterfaceStability;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Objects;

import static bz.stub.parallelconsumer.CommitFailureHandler.CommitFailureDecision.CONTINUE;
import static bz.stub.parallelconsumer.CommitFailureHandler.CommitFailureDecision.SHUT_DOWN;
import static bz.stub.parallelconsumer.internal.utils.StringUtils.msg;

/**
 * Canned {@link CommitFailureHandler} implementations.
 * <ul>
 * <li>{@link #shutDown()} - always fail fast; the default, and the historical behaviour.</li>
 * <li>{@link #continueBounded()} - continue until a bound trips, then shut down. The recommended way to opt in to
 * continuing.</li>
 * <li>{@link #continueUnbounded()} - never shut down for a commit failure; explicit opt-in only.</li>
 * </ul>
 * All are pure logic over the {@link CommitFailureContext} they receive: the bounded policy's graduation clocks are
 * computed from the context's fields, never a wall clock, so it is unit-testable without threads.
 *
 * @author Antony Stubbs
 * @see CommitFailureHandler
 * @see ParallelConsumerOptions.ParallelConsumerOptionsBuilder#commitFailureHandler
 */
@InterfaceStability.Evolving
public final class CommitFailurePolicies {

    /**
     * Default for {@link #continueBounded()}: how many consecutive exhausted budgets graduate to shut-down.
     */
    public static final int DEFAULT_MAX_CONSECUTIVE_EXHAUSTED_BUDGETS = 5;

    /**
     * Default for {@link #continueBounded()}: how long without any successful commit graduates to shut-down.
     */
    public static final Duration DEFAULT_MAX_TIME_SINCE_LAST_SUCCESSFUL_COMMIT = Duration.ofMinutes(5);

    /**
     * Default for {@link #continueBounded()}: how many exhaustions within {@link #DEFAULT_EXHAUSTION_WINDOW}
     * graduate to shut-down, regardless of intervening successes.
     */
    public static final int DEFAULT_MAX_EXHAUSTIONS_IN_WINDOW = 20;

    /**
     * Default for {@link #continueBounded()}: the rolling window {@link #DEFAULT_MAX_EXHAUSTIONS_IN_WINDOW} counts
     * within.
     */
    public static final Duration DEFAULT_EXHAUSTION_WINDOW = Duration.ofMinutes(30);

    /**
     * Shared instance, deliberately: {@link ParallelConsumerOptions#validate()} detects "the user configured a
     * non-default handler" by identity against it.
     */
    private static final CommitFailureHandler SHUT_DOWN_INSTANCE = new CommitFailureHandler() {
        @Override
        public CommitFailureDecision onCommitFailure(CommitFailureContext context) {
            return SHUT_DOWN;
        }

        @Override
        public String toString() {
            return "CommitFailurePolicies.shutDown()";
        }
    };

    private static final CommitFailureHandler CONTINUE_UNBOUNDED_INSTANCE = new CommitFailureHandler() {
        @Override
        public CommitFailureDecision onCommitFailure(CommitFailureContext context) {
            return CONTINUE;
        }

        @Override
        public String toString() {
            return "CommitFailurePolicies.continueUnbounded()";
        }
    };

    private CommitFailurePolicies() {
    }

    /**
     * Always {@link CommitFailureDecision#SHUT_DOWN} - fail fast on the first terminally failed commit. The default,
     * and the historical behaviour.
     */
    public static CommitFailureHandler shutDown() {
        return SHUT_DOWN_INSTANCE;
    }

    /**
     * Always {@link CommitFailureDecision#CONTINUE} - never shut down for a commit failure. Explicit opt-in only:
     * with no bound, a permanently broken commit path keeps processing (and re-delivering on eventual rebalance)
     * forever. Prefer {@link #continueBounded()}.
     */
    public static CommitFailureHandler continueUnbounded() {
        return CONTINUE_UNBOUNDED_INSTANCE;
    }

    /**
     * {@link #continueBounded(int, Duration, int, Duration)} with the default bounds.
     */
    public static CommitFailureHandler continueBounded() {
        return continueBounded(DEFAULT_MAX_CONSECUTIVE_EXHAUSTED_BUDGETS,
                DEFAULT_MAX_TIME_SINCE_LAST_SUCCESSFUL_COMMIT,
                DEFAULT_MAX_EXHAUSTIONS_IN_WINDOW,
                DEFAULT_EXHAUSTION_WINDOW);
    }

    /**
     * {@link CommitFailureDecision#CONTINUE} until a bound trips, then {@link CommitFailureDecision#SHUT_DOWN} - and
     * once graduated, it stays graduated. Three independent bounds:
     * <ul>
     * <li><b>consecutive</b>: {@code maxConsecutiveExhaustedBudgets} budgets exhausted in a row with no intervening
     * successful commit;</li>
     * <li><b>time</b>: {@code maxTimeSinceLastSuccessfulCommit} since a commit last succeeded (measured from
     * assignment start when none ever has - see
     * {@link CommitFailureContext#getTimeSinceLastSuccessfulCommit()});</li>
     * <li><b>rolling window</b>: {@code maxExhaustionsInWindow} exhaustions within {@code exhaustionWindow},
     * counted <em>across</em> intervening successes - so a flapping broker whose occasional single success keeps
     * resetting the first two clocks still graduates.</li>
     * </ul>
     * The window's clock is reconstructed from the contexts received (never a wall clock); the reconstruction drops
     * the unknowable gap between an exhaustion and the success that follows it, which slightly compresses the
     * timeline - erring towards graduating, never away from it. History resets when
     * {@link CommitFailureContext#getAssignmentEpoch()} changes.
     *
     * @param maxConsecutiveExhaustedBudgets shut down at this many consecutive exhausted budgets; at least 1
     * @param maxTimeSinceLastSuccessfulCommit shut down when this long has passed without a successful commit;
     *         positive
     * @param maxExhaustionsInWindow shut down at this many exhaustions inside {@code exhaustionWindow}; at least 1
     * @param exhaustionWindow the rolling window; positive
     */
    public static CommitFailureHandler continueBounded(int maxConsecutiveExhaustedBudgets,
                                                       Duration maxTimeSinceLastSuccessfulCommit,
                                                       int maxExhaustionsInWindow,
                                                       Duration exhaustionWindow) {
        requireAtLeastOne(maxConsecutiveExhaustedBudgets, "maxConsecutiveExhaustedBudgets");
        requirePositive(maxTimeSinceLastSuccessfulCommit, "maxTimeSinceLastSuccessfulCommit");
        requireAtLeastOne(maxExhaustionsInWindow, "maxExhaustionsInWindow");
        requirePositive(exhaustionWindow, "exhaustionWindow");

        return new BoundedContinue(maxConsecutiveExhaustedBudgets, maxTimeSinceLastSuccessfulCommit,
                maxExhaustionsInWindow, exhaustionWindow);
    }

    private static void requireAtLeastOne(int bound, String name) {
        if (bound < 1) {
            throw new IllegalArgumentException(msg("{} must be at least 1, was: {}", name, bound));
        }
    }

    private static void requirePositive(Duration bound, String name) {
        Objects.requireNonNull(bound, name);
        if (bound.isZero() || bound.isNegative()) {
            throw new IllegalArgumentException(msg("{} must be positive, was: {}", name, bound));
        }
    }

    /**
     * Stateful (one exhaustion event per invocation), relying on {@link CommitFailureHandler}'s single-threaded
     * invocation guarantee.
     */
    private static final class BoundedContinue implements CommitFailureHandler {

        private final int maxConsecutiveExhaustedBudgets;
        private final Duration maxTimeSinceLastSuccessfulCommit;
        private final int maxExhaustionsInWindow;
        private final Duration exhaustionWindow;

        /**
         * Global times of past exhaustions on the reconstructed clock, oldest first.
         */
        private final Deque<Duration> exhaustionTimes = new ArrayDeque<>();

        /**
         * Where the current between-successes segment starts on the reconstructed clock; an event's global time is
         * this plus the context's time-since-last-success.
         */
        private Duration clockBase = Duration.ZERO;

        private Duration lastEventTime = Duration.ZERO;

        private int lastConsecutive = 0;

        private Long lastAssignmentEpoch = null;

        private boolean graduated = false;

        private BoundedContinue(int maxConsecutiveExhaustedBudgets,
                                Duration maxTimeSinceLastSuccessfulCommit,
                                int maxExhaustionsInWindow,
                                Duration exhaustionWindow) {
            this.maxConsecutiveExhaustedBudgets = maxConsecutiveExhaustedBudgets;
            this.maxTimeSinceLastSuccessfulCommit = maxTimeSinceLastSuccessfulCommit;
            this.maxExhaustionsInWindow = maxExhaustionsInWindow;
            this.exhaustionWindow = exhaustionWindow;
        }

        @Override
        public CommitFailureDecision onCommitFailure(CommitFailureContext context) {
            if (graduated) {
                return SHUT_DOWN;
            }

            if (lastAssignmentEpoch == null || lastAssignmentEpoch != context.getAssignmentEpoch()) {
                // new assignment - the context's clocks restart from assignment start, and history from the
                // previous assignment no longer applies
                exhaustionTimes.clear();
                clockBase = Duration.ZERO;
                lastEventTime = Duration.ZERO;
                lastConsecutive = 0;
                lastAssignmentEpoch = context.getAssignmentEpoch();
            }

            if (context.getConsecutiveExhaustedBudgets() >= maxConsecutiveExhaustedBudgets
                    || context.getTimeSinceLastSuccessfulCommit().compareTo(maxTimeSinceLastSuccessfulCommit) >= 0) {
                graduated = true;
                return SHUT_DOWN;
            }

            // rolling window, on a clock reconstructed purely from contexts
            boolean successIntervened = context.getConsecutiveExhaustedBudgets() <= lastConsecutive;
            if (successIntervened) {
                // start a new segment at the last known time - the exhaustion-to-success gap is unknowable and
                // dropped, compressing the timeline towards graduation (conservative)
                clockBase = lastEventTime;
            }
            var eventTime = clockBase.plus(context.getTimeSinceLastSuccessfulCommit());
            if (eventTime.compareTo(lastEventTime) < 0) {
                eventTime = lastEventTime; // keep the reconstructed clock monotonic
            }
            exhaustionTimes.addLast(eventTime);
            lastEventTime = eventTime;
            lastConsecutive = context.getConsecutiveExhaustedBudgets();

            var windowStart = eventTime.minus(exhaustionWindow);
            while (!exhaustionTimes.isEmpty() && exhaustionTimes.peekFirst().compareTo(windowStart) < 0) {
                exhaustionTimes.pollFirst();
            }
            if (exhaustionTimes.size() >= maxExhaustionsInWindow) {
                graduated = true;
                return SHUT_DOWN;
            }

            return CONTINUE;
        }

        @Override
        public String toString() {
            return msg("CommitFailurePolicies.continueBounded(maxConsecutiveExhaustedBudgets={}, "
                            + "maxTimeSinceLastSuccessfulCommit={}, maxExhaustionsInWindow={}, exhaustionWindow={})",
                    maxConsecutiveExhaustedBudgets, maxTimeSinceLastSuccessfulCommit, maxExhaustionsInWindow,
                    exhaustionWindow);
        }
    }
}
