package bz.stub.parallelconsumer.internal;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.internal.utils.ThrowableUtils;
import lombok.experimental.UtilityClass;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.UnsupportedVersionException;

import java.time.Duration;

/**
 * The policy half of producer recovery - how long to wait, what cannot be retried, what may be logged - kept apart
 * from {@link ProducerManager}, which owns the locks and the producer. Nothing here takes a lock or touches a
 * client; every member is a pure function of its arguments, so the manager's threading story stays the manager's.
 */
@UtilityClass
class ProducerRecoveryPolicy {

    /** First delay between recovery attempts; doubles per attempt up to {@link #RECOVERY_BACKOFF_MAX}. */
    static final Duration RECOVERY_BACKOFF_INITIAL = Duration.ofSeconds(1);
    /** Cap on the delay between recovery attempts. Not options, because nobody has asked to tune them yet. */
    static final Duration RECOVERY_BACKOFF_MAX = Duration.ofSeconds(30);
    /**
     * How long closing the discarded producer may take. Bounded because it runs under the write lock, which the
     * revoke callback may be waiting on; a fenced producer's close is usually immediate.
     */
    static final Duration DISCARDED_PRODUCER_CLOSE_TIMEOUT = Duration.ofSeconds(10);

    /** The delay before attempt {@code attempts}: {@code initial}, doubling per attempt, capped at {@code max}. */
    static Duration backoffFor(int attempts, Duration initial, Duration max) {
        long millis = initial.toMillis();
        for (int i = 1; i < attempts && millis < max.toMillis(); i++) {
            millis *= 2;
        }
        return Duration.ofMillis(Math.min(millis, max.toMillis()));
    }

    /**
     * What retrying a replacement build cannot fix: the broker refusing the id or the feature, the factory breaking
     * its contract (deterministic - a caching factory caches on every rebuild), and an {@link Error} from the
     * factory, which arrives wrapped as user-function failure and is never a transient broker condition.
     */
    static boolean isTerminalBuildFailure(Throwable failure) {
        return ThrowableUtils.anyInCauseChain(failure,
                f -> f instanceof AuthorizationException || f instanceof UnsupportedVersionException
                        || f instanceof ProducerFactoryContractException || f instanceof Error);
    }

    /**
     * The failure with its stack trace but without its message, which for a configuration error carries the value.
     */
    static Throwable sanitised(Throwable failure) {
        var copy = new RuntimeException(failure.getClass().getName() + " (message redacted: it may carry configuration values)");
        copy.setStackTrace(failure.getStackTrace());
        return copy;
    }
}
