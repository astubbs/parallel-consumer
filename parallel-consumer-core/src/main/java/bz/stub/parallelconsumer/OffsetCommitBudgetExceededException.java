package bz.stub.parallelconsumer;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * A commit gave up because its configured budget was spent, rather than because the commit itself was
 * rejected. The broker's own exception is always the {@link #getCause() cause}.
 * <p>
 * This exists to be <b>actionable</b>. The underlying Kafka exception says a commit timed out; it
 * cannot say which of PC's options bounded it, what the relationship between that option and the
 * consumer's own timeouts is, or what the alternatives are - and users who met the bare
 * {@code TimeoutException} spent a long time looking in the wrong place for it (astubbs#177,
 * confluentinc#833). The message this carries names the knob that ran out, so the reader can act
 * without reading PC's source.
 * <p>
 * <b>What happens next is the application's decision.</b> This exception is the event the
 * commit-failure seam intercepts (astubbs#317): PC hands it, with the history carried here, to the
 * configured {@link CommitFailureHandler} as a {@link CommitFailureContext}. The default policy
 * ({@link CommitFailurePolicies#shutDown()}) preserves the historical fail-fast behaviour - the
 * instance closes with this as the failure cause - while {@code CONTINUE} policies keep the failed
 * offsets dirty for the next commit cycle's fresh budget.
 *
 * @author Antony Stubbs
 * @see CommitFailureHandler
 * @see CommitFailureContext
 */
// Hand-written ctors (not Lombok @StandardException) - see InternalRuntimeException for why.
public class OffsetCommitBudgetExceededException extends ParallelConsumerException {

    /**
     * @see #getAttemptsMade()
     */
    private final long attemptsMade;

    /**
     * @see #getElapsed()
     */
    private final Duration elapsed;

    /**
     * @see #getOffsets()
     */
    private final Map<TopicPartition, OffsetAndMetadata> offsets;

    public OffsetCommitBudgetExceededException(String message, Throwable cause) {
        this(message, cause, 0, Duration.ZERO, Collections.emptyMap());
    }

    /**
     * @param attemptsMade how many commit attempts the exhausted budget made
     * @param elapsed how long the exhausted budget's retry loop ran, first attempt to giving up
     * @param offsets the offsets the failed commit was trying to commit (defensively copied)
     */
    public OffsetCommitBudgetExceededException(String message, Throwable cause, long attemptsMade, Duration elapsed,
                                               Map<TopicPartition, OffsetAndMetadata> offsets) {
        super(message, cause);
        this.attemptsMade = attemptsMade;
        this.elapsed = elapsed;
        this.offsets = Collections.unmodifiableMap(new HashMap<>(offsets));
    }

    /**
     * How many commit attempts were made within the budget that this failure exhausted - feeds
     * {@link CommitFailureContext#getAttemptsMade()}.
     */
    public long getAttemptsMade() {
        return attemptsMade;
    }

    /**
     * How long was spent inside the exhausted budget's retry loop, from its first attempt to giving up - feeds
     * {@link CommitFailureContext#getElapsed()}.
     */
    public Duration getElapsed() {
        return elapsed;
    }

    /**
     * The offsets the failed commit was trying to commit - feeds {@link CommitFailureContext#getOffsets()}.
     * Unmodifiable.
     */
    public Map<TopicPartition, OffsetAndMetadata> getOffsets() {
        return offsets;
    }

}
