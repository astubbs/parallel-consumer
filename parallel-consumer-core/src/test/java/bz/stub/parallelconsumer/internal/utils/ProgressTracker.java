package bz.stub.parallelconsumer.internal.utils;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 * Modifications Copyright (C) 2026 Antony Stubbs and contributors
 */
import bz.stub.parallelconsumer.internal.PCInternalRuntimeException;
import lombok.Getter;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.Temporal;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static bz.stub.parallelconsumer.internal.utils.StringUtils.msg;
import static bz.stub.parallelconsumer.AbstractParallelEoSStreamProcessorTestBase.defaultTimeout;

/**
 * Used to check that progress has been made in some activity.
 */
public class ProgressTracker {

    public static final int WARMED_UP_AFTER_X_MESSAGES = 50;

    /**
     * The shared count of progress.
     */
    private final AtomicInteger processedCount;

    private final AtomicInteger lastSeen = new AtomicInteger(0);

    /**
     * How many times progress has been checked for.
     */
    @Getter
    private final AtomicInteger rounds = new AtomicInteger(0);

    @Getter
    private Duration timeout = defaultTimeout;

    private Integer roundsAllowed = 10;

    private final int coldRoundsAllowed = 20;

    @Getter
    private int highestRoundCountSeen = 0;
    private final Instant startTime = Instant.now();

    /** When the count last advanced - the reference for "flat for how long", reset alongside rounds. */
    private volatile Instant lastAdvance = Instant.now();

    /** Optional supplier of the consumer's own state; null until {@link #withDiagnostic(Supplier)}. */
    private Supplier<String> diagnostic = null;

    public ProgressTracker(final AtomicInteger processedCount, Integer roundsAllowed, Duration timeout) {
        this.processedCount = processedCount;
        if (roundsAllowed != null && timeout != null)
            throw new IllegalArgumentException("Can't provide both a timeout and a number of rounds");
        this.roundsAllowed = roundsAllowed;
        this.timeout = timeout;
    }

    public ProgressTracker(final AtomicInteger processedCount) {
        this.processedCount = processedCount;
    }

    /**
     * Checks progress has been made. Increments the count of rounds / checks.
     *
     * @return false if progress has been made, true otherwise
     */
    public boolean hasProgressNotBeenMade() {
        boolean progress = processedCount.get() > lastSeen.get();
        boolean warmedUp = processedCount.get() > WARMED_UP_AFTER_X_MESSAGES;
        boolean enoughAttempts = hasTimeoutPassed();
        if (warmedUp && !progress && enoughAttempts) {
            return true;
        } else if (!warmedUp && this.roundsAllowed != null && rounds.get() > coldRoundsAllowed) {
            return true;
        } else if (progress) {
            reset();
        }
        lastSeen.set(processedCount.get());
        rounds.incrementAndGet();
        return false;
    }

    private boolean hasTimeoutPassed() {
        // in the case both are present, prefer rounds to duration (legacy)
        if (roundsAllowed != null) {
            return rounds.get() > roundsAllowed;
        } else {
            Duration remainingTime = Duration.between(Instant.now(), getDeadline());
            return remainingTime.isNegative();
        }
    }

    private Temporal getDeadline() {
        return startTime.plus(timeout);
    }

    private void reset() {
        if (rounds.get() > highestRoundCountSeen)
            highestRoundCountSeen = rounds.get();
        rounds.set(0);
        lastAdvance = Instant.now();
    }

    /**
     * @throws Exception If no progress is made
     */
    public void checkForProgressExceptionally() throws Exception {
        boolean noProgress = hasProgressNotBeenMade();
        if (noProgress)
            throw constructError();
    }

    public Exception constructError() {
        return constructError("");
    }

    public Exception constructError(String messageToAppend) {
        return new PCInternalRuntimeException(msg("No progress beyond {} records after {} rounds. {} {}",
                processedCount, rounds, describeVerdict(), messageToAppend));
    }

    /**
     * Classifies WHY the wait is ending, rather than only reporting that it did.
     * <p>
     * A bare "timed out after N records" is the same sentence whether the run was crawling or
     * wedged, and those have opposite causes - the first is contention on the machine, the second is
     * a defect in the code under test. Telling them apart from a failure message is the difference
     * between a diagnosis and a re-run, and this repo's rule is to establish which before touching
     * a test that fails under load.
     * <p>
     * The discriminator is progress since the last time progress was seen, not progress overall: a
     * run that raced to a high count and then froze has still made a lot of progress, and judging on
     * the total reads that as healthy. {@code lastSeen} is reset on every observed advance, so a
     * flat stretch is visible however fast the start was.
     * <p>
     * {@link #withDiagnostic(Supplier)} adds the consumer's own view. That matters because an
     * external counter cannot see the difference between "not trying" and "trying and not
     * finishing" - only the instance knows whether it is holding work it is not completing.
     */
    public String describeVerdict() {
        int seen = processedCount.get();
        Duration flatFor = Duration.between(lastAdvance, Instant.now());
        String shape = seen > lastSeen.get()
                ? "STILL ADVANCING at the deadline - too slow, not stopped"
                : msg("FLAT for {}s - it stopped rather than ran out of time", flatFor.getSeconds());
        String elapsed = msg("elapsed={}s", Duration.between(startTime, Instant.now()).getSeconds());
        String internal = diagnostic == null
                ? "no consumer diagnostic supplied - pass withDiagnostic(pc::describeProgress) to get one"
                : diagnostic.get();
        return msg("[{} | {} | {}]", shape, elapsed, internal);
    }

    /**
     * Supply the consumer's own progress description, so a failure reports internal state next to the
     * external count. Typically {@code tracker.withDiagnostic(pc::describeProgress)}.
     */
    public ProgressTracker withDiagnostic(Supplier<String> consumerDiagnostic) {
        this.diagnostic = consumerDiagnostic;
        return this;
    }
}
