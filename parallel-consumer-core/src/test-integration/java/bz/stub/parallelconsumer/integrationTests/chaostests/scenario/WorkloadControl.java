package bz.stub.parallelconsumer.integrationTests.chaostests.scenario;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import java.time.Duration;

/**
 * The workload half of a scenario driver's control surface - what {@link WorkloadActions} call. These are
 * the levers that produce the conditions worth looking at but that membership churn alone cannot create:
 * a throughput ramp, a proportion of records failing, one key failing repeatedly so its shard backs up
 * behind retry backoff (head-of-line blocking), and a user function slow enough to fill the worker pool.
 * <p>
 * Implemented by {@link ScriptedWorkload} over a {@link WorkloadPublisher} and a {@link ScriptedFunction}.
 * Every method must be safe to call from the scenario thread while the workload runs on others.
 */
public interface WorkloadControl {

    /** Absolute publish rate, records per second. Zero pauses the producer without stopping it. */
    void setPublishRatePerSecond(int recordsPerSecond);

    /** Current publish rate, so a relative action can scale it. */
    int getPublishRatePerSecond();

    /**
     * Proportion of records the user function should fail, in {@code [0,1]}. Which records fail is a
     * function of the key, not of a random draw - see {@link ScriptedFunction}.
     */
    void setFailureProportion(double proportion);

    /**
     * Fail this one key on every delivery, forever, until cleared. This is the head-of-line-blocking
     * lever: the key's shard stalls behind retry backoff while the rest of the partition completes past
     * it, which is exactly the condition the offset view exists to show.
     */
    void setFailingKey(String key);

    /** Stop failing the pinned key. Its backlog then retries through and the committed offset lurches. */
    void clearFailingKey();

    /** How long the user function should dwell per record. Zero is as fast as it goes. */
    void setFunctionDelay(Duration delay);
}
