package bz.stub.parallelconsumer.integrationTests.chaostests.scenario;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import java.time.Duration;

/**
 * The default {@link WorkloadControl}: a {@link WorkloadPublisher} for the input side and a
 * {@link ScriptedFunction} for the processing side, presented to actions as one control surface.
 * <p>
 * Either half may be absent - a scenario that only slows the user function needs no publisher - and a
 * missing half fails loudly on first use rather than silently doing nothing, so a scenario cannot appear
 * to demonstrate something it has no machinery for.
 */
public class ScriptedWorkload implements WorkloadControl {

    private final WorkloadPublisher publisher;
    private final ScriptedFunction function;

    public ScriptedWorkload(WorkloadPublisher publisher, ScriptedFunction function) {
        this.publisher = publisher;
        this.function = function;
    }

    public WorkloadPublisher getPublisher() {
        return require(publisher, "publisher");
    }

    public ScriptedFunction getFunction() {
        return require(function, "scripted function");
    }

    @Override
    public void setPublishRatePerSecond(int recordsPerSecond) {
        require(publisher, "publisher").setRatePerSecond(recordsPerSecond);
    }

    @Override
    public int getPublishRatePerSecond() {
        return require(publisher, "publisher").getRatePerSecond();
    }

    @Override
    public void setFailureProportion(double proportion) {
        require(function, "scripted function").setFailureProportion(proportion);
    }

    @Override
    public void setFailingKey(String key) {
        require(function, "scripted function").setFailingKey(key);
    }

    @Override
    public void clearFailingKey() {
        require(function, "scripted function").setFailingKey(null);
    }

    @Override
    public void setFunctionDelay(Duration delay) {
        require(function, "scripted function").setDelay(delay);
    }

    private static <T> T require(T part, String what) {
        if (part == null) {
            throw new IllegalStateException("this workload has no " + what + " wired, but an action asked it to "
                    + "do something that needs one - wire it, or drop the action from the phase");
        }
        return part;
    }
}
