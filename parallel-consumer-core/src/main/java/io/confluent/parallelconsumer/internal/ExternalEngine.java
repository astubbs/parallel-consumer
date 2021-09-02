package io.confluent.parallelconsumer.internal;

/*-
 * Copyright (C) 2020-2021 Confluent, Inc.
 */

import com.google.common.flogger.FluentLogger;
import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.state.WorkContainer;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

/**
 * Overrides key aspects required in common for other threading engines like Vert.x and Reactor
 */
@Slf4j
public abstract class ExternalEngine<K, V> extends AbstractParallelEoSStreamProcessor<K, V> {

    private static final FluentLogger flog = FluentLogger.forEnclosingClass();

    protected ExternalEngine(final ParallelConsumerOptions<K, V> newOptions) {
        super(newOptions);
    }

    /**
     * @return the number of records to try to get, based on the current count of records outstanding
     */
    @Override
    protected int calculateQuantityToRequest() {
        int numberRecordsOutForProcessing = wm.getNumberRecordsOutForProcessing();
        int maxConcurrency = getOptions().getMaxConcurrency();
        int rawDelta = maxConcurrency - numberRecordsOutForProcessing;

        //
        int currentFactor = dynamicExtraLoadFactor.getCurrentFactor();
        int pressured = maxConcurrency * currentFactor;
        int pressuredDelta = pressured - numberRecordsOutForProcessing;

        //
        int delta = Math.max(0, pressuredDelta);
        logger.at(Level.FINE).atMostEvery(1, TimeUnit.SECONDS)
                .log("Target: %s, pressure: %s, factor: %s. Out: %s, to request: %s", maxConcurrency, pressured, currentFactor, numberRecordsOutForProcessing, delta);
        return delta;
    }

    @Override
    protected void checkPressure() {
        int maxConcurrency = getOptions().getMaxConcurrency();
        boolean moreWorkInQueuesAvailableThatHaveNotBeenPulled = wm.getWorkQueuedInMailboxCount() > maxConcurrency;

//        boolean close = MathUtils.isWithin(wm.getNumberRecordsOutForProcessing(), maxConcurrency, 10);
        //if ((Math.abs(wm.getNumberRecordsOutForProcessing() - maxConcurrency) > maxConcurrency * 0.1)
        if (wm.getNumberRecordsOutForProcessing() < (maxConcurrency * 0.9)
//                && dynamicExtraLoadFactor.isWarmUpPeriodOver()
                && moreWorkInQueuesAvailableThatHaveNotBeenPulled) {

            boolean steppedUp = dynamicExtraLoadFactor.maybeStepUp();

            if (steppedUp) {
                log.debug("isPoolQueueLow(): Executor pool queue is not loaded with enough work (queue: {} vs target: {}), stepped up loading factor to {}",
                        wm.getNumberRecordsOutForProcessing(), maxConcurrency, dynamicExtraLoadFactor.getCurrentFactor());
            } else if (dynamicExtraLoadFactor.isMaxReached()) {
                log.warn("isPoolQueueLow(): Max loading factor steps reached: {}/{}", dynamicExtraLoadFactor.getCurrentFactor(), dynamicExtraLoadFactor.getMaxFactor());
            }
        }
    }

    /**
     * The vert.x module doesn't use any thread pool for dispatching work, as the work is all done by the vert.x engine.
     * This thread is only used to dispatch the work to vert.x.
     * <p>
     * TODO optimise thread usage by not using any extra thread here at all - go straight from the control thread to
     * vert.x.
     */
    @Override
    protected ThreadPoolExecutor setupWorkerPool(int poolSize) {
        return super.setupWorkerPool(1);
    }

    /**
     * With Vertx and Reactor, a function hasn't succeeded until the inner vertx function has also succeeded logging
     */
    @Override
    protected void onUserFunctionSuccess(WorkContainer<K, V> wc, List<?> resultsFromUserFunction) {
        if (isAsyncFutureWork(resultsFromUserFunction)) {
            log.debug("Reactor creation function success, user's function success");
        } else {
            super.onUserFunctionSuccess(wc, resultsFromUserFunction);
        }
    }

    /**
     * With Vertx and Reactor, a function hasn't succeeded until the inner vertx function has also succeeded no op
     */
    @Override
    protected void addToMailBoxOnUserFunctionSuccess(WorkContainer<K, V> wc, List<?> resultsFromUserFunction) {
        if (isAsyncFutureWork(resultsFromUserFunction)) {
            log.debug("User function success but not adding vertx vertical to mailbox yet");
        } else {
            super.addToMailBoxOnUserFunctionSuccess(wc, resultsFromUserFunction);
        }
    }

    /**
     * System dependent way for the core engine to know if an external scheduler needs to be relied on for completion,
     * as opposed to the internal threading system in the core module.
     *
     * @param resultsFromUserFunction the results to test
     * @return true if the work needs special treatment
     */
    // TODO: Now that the modules don't use the internal threading systems at all, is this method redundant as all work from a module extension would return true
    protected abstract boolean isAsyncFutureWork(List<?> resultsFromUserFunction);

}
