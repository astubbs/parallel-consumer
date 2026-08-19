package bz.stub.parallelconsumer.internal;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 * Modifications Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.PollContextInternal;
import bz.stub.parallelconsumer.state.WorkContainer;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.concurrent.ThreadPoolExecutor;

import bz.stub.parallelconsumer.PCRetriableException;
import bz.stub.parallelconsumer.internal.utils.ThrowableUtils;

import static bz.stub.parallelconsumer.internal.utils.StringUtils.msg;
import static bz.stub.parallelconsumer.internal.utils.ThrowableUtils.describeWithRootCause;

/**
 * Overrides key aspects required in common for other threading engines like Vert.x and Reactor
 */
@Slf4j
public abstract class ExternalEngine<K, V> extends AbstractParallelEoSStreamProcessor<K, V> {

    protected ExternalEngine(final ParallelConsumerOptions<K, V> newOptions) {
        super(newOptions);

        validate(options);
    }

    private void validate(ParallelConsumerOptions options) {
        if (options.isUsingTransactionCommitMode()) {
            throw new IllegalStateException(msg("External engines (such as Vert.x and Reactor) do not support transactions / EoS ({})", ParallelConsumerOptions.CommitMode.PERIODIC_TRANSACTIONAL_PRODUCER));
        }
    }

    /**
     * @return the number of records to try to get, based on the current count of records outstanding - but unlike core,
     *         we don't pipeline messages into the executor pool for processing.
     */
    protected int getTargetOutForProcessing() {
        return getOptions().getTargetAmountOfRecordsInFlight();
    }

    @Override
    protected void checkPipelinePressure() {
        // no-op - as calculateQuantityToRequest does not use a pressure system, unlike the core module
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
    protected void addToMailBoxOnUserFunctionSuccess(final PollContextInternal<K, V> context, WorkContainer<K, V> wc, List<?> resultsFromUserFunction) {
        if (isAsyncFutureWork(resultsFromUserFunction)) {
            log.debug("User function success but not adding vertx vertical to mailbox yet");
        } else {
            super.addToMailBoxOnUserFunctionSuccess(context, wc, resultsFromUserFunction);
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

    /**
     * Records an async failure against every container in the batch and returns each to the mailbox - each container
     * independent of the others, and all of it before the failure is rendered anywhere.
     * <p>
     * Record BEFORE rendering, because {@code throwable} is the user's own async failure: logging it runs their
     * {@code getCause}/{@code getMessage} inside the logging binding's stack-trace walk, and if that throws, any
     * container not yet completed stays marked in flight forever - the failure is the thing that must be recorded,
     * the log line is the thing that can be lost. So callers log their fail signal AFTER this returns, guarded with
     * {@link bz.stub.parallelconsumer.internal.utils.ThrowableUtils#logWithoutEscaping}.
     * <p>
     * Per container, independent of the others - the same shape core's {@code runUserFunction} loop uses, and for
     * the same reason. {@link WorkContainer#onUserFunctionFailure} runs USER code (the retryDelayProvider, via
     * updateFailureHistory), so one container's failure must not stop the batch: every container after it would
     * then never reach {@link #addToMailbox} and would stay in flight forever.
     */
    /**
     * The whole async-failure path for a batch engine: record the failure against every container, then render it.
     * <p>
     * Reactor's and Mutiny's versions of this differed only by a log string and whether the framework's own wrapper
     * needed peeling first, so they are one method with those two seams. Keeping two copies is how the ordering and
     * the guard drift apart - which is exactly what happened to this code once already, when the record-before-render
     * fix was applied to one engine and not its siblings.
     * <p>
     * Vert.x deliberately does not use this: it handles a single container per failure rather than a batch, so it has
     * no loop to make independent and its own handler stays separate.
     *
     * @param failSignalMessage this engine's log message, passed rather than derived so the strings users may already
     *                          grep for do not change
     */
    protected void onAsyncFailure(PollContextInternal<K, V> pollContext, Throwable throwable, String failSignalMessage) {
        // record first, render after - the reasoning is on recordFailureAndReturnBatchToMailbox
        recordFailureAndReturnBatchToMailbox(pollContext, throwable);
        ThrowableUtils.logWithoutEscaping(throwable, () -> {
            if (PCRetriableException.isPresentIn(unwrapFrameworkWrapper(throwable))) {
                log.debug(failSignalMessage, throwable);
            } else {
                log.error(failSignalMessage, throwable);
            }
        });
    }

    /**
     * Peels the framework's own wrapper before asking whether the failure underneath is one the user marked expected.
     * <p>
     * Identity by default. An engine whose framework repackages what it propagates overrides this with that
     * framework's helper - core cannot name those wrapper types, so it cannot do the peeling itself.
     */
    protected Throwable unwrapFrameworkWrapper(Throwable throwable) {
        return throwable;
    }

    protected void recordFailureAndReturnBatchToMailbox(PollContextInternal<K, V> pollContext, Throwable throwable) {
        pollContext.streamWorkContainers().forEach(wc -> {
            try {
                wc.onUserFunctionFailure(throwable);
            } catch (Throwable bookkeepingThrew) {
                log.error("Failed to record the user function failure against {} - the record is still returned to " +
                        "the mailbox below. Cause: {}", wc, describeWithRootCause(bookkeepingThrew));
            }
            try {
                addToMailbox(pollContext, wc);
            } catch (Throwable mailboxingThrew) {
                log.error("Failed to return {} to the mailbox - it may stay in flight. Cause: {}", wc,
                        describeWithRootCause(mailboxingThrew));
            }
        });
    }

}
