package bz.stub.parallelconsumer.internal;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 * Modifications Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumer;
import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.PollContextInternal;
import bz.stub.parallelconsumer.state.WorkContainer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import pl.tlinkowski.unij.api.UniLists;

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.function.Consumer;
import java.util.function.Function;

import bz.stub.parallelconsumer.PCRetriableException;
import bz.stub.parallelconsumer.internal.utils.ThrowableUtils;

import static bz.stub.parallelconsumer.internal.utils.StringUtils.msg;
import static bz.stub.parallelconsumer.internal.utils.ThrowableUtils.describeWithRootCause;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Overrides key aspects required in common for other threading engines like Vert.x and Reactor.
 * <p>
 * <b>The pipeline buffer, and the ceiling that has to bound it.</b> External engines request the same pipelined
 * target as core - {@code getQueueTargetLoaded()}, the in-flight target times the load factor - NOT merely the
 * shortfall against records in flight. That buffer is what removes a whole control-loop iteration from the path
 * between one record completing and the next being dispatched.
 * <p>
 * From 0.4.0.0 to 0.6.0.0 a {@code getTargetOutForProcessing()} override cut the request to only
 * {@code getTargetAmountOfRecordsInFlight()}, on the reasoning that there is no executor pool here to pipeline into.
 * The reasoning confused the pool with the pipeline - but deleting the override on its own is not enough either,
 * because <b>for an external engine there is nowhere for a buffer to wait</b>: the single dispatch thread hands every
 * record it is given straight to the async engine and returns, so records taken from the work manager become records
 * concurrently in flight in the user's engine. Requesting the pipelined target and nothing else therefore puts the
 * buffer THROUGH the ceiling rather than behind it, and {@code maxConcurrency} is breached by the load factor.
 * <p>
 * So the buffer is kept and a hard ceiling is placed where an external engine actually needs one - at dispatch.
 * {@link #runUserFunction} takes one permit per record from a semaphore sized at
 * {@link ParallelConsumerOptions#getTargetAmountOfRecordsInFlight()} and blocks the dispatch thread until capacity
 * exists; {@link #addToMailbox} returns the permit when the record's flight ends. The surplus the pipelined request
 * pulled in then waits in the worker pool's queue - behind the ceiling, exactly where core's buffer waits - and the
 * next dispatch is driven by an async completion rather than by the next control-loop iteration, which is where the
 * throughput comes from.
 * <p>
 * Measured on the Reactor engine in the regime this exists for - short work, low ceiling, so the control loop and
 * not the work is the limit: 2ms per record, {@code maxConcurrency} 100, 50,000 records, four runs an arm.
 * 25,303-27,144 msg/s with the 0.4.0.0 override; 28,011-29,691 msg/s with the buffer and this gate, about +11%, at
 * a peak in flight of exactly 100 in both. The buffer WITHOUT the gate reads 34,435-36,954 msg/s and peaks at 200 -
 * so most of that larger number is the ceiling being breached rather than the pipeline being filled, and it is not
 * a throughput this library may offer. Reactor and Mutiny peak at 1.5-1.95x {@code maxConcurrency} without the
 * gate; Vert.x does not, because its WebClient connection pool is a second ceiling of its own, which is why an
 * earlier Vert.x-only benchmark read the ungated buffer as safe for every engine. The version-by-version bisection
 * of the original regression is on astubbs/parallel-consumer#363 - cited as the PR rather than as its
 * {@code docs/inflight/} note, which lives only on that branch and would dangle from here whichever merges first.
 */
@Slf4j
public abstract class ExternalEngine<K, V> extends AbstractParallelEoSStreamProcessor<K, V> {

    /**
     * How long a blocked dispatch waits before re-checking whether the controller is shutting down. It only bounds
     * how long a close can be delayed by a dispatch waiting on capacity - it is not a dispatch latency, because a
     * returning permit wakes the waiter immediately.
     */
    private static final long DISPATCH_CAPACITY_SHUTDOWN_POLL_MS = 100;

    /**
     * The hard ceiling on records dispatched into the external engine at once. Sized at
     * {@link ParallelConsumerOptions#getTargetAmountOfRecordsInFlight()}, which is
     * {@code maxConcurrency x batchSize} - so a whole batch always fits and a full batch can never deadlock itself.
     */
    private final Semaphore dispatchCeiling;

    /**
     * The records currently holding a permit, by IDENTITY - {@link WorkContainer#equals} is offset-based, and two
     * generations of the same offset are different flights. Membership is what makes the return idempotent: a
     * completion signal delivered more than once for one record (a user {@code Publisher} that emits several
     * elements reaches the mailbox once per element) must not hand back a permit it does not hold, or the ceiling
     * would ratchet upwards for the life of the process.
     */
    private final Set<WorkContainer<K, V>> holdingDispatchPermit =
            Collections.newSetFromMap(Collections.synchronizedMap(new IdentityHashMap<>()));

    protected ExternalEngine(final ParallelConsumerOptions<K, V> newOptions) {
        super(newOptions);

        validate(options);

        this.dispatchCeiling = new Semaphore(options.getTargetAmountOfRecordsInFlight());
    }

    private void validate(ParallelConsumerOptions options) {
        if (options.isUsingTransactionCommitMode()) {
            throw new IllegalStateException(msg("External engines (such as Vert.x and Reactor) do not support transactions / EoS ({})", ParallelConsumerOptions.CommitMode.PERIODIC_TRANSACTIONAL_PRODUCER));
        }
    }

    @Override
    protected void checkPipelinePressure() {
        // Deliberately still a no-op: the load factor stays at its initial value, so the pipelined request - and
        // therefore the queue of records waiting on a dispatch permit - is bounded at initialLoadFactor x the
        // in-flight target. The patch experiment's second arm showed the BUFFER is the recovery and the pressure
        // system's stepping adds nothing on top for these engines; letting it step would only deepen the wait queue
        // and the memory held behind the ceiling, never the concurrency, which #dispatchCeiling fixes.
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
     * Enforces the external dispatch ceiling: no record reaches the user's async engine until a permit is free.
     * <p>
     * This is the whole reason the pipelined request is safe here. The control loop deliberately takes more work
     * than {@code maxConcurrency} so a buffer exists; this is the point at which that surplus stops, on the single
     * dispatch thread, and waits for an in-flight record to finish rather than being handed to the engine as extra
     * concurrency.
     */
    @Override
    protected <R> List<ParallelConsumer.Tuple<ConsumerRecord<K, V>, R>> runUserFunction(Function<PollContextInternal<K, V>, List<R>> usersFunction,
                                                                                        Consumer<R> callback,
                                                                                        List<WorkContainer<K, V>> workContainerBatch) {
        if (!takeDispatchCapacity(workContainerBatch)) {
            // Same disposition as a submission the pool rejects while shutting down: the batch is dropped
            // uncommitted, so it is redelivered after the rebalance. No permit was taken, so none is owed.
            return UniLists.of();
        }
        return super.runUserFunction(usersFunction, callback, workContainerBatch);
    }

    /**
     * Returns the permit taken for this record. Every route a record can leave its flight by - async success, async
     * failure, a synchronous throw from the user function, and stale work skipped before it ever ran - funnels
     * through here, which is why the permit is returned here rather than in any one of them.
     */
    @Override
    protected void addToMailbox(PollContextInternal<K, V> pollContext, WorkContainer<K, V> wc)
            throws PCInternalRuntimeException {
        try {
            super.addToMailbox(pollContext, wc);
        } finally {
            if (holdingDispatchPermit.remove(wc)) {
                dispatchCeiling.release();
            }
        }
    }

    /**
     * @return false if the controller is shutting down and the batch must not be dispatched
     */
    private boolean takeDispatchCapacity(List<WorkContainer<K, V>> batch) {
        int records = batch.size();
        while (true) {
            try {
                if (dispatchCeiling.tryAcquire(records, DISPATCH_CAPACITY_SHUTDOWN_POLL_MS, MILLISECONDS)) {
                    // Registered before the user function runs, so a completion that fires immediately still finds
                    // the record here to hand its permit back for.
                    holdingDispatchPermit.addAll(batch);
                    return true;
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.debug("Interrupted waiting for external dispatch capacity, dropping {} record(s) for redelivery", records);
                return false;
            }
            if (isShuttingDown()) {
                log.debug("Shutting down ({}) while waiting for external dispatch capacity, dropping {} record(s) for redelivery", getState(), records);
                return false;
            }
        }
    }

    private boolean isShuttingDown() {
        State current = getState();
        return current == State.CLOSING || current == State.CLOSED;
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
            } catch (PCInternalRuntimeException pcInvariantBroke) {
                // The EXPECTED shape, named so the code says what it is guarding against: one of PC's own
                // invariants. The known route was ProduceLockNotHeldException out of the produce-lock release
                // inside addToMailbox; astubbs#257 moved that release to cleanUpContext, so no named route is
                // left and this arm is now a classifier rather than a guard against a specific bug. Kept: the
                // override below still runs PC's own code. Terminal either way - see
                // failFatallyOnUnmailboxableRecord.
                failFatallyOnUnmailboxableRecord(wc, pcInvariantBroke);
            } catch (Throwable nothingElseIsExpected) {
                // Backstop, and it stays broad on purpose: anything escaping this loop strands every container
                // behind it, which is the stall this shape exists to prevent. Reaching here means a route nobody
                // has enumerated, so it is worth telling apart from the arm above rather than merging them.
                failFatallyOnUnmailboxableRecord(wc, nothingElseIsExpected);
            }
        });
    }

}
