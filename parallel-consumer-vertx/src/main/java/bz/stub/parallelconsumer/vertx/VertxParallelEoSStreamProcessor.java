package bz.stub.parallelconsumer.vertx;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 * Modifications Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.PCRetriableException;
import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.PollContext;
import bz.stub.parallelconsumer.PollContextInternal;
import bz.stub.parallelconsumer.internal.AbstractParallelEoSStreamProcessor;
import bz.stub.parallelconsumer.internal.ExternalEngine;
import bz.stub.parallelconsumer.internal.MdcPropagation;
import bz.stub.parallelconsumer.internal.PCInternalRuntimeException;
import bz.stub.parallelconsumer.state.WorkContainer;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.web.client.HttpRequest;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.utils.Time;
import pl.tlinkowski.unij.api.UniLists;
import pl.tlinkowski.unij.api.UniMaps;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeoutException;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

import static bz.stub.parallelconsumer.internal.UserFunctions.carefullyRun;
import static bz.stub.parallelconsumer.internal.utils.ThrowableUtils.describeWithRootCause;
import static bz.stub.parallelconsumer.internal.utils.ThrowableUtils.logWithoutEscaping;


/**
 * <b>HTTP status codes are the caller's concern, not this library's.</b> A Vert.x {@code WebClient}
 * future completes successfully for any response that arrives, whatever its status, and fails only
 * when the request does not complete at all - connection refused, timeout, TLS failure. This engine
 * treats that future's outcome as the work's outcome, so a delivered <b>4xx or 5xx marks the record
 * processed and its offset is committed</b>, while a transport failure marks it failed and the
 * record is retried.
 * <p>
 * That is deliberate, and it matches how parallel-consumer treats the rest of the user's domain: it
 * hands you the tools and takes no position on what counts as a business failure. To make a status
 * code retryable, say so in your own function - attach a Vert.x
 * {@code ResponsePredicate} to the request, or inspect the response and throw.
 * <p>
 * Both halves of this boundary are pinned by {@code VertxTest.serverErrorStatusStillCommits} and
 * {@code VertxTest.transportFailureIsDistinctFromANonSuccessStatus}, so it cannot change silently.
 */
@Slf4j
public class VertxParallelEoSStreamProcessor<K, V> extends ExternalEngine<K, V>
        implements VertxParallelStreamProcessor<K, V> {

    /**
     * @see WorkContainer#getWorkType()
     */
    private static final String VERTX_TYPE = "vert.x-type";

    /**
     * The Vertx engine to use.
     * <p>
     * Package-private getter, not protected: the only readers are same-package tests observing whether close
     * released the engine this processor built, and PROTECTED would put a test-only seam on the extension surface
     * of a public class.
     */
    @Getter(AccessLevel.PACKAGE)
    private final Vertx vertx;

    /**
     * Whether {@link #vertx} was built by this processor rather than supplied by the caller - and so is this
     * processor's to close. Decided once, in the constructor that does the building, and read only by
     * {@link #releaseOwnedVertxEngine(Duration)}.
     */
    private final boolean ownsVertx;

    /**
     * The Vertx webclient for making HTTP requests. Package-private getter for the same reason as {@link #vertx}.
     */
    @Getter(AccessLevel.PACKAGE)
    private final WebClient webClient;

    /**
     * Whether {@link #webClient} was built by this processor. Independent of {@link #ownsVertx}: a caller may share
     * their {@link Vertx} and still leave the {@link WebClient} for this processor to build, and then only the
     * client is this processor's to close.
     */
    private final boolean ownsWebClient;

    /**
     * Extension point for running after Vertx {@link io.vertx.core.Verticle}s finish.
     */
    private Optional<Runnable> onVertxCompleteHook = Optional.empty();

    /**
     * Simple constructor. This processor builds its own Vertx engine and {@link WebClient}, owns both, and closes
     * both when it closes.
     */
    public VertxParallelEoSStreamProcessor(ParallelConsumerOptions options) {
        this(null, null, options);
    }

    /**
     * Provide your own instances of the Vertx engine and it's webclient.
     * <p>
     * Use this to share a Vertx runtime with different systems for efficiency, or to customise configuration. An
     * instance you supply here stays yours: this processor leaves it running when it closes, for you to close when
     * every system sharing it is done. Pass {@code null} for either argument and this processor builds that one
     * itself, owns it, and closes it when it closes.
     * <p>
     * By default Vert.x's {@link WebClient} uses quite small connection limits to servers. PC overrides this to {@link
     * ParallelConsumerOptions#getMaxConcurrency()}. You can configure these yourself by providing a configured Vert.x
     * {@link WebClient} with {@link WebClientOptions} set to how you please. Consider also looking at other options
     * below.
     *
     * @param vertx     the engine to run on, or {@code null} to have this processor build and own one
     * @param webClient the client to make requests with, or {@code null} to have this processor build and own one on
     *                  {@code vertx}
     * @see WebClientOptions#setMaxPoolSize
     * @see WebClientOptions#setMaxWaitQueueSize(int)
     * @see WebClientOptions#setPipelining(boolean)
     * @see WebClientOptions#setPipeliningLimit(int)
     * @see WebClientOptions#setHttp2MaxPoolSize(int)
     * @see WebClientOptions#setHttp2MultiplexingLimit(int)
     */
    public VertxParallelEoSStreamProcessor(Vertx vertx,
                                           WebClient webClient,
                                           ParallelConsumerOptions options) {
        super(options);

        int cores = Runtime.getRuntime().availableProcessors();
        VertxOptions vertxOptions = new VertxOptions().setWorkerPoolSize(cores);

        int maxConcurrency = options.getMaxConcurrency();

        // should this be user configurable? - probably
        WebClientOptions webClientOptions = new WebClientOptions()
                .setMaxPoolSize(maxConcurrency) // defaults to 5
                .setHttp2MaxPoolSize(maxConcurrency) // defaults to 1
                ;

        // Ownership follows construction: what this processor builds, it closes; what the caller supplied, the caller
        // closes. Decided here and nowhere else, so the teardown cannot disagree with the constructor about it.
        this.ownsVertx = vertx == null;
        this.vertx = ownsVertx ? Vertx.vertx(vertxOptions) : vertx;
        this.ownsWebClient = webClient == null;
        this.webClient = ownsWebClient ? WebClient.create(this.vertx, webClientOptions) : webClient;
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

    @Override
    public void vertxHttpReqInfo(Function<PollContext<K, V>, RequestInfo> requestInfoFunction,
                                 Consumer<Future<HttpResponse<Buffer>>> onSend,
                                 Consumer<AsyncResult<HttpResponse<Buffer>>> onWebRequestComplete) {
        vertxHttpRequest((WebClient webClient, PollContext<K, V> rec) -> {
            RequestInfo reqInf = carefullyRun(requestInfoFunction, rec);

            HttpRequest<Buffer> req = webClient.get(reqInf.getPort(), reqInf.getHost(), reqInf.getContextPath());
            Map<String, String> params = reqInf.getParams();
            for (var entry : params.entrySet()) {
                req = req.addQueryParam(entry.getKey(), entry.getValue());
            }
            return req;
        }, onSend, onWebRequestComplete);
    }

    @Override
    public void vertxHttpRequest(BiFunction<WebClient, PollContext<K, V>, HttpRequest<Buffer>> webClientRequestFunction,
                                 Consumer<Future<HttpResponse<Buffer>>> onSend,
                                 Consumer<AsyncResult<HttpResponse<Buffer>>> onWebRequestComplete) {

        vertxHttpWebClient((webClient, record) -> {
            HttpRequest<Buffer> call = carefullyRun(webClientRequestFunction, webClient, record);

            Future<HttpResponse<Buffer>> send = call.send(); // dispatches the work to vertx

            // the user's callback runs on the vert.x event loop, which is a second thread boundary - carry the
            // diagnostic context of this (worker) thread over it
            var eventLoopContext = getMdcPropagation().capture();

            // hook in the users' call back for when the web request gets a response
            send.onComplete(ar -> {
                try (var mdcScope = getMdcPropagation().enter(eventLoopContext)) {
                    onWebRequestComplete.accept(ar);
                }
            });

            return send;
        }, onSend);
    }

    @Override
    public void vertxHttpWebClient(BiFunction<WebClient, PollContext<K, V>, Future<HttpResponse<Buffer>>> webClientRequestFunction,
                                   Consumer<Future<HttpResponse<Buffer>>> onWebRequestSentCallback) {

        // wrap single record function in batch function
        Function<PollContextInternal<K, V>, List<Future<HttpResponse<Buffer>>>> userFuncWrapper = (context) -> {
            log.trace("Consumed a record ({}), executing void function...", context);

            Future<HttpResponse<Buffer>> futureWebResponse = carefullyRun(webClientRequestFunction, webClient, context.getPollContext());

            // execute user's onSend callback
            onWebRequestSentCallback.accept(futureWebResponse);

            addVertxHooks(context, futureWebResponse);

            return UniLists.of(futureWebResponse);
        };

        Consumer<Future<HttpResponse<Buffer>>> noOp = (ignore) -> {
        }; // don't need it, we attach to vertx futures for callback

        super.supervisorLoop(userFuncWrapper, noOp);
    }

    private void addVertxHooks(final PollContextInternal<K, V> context, final Future<?> send) {
        // called on the worker thread, where the caller's context is established - these handlers however run on the
        // vert.x event loop, so the context has to be carried explicitly
        final MdcPropagation mdc = getMdcPropagation();
        final Map<String, String> eventLoopContext = mdc.capture();

        context.streamWorkContainers().forEach(wc -> {
            // attach internal handler
            wc.setWorkType(VERTX_TYPE);

            send.onSuccess(h -> {
                try (var mdcScope = mdc.enter(eventLoopContext)) {
                    log.debug("Vert.x Vertical success");
                    wc.onUserFunctionSuccess();
                    addToMailbox(context, wc);
                }
            });
            send.onFailure(h -> {
                // master's MDC scope, this branch's guards: the handlers run on the vert.x event loop, so
                // the caller's diagnostic context has to be carried explicitly, and everything below has to
                // happen inside it or the failure is logged without the context that identifies it.
                try (var mdcScope = mdc.enter(eventLoopContext)) {
                    // Record the failure BEFORE rendering it. Logging a throwable hands it to the logging binding,
                    // which walks the cause chain itself to build a stack trace - unbounded, and running the
                    // throwable author's overrides. If that throws, everything after it is skipped, and what would be
                    // skipped here is the work container's own completion: the record would stay marked in flight
                    // forever, stalling ordering and draining. The failure is the thing that must be recorded; the
                    // log line is the thing that can be lost.
                    // Each step guarded separately, because vert.x will NOT contain a throw for us: FutureImpl's
                    // listener array iterates its listeners with no per-listener try/catch, so anything escaping this
                    // handler skips every remaining listener - including the sibling containers' own handlers, which
                    // strands their records in flight forever. Core, Reactor and Mutiny all guard this; this was the
                    // last engine that did not.
                    try {
                        wc.onUserFunctionFailure(h);
                    } catch (Throwable bookkeepingThrew) {
                        // Logged, not fatal, and bounded: what threw is USER code - the retryDelayProvider, reached via
                        // updateFailureHistory - and onUserFunctionFailure records the verdict in a finally, so the
                        // container leaves its in-flight state even on this path. What is lost is retry METADATA for
                        // this one record (attempt count, retryDueAt), not the record: it is still mailboxed on the
                        // next lines. Making it fatal would let a user callback stop the consumer, which is the whole
                        // defect class this handler exists to close.
                        log.error("Failed to record the send failure against {} - the record is still returned to the " +
                                "mailbox below. Cause: {}", wc, describeWithRootCause(bookkeepingThrew));
                    }
                    try {
                        addToMailbox(context, wc);
                    } catch (PCInternalRuntimeException pcInvariantBroke) {
                        // The EXPECTED shape - one of PC's own invariants. It was reachable here as
                        // ProduceLockNotHeldException from the produce-lock release inside addToMailbox until
                        // astubbs#257 made cleanUpContext the single release point. Terminal, per
                        // the operator ruling: if the record cannot be posted, PC can no longer account for it, and
                        // continuing risks a silent skip. Escalation only records the reason and moves the state,
                        // because throwing would skip vert.x's remaining listeners and strand the sibling containers,
                        // and blocking would hold the event loop.
                        failFatallyOnUnmailboxableRecord(wc, pcInvariantBroke);
                    } catch (Throwable nothingElseIsExpected) {
                        // Backstop for a route nobody has enumerated. Broad on purpose, for the same reason the arm
                        // above must not rethrow.
                        failFatallyOnUnmailboxableRecord(wc, nothingElseIsExpected);
                    }

                    // the throwable rather than its message: this is the only record of why a send failed, and
                    // getMessage() alone drops the type, the cause chain and the stack - and reads "fail: null"
                    // for anything thrown without a message. Guarded, because h is the user's throwable and the
                    // logging binding walks its cause chain unbounded.
                    logWithoutEscaping(h, () -> {
                        // DEBUG for a retriable failure, ERROR otherwise: PCRetriableException is the user's documented
                        // way of saying "this one is expected, hand it back to me later", so it is a normal step in a
                        // working retry loop rather than a fault. Logged at ERROR it would report healthy operation as
                        // broken, and at the rate a retry loop runs it would bury the failures that are.
                        if (PCRetriableException.isPresentIn(h)) {
                            log.debug("Vert.x Vertical fail", h);
                        } else {
                            log.error("Vert.x Vertical fail", h);
                        }
                    });
                }
            });

            // add plugin callback hook
            send.onComplete(ar -> {
                try (var mdcScope = mdc.enter(eventLoopContext)) {
                    log.trace("Running plugin hook");
                    this.onVertxCompleteHook.ifPresent(Runnable::run);
                }
            });
        });
    }

    @Override
    public void vertxFuture(final Function<PollContext<K, V>, Future<?>> result) {

        // wrap single record function in batch function
        Function<PollContextInternal<K, V>, List<Future<?>>> userFuncWrapper = context -> {
            log.trace("Consumed a record ({}), executing void function...", context);

            Future<?> send = carefullyRun(result, context.getPollContext());

            addVertxHooks(context, send);

            return UniLists.of(send);
        };

        Consumer<Future<?>> noOp = ignore -> {
        }; // don't need it, we attach to vertx futures for callback

        super.supervisorLoop(userFuncWrapper, noOp);
    }

    @Override
    public void batchVertxFuture(final Function<PollContext<K, V>, Future<?>> result) {

        Function<PollContextInternal<K, V>, List<Future<?>>> userFuncWrapper = context -> {

            Future<?> send = carefullyRun(result, context.getPollContext());

            addVertxHooks(context, send);

            return UniLists.of(send);
        };

        Consumer<Future<?>> noOp = ignore -> {
        }; // don't need it, we attach to vertx futures for callback

        super.supervisorLoop(userFuncWrapper, noOp);
    }

    /**
     * @see #onVertxCompleteHook
     */
    public void addVertxOnCompleteHook(Runnable hookFunc) {
        this.onVertxCompleteHook = Optional.of(hookFunc);
    }

    /**
     * Basic information to perform a web request.
     */
    @Setter
    @Getter
    @AllArgsConstructor
    public static class RequestInfo {
        public static final int DEFAULT_PORT = 8080;
        private final String host;
        private final int port;
        private final String contextPath;
        private Map<String, String> params;

        public RequestInfo(String host, String contextPath, Map<String, String> params) {
            this(host, DEFAULT_PORT, contextPath, params);
        }

        public RequestInfo(String host, String contextPath) {
            this(host, DEFAULT_PORT, contextPath, UniMaps.of());
        }

    }

    @Override
    protected void onUserFunctionSuccess(WorkContainer<K, V> wc, List<?> resultsFromUserFunction) {
        // with vertx, a function hasn't succeeded until the inner vertx function has also succeeded
        // logging
        if (isAsyncFutureWork(resultsFromUserFunction)) {
            log.debug("Vertx creation function success, user's function success");
        } else {
            super.onUserFunctionSuccess(wc, resultsFromUserFunction);
        }
    }

    @Override
    protected void addToMailBoxOnUserFunctionSuccess(final PollContextInternal<K, V> context, WorkContainer<K, V> wc, List<?> resultsFromUserFunction) {
        // with vertx, a function hasn't succeeded until the inner vertx function has also succeeded
        // no op
        if (isAsyncFutureWork(resultsFromUserFunction)) {
            log.debug("User function success but not adding vertx vertical to mailbox yet");
        } else {
            super.addToMailBoxOnUserFunctionSuccess(context, wc, resultsFromUserFunction);
        }
    }

    /**
     * Determines if any of the elements in the supplied list is a Vertx Future type
     */
    @Override
    protected boolean isAsyncFutureWork(List<?> resultsFromUserFunction) {
        for (Object object : resultsFromUserFunction) {
            return (object instanceof Future);
        }
        return false;
    }

    /**
     * Close the concurrent Vertx consumer system.
     * <p>
     * This is the single method every {@link bz.stub.parallelconsumer.internal.DrainingCloseable} entry
     * point resolves to: the no-argument {@code close()}, {@code closeDrainFirst()} and
     * {@code closeDontDrainFirst()} default methods call {@code close(DrainingMode)} directly, and
     * {@link AbstractParallelEoSStreamProcessor#close(Duration, DrainingMode)} records the caller's timeout
     * in a field and then delegates to {@code close(DrainingMode)} too - so overriding only this overload,
     * rather than the {@link Duration}-taking one, is what makes every entry point release the Vert.x
     * engine. (Overriding both would double-run this teardown: the base class's {@code close(Duration,
     * DrainingMode)} calls {@code close(drainMode)} internally, which dispatches virtually back to
     * whichever override is more derived.)
     * <p>
     * The Vert.x teardown runs whether or not {@code super.close(...)} threw, so a failing shutdown still releases
     * what this processor owns rather than stranding it - but never at the price of the diagnosis. A teardown
     * failure while {@code super.close(...)} is already failing is attached to that failure as suppressed, so the
     * caller sees the real shutdown error with the teardown's underneath it; only when there is no close failure
     * to outrank it is the teardown's own exception thrown. That is the guard
     * {@link AbstractParallelEoSStreamProcessor}'s own {@code doClose} puts around each step of its
     * {@code finally}, applied one level up - written as a sequence rather than a {@code finally} because a throw
     * from a {@code finally} is exactly the replacing shape it exists to prevent.
     *
     * @param drainMode wait for messages already consumed from the broker to be processed before closing
     */
    @SneakyThrows
    @Override
    public void close(DrainingMode drainMode) {
        log.info("Vert.x async consumer closing...");
        Throwable closeFailure = null;
        try {
            super.close(drainMode);
        } catch (Throwable closeThrew) {
            closeFailure = closeThrew;
        }

        try {
            releaseOwnedVertxEngine(getShutdownTimeout());
        } catch (Throwable teardownThrew) {
            if (closeFailure == null) {
                throw teardownThrew;
            }
            log.warn("Releasing the Vert.x engine failed while close was already failing - attached to the close " +
                    "failure as suppressed, so it does not replace it. Cause: {}", describeWithRootCause(teardownThrew));
            closeFailure.addSuppressed(teardownThrew);
        }

        if (closeFailure != null) {
            throw closeFailure;
        }
    }

    /**
     * Releases whatever Vert.x resources this processor built - see {@link #ownsVertx} and
     * {@link #ownsWebClient} - and leaves anything the caller supplied running for them to close. The wait for the
     * engine to close is bounded by {@code timeout}: the caller's own {@link Duration} when they closed through
     * {@link AbstractParallelEoSStreamProcessor#close(Duration, DrainingMode)}, and the configured
     * {@link ParallelConsumerOptions#getShutdownTimeout()} otherwise.
     */
    private void releaseOwnedVertxEngine(Duration timeout) throws InterruptedException, TimeoutException {
        if (ownsWebClient) {
            webClient.close();
        } else {
            log.debug("The WebClient was supplied by the caller - leaving it open for them to close");
        }
        if (!ownsVertx) {
            log.debug("The Vertx engine was supplied by the caller - leaving it running for them to close");
            return;
        }
        Future<Void> close = vertx.close();
        var timer = Time.SYSTEM.timer(timeout);
        while (!close.isComplete()) {
            log.trace("Waiting on close to complete");
            Thread.sleep(100);
            timer.update();
            if (timer.isExpired()) {
                throw new TimeoutException("Timed out after " + timeout + " waiting for the Vert.x engine to close");
            }
        }
    }

}
