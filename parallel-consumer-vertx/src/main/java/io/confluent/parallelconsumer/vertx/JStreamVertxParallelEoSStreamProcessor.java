package io.confluent.parallelconsumer.vertx;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 * Modifications Copyright (C) 2026 Antony Stubbs and contributors
 */

import io.confluent.csid.utils.Java8StreamUtils;
import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.PollContext;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.web.client.HttpRequest;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.Optional;
import io.confluent.parallelconsumer.internal.JStreamResultDeques;

import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

import static io.confluent.parallelconsumer.internal.UserFunctions.carefullyRun;

/**
 * @deprecated Being removed — the JStream interface is not widely used and its unbounded result deque
 * can cause memory leaks if the stream is not actively consumed. Use the callback-based API instead.
 * See <a href="https://github.com/confluentinc/parallel-consumer/issues/912">confluentinc/parallel-consumer#912</a>.
 */
@Slf4j
@Deprecated
public class JStreamVertxParallelEoSStreamProcessor<K, V> extends VertxParallelEoSStreamProcessor<K, V>
        implements JStreamVertxParallelStreamProcessor<K, V> {

    /**
     * The stream of results, constructed from the Queue {@link #userProcessResultsStream}.
     * <p>
     * <b>WARNING:</b> This stream MUST be actively consumed in a separate thread, or results
     * will accumulate in memory indefinitely. If you don't need the result stream, use the
     * callback-based API instead.
     *
     * @see <a href="https://github.com/confluentinc/parallel-consumer/issues/912">confluentinc/parallel-consumer#912</a>
     */
    private final Stream<VertxCPResult<K, V>> stream;

    /**
     * The Queue of results. Unbounded — will grow indefinitely if the stream is not consumed.
     */
    private final ConcurrentLinkedDeque<VertxCPResult<K, V>> userProcessResultsStream;

    private final AtomicLong resultsAdded = new AtomicLong();

    /**
     * Provide your own instances of the Vertx engine and it's webclient.
     * <p>
     * Use this to share a Vertx runtime with different systems for efficiency.
     */
    public JStreamVertxParallelEoSStreamProcessor(Vertx vertx,
                                                  WebClient webClient,
                                                  ParallelConsumerOptions<K, V> options) {
        super(vertx, webClient, options);

        this.userProcessResultsStream = new ConcurrentLinkedDeque<>();

        this.stream = Java8StreamUtils.setupStreamFromDeque(this.userProcessResultsStream);
    }

    /**
     * Simple constructor. Internal Vertx objects will be created.
     */
    public JStreamVertxParallelEoSStreamProcessor(ParallelConsumerOptions<K, V> options) {
        this(null, null, options);
    }

    @Override
    public Stream<VertxCPResult<K, V>> vertxHttpReqInfoStream(Function<PollContext<K, V>, RequestInfo> requestInfoFunction) {

        VertxCPResult.VertxCPResultBuilder<K, V> result = VertxCPResult.builder();

        Function<PollContext<K, V>, RequestInfo> requestInfoFunctionWrapped = x -> {
            result.in(x);
            RequestInfo apply = carefullyRun(requestInfoFunction, x);
            result.requestInfo(Optional.of(apply));
            return apply;
        };

        Consumer<Future<HttpResponse<Buffer>>> onSendCallBack = future -> {
            // stream
            result.asr(future);
            VertxCPResult<K, V> build = result.build();
            addResultAndWarnIfBacklogged(build);
        };

        super.vertxHttpReqInfo(requestInfoFunctionWrapped, onSendCallBack, (ignore) -> {
        });

        return stream;
    }

    @Override
    public Stream<VertxCPResult<K, V>> vertxHttpRequestStream(BiFunction<WebClient, PollContext<K, V>, HttpRequest<Buffer>> webClientRequestFunction) {

        VertxCPResult.VertxCPResultBuilder<K, V> result = VertxCPResult.builder();

        BiFunction<WebClient, PollContext<K, V>, HttpRequest<Buffer>> requestInfoFunctionWrapped = (wc, x) -> {
            result.in(x);
            HttpRequest<Buffer> apply = carefullyRun(webClientRequestFunction, wc, x);

            result.httpReq(Optional.of(apply));
            return apply;
        };

        Consumer<Future<HttpResponse<Buffer>>> onSendCallBack = future -> {
            // stream
            result.asr(future);
            VertxCPResult<K, V> build = result.build();
            addResultAndWarnIfBacklogged(build);
        };

        super.vertxHttpRequest(requestInfoFunctionWrapped, onSendCallBack, (ignore) -> {
        });
        return stream;
    }

    @Override
    public Stream<VertxCPResult<K, V>> vertxHttpWebClientStream(
            BiFunction<WebClient, PollContext<K, V>, Future<HttpResponse<Buffer>>> webClientRequestFunction) {

        VertxCPResult.VertxCPResultBuilder<K, V> result = VertxCPResult.builder();

        BiFunction<WebClient, PollContext<K, V>, Future<HttpResponse<Buffer>>> wrappedFunc = (x, y) -> {
            // capture
            result.in(y);
            Future<HttpResponse<Buffer>> apply = carefullyRun(webClientRequestFunction, x, y);

            result.asr(apply);
            return apply;
        };

        Consumer<Future<HttpResponse<Buffer>>> onSendCallBack = future -> {
            // stream
            result.asr(future);
            VertxCPResult<K, V> build = result.build();
            addResultAndWarnIfBacklogged(build);
        };

        super.vertxHttpWebClient(wrappedFunc, onSendCallBack);

        return stream;
    }

    private void addResultAndWarnIfBacklogged(VertxCPResult<K, V> result) {
        JStreamResultDeques.addAndWarnIfBacklogged(userProcessResultsStream, resultsAdded, result);
    }

    /**
     * Clears any unconsumed results from the deque on close to prevent memory leaks.
     *
     * @see <a href="https://github.com/confluentinc/parallel-consumer/issues/912">confluentinc/parallel-consumer#912</a>
     */
    @Override
    public void close() {
        JStreamResultDeques.clearOnClose(userProcessResultsStream);
        super.close();
    }

    /**
     * @param <K>
     * @param <V>
     * @see ConsumeProduceResult
     */
    @Getter
    @Builder
    public static class VertxCPResult<K, V> {
        private final PollContext<K, V> in;
        private final Future<HttpResponse<Buffer>> asr;

        // todo change to class generic type variables? 2 fields become 1. Not worth the hassle atm.
        // Currently our vertx usage has two types of results. This is a quick way to model that. Should consider improving.
        @Builder.Default
        private final Optional<RequestInfo> requestInfo = Optional.empty();
        @Builder.Default
        private final Optional<HttpRequest<Buffer>> httpReq = Optional.empty();
    }

}
