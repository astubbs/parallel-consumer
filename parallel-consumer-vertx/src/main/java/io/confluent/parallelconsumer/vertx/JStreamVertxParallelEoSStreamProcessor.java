package io.confluent.parallelconsumer.vertx;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 * Modifications Copyright (C) 2026 Antony Stubbs and contributors
 */

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
import io.confluent.parallelconsumer.internal.DrainingCloseable.DrainingMode;
import io.confluent.parallelconsumer.internal.JStreamResultBuffer;

import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

import static io.confluent.parallelconsumer.internal.UserFunctions.carefullyRun;

/**
 * @deprecated Superseded by the callback-based API, which does the same job without a result buffer.
 * The buffer here is now bounded and applies backpressure (see {@link JStreamResultBuffer}), so it no
 * longer leaks, but it remains a second way to do what {@link VertxParallelEoSStreamProcessor} already does.
 * See <a href="https://github.com/confluentinc/parallel-consumer/issues/912">confluentinc/parallel-consumer#912</a>.
 */
@Slf4j
@Deprecated
public class JStreamVertxParallelEoSStreamProcessor<K, V> extends VertxParallelEoSStreamProcessor<K, V>
        implements JStreamVertxParallelStreamProcessor<K, V> {

    private final JStreamResultBuffer<VertxCPResult<K, V>> results;

    /**
     * Provide your own instances of the Vertx engine and it's webclient.
     * <p>
     * Use this to share a Vertx runtime with different systems for efficiency.
     */
    public JStreamVertxParallelEoSStreamProcessor(Vertx vertx,
                                                  WebClient webClient,
                                                  ParallelConsumerOptions<K, V> options) {
        this(vertx, webClient, options, JStreamResultBuffer.DEFAULT_CAPACITY);
    }

    /**
     * @param resultBufferCapacity how many unconsumed results to hold before the producer is made to wait
     */
    public JStreamVertxParallelEoSStreamProcessor(Vertx vertx,
                                                  WebClient webClient,
                                                  ParallelConsumerOptions<K, V> options,
                                                  int resultBufferCapacity) {
        super(vertx, webClient, options);

        this.results = new JStreamResultBuffer<>(resultBufferCapacity);
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

        return results.getStream();
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
        return results.getStream();
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

        return results.getStream();
    }

    private void addResultAndWarnIfBacklogged(VertxCPResult<K, V> result) {
        results.add(result);
    }

    /**
     * Ends the result stream once shutdown completes, so a consuming {@code forEach} returns.
     * <p>
     * Overrides the {@link DrainingMode}-taking close, which is the single method every other entry point
     * funnels through - see the sibling override in {@code JStreamParallelEoSStreamProcessor}. The close
     * happens after shutdown, since a draining close keeps producing results the consumer should receive.
     *
     * @see <a href="https://github.com/confluentinc/parallel-consumer/issues/912">confluentinc/parallel-consumer#912</a>
     */
    @Override
    public void close(DrainingMode drainMode) {
        super.close(drainMode);
        results.close();
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
