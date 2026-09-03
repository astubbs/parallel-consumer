package bz.stub.parallelconsumer.vertx;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 * Modifications Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.internal.utils.Java8StreamUtils;
import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.PollContext;
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

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

import static bz.stub.parallelconsumer.internal.UserFunctions.carefullyRun;

@Slf4j
public class JStreamVertxParallelEoSStreamProcessor<K, V> extends VertxParallelEoSStreamProcessor<K, V>
        implements JStreamVertxParallelStreamProcessor<K, V> {

    /**
     * The stream of results, drained from {@link #userProcessResultsStream} as they arrive and ending when
     * this processor closes.
     */
    private final Stream<VertxCPResult<K, V>> stream;

    /**
     * The queue of results, filled by the worker threads and drained by whoever consumes the stream.
     */
    private final BlockingQueue<VertxCPResult<K, V>> userProcessResultsStream;

    /**
     * Provide your own instances of the Vertx engine and it's webclient.
     * <p>
     * Use this to share a Vertx runtime with different systems for efficiency.
     */
    public JStreamVertxParallelEoSStreamProcessor(Vertx vertx,
                                                  WebClient webClient,
                                                  ParallelConsumerOptions<K, V> options) {
        super(vertx, webClient, options);

        this.userProcessResultsStream = new LinkedBlockingQueue<>();

        this.stream = Java8StreamUtils.setupStreamFromQueue(this.userProcessResultsStream, this::isClosedOrFailed);
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
            userProcessResultsStream.add(build);
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
            userProcessResultsStream.add(build);
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
            userProcessResultsStream.add(build);
        };

        super.vertxHttpWebClient(wrappedFunc, onSendCallBack);

        return stream;
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
