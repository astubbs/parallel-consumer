package bz.stub.parallelconsumer.vertx;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 * Modifications Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumer;
import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.PollContext;
import io.vertx.core.Future;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.web.client.HttpRequest;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;

import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * Result streaming version of {@link VertxParallelEoSStreamProcessor}.
 * <p>
 * The stream is live and ends when this processor closes - see
 * {@link bz.stub.parallelconsumer.JStreamParallelStreamProcessor} for what that means for the consumer.
 *
 * @see <a href="https://github.com/astubbs/parallel-consumer/issues/122">astubbs#122</a>
 * @see <a href="https://github.com/confluentinc/parallel-consumer/issues/912">confluentinc#912</a>
 */
public interface JStreamVertxParallelStreamProcessor<K, V> extends ParallelConsumer<K, V> {

    static <KK, VV> JStreamVertxParallelStreamProcessor<KK, VV> createEosStreamProcessor(ParallelConsumerOptions<KK, VV> options) {
        return new JStreamVertxParallelEoSStreamProcessor<>(options);
    }

    /**
     * Streaming version
     *
     * @see VertxParallelEoSStreamProcessor#vertxHttpReqInfo
     */
    Stream<JStreamVertxParallelEoSStreamProcessor.VertxCPResult<K, V>> vertxHttpReqInfoStream(Function<PollContext<K, V>,
            VertxParallelEoSStreamProcessor.RequestInfo> requestInfoFunction);

    /**
     * Streaming version
     *
     * @see VertxParallelEoSStreamProcessor#vertxHttpRequest
     */
    Stream<JStreamVertxParallelEoSStreamProcessor.VertxCPResult<K, V>> vertxHttpRequestStream(BiFunction<WebClient,
            PollContext<K, V>, HttpRequest<Buffer>> webClientRequestFunction);

    /**
     * Streaming version
     *
     * @see VertxParallelEoSStreamProcessor#vertxHttpWebClient
     */
    Stream<JStreamVertxParallelEoSStreamProcessor.VertxCPResult<K, V>> vertxHttpWebClientStream(
            BiFunction<WebClient, PollContext<K, V>, Future<HttpResponse<Buffer>>> webClientRequestFunction);
}
