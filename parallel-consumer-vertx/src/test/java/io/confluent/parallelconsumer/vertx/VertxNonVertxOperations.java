package io.confluent.parallelconsumer.vertx;

/*-
 * Copyright (C) 2020 Confluent, Inc.
 */

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.ParallelEoSStreamProcessor;
import io.confluent.parallelconsumer.ParallelEoSStreamProcessorTest;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.ext.web.client.WebClient;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

import java.util.function.Consumer;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;

/**
 * Ensure all plain operations from {@link ParallelEoSStreamProcessorTest} still work with the extended vertx consumer
 */
@Slf4j
public class VertxNonVertxOperations extends ParallelEoSStreamProcessorTest {

    VertxParallelEoSStreamProcessor<String, String> vertxPC;
    static WireMockServer wm;

    @Override
    protected ParallelEoSStreamProcessor initAsyncConsumer(ParallelConsumerOptions parallelConsumerOptions) {
        VertxOptions vertxOptions = new VertxOptions();
        Vertx vertx = Vertx.vertx(vertxOptions);
        vertxPC = new VertxParallelEoSStreamProcessor<>(vertx, WebClient.create(vertx), parallelConsumerOptions);
        this.parallelConsumer = vertxPC;
        return this.parallelConsumer;
    }

    @BeforeAll
    static void startWM() {
        WireMockConfiguration options = wireMockConfig().dynamicPort()
                .containerThreads(10);
        wm = new WireMockServer(options);
        wm.stubFor(get(urlPathEqualTo("/" ))
                .willReturn(aResponse()));
        wm.addMockServiceRequestListener((request, response) ->
                log.debug("req: {}", request)
        );
        wm.start();
    }

    @AfterAll
    static void stopWM() {
        wm.stop();
    }

    @Override
    protected void runThingy(final Consumer<ConsumerRecord<String, String>> end_of_message_processing) {
//        vertxPC.vertxHttpReqInfo(stringStringConsumerRecord -> {
//            end_of_message_processing.accept(stringStringConsumerRecord);

//            return new VertxParallelEoSStreamProcessor.RequestInfo("localhost", wm.port(), "/", UniMaps.of());
//        }, httpResponseFuture -> {
//        }, result -> {
//            log.warn("{} succeeded: {}", end_of_message_processing, result.succeeded());
//            log.warn("complete: {}", result);
//        });
//        parallelConsumer. (end_of_message_processing);
        vertxPC.poll(end_of_message_processing);
    }

}
