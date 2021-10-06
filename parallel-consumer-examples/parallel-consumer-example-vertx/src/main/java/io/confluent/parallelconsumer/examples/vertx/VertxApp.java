package io.confluent.parallelconsumer.examples.vertx;

/*-
 * Copyright (C) 2020-2021 Confluent, Inc.
 */

import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.vertx.JStreamVertxParallelStreamProcessor;
import io.confluent.parallelconsumer.vertx.VertxParallelEoSStreamProcessor.RequestInfo;
import io.confluent.parallelconsumer.vertx.VertxParallelStreamProcessor;
import io.github.bucket4j.AsyncVerboseBucket;
import io.github.bucket4j.Bandwidth;
import io.github.bucket4j.Bucket;
import io.github.bucket4j.Bucket4j;
import io.vertx.core.Vertx;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import pl.tlinkowski.unij.api.UniMaps;

import java.time.Duration;
import java.util.Map;
import java.util.Properties;

import static pl.tlinkowski.unij.api.UniLists.of;

@Slf4j
public class VertxApp {

  static String inputTopic = "input-topic-" + RandomUtils.nextInt();

  Consumer<String, String> getKafkaConsumer() {
    return new KafkaConsumer<>(new Properties());
  }

  Producer<String, String> getKafkaProducer() {
    return new KafkaProducer<>(new Properties());
  }

  JStreamVertxParallelStreamProcessor<String, String> streamingParallelConsumer;


  void run() {
    Consumer<String, String> kafkaConsumer = getKafkaConsumer();
    Producer<String, String> kafkaProducer = getKafkaProducer();
    var options = ParallelConsumerOptions.<String, String>builder()
            .ordering(ParallelConsumerOptions.ProcessingOrder.KEY)
            .consumer(kafkaConsumer)
            .producer(kafkaProducer)
            .build();

    this.streamingParallelConsumer = JStreamVertxParallelStreamProcessor.createEosStreamProcessor(options);
    streamingParallelConsumer.subscribe(of(inputTopic));

    Bandwidth limit = Bandwidth.simple(100, Duration.ofMinutes(1));
    Bandwidth lowerLimit = Bandwidth.simple(10, Duration.ofSeconds(1));


    Bucket bucket = Bucket4j.builder()
            .addLimit(limit)
            .addLimit(lowerLimit)
            .build();
    AsyncVerboseBucket asyncBucket = bucket.asAsync().asVerbose();

    Vertx vertx = Vertx.vertx();

    VertxParallelStreamProcessor<String, String> parallelConsumer = VertxParallelStreamProcessor.createEosStreamProcessor(options);
//    parallelConsumer.vertxFuture(stringStringConsumerRecord -> {
//
//      CompletableFuture<VerboseResult<Boolean>> verboseResultCompletableFuture = asyncBucket.tryConsume(1);
//      final Context context = Vertx.currentContext();
//      verboseResultCompletableFuture.whenComplete((res, error) -> {
//        if (context == Vertx.currentContext()) {
//          verboseResultCompletableFuture.complete(res, error);
//        } else {
//          context.runOnContext(v -> verboseResultCompletableFuture.complete(result, error));
//        }
//      });
//      return verboseResultCompletableFuture.handleAsync((booleanVerboseResult, throwable) -> {
//        if (booleanVerboseResult.getValue()) {
//          return vertx.createHttpClient().request(HttpMethod.GET, "");
//        } else throw new RuntimeException("Rate limit exceeded");
//      });
//    });

    postSetup();

    int port = getPort();

    // tag::example[]
    var resultStream = streamingParallelConsumer.vertxHttpReqInfoStream(record -> {
      log.info("Concurrently constructing and returning RequestInfo from record: {}", record);
      Map<String, String> params = UniMaps.of("recordKey", record.key(), "payload", record.value());
      return new RequestInfo("localhost", port, "/api", params); // <1>
    });
    // end::example[]

    resultStream.forEach(x -> {
      log.info("From result stream: {}", x);
    });

  }

  protected int getPort() {
    return 8080;
  }

  void close() {
    this.streamingParallelConsumer.closeDrainFirst();
  }

  protected void postSetup() {
    // no-op, for testing
  }

}
