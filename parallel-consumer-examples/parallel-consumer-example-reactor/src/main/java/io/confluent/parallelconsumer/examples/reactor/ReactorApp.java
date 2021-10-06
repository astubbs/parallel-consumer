package io.confluent.parallelconsumer.examples.reactor;

/*-
 * Copyright (C) 2020-2021 Confluent, Inc.
 */

import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.reactor.ReactorProcessor;
import io.github.bucket4j.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import pl.tlinkowski.unij.api.UniMaps;
import reactor.core.publisher.Mono;

import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

import static pl.tlinkowski.unij.api.UniLists.of;

@Slf4j
public class ReactorApp {

  static String inputTopic = "input-topic-" + RandomUtils.nextInt();

  Consumer<String, String> getKafkaConsumer() {
    return new KafkaConsumer<>(new Properties());
  }

  Producer<String, String> getKafkaProducer() {
    return new KafkaProducer<>(new Properties());
  }

  ReactorProcessor<String, String> parallelConsumer;


  void run() {
    Consumer<String, String> kafkaConsumer = getKafkaConsumer();
    Producer<String, String> kafkaProducer = getKafkaProducer();
    var options = ParallelConsumerOptions.<String, String>builder()
            .ordering(ParallelConsumerOptions.ProcessingOrder.KEY)
            .consumer(kafkaConsumer)
            .producer(kafkaProducer)
            .build();

    this.parallelConsumer = new ReactorProcessor<>(options);
    parallelConsumer.subscribe(of(inputTopic));

    Bandwidth limit = Bandwidth.simple(100, Duration.ofMinutes(1));
    Bandwidth lowerLimit = Bandwidth.simple(10, Duration.ofSeconds(1));


    Bucket bucket = Bucket4j.builder()
            .addLimit(limit)
            .addLimit(lowerLimit)
            .build();
    AsyncVerboseBucket asyncBucket = bucket.asAsync().asVerbose();

    postSetup();

    int port = getPort();
    HttpClient httpClient = HttpClient.newHttpClient();

    // tag::example[]
    parallelConsumer.react(record -> {
      log.info("Concurrently constructing and returning RequestInfo from record: {}", record);
      Map<String, String> params = UniMaps.of("recordKey", record.key(), "payload", record.value());

      CompletableFuture<VerboseResult<Boolean>> verboseResultCompletableFuture = asyncBucket.tryConsume(1);

      Mono<VerboseResult<Boolean>> verboseResultMono = Mono.fromFuture(verboseResultCompletableFuture);

      return verboseResultMono.flux().map(booleanVerboseResult -> {
        boolean tokenAvailable = Boolean.TRUE.equals(booleanVerboseResult.getValue());
        if (tokenAvailable) {

//          return Mono.just("Call webservice");

          return httpClient.sendAsync(HttpRequest.newBuilder().build(), HttpResponse.BodyHandlers.ofString())
                  .thenAcceptAsync(stringHttpResponse -> {
                    HttpRequest request = stringHttpResponse.request();
                    String body = stringHttpResponse.body();
                    log.info("Do something with req {} and response body {}", request, body);
                  });
        } else {
          return Mono.error(new RateLimitExceeded());
        }
      });

    });
    // end::example[]

  }

  static class RateLimitExceeded extends RuntimeException {
  }

  protected int getPort() {
    return 8080;
  }

  void close() {
    this.parallelConsumer.closeDrainFirst();
  }

  protected void postSetup() {
    // no-op, for testing
  }

}
