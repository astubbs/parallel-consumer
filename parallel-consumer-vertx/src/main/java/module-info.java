module parallel.consumer.vertx {
    requires io.vertx.core;
    requires io.vertx.web.client;
    requires kafka.clients;
    requires pl.tlinkowski.unij.api;
    requires wiremock.jre8.standalone;
    requires parallel.consumer.core;

    exports io.confluent.parallelconsumer.vertx;
}