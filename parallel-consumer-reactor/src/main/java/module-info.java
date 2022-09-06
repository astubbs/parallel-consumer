module parallel.consumer.reactor {
    requires kafka.clients;
    requires org.reactivestreams;
    requires pl.tlinkowski.unij.api;
    requires reactor.core;
    requires wiremock.jre8.standalone;
    requires parallel.consumer.core;

    exports io.confluent.parallelconsumer.reactor;
}