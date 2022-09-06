module parallel.consumer.example.reactor {
    requires kafka.clients;
    requires org.apache.commons.lang3;
    requires org.reactivestreams;
    requires pl.tlinkowski.unij.api;
    requires reactor.core;
    requires wiremock.jre8.standalone;
    requires parallel.consumer.core;
    requires parallel.consumer.reactor;
}