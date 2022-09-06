module parallel.consumer.core {
    requires java.naming;
    requires com.github.luben.zstd_jni;
    requires kafka.clients;
    requires lombok;
    requires pl.tlinkowski.unij.api;
    requires snappy.java;
    requires wiremock.jre8.standalone;
    requires org.slf4j;

    exports io.confluent.csid.utils;
    exports io.confluent.parallelconsumer;
    exports io.confluent.parallelconsumer.internal;
    exports io.confluent.parallelconsumer.state;
}