package io.confluent.parallelconsumer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.errors.ProducerFencedException;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

/**
 * todo docs
 * All methods must be multi thread safe.
 */
@SuppressWarnings("ClassWithTooManyMethods")
@Slf4j
@RequiredArgsConstructor
public class ProducerFacade<K, V> implements Producer<K, V> {

//    @Delegate
//    Producer<K, V> facade;

    @Override
    public Future<RecordMetadata> send(final ProducerRecord record) {
        return null;
    }

    @Override
    public Future<RecordMetadata> send(final ProducerRecord record, final Callback callback) {
        return null;
    }

    @Override
    public void flush() {

    }

    @Override
    public List<PartitionInfo> partitionsFor(final String topic) {
        return null;
    }

    @Override
    public Map<MetricName, ? extends Metric> metrics() {
        return null;
    }

    private void throwNotAllowedException() {
        throw new IllegalStateException("Method not allowed");
    }

    // no ops
    @Override
    public void initTransactions() {
        throwNotAllowedException();
    }

    @Override
    public void beginTransaction() throws ProducerFencedException {
        throwNotAllowedException();
    }

    @Override
    public void commitTransaction() throws ProducerFencedException {
        throwNotAllowedException();
    }

    @Override
    public void abortTransaction() throws ProducerFencedException {
        throwNotAllowedException();
    }

    @Override
    public void close() {
        throwNotAllowedException();
    }

    @Override
    public void close(final Duration timeout) {
        throwNotAllowedException();
    }

    @Override
    public void sendOffsetsToTransaction(final Map offsets, final ConsumerGroupMetadata groupMetadata) throws ProducerFencedException {
        throwNotAllowedException();
    }

    @Override
    public void sendOffsetsToTransaction(final Map offsets, final String consumerGroupId) throws ProducerFencedException {
        throwNotAllowedException();
    }
}
