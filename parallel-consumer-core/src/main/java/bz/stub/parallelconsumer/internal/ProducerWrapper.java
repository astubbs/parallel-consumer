package bz.stub.parallelconsumer.internal;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 * Modifications Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions;
import lombok.*;
import lombok.experimental.Delegate;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.internals.TransactionManager;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ProducerFencedException;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;

import static bz.stub.parallelconsumer.internal.ProducerWrapper.ProducerState.*;
import static bz.stub.parallelconsumer.internal.utils.StringUtils.msg;

/**
 * Our extension of the standard Producer to mostly add some introspection functions and state tracking.
 *
 * @author Antony Stubbs
 */
@Slf4j
@RequiredArgsConstructor
public class ProducerWrapper<K, V> implements Producer<K, V> {

    /**
     * Used to track Producer's transaction state, as it' isn't otherwise exposed.
     */
    public enum ProducerState {
        INSTANTIATED, INIT, BEGIN, COMMIT, ABORT, CLOSE
    }

    /**
     * Tracks the internal transaction state of the Prodocer
     */
    @ToString.Include
    @Getter
    private volatile ProducerState producerState = ProducerState.INSTANTIATED;


    @NonNull
    private final ParallelConsumerOptions<K, V> options;

    /**
     * Cached discovery of whether the underlying Producer has been set up for transactions or not.
     */
    private final boolean producerIsConfiguredForTransactions;

    // nasty reflection
    private Field txManagerField;
    private Method txManagerMethodIsCompleting;
    private Method txManagerMethodIsReady;

    @NonNull
    @Delegate(excludes = Excludes.class)
    private final Producer<K, V> producer;

    /**
     * The producer-instance path: wraps the caller's finished producer and discovers whether it is transactional.
     */
    @SuppressWarnings("deprecation") // the instance option is deprecated, and this is the one place that reads it
    public ProducerWrapper(ParallelConsumerOptions<K, V> options) {
        this(options, options.getProducer(), Optional.empty());
    }

    /**
     * The PC-built path: wraps a producer the {@link bz.stub.parallelconsumer.ProducerFactory} built from
     * configuration PC resolved, so whether it is transactional is already known - and checked. Discovery still runs
     * against a {@link KafkaProducer}, and a producer whose discovered flag disagrees with the configuration PC handed
     * the factory is rejected here, at construction, rather than failing at its first transactional call.
     *
     * @param expectedTransactional whether the configuration the factory received carried a {@code transactional.id}
     * @throws ProducerFactoryContractException when a {@link KafkaProducer} was not built from that configuration
     */
    public static <K, V> ProducerWrapper<K, V> forPcBuilt(ParallelConsumerOptions<K, V> options,
                                                          Producer<K, V> producer,
                                                          boolean expectedTransactional) {
        return new ProducerWrapper<>(options, producer, Optional.of(expectedTransactional));
    }

    private ProducerWrapper(ParallelConsumerOptions<K, V> options, Producer<K, V> producer, Optional<Boolean> expectedTransactional) {
        this.options = options;
        this.producer = producer;
        boolean discovered = discoverIfProducerIsConfiguredForTransactions();
        if (expectedTransactional.isPresent() && producer instanceof KafkaProducer && discovered != expectedTransactional.get()) {
            throw new ProducerFactoryContractException(msg("The ProducerFactory returned a KafkaProducer that is {} " +
                            "transactional, but the configuration PC handed it {} a {}. A factory must build the " +
                            "producer from the map it is given, with {} unaltered: dropping or changing it disables " +
                            "the fencing a replacement producer relies on and voids the TransactionalId ACL prefix.",
                    discovered ? "" : "not",
                    expectedTransactional.get() ? "carried" : "did not carry",
                    ProducerConfig.TRANSACTIONAL_ID_CONFIG,
                    ProducerConfig.TRANSACTIONAL_ID_CONFIG));
        }
        // a KafkaProducer's discovery is authoritative and already agrees with the expectation; for any other type
        // discovery is a guess (MockProducer defers to the options, unknown types report false), so the configuration
        // PC handed the factory is the better answer where one was given
        this.producerIsConfiguredForTransactions = expectedTransactional.isPresent() && !(producer instanceof KafkaProducer)
                ? expectedTransactional.get()
                : discovered;
    }

    public boolean isMockProducer() {
        return producer instanceof MockProducer;
    }

    public boolean isConfiguredForTransactions() {
        return this.producerIsConfiguredForTransactions;
    }

    /**
     * Type erasure issue fix
     */
    interface Excludes {
        void sendOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata> offsets,
                                      String consumerGroupId) throws ProducerFencedException;

        void sendOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata> offsets,
                                      ConsumerGroupMetadata groupMetadata) throws ProducerFencedException;
    }

    /**
     * @deprecated use {@link #sendOffsetsToTransaction(Map, ConsumerGroupMetadata)}
     */
    @Deprecated
    public void sendOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata> offsets,
                                         String consumerGroupId) throws ProducerFencedException {
        sendOffsetsToTransaction(offsets, new ConsumerGroupMetadata(consumerGroupId));
    }

    public void sendOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata> offsets,
                                         ConsumerGroupMetadata groupMetadata) throws ProducerFencedException {
        producer.sendOffsetsToTransaction(offsets, groupMetadata);
    }


    /**
     * @return boolean which shows if we are set up for transactions or not
     */
    @SneakyThrows
    private boolean discoverIfProducerIsConfiguredForTransactions() {
        if (producer instanceof KafkaProducer) {
            txManagerField = producer.getClass().getDeclaredField("transactionManager");
            txManagerField.setAccessible(true);

            boolean producerIsConfiguredForTransactions = getProducerIsTransactional();
            if (producerIsConfiguredForTransactions) {
                TransactionManager transactionManager = getTransactionManager();
                txManagerMethodIsCompleting = transactionManager.getClass().getDeclaredMethod("isCompleting");
                txManagerMethodIsCompleting.setAccessible(true);

                txManagerMethodIsReady = transactionManager.getClass().getDeclaredMethod("isReady");
                txManagerMethodIsReady.setAccessible(true);
            }
            return producerIsConfiguredForTransactions;
        } else if (producer instanceof MockProducer) {
            // can act as both, delegate to user selection
            return options.isUsingTransactionalProducer();
        } else {
            // unknown
            return false;
        }
    }

    /**
     * Nasty reflection but better than relying on user supplying a copy of their config, maybe
     *
     * @see AbstractParallelEoSStreamProcessor#checkAutoCommitIsDisabled
     */
    @SneakyThrows
    private boolean getProducerIsTransactional() {
        if (producer instanceof MockProducer) {
            // can act as both, delegate to user selection
            return options.isUsingTransactionalProducer();
        } else {
            TransactionManager transactionManager = getTransactionManager();
            if (transactionManager == null) {
                return false;
            } else {
                return transactionManager.isTransactional();
            }
        }
    }

    @SneakyThrows
    private TransactionManager getTransactionManager() {
        if (txManagerField == null) return null;
        TransactionManager transactionManager = (TransactionManager) txManagerField.get(producer);
        return transactionManager;
    }

    @SneakyThrows
    protected boolean isTransactionCompleting() {
        if (producer instanceof MockProducer) return false;
        return (boolean) txManagerMethodIsCompleting.invoke(getTransactionManager());
    }

    @SneakyThrows
    protected boolean isTransactionReady() {
        if (producer instanceof MockProducer) return true;
        return (boolean) txManagerMethodIsReady.invoke(getTransactionManager());
    }

    @Override
    public void initTransactions() {
        producer.initTransactions();
        this.producerState = INIT;
    }

    @Override
    public void beginTransaction() throws ProducerFencedException {
        producer.beginTransaction();
        this.producerState = BEGIN;
    }

    @Override
    public void commitTransaction() throws ProducerFencedException {
        log.debug("Committing transaction...");
        producer.commitTransaction();
        this.producerState = COMMIT;
    }

    @Override
    public void abortTransaction() throws ProducerFencedException {
        producer.abortTransaction();
        this.producerState = ABORT;
    }

    @Override
    public void close() {
        producer.close();
        this.producerState = CLOSE;
    }

    @Override
    public void close(final Duration timeout) {
        producer.close(timeout);
        this.producerState = CLOSE;
    }

    /**
     * According to our state tracking, does the Producer have an open transaction
     *
     * @return true if there's an open transaction
     */
    public boolean isTransactionOpen() {
        return this.producerState.equals(BEGIN);
    }
}
