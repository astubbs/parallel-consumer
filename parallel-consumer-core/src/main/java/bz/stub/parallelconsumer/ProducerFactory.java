package bz.stub.parallelconsumer;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;

import java.util.Map;

/**
 * Builds the {@link Producer} Parallel Consumer uses for the produce flows, from configuration PC resolved.
 * <p>
 * PC owns the producer's lifecycle: it builds one at start-up, and when the broker reports that producer invalid
 * (fenced, an expired producer id, a stale epoch, a lost group generation) it discards it and asks this factory for
 * another. That is only possible because PC holds the configuration rather than a finished instance, which is why
 * this replaces {@link ParallelConsumerOptions#getProducer()}.
 * <p>
 * <b>Contract.</b> Every call returns a <em>new</em> producer built from the map it is given, with every key
 * present - {@code transactional.id} above all. PC derives that id (see {@link ParallelConsumerOptions#getProducerConfig()}
 * for how) and reuses it for every replacement, so that re-initialising the replacement fences the producer it
 * replaces; a factory that drops or changes the id disables that fencing and voids the TransactionalId ACL prefix the
 * derived id was designed to fit. A factory that returns a cached or previously returned instance is rejected.
 * Wrapping, instrumenting or substituting the producer is what overriding this is for; the configuration is not
 * negotiable.
 *
 * @param <K> key type
 * @param <V> value type
 * @see ParallelConsumerOptions.ParallelConsumerOptionsBuilder#producerFactory(ProducerFactory)
 */
@FunctionalInterface
public interface ProducerFactory<K, V> {

    /**
     * @param resolvedConfig the producer configuration PC resolved - the caller's {@code producerConfig} plus the
     *                       {@code transactional.id} PC derived (transactional commit mode), or minus any
     *                       {@code transactional.id} (every other mode). Never mutated by PC after this call.
     * @return a newly constructed producer built from exactly that configuration
     */
    Producer<K, V> create(Map<String, Object> resolvedConfig);

    /**
     * The default: a {@link KafkaProducer} built from the configuration, serializers included, exactly as
     * {@code new KafkaProducer<>(config)} would.
     */
    static <K, V> ProducerFactory<K, V> kafkaProducer() {
        return KafkaProducer::new;
    }
}
