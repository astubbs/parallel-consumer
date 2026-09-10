package bz.stub.parallelconsumer;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 * Modifications Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.fluent.ParallelConsumerDefinition;
import bz.stub.parallelconsumer.internal.AbstractParallelEoSStreamProcessor;
import bz.stub.parallelconsumer.internal.DrainingCloseable;
import lombok.Data;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.annotation.InterfaceStability;

import java.util.Collection;
import java.util.Properties;
import java.util.regex.Pattern;

// tag::javadoc[]
/**
 * Asynchronous / concurrent message consumer for Kafka.
 * <p>
 * Currently, there is no direct implementation, only the {@link ParallelStreamProcessor} version (see
 * {@link AbstractParallelEoSStreamProcessor}), but there may be in the future.
 *
 * @param <K> key consume / produce key type
 * @param <V> value consume / produce value type
 * @see AbstractParallelEoSStreamProcessor
 */
// end::javadoc[]
public interface ParallelConsumer<K, V> extends DrainingCloseable {

    /**
     * Begin a definition on the <b>fluent API</b>: connection properties in, one typed route per topic with its own
     * processing function and policy, and a handle out.
     * <p>
     * <b>Nothing is connected here.</b> The name follows Kafka's own client, which takes its configuration at
     * construction and reaches the cluster on its first poll: this call validates nothing and opens nothing, every
     * definition-time check runs when the routes are complete, and the consumer - and the producer, if the definition
     * needs one - is built when {@code start()} is called. A definition that is written and never started constructs
     * no client at all.
     * <p>
     * The fluent API ships beside the options-builder API above it as an equal - neither is deprecated, both are
     * documented, and this factory is the one addition the classic surface takes for it. The classic API remains the
     * right choice for a running application that needs nothing new; an existing user with hand-built clients can
     * pass them to a definition with {@code consumer(...)} and {@code producer(...)} rather than migrate.
     * <p>
     * <b>Incubating.</b> Everything the returned definition exposes is
     * {@link org.apache.kafka.common.annotation.InterfaceStability.Unstable} while the surface settles - see
     * {@link bz.stub.parallelconsumer.fluent} for what that means and when it changes.
     *
     * @param connectionProperties the consumer, and where needed producer, configuration; the facade sets the
     *                             serialisers itself, since each route applies its own
     * @see ParallelConsumerDefinition
     */
    @InterfaceStability.Unstable
    static ParallelConsumerDefinition connect(Properties connectionProperties) {
        return new ParallelConsumerDefinition(connectionProperties);
    }

    /**
     * @return true if the system has either closed, or has crashed
     */
    boolean isClosedOrFailed();

    /**
     * @see KafkaConsumer#subscribe(Collection)
     */
    void subscribe(Collection<String> topics);

    /**
     * @see KafkaConsumer#subscribe(Pattern)
     */
    void subscribe(Pattern pattern);

    /**
     * @see KafkaConsumer#subscribe(Collection, ConsumerRebalanceListener)
     */
    void subscribe(Collection<String> topics, ConsumerRebalanceListener callback);

    /**
     * @see KafkaConsumer#subscribe(Pattern, ConsumerRebalanceListener)
     */
    void subscribe(Pattern pattern, ConsumerRebalanceListener callback);

    /**
     * Pause this consumer (i.e. stop processing of messages).
     * <p>
     * This operation only has an effect if the consumer is currently running. In all other cases calling this method
     * will be silent a no-op.
     * <p>
     * Once the consumer is paused, the system will stop submitting work to the processing pool, and work that is
     * <b>already inside a user function</b> is finished.
     * <p>
     * Work that was submitted to the pool but has <b>not been picked up by a worker yet is handed back</b> rather
     * than started: the user function is not called for it, no failed attempt is counted against it, and it is
     * processed when the consumer resumes. Until 0.6, such a batch was started anyway, so a pause did not stop
     * processing for as long as the pool's queue took to empty.
     * <p>
     * General remarks:
     * <ul>
     * <li>A paused consumer may still keep polling for new work until internal buffers are filled.</li>
     * <li>This operation does not actively pause the subscription on the underlying Kafka Broker (compared to
     * {@link KafkaConsumer#pause KafkaConsumer#pause}).</li>
     * <li>Pending offset commits will still be performed when the consumer is paused.</li>
     * </p>
     */
    void pauseIfRunning();

    /**
     * Resume this consumer (i.e. continue processing of messages).
     * <p>
     * This operation only has an effect if the consumer is currently paused. In all other cases calling this method
     * will be a silent no-op.
     * </p>
     */
    void resumeIfPaused();

    /**
     * A simple tuple structure.
     *
     * @param <L>
     * @param <R>
     */
    @Data
    class Tuple<L, R> {
        private final L left;
        private final R right;

        public static <LL, RR> Tuple<LL, RR> pairOf(LL l, RR r) {
            return new Tuple<>(l, r);
        }
    }

}
