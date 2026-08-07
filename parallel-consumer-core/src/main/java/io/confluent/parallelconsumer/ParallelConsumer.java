package io.confluent.parallelconsumer;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 * Modifications Copyright (C) 2026 Antony Stubbs and contributors
 */

import io.confluent.parallelconsumer.internal.AbstractParallelEoSStreamProcessor;
import io.confluent.parallelconsumer.internal.DrainingCloseable;
import lombok.Data;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collection;
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
     * @return true if the system has either closed, or has crashed
     */
    boolean isClosedOrFailed();

    /**
     * A snapshot of the health of this instance, taken now - the cast-free way to ask whether this consumer needs
     * restarting.
     * <p>
     * Unlike {@link #isClosedOrFailed()}, which collapses a clean shutdown and a crash into one boolean, the snapshot
     * reports the controller's run {@link State}, the broker poller's run {@link State}, and the failure cause
     * separately, plus a single derived verdict in {@link PCHealth#isHealthy()}.
     * <p>
     * <strong>A healthy verdict means "not shut down and not failed" - it does not mean the instance is making
     * progress.</strong> See {@link PCHealth} for what the verdict does and does not claim, and for the {@code pc.*}
     * Micrometer meters that show progress.
     *
     * <h2>This default implementation is a coarse fallback</h2>
     * <p>
     * The default exists purely so that adding this method does not break third-party implementors of this interface,
     * and it derives everything it can from the one health-adjacent method every implementor already provides,
     * {@link #isClosedOrFailed()}: not closed is reported as {@link State#RUNNING}, closed as {@link State#CLOSED},
     * for both the controller and the poller state.
     * <p>
     * <strong>Those state values are derived, not observed.</strong> This implementation has no access to a real run
     * state, so it cannot report {@link State#UNUSED}, {@link State#PAUSED}, {@link State#DRAINING} or
     * {@link State#CLOSING} at all, and it reports the same value for both subsystems because it cannot tell them
     * apart.
     * <p>
     * <strong>Equally, the empty failure cause it returns carries no clean-versus-crash meaning.</strong>
     * {@link #isClosedOrFailed()} is true for both outcomes and the default has nothing else to consult, so a crashed
     * instance is reported here exactly as a cleanly closed one - {@link PCHealth#getFailureCause()} empty. Only an
     * implementation that overrides this method with a real, state-backed snapshot - as
     * {@link AbstractParallelEoSStreamProcessor} does - can distinguish the two.
     * <p>
     * Because a caller generally receives a {@link PCHealth} without knowing which implementation produced it, that
     * limitation is carried on the value itself rather than left in this Javadoc: snapshots from here report
     * {@link PCHealth#isStateObserved()} as {@code false}.
     *
     * @return the current health of this instance - never {@code null}
     * @see PCHealth
     * @see State
     */
    default PCHealth getHealth() {
        State derived = isClosedOrFailed()
                ? State.CLOSED
                : State.RUNNING;
        return PCHealth.builder()
                .controllerState(derived)
                .pollerState(derived)
                .stateObserved(false)
                .build();
    }

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
     * Once the consumer is paused, the system will stop submitting work to the processing pool. Already submitted in
     * flight work however will be finished. This includes work that is currently being processed inside a user function
     * as well as work that has already been submitted to the processing pool but has not been picked up by a free
     * worker yet.
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
