package io.confluent.parallelconsumer;

import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.internals.ConsumerCoordinator;

import java.lang.reflect.Field;
import java.util.Properties;

import static io.confluent.csid.utils.StringUtils.msg;

@RequiredArgsConstructor
public class ConfigurationValidator<K, V> {

    public static final IllegalStateException AUTO_COMMIT_ENABLED_ERROR_ERR = new IllegalStateException("Consumer auto commit must be disabled, as commits are handled by the library.");
    final ParallelConsumerOptions options;

    public void validate() {
        clients();

        transactional();

        defaults();
    }

    private void defaults() {
        //
        WorkContainer.setDefaultRetryDelay(options.getDefaultMessageRetryDelay());
    }

    private void transactional() {
        if (options.isUsingTransactionalProducer() && options.getProducer() == null) {
            throw new IllegalArgumentException(msg("Wanting to use Transaction Producer mode ({}) without supplying a Producer instance",
                    options.getCommitMode()));
        }
    }

    private void clients() {
        checkClientSetup();

        instantiateClients();

        checkAutoCommitIsDisabled();
    }

    private void instantiateClients() {
        options.setConsumer(options.getConsumerSupplier().get());
        options.setProducer(options.getProducerSupplier().get());
    }

    private void checkClientSetup() {
        if (options.getConsumerConfig() != null ^ options.getConsumerSupplier() != null) {
            throw new IllegalArgumentException("Either supply a Consumer config set to be used, or a Consumer supplier function, but not both.");
        }

        if (options.getProducerConfig() != null ^ options.getProducerSupplier() != null) {
            throw new IllegalArgumentException("Either supply a Producer config set to be used, or a Consumer supplier function, but not both.");
        }
    }

    private void checkAutoCommitIsDisabled() {
        if (options.getConsumerConfig() != null)
            checkAutoCommitIsDisabled(options.getConsumerConfig());
        else
            checkAutoCommitIsDisabled(options.getConsumer());
    }

    /**
     * A more reliable method than the dirty reflection method {@link #checkAutoCommitIsDisabled(Consumer)}.
     *
     * @param consumerConfig
     */
    private void checkAutoCommitIsDisabled(final Properties consumerConfig) {
        String stringBoolean = consumerConfig.getProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG);
        Boolean enabled = Boolean.valueOf(stringBoolean);
        if (enabled)
            throw AUTO_COMMIT_ENABLED_ERROR_ERR;
    }


    /**
     * Nasty reflection to check if auto commit is disabled. Required evil if user isn't using the {@link
     * ParallelConsumerOptions#getConsumerConfig()} methods.
     * <p>
     * This is more reliable in a correctness sense, but britle in terms of coupling to internal implementation.
     * Consider requesting ability to inspect configuration at runtime.
     */
    @SneakyThrows
    private void checkAutoCommitIsDisabled(org.apache.kafka.clients.consumer.Consumer<K, V> consumer) {
        if (consumer instanceof KafkaConsumer) {
            // Commons lang FieldUtils#readField - avoid needing commons lang
            Field coordinatorField = consumer.getClass().getDeclaredField("coordinator"); //NoSuchFieldException
            coordinatorField.setAccessible(true);
            ConsumerCoordinator coordinator = (ConsumerCoordinator) coordinatorField.get(consumer); //IllegalAccessException

            Field autoCommitEnabledField = coordinator.getClass().getDeclaredField("autoCommitEnabled");
            autoCommitEnabledField.setAccessible(true);
            Boolean isAutoCommitEnabled = (Boolean) autoCommitEnabledField.get(coordinator);

            if (isAutoCommitEnabled)
                throw AUTO_COMMIT_ENABLED_ERROR_ERR;
        } else {
            // noop - probably MockConsumer being used in testing - which doesn't do auto commits
        }
    }

}
