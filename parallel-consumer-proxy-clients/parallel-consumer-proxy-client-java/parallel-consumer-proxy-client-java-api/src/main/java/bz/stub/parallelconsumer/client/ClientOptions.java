package bz.stub.parallelconsumer.client;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

/**
 * Connect-time configuration for a {@link ParallelConsumerClient}: what the user tells Parallel Consumer,
 * expressed once for every transport. Configuration is code, handed to the client at construction (KTD5) -
 * the wrapper reads no file, no environment variable and no shell; the direct transport applies these values
 * to core in-process, the gRPC transport carries them to the sidecar in its connect-time {@code Configure}.
 * <p>
 * <b>Unset means "take the engine's default"</b>, whichever transport supplies the engine - so this class
 * holds no default values of its own, and never guesses one. Only the subscription is required.
 * <p>
 * {@code kafkaProperties} is the Kafka client configuration - bootstrap servers, group id, credentials. It is
 * credential-bearing: transports hand it to the layer that constructs the Kafka clients and must never log it
 * (R48). This class's {@link #toString()} accordingly omits it.
 * <p>
 * The spike (the plan's U29) carries the fields the end-to-end slice needs; the schema freeze (U18) is where
 * the full option set is settled, and fields added there are additive here.
 *
 * @author Antony Stubbs
 */
public final class ClientOptions {

    private final List<String> topics;
    private final Integer maxConcurrency;
    private final ProcessingOrder ordering;
    private final Duration commitInterval;
    private final Duration defaultMessageRetryDelay;
    private final Map<String, String> kafkaProperties;

    private ClientOptions(Builder builder) {
        if (builder.topics.isEmpty()) {
            throw new IllegalArgumentException("ClientOptions needs at least one topic to subscribe to");
        }
        this.topics = Collections.unmodifiableList(new ArrayList<>(builder.topics));
        this.maxConcurrency = builder.maxConcurrency;
        this.ordering = builder.ordering;
        this.commitInterval = builder.commitInterval;
        this.defaultMessageRetryDelay = builder.defaultMessageRetryDelay;
        this.kafkaProperties = Collections.unmodifiableMap(new LinkedHashMap<>(builder.kafkaProperties));
    }

    public static Builder builder() {
        return new Builder();
    }

    /** The subscription, fixed for the client's lifetime (R36). */
    public List<String> topics() {
        return topics;
    }

    /** Maximum records in flight to this client at once; empty means the engine's default. */
    public OptionalInt maxConcurrency() {
        return maxConcurrency == null ? OptionalInt.empty() : OptionalInt.of(maxConcurrency);
    }

    /** The ordering guarantee; empty means the engine's default. */
    public Optional<ProcessingOrder> ordering() {
        return Optional.ofNullable(ordering);
    }

    /** How often offsets are committed; empty means the engine's default. */
    public Optional<Duration> commitInterval() {
        return Optional.ofNullable(commitInterval);
    }

    /** How long a failed record waits before redelivery; empty means the engine's default. */
    public Optional<Duration> defaultMessageRetryDelay() {
        return Optional.ofNullable(defaultMessageRetryDelay);
    }

    /** The Kafka client configuration. Credential-bearing: never log it (R48). */
    public Map<String, String> kafkaProperties() {
        return kafkaProperties;
    }

    @Override
    public String toString() {
        // deliberately omits kafkaProperties: it may carry credentials (R48)
        return "ClientOptions(topics=" + topics + ", maxConcurrency=" + maxConcurrency
                + ", ordering=" + ordering + ", commitInterval=" + commitInterval
                + ", defaultMessageRetryDelay=" + defaultMessageRetryDelay + ")";
    }

    public static final class Builder {
        private final List<String> topics = new ArrayList<>();
        private Integer maxConcurrency;
        private ProcessingOrder ordering;
        private Duration commitInterval;
        private Duration defaultMessageRetryDelay;
        private final Map<String, String> kafkaProperties = new LinkedHashMap<>();

        private Builder() {
        }

        /** Adds topics to the subscription. At least one is required. */
        public Builder topics(List<String> topics) {
            this.topics.addAll(topics);
            return this;
        }

        public Builder maxConcurrency(int maxConcurrency) {
            if (maxConcurrency < 1) {
                throw new IllegalArgumentException("maxConcurrency must be at least 1, got " + maxConcurrency);
            }
            this.maxConcurrency = maxConcurrency;
            return this;
        }

        public Builder ordering(ProcessingOrder ordering) {
            this.ordering = ordering;
            return this;
        }

        public Builder commitInterval(Duration commitInterval) {
            this.commitInterval = commitInterval;
            return this;
        }

        public Builder defaultMessageRetryDelay(Duration defaultMessageRetryDelay) {
            this.defaultMessageRetryDelay = defaultMessageRetryDelay;
            return this;
        }

        /** Adds Kafka client configuration entries. Credential-bearing: never log them (R48). */
        public Builder kafkaProperties(Map<String, String> kafkaProperties) {
            this.kafkaProperties.putAll(kafkaProperties);
            return this;
        }

        public ClientOptions build() {
            return new ClientOptions(this);
        }
    }
}
