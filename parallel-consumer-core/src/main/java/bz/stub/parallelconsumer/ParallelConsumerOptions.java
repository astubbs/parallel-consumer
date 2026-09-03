package bz.stub.parallelconsumer;

/*-
 * Copyright (C) 2020-2024 Confluent, Inc.
 * Modifications Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.internal.AbstractParallelEoSStreamProcessor;
import bz.stub.parallelconsumer.internal.DynamicLoadFactor;
import bz.stub.parallelconsumer.internal.MdcPropagation;
import bz.stub.parallelconsumer.metrics.PCMetricsDef;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import lombok.ToString;
import lombok.experimental.FieldNameConstants;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.annotation.InterfaceStability;

import java.time.Duration;
import java.util.Objects;
import java.util.function.Function;

import static bz.stub.parallelconsumer.internal.utils.StringUtils.msg;
import static bz.stub.parallelconsumer.ParallelConsumerOptions.CommitMode.PERIODIC_TRANSACTIONAL_PRODUCER;
import static java.time.Duration.ofMillis;

/**
 * The options for the {@link AbstractParallelEoSStreamProcessor} system.
 * <p>
 * The important options to look at are:
 * <p>
 * {@link #ordering}, {@link #maxConcurrency} and {@link #batchSize}.
 * <p>
 * If you want to go deeper, look at {@link #defaultMessageRetryDelay}, {@link #retryDelayProvider} and
 * {@link #commitMode}.
 * <p>
 * Note: The only required option is the {@link #consumer} ({@link #producer} is only needed if you use the Produce
 * flows). All other options have sensible defaults.
 *
 * @author Antony Stubbs
 * @see #builder()
 * @see ParallelConsumerOptions.ParallelConsumerOptionsBuilder
 */
@Getter
@Builder(toBuilder = true)
@ToString
@FieldNameConstants
@InterfaceStability.Evolving
public class ParallelConsumerOptions<K, V> {

    /**
     * Required parameter for all use.
     */
    private final Consumer<K, V> consumer;

    /**
     * Supplying a producer is only needed if using the produce flows.
     *
     * @see ParallelStreamProcessor
     */
    private final Producer<K, V> producer;

    /**
     * Path to Managed executor service for Java EE
     */
    @Builder.Default
    private final String managedExecutorService = "java:comp/DefaultManagedExecutorService";

    /**
     * Path to Managed thread factory for Java EE
     */
    @Builder.Default
    private final String managedThreadFactory = "java:comp/DefaultManagedThreadFactory";

    /**
     * Micrometer MeterRegistry
     * <p>
     * Optional - if not specified CompositeMeterRegistry will be used which is NoOp
     */
    private final MeterRegistry meterRegistry;

    /**
     * PC Instance metrics tag value - if specified - should be unique to allow instance specific meters to be created
     * and cleared. Used with Tag key {@link PCMetricsDef#PC_INSTANCE_TAG}
     * <p>
     * If not set - unique UUID will be generated for it
     */
    private final String pcInstanceTag;

    /**
     * Additional common metrics tags - will be added to all created meters
     */
    @Builder.Default
    private final Iterable<Tag> metricsTags = Tags.empty();

    /**
     * The ordering guarantee to use.
     */
    public enum ProcessingOrder {

        /**
         * No ordering is guaranteed, not even partition order. Fastest. Concurrency is at most the max number of
         * concurrency or max number of uncommitted messages, limited by the max concurrency or uncommitted settings.
         */
        UNORDERED,

        /**
         * Process messages within a partition in order, but process multiple partitions in parallel. Similar to running
         * more consumer for a topic. Concurrency is at most the number of partitions.
         */
        PARTITION,

        /**
         * Process messages in key order. Concurrency is at most the number of unique keys in a topic, limited by the
         * max concurrency or uncommitted settings.
         */
        KEY
    }

    /**
     * The type of commit to be made, with either a transactions configured Producer where messages produced are
     * committed back to the Broker along with the offsets they originated from, or with the faster simpler Consumer
     * offset system either synchronously or asynchronously
     */
    public enum CommitMode {

        // tag::transactionalJavadoc[]
        /**
         * Periodically commits through the Producer using transactions.
         * <p>
         * Messages sent in parallel by different workers get added to the same transaction block - you end up with
         * transactions 100ms (by default) "large", containing all records sent during that time period, from the
         * offsets being committed.
         * <p>
         * Of no use, if not also producing messages (i.e. using a {@link ParallelStreamProcessor#pollAndProduce}
         * variation).
         * <p>
         * Note: Records being sent by different threads will all be in a single transaction, as PC shares a single
         * Producer instance. This could be seen as a performance overhead advantage, efficient resource use, in
         * exchange for a loss in transaction granularity.
         * <p>
         * The benefits of using this mode are:
         * <p>
         * a) All records produced from a given source offset will either all be visible, or none will be
         * ({@link org.apache.kafka.common.IsolationLevel#READ_COMMITTED}).
         * <p>
         * b) If any records making up a transaction have a terminal issue being produced, or the system crashes before
         * finishing sending all the records and committing, none will ever be visible and the system will eventually
         * retry them in new transactions - potentially with different combinations of records from the original.
         * <p>
         * c) A source offset, and it's produced records will be committed as an atomic set. Normally: either the record
         * producing could fail, or the committing of the source offset could fail, as they are separate individual
         * operations. When using Transactions, they are committed together - so if either operations fails, the
         * transaction will never get committed, and upon recovery, the system will retry the set again (and no
         * duplicates will be visible in the topic).
         * <p>
         * This {@code CommitMode} is the slowest of the options, but there will be no duplicates in Kafka caused by
         * producing a record multiple times if previous offset commits have failed or crashes have occurred (however
         * message replay may cause duplicates in external systems which is unavoidable - external systems must be
         * idempotent).
         * <p>
         * The default commit interval {@link AbstractParallelEoSStreamProcessor#KAFKA_DEFAULT_AUTO_COMMIT_FREQUENCY}
         * gets automatically reduced from the default of 5 seconds to 100ms (the same as Kafka Streams <a
         * href=https://docs.confluent.io/platform/current/streams/developer-guide/config-streams.html">commit.interval.ms</a>).
         * Reducing this configuration places higher load on the broker, but will reduce (but cannot eliminate) replay
         * upon failure. Note also that when using transactions in Kafka, consumption in {@code READ_COMMITTED} mode is
         * blocked up to the offset of the first STILL open transaction. Using a smaller commit frequency reduces this
         * minimum consumption latency - the faster transactions are closed, the faster the transaction content can be
         * read by {@code READ_COMMITTED} consumers. More information about this can be found on the Confluent blog
         * post:
         * <a href="https://www.confluent.io/blog/enabling-exactly-once-kafka-streams/">Enabling Exactly-Once in Kafka
         * Streams</a>.
         * <p>
         * When producing multiple records (see {@link ParallelStreamProcessor#pollAndProduceMany}), all records must
         * have been produced successfully to the broker before the transaction will commit, after which all will be
         * visible together, or none.
         * <p>
         * Records produced while running in this mode, won't be seen by consumer running in
         * {@link ConsumerConfig#ISOLATION_LEVEL_CONFIG} {@link org.apache.kafka.common.IsolationLevel#READ_COMMITTED}
         * mode until the transaction is complete and all records are produced successfully. Records produced into a
         * transaction that gets aborted or timed out, will never be visible.
         * <p>
         * The system must prevent records from being produced to the brokers whose source consumer record offsets has
         * not been included in this transaction. Otherwise, the transactions would include produced records from
         * consumer offsets which would only be committed in the NEXT transaction, which would break the EoS guarantees.
         * To achieve this, first work processing and record producing is suspended (by acquiring the commit lock -
         * see{@link #commitLockAcquisitionTimeout}, as record processing requires the produce lock), then succeeded
         * consumer offsets are gathered, transaction commit is made, then when the transaction has finished, processing
         * resumes by releasing the commit lock. This periodically slows down record production during this phase, by
         * the time needed to commit the transaction.
         * <p>
         * This is all separate from using an IDEMPOTENT Producer, which can be used, along with the
         * {@link ParallelConsumerOptions#commitMode} {@link CommitMode#PERIODIC_CONSUMER_SYNC} or
         * {@link CommitMode#PERIODIC_CONSUMER_ASYNCHRONOUS}.
         * <p>
         * Failure:
         * <p>
         * Commit lock: If the system cannot acquire the commit lock in time, it will shut down for whatever reason, the
         * system will shut down (fail fast) - during the shutdown a final commit attempt will be made. The default
         * timeout for acquisition is very high though - see {@link #commitLockAcquisitionTimeout}. This can be caused
         * by the user processing function taking too long to complete.
         * <p>
         * Produce lock: If the system cannot acquire the produce lock in time, it will fail the record processing and
         * retry the record later. This can be caused by the controller taking too long to commit for some reason. See
         * {@link #produceLockAcquisitionTimeout}. If using {@link #allowEagerProcessingDuringTransactionCommit}, this
         * may cause side effect replay when the record is retried, otherwise there is no replay. See
         * {@link #allowEagerProcessingDuringTransactionCommit} for more details.
         *
         * @see ParallelConsumerOptions.ParallelConsumerOptionsBuilder#commitInterval
         */
        // end::transactionalJavadoc[]
        PERIODIC_TRANSACTIONAL_PRODUCER,

        /**
         * Periodically synchronous commits with the Consumer. Much faster than
         * {@link #PERIODIC_TRANSACTIONAL_PRODUCER}. Slower but potentially fewer duplicates than
         * {@link #PERIODIC_CONSUMER_ASYNCHRONOUS} upon replay.
         */
        PERIODIC_CONSUMER_SYNC,

        /**
         * Periodically commits offsets asynchronously. The fastest option, under normal conditions will have few or no
         * duplicates. Under failure recovery may have more duplicates than {@link #PERIODIC_CONSUMER_SYNC}.
         */
        PERIODIC_CONSUMER_ASYNCHRONOUS

    }

    /**
     * Kafka's default auto commit interval - which is 5000ms.
     *
     * @see org.apache.kafka.clients.consumer.ConsumerConfig#AUTO_COMMIT_INTERVAL_MS_CONFIG
     * @see org.apache.kafka.clients.consumer.ConsumerConfig#CONFIG
     */
    public static final int KAFKA_DEFAULT_AUTO_COMMIT_INTERVAL_MS = 5000;

    public static final Duration DEFAULT_COMMIT_INTERVAL = ofMillis(KAFKA_DEFAULT_AUTO_COMMIT_INTERVAL_MS);

    /*
     * The same as Kafka Streams
     */
    public static final Duration DEFAULT_COMMIT_INTERVAL_FOR_TRANSACTIONS = ofMillis(100);

    /**
     * When using {@link CommitMode#PERIODIC_TRANSACTIONAL_PRODUCER}, allows new records to be processed UP UNTIL the
     * result record SENDING ({@link Producer#send}) step, potentially while a transaction is being committed. Disabled
     * by default as to prevent replay side effects when records need to be retried in some scenarios.
     * <p>
     * Doesn't interfere with the transaction itself, just reduces side effects.
     * <p>
     * Recommended to leave this off to avoid side effect duplicates upon rebalances after a crash. Enabling could
     * improve performance as the produce lock will only be taken right before it's needed (optimistic locking) to
     * produce the result record, instead of pessimistically locking.
     */
    @Builder.Default
    private boolean allowEagerProcessingDuringTransactionCommit = false;

    /**
     * Time to allow for acquiring the commit lock. If record processing or producing takes a long time, you may need to
     * increase this. If this fails, the system will shut down (fail fast) and attempt to commit once more.
     */
    @Builder.Default
    private Duration commitLockAcquisitionTimeout = Duration.ofMinutes(5);

    /**
     * Time to allow for acquiring the produce lock. If transaction committing a long time, you may need to increase
     * this. If this fails, the record will be returned to the processing queue for later retry.
     */
    @Builder.Default
    private Duration produceLockAcquisitionTimeout = Duration.ofMinutes(1);

    /**
     * Time between commits. Using a higher frequency (a lower value) will put more load on the brokers.
     * <p>
     * Left {@code null} until the user (or the deprecated setter) explicitly provides a value - resolved to
     * {@link #DEFAULT_COMMIT_INTERVAL} by {@link #getCommitInterval()}. {@code null} is the "did the user set this?"
     * signal that {@link #transactionsValidation()} reduces under transactions; it is deliberately not an object
     * reference or value comparison against {@link #DEFAULT_COMMIT_INTERVAL} - see that method's javadoc.
     */
    @Getter(AccessLevel.NONE)
    private Duration commitInterval;

    public Duration getCommitInterval() {
        return commitInterval == null ? DEFAULT_COMMIT_INTERVAL : commitInterval;
    }

    /**
     * @deprecated only settable during {@code deprecation phase} - use
     *         {@link ParallelConsumerOptions.ParallelConsumerOptionsBuilder#commitInterval}} instead.
     */
    // todo delete in next major version
    @Deprecated
    public void setCommitInterval(Duration commitInterval) {
        this.commitInterval = commitInterval;
    }

    /**
     * The {@link ProcessingOrder} type to use
     */
    @Builder.Default
    private final ProcessingOrder ordering = ProcessingOrder.KEY;

    /**
     * The {@link CommitMode} to be used
     */
    @Builder.Default
    private final CommitMode commitMode = CommitMode.PERIODIC_CONSUMER_ASYNCHRONOUS;

    /**
     * Controls the maximum degree of concurrency to occur. Used to limit concurrent calls to external systems to a
     * maximum to prevent overloading them or to a degree, using up quotas.
     * <p>
     * When using {@link #getBatchSize()}, this is over and above the batch size setting. So for example, a
     * {@link #getMaxConcurrency()} of {@code 2} and a batch size of {@code 3} would result in at most {@code 15}
     * records being processed at once.
     * <p>
     * A note on quotas - if your quota is expressed as maximum concurrent calls, this works well. If it's limited in
     * total requests / sec, this may still overload the system. See towards the distributed rate limiting feature for
     * this to be properly addressed: https://github.com/confluentinc/parallel-consumer/issues/24 Add distributed rate
     * limiting support confluentinc#24.
     * <p>
     * In the core module, this sets the number of threads to use in the core's thread pool.
     * <p>
     * It's recommended to set this quite high, much higher than core count, as it's expected that these threads will
     * spend most of their time blocked waiting for IO. For automatic setting of this variable, look out for issue
     * https://github.com/confluentinc/parallel-consumer/issues/21 Dynamic concurrency control with flow control or tcp
     * congestion control theory confluentinc#21.
     */
    @Builder.Default
    private final int maxConcurrency = DEFAULT_MAX_CONCURRENCY;

    public static final int DEFAULT_MAX_CONCURRENCY = 16;

    public static final Duration DEFAULT_STATIC_RETRY_DELAY = Duration.ofSeconds(1);

    // Default backoff for SaslAuthenticationException retry durion ConsumerManager.commitSync and ConsumerManager.poll.
    public static final Duration SASL_AUTHENTICATION_EXCEPTION_RETRY_BACKOFF = Duration.ofSeconds(5);

    /**
     * Error handling strategy to use when PC is assigned a partition whose committed offset metadata this build cannot
     * read - a consumer group previously owned by Kafka Streams, by another framework, by operator tooling, or written
     * by a <em>newer</em> PC using an encoding that did not exist when this version was built.
     * <p>
     * The policy governs <em>every</em> such case uniformly. It previously governed only bytes PC could positively
     * identify as Kafka Streams'; anything else bypassed it, which is what made the option unreachable for the
     * forward-compatibility case it exists to handle (astubbs#197, release-ledger item 5).
     */
    public enum InvalidOffsetMetadataHandlingPolicy {
        /**
         * Fail and shut down rather than silently discard the offset map. Dropping the map replays records that were
         * completed but not yet committed, so this is the choice for a deployment that would rather stop than
         * reprocess. Opt in: it is no longer the default - see {@link #invalidOffsetMetadataPolicy}.
         */
        FAIL,
        /**
         * Log a warning, discard the unreadable metadata and resume from the last committed offset. The default.
         */
        IGNORE
    }

    /**
     * Controls what happens when PC is assigned a partition whose committed offset metadata it cannot read. See
     * {@link InvalidOffsetMetadataHandlingPolicy}.
     * <p>
     * <b>Default is {@link InvalidOffsetMetadataHandlingPolicy#IGNORE}, changed from {@code FAIL}.</b> Pointing PC at a
     * consumer group that already has metadata in it is the first thing anyone adopting PC does, and dying during the
     * rebalance callback is the reported failure of astubbs#118 / confluentinc#326. That was previously survivable only
     * because undecodable metadata bypassed this option entirely; now that the option genuinely governs every
     * unreadable path, leaving the default at {@code FAIL} would make that report's exact scenario fatal again for
     * anyone who configures nothing. {@code FAIL} remains available and now means what it says.
     */
    @Builder.Default
    private final InvalidOffsetMetadataHandlingPolicy invalidOffsetMetadataPolicy = InvalidOffsetMetadataHandlingPolicy.IGNORE;
    /**
     * When a message fails, how long the system should wait before trying that message again. Note that this will not
     * be exact, and is just a target.
     *
     * @deprecated will be renamed to static retry delay
     */
    @Deprecated
    @Builder.Default
    private final Duration defaultMessageRetryDelay = DEFAULT_STATIC_RETRY_DELAY;

    /**
     * When present, use this to generate a dynamic retry delay, instead of a static one with
     * {@link #getDefaultMessageRetryDelay()}.
     * <p>
     * Overrides {@link #defaultMessageRetryDelay}, even if it's set.
     */
    private final Function<RecordContext<K, V>, Duration> retryDelayProvider;

    /**
     * Controls how long to block while waiting for the {@link Producer#send} to complete for any ProducerRecords
     * returned from the user-function. Only relevant if using one of the produce-flows and providing a
     * {@link ParallelConsumerOptions#producer}. If the timeout occurs the record will be re-processed in the
     * user-function.
     * <p>
     * Consider aligning the value with the {@link ParallelConsumerOptions#producer}-options to avoid unnecessary
     * re-processing and duplicates on slow {@link Producer#send} calls.
     *
     * @see org.apache.kafka.clients.producer.ProducerConfig#DELIVERY_TIMEOUT_MS_CONFIG
     */
    @Builder.Default
    private final Duration sendTimeout = Duration.ofSeconds(10);

    /**
     * Controls how long to block while waiting for offsets to be committed. Only relevant if using
     * {@link CommitMode#PERIODIC_CONSUMER_SYNC} commit-mode.
     */
    @Builder.Default
    private final Duration offsetCommitTimeout = Duration.ofSeconds(10);

    /**
     * Controls how long for Kafka consumer.poll() to be retried upon SaslAuthenticationException.
     *
     * Occasionally, consumer.poll() throws SaslAuthenticationException due to temporary external system failures.
     *
     * In this case, consumers are stopped immediately. It is actually retryable.
     * This timeout is zero by default, meaning no retry will be performed.
     * When set to a duration that is larger than 0, the consumer.poll() will ignore SaslAuthenticationException and continue retrying
     * until this timeout is elaposed.
     */
    @Builder.Default
    private final Duration saslAuthenticationRetryTimeout = Duration.ofSeconds(0);

    /**
     * Controls when SaslAuthenticationException is encountered, how long to backoff before next try.
     * The backoff still watches the shutdownRequest every 100ms and will exit as soon as (within 100ms)
     * the shutdown request had been received.
     */
    @Builder.Default
    private final Duration saslAuthenticationExceptionRetryBackoff = SASL_AUTHENTICATION_EXCEPTION_RETRY_BACKOFF;

    /**
     * The maximum number of messages to attempt to pass into the user functions.
     * <p>
     * Batch sizes may sometimes be less than this size, but will never be more.
     * <p>
     * The system will treat the messages as a set, so if an error is thrown by the user code, then all messages will be
     * marked as failed and be retried (Note that when they are retried, there is no guarantee they will all be in the
     * same batch again). So if you're going to process messages individually, then don't set a batch size.
     * <p>
     * Otherwise, if you're going to process messages in sub sets from this batch, it's better to instead adjust the
     * {@link ParallelConsumerOptions#getBatchSize()} instead to the actual desired size, and process them as a whole.
     * <p>
     * Note that there is no relationship between the {@link ConsumerConfig} setting of
     * {@link ConsumerConfig#MAX_POLL_RECORDS_CONFIG} and this configured batch size, as this library introduces a large
     * layer of indirection between the managed consumer, and the managed queues we use.
     * <p>
     * This indirection effectively disconnects the processing of messages from "polling" them from the managed client,
     * as we do not wait to process them before calling poll again. We simply call poll as much as we need to, in order
     * to keep our queues full of enough work to satisfy demand.
     * <p>
     * If we have enough, then we actively manage pausing our subscription so that we can continue calling {@code poll}
     * without pulling in even more messages.
     * <p>
     *
     * @see ParallelConsumerOptions#getBatchSize()
     */
    @Builder.Default
    private final Integer batchSize = 1;

    /**
     * Configure the amount of delay a record experiences, before a warning is logged.
     */
    @Builder.Default
    private final Duration thresholdForTimeSpendInQueueWarning = Duration.ofSeconds(10);

    public boolean isUsingBatching() {
        return getBatchSize() > 1;
    }

    @Builder.Default
    private final int maxFailureHistory = 10;

    /**
     * @return the combined target of the desired concurrency by the configured batch size
     */
    public int getTargetAmountOfRecordsInFlight() {
        return getMaxConcurrency() * getBatchSize();
    }

    public void validate() {
        Objects.requireNonNull(consumer, "A consumer must be supplied");

        transactionsValidation();
        loadFactorValidation();
    }

    /**
     * "Did the user set a commit interval?" is answered by the raw {@link #commitInterval} field being
     * {@code null}, never by comparing the resolved {@link #getCommitInterval()} value - by reference (the previous
     * approach) or by {@link Duration#equals}. Reference comparison broke the one time a caller passed back the
     * {@link #DEFAULT_COMMIT_INTERVAL} constant object itself: identical to the unset case, so an explicit call was
     * silently reduced to the transactional default anyway. {@code equals} would be worse, not a fix: any explicit
     * value that merely happens to number 5 seconds - a fresh {@code Duration.ofSeconds(5)}, never {@code ==} to the
     * constant - would then also be silently reduced, exactly the failure {@code docs/features/commit-interval.yaml}
     * documents as not happening ("an explicitly set value is kept").
     */
    private void transactionsValidation() {
        boolean commitIntervalHasNotBeenSet = commitInterval == null;

        if (isUsingTransactionCommitMode()) {
            if (producer == null) {
                throw new IllegalArgumentException(msg("Cannot set {} to Transaction Producer mode ({}) without supplying a Producer instance",
                        Fields.commitMode,
                        commitMode));
            }

            // update commit frequency
            if (commitIntervalHasNotBeenSet) {
                this.commitInterval = DEFAULT_COMMIT_INTERVAL_FOR_TRANSACTIONS;
            }
        }

        // inverse
        if (!isUsingTransactionCommitMode()) {
            if (isAllowEagerProcessingDuringTransactionCommit()) {
                throw new IllegalArgumentException(msg("Cannot set {} (eager record processing) when not using transactional commit mode ({}={}).",
                        Fields.allowEagerProcessingDuringTransactionCommit,
                        Fields.commitMode,
                        commitMode));
            }
        }
    }

    /**
     * The load factor bounds have only one meaningful ordering: {@link #initialLoadFactor} is where the dynamic load
     * factor starts, and {@link #maximumLoadFactor} is the ceiling it is allowed to step up to. An inverted pair can
     * never step, so it is a typo rather than a request. Unchecked it is accepted and pinned at the initial value,
     * surfacing at best as an inverted {@code 100/10} inside the rate-limited saturation warning - which only fires
     * under load, and reads as a capacity signal rather than as the misconfiguration it is.
     * <p>
     * Checked whether or not {@link #messageBufferSize} is set. A buffer size makes the pair <em>unused</em>, not
     * sensible, and accepting a nonsensical value is how it survives to the configuration change that starts reading
     * it again.
     */
    private void loadFactorValidation() {
        if (initialLoadFactor > maximumLoadFactor) {
            throw new IllegalArgumentException(msg("Cannot set {} ({}) above {} ({}) - the initial load factor is "
                            + "where the dynamic load factor starts and the maximum is the ceiling it may step up "
                            + "to, so an inverted pair can never step",
                    Fields.initialLoadFactor,
                    initialLoadFactor,
                    Fields.maximumLoadFactor,
                    maximumLoadFactor));
        }
    }

    /**
     * @deprecated use {@link #isUsingTransactionCommitMode()}
     */
    @Deprecated
    public boolean isUsingTransactionalProducer() {
        return isUsingTransactionCommitMode();
    }

    /**
     * @see CommitMode#PERIODIC_TRANSACTIONAL_PRODUCER
     */
    public boolean isUsingTransactionCommitMode() {
        return commitMode.equals(PERIODIC_TRANSACTIONAL_PRODUCER);
    }

    public boolean isProducerSupplied() {
        return getProducer() != null;
    }

    /**
     * Timeout for shutting down execution pool during shutdown in DONT_DRAIN mode. Should be high enough to allow for
     * inflight messages to finish processing, but low enough to kill any blocked thread to allow to rebalance in a
     * timely manner, especially if shutting down on error.
     */
    @Builder.Default
    public final Duration shutdownTimeout = Duration.ofSeconds(10);

    /**
     * Timeout for draining queue during shutdown in DRAIN mode. Should be high enough to allow for all queued messages
     * to process.
     */
    @Builder.Default
    public final Duration drainTimeout = Duration.ofSeconds(30);

    /**
     * Message buffer size - overrides use of dynamic load factor to set specific fixed size of message buffer. Useful
     * when using low concurrency modes - like partition based ordering to get a better mix of messages in the buffer.
     * As the buffer is shared across multiple partitions - with small buffer it is easy to starve processing threads as
     * single Consumer poll would will the buffer from single (or low number) of partitions. Setting this value would
     * effectively set low and high bound of dynamic load factor to a fixed value so that
     * {@link #getTargetAmountOfRecordsInFlight * loadingFactor} is equal to this size.
     */
    public final int messageBufferSize;

    /**
     * Initial load factor - overrides default starting load factor
     * {@link bz.stub.parallelconsumer.internal.DynamicLoadFactor#DEFAULT_INITIAL_LOADING_FACTOR}
     * <p>
     * Ignored if {@link #messageBufferSize} is specified as dynamic load factor system is set to static load factor to
     * match requested buffer size.
     */
    @Builder.Default
    public final int initialLoadFactor = DynamicLoadFactor.DEFAULT_INITIAL_LOADING_FACTOR;

    /**
     * Initial load factor - overrides default maximum load factor
     * {@link bz.stub.parallelconsumer.internal.DynamicLoadFactor#DEFAULT_MAX_LOADING_FACTOR}
     * <p>
     * Ignored if {@link #messageBufferSize} is specified as dynamic load factor system is set to static load factor to
     * match requested buffer size.
     */
    @Builder.Default
    public final int maximumLoadFactor = DynamicLoadFactor.DEFAULT_MAX_LOADING_FACTOR;

    /**
     * The purpose of the flag is to be a last resort / temporary work-around for changes introduced in newer Kafka
     * Clients that break reflective access and for using wrapped, custom or extended KafkaConsumer classes that fail
     * reflection checks - setting the flag to true will ignore reflection access exceptions during this check.
     * <p>
     * Note: that library will still try to access auto commit field on the consumer object and if it is accessible and
     * not disabled - the Parallel Consumer will shut down.
     */
    @Builder.Default
    public final boolean ignoreReflectiveAccessExceptionsForAutoCommitDisabledCheck = false;

    /**
     * Whether the SLF4J {@link org.slf4j.MDC} (Mapped Diagnostic Context) of the thread that starts Parallel Consumer
     * is carried into the threads that run your function - the worker pool, and the Vert.x / Reactor / Mutiny engines.
     * On by default.
     * <p>
     * With this on, diagnostic context you have already established - a {@code trace_id}, a {@code request_id}, a
     * tenant - is visible in the logs your function writes, and in Parallel Consumer's own log lines. The context is
     * snapshotted once, when you call {@code poll*}, so put what you want propagated into the MDC before then; a
     * request-scoped value set at that moment will be pinned to the consumer for its whole life, which is unlikely to
     * be what you want.
     * <p>
     * Parallel Consumer's own keys take precedence on a collision: {@code pcId}
     * ({@link AbstractParallelEoSStreamProcessor#MDC_INSTANCE_ID}) and {@code offset} are applied after yours.
     * <p>
     * Switching this off restores the pre-0.6.0.1 behaviour exactly: no context crosses into the worker pool, and
     * anything your function puts into the MDC is left on the pooled thread for the next, unrelated, record to
     * inherit.
     * <p>
     * <b>On by default deliberately, and settled</b> (astubbs#205). Not propagating fails silently for everyone who
     * has established a context; propagating fails visibly - an unexpected key in a log line - and has this switch.
     * The pinning described above is the known cost of that choice and was accepted along with it. Flipping the
     * default is a one-line change, but it takes evidence of the pinning actually biting rather than a re-reading of
     * the same trade.
     * <p>
     * <b>Known gap on the reactive engines.</b> For Reactor and Mutiny this covers the invocation of your function and
     * Parallel Consumer's own terminal signal handling. It does not follow the operators of the {@code Publisher} /
     * {@code Uni} you return onto further schedulers - that needs Reactor's own
     * {@code io.micrometer:context-propagation}, and is your call rather than Parallel Consumer's. It is a gap by
     * decision, not an oversight.
     *
     * @see MdcPropagation
     */
    @Builder.Default
    private final boolean propagateMdc = true;
}
