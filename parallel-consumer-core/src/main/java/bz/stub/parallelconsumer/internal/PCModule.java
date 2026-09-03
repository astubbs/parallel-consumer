package bz.stub.parallelconsumer.internal;

/*-
 * Copyright (C) 2020-2024 Confluent, Inc.
 * Modifications Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.internal.utils.TimeUtils;
import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.ParallelEoSStreamProcessor;
import bz.stub.parallelconsumer.metrics.PCMetrics;
import bz.stub.parallelconsumer.state.ShardManager;
import bz.stub.parallelconsumer.state.WorkManager;
import bz.stub.parallelconsumer.ProducerFactory;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.time.Clock;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.Set;
import java.util.Collections;
import java.util.Optional;
import java.util.UUID;

/**
 * Minimum dependency injection system, modled on how Dagger works.
 * <p>
 * Note: Not using Dagger as PC has a zero dependency policy, and franky it would be overkill for our needs.
 *
 * @author Antony Stubbs
 */
@Slf4j
public class PCModule<K, V> {

    protected ParallelConsumerOptions<K, V> optionsInstance;

    @Setter
    protected AbstractParallelEoSStreamProcessor<K, V> parallelEoSStreamProcessor;

    public PCModule(ParallelConsumerOptions<K, V> options) {
        this.optionsInstance = options;
    }

    /**
     * Rate limits the "your retryDelayProvider threw" warning.
     * <p>
     * Per PC instance rather than static, so one instance's broken provider cannot silence a DIFFERENT instance's
     * independently broken one - which is the case where the warning matters most, and where a JVM-wide limiter
     * would hide a second, unrelated misconfiguration behind the first.
     * <p>
     * Not thread-safe, deliberately: the worst a race costs is one extra log line, the same trade the queue-stats
     * limiter makes.
     */
    private final RateLimiter brokenRetryDelayProviderWarnLimiter = new RateLimiter(30);

    public RateLimiter brokenRetryDelayProviderWarnLimiter() {
        return brokenRetryDelayProviderWarnLimiter;
    }

    public ParallelConsumerOptions<K, V> options() {
        return optionsInstance;
    }

    private ProducerWrapper<K, V> producerWrapper;

    /**
     * The suffix of every {@code transactional.id} this module derives: generated once, so the producer PC builds at
     * start-up and every replacement it builds after a recovery share one id, and re-initialising a replacement
     * fences exactly the producer it replaces.
     */
    private final UUID producerInstanceId = UUID.randomUUID();

    /**
     * Every producer the factory has handed back, held weakly and compared by identity: a factory returning any of
     * them again is a caching or pooling factory, which breaks the contract {@link ProducerFactory} states. All of
     * them, not the last one: a pool alternating two instances would pass a last-only check on its third call,
     * then fail {@code initTransactions()} on a producer PC had already closed - and retry that forever as if it
     * were transient. Weak, so a discarded producer is not kept alive by the check that rejects its return.
     * Written on the constructing thread and, after that, only by the control thread's recovery pass.
     */
    private final Set<Producer<K, V>> producersHandedOut = Collections.newSetFromMap(new WeakHashMap<>());

    /**
     * The caller's producer configuration with the {@code transactional.id} PC derives set (or, in a non-transactional
     * commit mode, removed). Resolved once: the producer PC starts with and every replacement it builds are made from
     * the same map, and the WARN a caller-set id earns is emitted once per instance rather than once per rebuild.
     */
    private Map<String, Object> resolvedProducerConfig;

    private Map<String, Object> resolvedProducerConfig() {
        if (resolvedProducerConfig == null) {
            resolvedProducerConfig = TransactionalIdDerivation.resolve(options().getProducerConfig(),
                    options().isUsingTransactionCommitMode(), groupIdForDerivation(), producerInstanceId);
        }
        return resolvedProducerConfig;
    }

    /**
     * The wrapper around the producer PC starts with - the caller's instance on the deprecated path, or the first
     * producer built from configuration through the factory.
     */
    protected ProducerWrapper<K, V> producerWrap() {
        if (this.producerWrapper == null) {
            this.producerWrapper = options().isProducerInstanceSupplied()
                    ? new ProducerWrapper<>(options())
                    : buildProducerWrapperFromConfiguration();
        }
        return producerWrapper;
    }

    /**
     * How a replacement producer is built after the broker invalidates the current one: present only where PC built
     * the producer itself, because a caller's finished instance carries no configuration to rebuild from. Each call of
     * the supplier resolves the same configuration - the same derived {@code transactional.id} included - and asks
     * the factory for a new producer.
     */
    public Optional<ReplacementProducerSource<K, V>> replacementProducerWrap() {
        if (options().isProducerInstanceSupplied()) {
            return Optional.empty();
        }
        // null in a non-transactional commit mode, where resolve() removes the key
        String transactionalId = (String) resolvedProducerConfig().get(ProducerConfig.TRANSACTIONAL_ID_CONFIG);
        return Optional.of(new ReplacementProducerSource<>(this::buildProducerWrapperFromConfiguration, transactionalId));
    }

    private ProducerWrapper<K, V> buildProducerWrapperFromConfiguration() {
        boolean transactional = options().isUsingTransactionCommitMode();
        // a copy per call: the map is the factory's to read, and a factory that mutates it must not mutate the memo
        Map<String, Object> resolved = new LinkedHashMap<>(resolvedProducerConfig());
        // user code, wrapped as every other user function is: a factory that throws an Error (a serializer's static
        // initialiser failing, say) would otherwise escape every catch on the recovery path, leaving the instance
        // RUNNING with its workers parked on the produce lock for good
        Producer<K, V> producer = UserFunctions.carefullyRun(options().getProducerFactory()::create, resolved);
        if (producer == null) {
            throw new ProducerFactoryContractException("The ProducerFactory returned null; every call must return a new Producer");
        }
        if (producersHandedOut.contains(producer)) {
            throw new ProducerFactoryContractException("The ProducerFactory returned a producer it had already returned; every " +
                    "call must return a new Producer, because PC discards the previous one when the broker invalidates it");
        }
        producersHandedOut.add(producer);
        try {
            ProducerWrapper<K, V> wrapper = ProducerWrapper.forPcBuilt(options(), producer, transactional);
            log.info("Built producer from configuration (transactional: {}): {}", transactional, ProducerConfigRedaction.render(resolved));
            return wrapper;
        } catch (Throwable rejected) {
            // The producer failed the construction check, or the wrapper could not be built around it - a subclass of
            // KafkaProducer does not declare the field transactional discovery reads, and that reflective failure is
            // a checked exception thrown sneakily, which is why this is Throwable. Either way it will never be used:
            // do not leak its threads.
            closeQuietly(producer, "a rejected producer");
            throw rejected;
        }
    }

    private void closeQuietly(Producer<K, V> producer, String what) {
        try {
            producer.close(Duration.ZERO);
        } catch (RuntimeException closeFailed) {
            log.debug("Closing {} also failed", what, closeFailed);
        }
    }

    private String groupIdForDerivation() {
        var metadata = consumerManager().groupMetadata();
        if (metadata == null || metadata.groupId() == null) {
            throw new IllegalArgumentException("Cannot derive a transactional.id without the consumer's group.id - the " +
                    "consumer must be configured with a group.id before PC can build a producer");
        }
        return metadata.groupId();
    }

    private ProducerManager<K, V> producerManager;

    protected ProducerManager<K, V> producerManager() {
        if (producerManager == null) {
            ProducerWrapper<K, V> wrapper = producerWrap();
            try {
                this.producerManager = new ProducerManager<>(wrapper, consumerManager(), workManager(), options(), replacementProducerWrap());
            } catch (Throwable constructionFailed) {
                // The manager's constructor registers a gauge and initialises transactions, either of which can
                // throw (a coordinator that is not there yet, say). On the configuration path the producer it was
                // handed is PC's own, nobody else holds it, and the processor that failed to construct is never
                // returned to the caller - so without this, every failed start-up leaks a producer and its network
                // thread. The caller's own instance is the caller's to close.
                if (!options().isProducerInstanceSupplied()) {
                    closeQuietly(wrapper, "the producer built for a manager that failed to construct");
                }
                throw constructionFailed;
            }
        }
        return producerManager;
    }

    public Producer<K, V> producer() {
        return optionsInstance.getProducer();
    }

    public Consumer<K, V> consumer() {
        return optionsInstance.getConsumer();
    }

    private ConsumerManager<K, V> consumerManager;

    protected ConsumerManager<K, V> consumerManager() {
        if (consumerManager == null) {
            // Wrap the user's consumer in a thread-confinement guard. Ownership is claimed
            // by the poll thread when BrokerPollSystem.controlLoop starts. Before that,
            // init-time calls (subscribe, groupMetadata) are allowed from any thread.
            // See confluentinc#857.
            var confinedConsumer = new ThreadConfinedConsumer<>(optionsInstance.getConsumer());
            consumerManager = new ConsumerManager<>(confinedConsumer,
                    optionsInstance.getOffsetCommitTimeout(),
                    optionsInstance.getSaslAuthenticationRetryTimeout(),
                    optionsInstance.getSaslAuthenticationExceptionRetryBackoff());
            consumerManager.init();
        }
        return consumerManager;
    }

    @Setter
    private WorkManager<K, V> workManager;

    public WorkManager<K, V> workManager() {
        if (workManager == null) {
            workManager = new WorkManager<>(this, dynamicExtraLoadFactor());
        }
        return workManager;
    }

    private ShardManager<K, V> shardManager;
    private WorkManager<K, V> shardManagerOwner;

    /**
     * Resolved through the module (like every other collaborator here) rather than constructed inline by
     * {@link WorkManager}, so the DI seam is uniform. Takes the owning {@link WorkManager} as a parameter
     * because the two are mutually dependent and the {@link WorkManager} constructor is mid-flight when it
     * resolves this - {@link #workManager()} would recurse.
     * <p>
     * <b>Memoised, so it is bound to ONE owner for the life of the module</b>, and asking for a second one is
     * rejected rather than silently answered with the first's. Before this seam existed each {@link WorkManager}
     * built its own {@link ShardManager}, so a second manager on one module was merely unusual; a cache that
     * ignored its argument would instead hand the newcomer a shard manager still pointing at the first, leaving
     * its {@code PartitionStateManager} and its shard manager operating on different owners. Reachable through
     * the public {@link WorkManager} constructor and {@link #setWorkManager(WorkManager)}, and there is no
     * assignment of blame available at the point the damage shows up - hence a guard rather than a javadoc
     * warning. A module is one PC instance's collaborator graph; build a second graph with a second module.
     *
     * <b>Final, and substitution goes through {@link #createShardManager(WorkManager)} instead</b> - a subclass
     * overriding this method would take the memoisation and the guard with it, which is how the integration
     * test that needs a pausable shard manager silently reintroduced the very defect guarded against here.
     *
     * @throws IllegalStateException if a different {@link WorkManager} than the memoised owner asks for one
     */
    public final ShardManager<K, V> shardManager(WorkManager<K, V> workManagerInstance) {
        if (shardManager == null) {
            shardManager = createShardManager(workManagerInstance);
            shardManagerOwner = workManagerInstance;
        } else if (shardManagerOwner != workManagerInstance) {
            throw new IllegalStateException("This PCModule's ShardManager is already bound to a different "
                    + "WorkManager. A module holds one PC instance's collaborator graph - construct a second "
                    + "PCModule rather than a second WorkManager against this one.");
        }
        return shardManager;
    }

    /**
     * The substitution seam for {@link #shardManager(WorkManager)} - override this, not the memoising getter, so
     * that a substituted {@link ShardManager} still gets the one-owner guard rather than opting out of it.
     * Called at most once per module, from inside the memoisation, so an override need not cache anything.
     */
    protected ShardManager<K, V> createShardManager(WorkManager<K, V> workManagerInstance) {
        return new ShardManager<>(this, workManagerInstance);
    }

    protected AbstractParallelEoSStreamProcessor<K, V> pc() {
        if (parallelEoSStreamProcessor == null) {
            parallelEoSStreamProcessor = new ParallelEoSStreamProcessor<>(options(), this);
        }
        return parallelEoSStreamProcessor;
    }

    private DynamicLoadFactor dynamicLoadFactor;

    protected DynamicLoadFactor dynamicExtraLoadFactor() {
        if (dynamicLoadFactor == null) {
            dynamicLoadFactor = initDynamicLoadFactor();
        }
        return dynamicLoadFactor;
    }

    private BrokerPollSystem<K, V> brokerPollSystem;
    private AbstractParallelEoSStreamProcessor<K, V> brokerPollSystemOwner;

    /**
     * Same shape as {@link #shardManager(WorkManager)} and guarded for the same reason: memoised, but takes its
     * owner as a parameter, so a cache that ignored that parameter would answer a second processor with a
     * {@link BrokerPollSystem} wired to the first. That poller drives the first processor's control loop - it
     * calls back into {@code pc} to signal work, and registers this module's {@link PCMetrics} gauges against
     * itself - so the newcomer would poll a broker on behalf of a processor it does not own, and never receive
     * the records. Nothing throws at that point; the second processor simply never makes progress.
     * <p>
     * Today the only caller is {@link AbstractParallelEoSStreamProcessor}'s constructor, once per module, so the
     * collision is unreachable - but that is a property of the callers, not of this method, and the guard is
     * what makes it a property of this method. A module is one PC instance's collaborator graph; build a second
     * graph with a second module.
     * <p>
     * Final for the same reason {@link #shardManager(WorkManager)} is: an override would carry the guard away
     * with it. Nothing substitutes a poller today, so there is no factory seam beside it yet - add one the way
     * {@link #createShardManager(WorkManager)} is written, rather than dropping the {@code final}.
     *
     * @throws IllegalStateException if a different processor than the memoised owner asks for one
     */
    protected final BrokerPollSystem<K, V> brokerPoller(AbstractParallelEoSStreamProcessor<K, V> pc) {
        if (brokerPollSystem == null) {
            brokerPollSystem = new BrokerPollSystem<>(consumerManager(), workManager(), pc, options());
            brokerPollSystemOwner = pc;
        } else if (brokerPollSystemOwner != pc) {
            throw new IllegalStateException("This PCModule's BrokerPollSystem is already bound to a different "
                    + "ParallelConsumer. A module holds one PC instance's collaborator graph - construct a second "
                    + "PCModule rather than a second ParallelConsumer against this one.");
        }
        return brokerPollSystem;
    }

    public Clock clock() {
        return TimeUtils.getClock();
    }

    private MdcPropagation mdcPropagation;

    /**
     * @see ParallelConsumerOptions#isPropagateMdc()
     */
    public MdcPropagation mdcPropagation() {
        if (mdcPropagation == null) {
            mdcPropagation = new MdcPropagation(options().isPropagateMdc());
        }
        return mdcPropagation;
    }

    private PCMetrics pcMetrics;

    public PCMetrics pcMetrics() {
        if (pcMetrics == null) {
            pcMetrics = new PCMetrics(options().getMeterRegistry(), optionsInstance.getMetricsTags(), optionsInstance.getPcInstanceTag());
        }
        return pcMetrics;
    }

    /**
     * A configured {@link ParallelConsumerOptions#messageBufferSize} pins the load factor to whatever multiple of the
     * in-flight target produces that buffer - the factor is then fixed for the lifetime of the instance, and never
     * steps.
     */
    private DynamicLoadFactor initDynamicLoadFactor() {
        if (options().getMessageBufferSize() > 0) {
            int staticLoadFactor = (options().getMessageBufferSize() / options().getTargetAmountOfRecordsInFlight()) + (options().getMessageBufferSize() % options().getTargetAmountOfRecordsInFlight() == 0 ? 0 : 1);
            return DynamicLoadFactor.fixedAt(staticLoadFactor);
        } else {
            return new DynamicLoadFactor(options().initialLoadFactor, options().maximumLoadFactor);
        }
    }
}