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
import lombok.Setter;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;

import java.time.Clock;

/**
 * Minimum dependency injection system, modled on how Dagger works.
 * <p>
 * Note: Not using Dagger as PC has a zero dependency policy, and franky it would be overkill for our needs.
 *
 * @author Antony Stubbs
 */
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

    protected ProducerWrapper<K, V> producerWrap() {
        if (this.producerWrapper == null) {
            this.producerWrapper = new ProducerWrapper<>(options());
        }
        return producerWrapper;
    }

    private ProducerManager<K, V> producerManager;

    protected ProducerManager<K, V> producerManager() {
        if (producerManager == null) {
            this.producerManager = new ProducerManager<>(producerWrap(), consumerManager(), workManager(), options());
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
            consumerManager = new ConsumerManager<>(optionsInstance.getConsumer(),
                    optionsInstance.getOffsetCommitTimeout(),
                    optionsInstance.getSaslAuthenticationRetryTimeout(),
                    optionsInstance.getSaslAuthenticationExceptionRetryBackoff());
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