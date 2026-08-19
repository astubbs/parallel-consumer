package bz.stub.parallelconsumer.metrics;

/*-
 * Copyright (C) 2020-2023 Confluent, Inc.
 * Modifications Copyright (C) 2026 Antony Stubbs and contributors
 */

import io.micrometer.core.instrument.*;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.micrometer.core.instrument.search.Search;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.ToDoubleFunction;

import static java.util.Collections.singleton;

/**
 * Main metrics collection and initialization service. Singleton - makes it easier to add metrics throughout the code
 */
@Slf4j
public class PCMetrics {

    /**
     * Meter registry used for metrics - set through init call on singleton initialization. Configurable through
     * Parallel Consumer Options.
     */
    @Getter
    private MeterRegistry meterRegistry;

    /**
     * Tracking of registered meters for removal from registry on shutdown.
     */
    private List<Meter.Id> registeredMeters = new ArrayList<>();

    /**
     * Common metrics tags added to all meters - for example PC instance. Configurable through Parallel Consumer
     * Options.
     */
    @Getter
    private Iterable<Tag> commonTags;

    @Getter
    private Tag instanceTag;

    private final AtomicBoolean isClosed = new AtomicBoolean(true);

    private final boolean isNoop;

    /**
     * @param meterRegistry: meterRegistry to use for meter registration - configured through
     *                       {@link bz.stub.parallelconsumer.ParallelConsumerOptions} on PC initialization
     * @param commonTags:    set of tags to add to all meters - for example - PC instance.
     */
    public PCMetrics(MeterRegistry meterRegistry, Iterable<Tag> commonTags, String instanceTag) {
        if (meterRegistry == null) {
            this.isNoop = true;
            this.meterRegistry = new CompositeMeterRegistry();
        } else {
            this.isNoop = false;
            this.meterRegistry = meterRegistry;
        }
        if (instanceTag != null) {
            this.instanceTag = Tag.of(PCMetricsDef.PC_INSTANCE_TAG, instanceTag);
        } else {
            this.instanceTag = generateUniqueInstanceTag();
        }
        this.commonTags = combine(this.instanceTag, commonTags);
        this.isClosed.set(false);
    }

    /**
     * Combines instance tag and common tags specified while ensuring there are no tags with same tag key.
     *
     * @param instanceTag
     * @param commonTags
     * @return combined tag collection with unique tag keys
     */
    private Iterable<Tag> combine(Tag instanceTag, Iterable<Tag> commonTags) {
        Set<String> tagKeys = new HashSet<>();
        List<Tag> tags = new LinkedList<>();

        tagKeys.add(instanceTag.getKey());
        tags.add(instanceTag);
        commonTags.forEach(tag -> {
            if (!tagKeys.contains(tag.getKey())) {
                tagKeys.add(tag.getKey());
                tags.add(tag);
            } else {
                log.warn("Duplicate metrics tag specified : {}", tag.getKey());
            }
        });
        return tags;
    }


    private Tag generateUniqueInstanceTag() {
        boolean inUse;
        Tag tagToUse;
        do {
            tagToUse = Tag.of(PCMetricsDef.PC_INSTANCE_TAG, UUID.randomUUID().toString());
            inUse = Search.in(meterRegistry).tags(singleton(instanceTag)).meter() != null;
        } while (inUse);
        return tagToUse;
    }

    /**
     * Returns a counter from the metric definition. The counter will be registered with the meter.
     *
     * @param metricDef:      the metric definition to use.
     * @param additionalTags: additional tags to add to the counter.
     */
    public Counter getCounterFromMetricDef(PCMetricsDef metricDef, Tag... additionalTags) {
        Counter counter = Counter.builder(metricDef.getName())
                .description(metricDef.getDescription())
                .tags(commonTags)
                .tags(metricDef.getSubsystemAsTagsOrEmpty())
                .tags(Arrays.asList(additionalTags))
                .register(this.meterRegistry);
        registeredMeters.add(counter.getId());
        return counter;
    }

    /**
     * Returns a timer from the metric definition. The timer will be registered with the meter.
     *
     * @param metricDef:      the metric definition to use.
     * @param additionalTags: additional tags to add to the timer.
     */
    public Timer getTimerFromMetricDef(PCMetricsDef metricDef, Tag... additionalTags) {
        Timer timer = Timer.builder(metricDef.getName())
                .publishPercentiles(0, 0.5, 0.75, 0.95, 0.99, 0.999)
                .description(metricDef.getDescription())
                .tags(commonTags)
                .tags(metricDef.getSubsystemAsTagsOrEmpty())
                .tags(Arrays.asList(additionalTags))
                .register(this.meterRegistry);
        registeredMeters.add(timer.getId());
        return timer;
    }

    /**
     * Returns a gauge from the metric definition. The gauge will be registered with the meter. The returned Gauge
     * instance is not useful except in testing, as the gauge is already set up to track a value automatically upon
     * registration.
     *
     * <p><strong>Note: Make sure you hold a strong reference to your object. Otherwise once the
     * object being gauged is re-referenced and is garbage collected, micrometer starts reporting NaN or nothing for a
     * gauge</strong>
     *
     * <p>See <a
     * href="https://github.com/micrometer-metrics/micrometer-docs/blob/main/src/docs/concepts/gauges.adoc#why-is-my-gauge-reporting-nan-or-disappearing">micrometer
     * docs</a> for more info
     *
     * @param metricDef:      the metric definition to use.
     * @param stateObject:    object to collect metrics from
     * @param valueFunction:  function of the stateObject that is invoked on gauge observation to return the value
     * @param additionalTags: additional tags to add to the gauge.
     * @return the Gauge instance.
     */
    public <T> Gauge gaugeFromMetricDef(
            PCMetricsDef metricDef,
            T stateObject,
            ToDoubleFunction<T> valueFunction,
            Tag... additionalTags) {
        Gauge gauge = Gauge.builder(metricDef.getName(), stateObject, valueFunction)
                .description(metricDef.getDescription())
                .tags(commonTags)
                .tags(metricDef.getSubsystemAsTagsOrEmpty())
                .tags(Arrays.asList(additionalTags))
                .strongReference(true)
                .register(this.meterRegistry);
        registeredMeters.add(gauge.getId());
        return gauge;
    }

    /**
     * Returns a distribution summary from the metric definition. The distribution summary will be registered with the
     * meter.
     *
     * @param metricDef:      the metric definition to use.
     * @param additionalTags: additional tags to add to the distribution summary.
     * @return the DistributionSummary instance.
     */
    public DistributionSummary getDistributionSummaryFromMetricDef(
            PCMetricsDef metricDef, Tag... additionalTags) {
        DistributionSummary distributionSummary = DistributionSummary.builder(metricDef.getName())
                .publishPercentiles(0, 0.5, 0.75, 0.95, 0.99, 0.999)
                .description(metricDef.getDescription())
                .tags(commonTags)
                .tags(metricDef.getSubsystemAsTagsOrEmpty())
                .tags(Arrays.asList(additionalTags))
                .register(this.meterRegistry);
        registeredMeters.add(distributionSummary.getId());
        return distributionSummary;
    }

    /**
     * Closes PCMetrics object and cleans up all meters from registry - should be recreated before using it again.
     */
    public synchronized void close() {
        if (this.isClosed.getAndSet(true)) {
            //Instance already closed - warn and ignore.
            log.warn("Trying to close PCMetrics instance that is already closed.");
            return;
        }
        log.debug("Closing PCMetrics");
        // clean up the instance resources. Same never-throws contract as removeMeter, and it has to
        // be repeated here because this iterates the registry directly rather than going through it.
        // Missing this was invisible while the only caller wrapped the call itself: doClose's finally
        // catches, so the escape only appeared once the fix was lifted onto a branch without that
        // wrapper. A contract that depends on every caller guarding it is not a contract.
        this.registeredMeters.forEach(meterId -> {
            try {
                this.meterRegistry.remove(meterId);
            } catch (Exception e) {
                log.warn("Failed to remove meter {} while closing the metrics subsystem - it may be " +
                        "left behind in the registry. Continuing: metrics teardown must not be able " +
                        "to break shutting down. Cause: {}", meterId, e.toString(), e);
            }
        });
        this.registeredMeters.clear();
        if (isNoop) {
            // Only ever OUR CompositeMeterRegistry, never the user's - but guarded anyway, because
            // the reason to guard is the caller's inability to recover, not the callee's identity.
            try {
                this.meterRegistry.close();
            } catch (Exception e) {
                log.warn("Failed to close the internal no-op meter registry. Continuing. Cause: {}",
                        e.toString(), e);
            }
        }
    }

    /**
     * Removes the metric from the singletons meter registry.
     * <p>
     * Synchronized with close method to avoid concurrent modification race on shutdown between removal of partition
     * meters on revocation and closing metrics subsystem
     *
     * @param meter to remove.
     */
    public synchronized void removeMeter(Meter meter) {
        if (meter != null) {
            removeMeter(meter.getId());
        }
    }


    /**
     * Removes a meter, and <b>never throws</b> - see the class-level note on why teardown is
     * best-effort. Guarded HERE rather than at the call sites because there are eleven of them,
     * across {@link bz.stub.parallelconsumer.state.PartitionState},
     * {@link bz.stub.parallelconsumer.state.PartitionStateManager} and
     * {@link bz.stub.parallelconsumer.state.WorkManager}, and one missed site is enough to reproduce
     * the failure this prevents.
     */
    private void removeMeter(Meter.Id meterId) {
        if (this.isClosed.get()) {
            //Already closed metrics subsystem - ignore
            log.debug("Trying to remove meter when metrics subsystem is already closed. Meter Id {}", meterId);
            return;
        }
        log.debug("Removing meter: {}", meterId);
        try {
            this.meterRegistry.remove(meterId);
        } catch (Exception e) {
            // The registry is usually the USER'S, so this is third-party code. A throw here escapes
            // through PartitionState.deregisterMetrics into onPartitionsRevoked, which runs on the
            // broker-poll thread inside poll() - killing the only producer of commit responses, so
            // every later commit blocks until it times out. A leaked meter is a far smaller problem.
            log.warn("Failed to remove meter {} from the registry - it may be left behind there. " +
                    "Continuing: metrics teardown is reporting, and must not be able to break " +
                    "consuming or shutting down. Cause: {}", meterId, e.toString(), e);
        }
        // Always dropped from OUR map, whatever the registry did, so a failing registry cannot also
        // grow this collection without bound.
        this.registeredMeters.remove(meterId);
    }

    public void removeMetersByPrefixAndCommonTags(String meterNamePrefix) {
        if (this.isClosed.get()) {
            //Already closed metrics subsystem - ignore
            log.debug("Trying to remove meters when metrics subsystem is already closed.");
            return;
        }
        try {
            Search.in(meterRegistry).name(name -> name.startsWith(meterNamePrefix))
                    .tags(commonTags).meters().forEach(meterRegistry::remove);
        } catch (Exception e) {
            // Same contract as removeMeter: never throw. This one runs inside doClose's finally,
            // where an escape would replace the in-flight exception and skip the state transition.
            log.warn("Failed to remove meters with prefix '{}' from the registry - they may be left " +
                    "behind there. Continuing: metrics teardown must not be able to break shutting " +
                    "down. Cause: {}", meterNamePrefix, e.toString(), e);
        }
    }
}