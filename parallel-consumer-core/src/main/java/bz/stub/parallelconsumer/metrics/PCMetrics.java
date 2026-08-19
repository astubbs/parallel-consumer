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
import lombok.Synchronized;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.ToDoubleFunction;

import static java.util.Collections.singleton;

/**
 * Main metrics collection and initialization service. Singleton - makes it easier to add metrics throughout the code
 *
 * <p><b>Teardown is best-effort and never throws.</b> The meter registry is usually the <em>user's</em>, so every
 * call into it is third-party code running on PC's critical paths: meter removal runs inside
 * {@code onPartitionsRevoked} on the broker-poll thread, and {@link #close()} runs in
 * {@code AbstractParallelEoSStreamProcessor.doClose()}'s {@code finally}. A throw from the first kills the only
 * producer of commit responses, after which every commit blocks until it times out; a throw from the second
 * replaces the exception already in flight and strands callers polling {@code isClosedOrFailed()}. Metrics are
 * reporting, and must not be able to break consuming or shutting down - so every registry removal goes through
 * {@link #removeQuietly(Meter.Id, String)}, and a leaked meter is accepted as the far smaller problem.
 *
 * <p>Registration is <em>not</em> covered by that contract - see
 * {@code docs/inflight/bug-shutdown-teardown-race.md}.
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
     * Named monitor used by {@code @Synchronized("metersLock")} (and the narrow {@link #track(Meter.Id)}
     * add): guards {@link #registeredMeters} and the paired {@code meterRegistry} mutations that must be
     * atomic with it. A private lock, not {@code synchronized(this)}, so external holders of a
     * {@code PCMetrics} reference cannot interfere with the monitor.
     */
    private final Object metersLock = new Object();

    /**
     * Tracking of registered meters, for removal from the registry on shutdown.
     *
     * <p>A {@link LinkedHashSet} (not a {@link java.util.List}) so re-registering the same meter -
     * which happens once per partition per assignment, so on every rebalance - doesn't accumulate
     * duplicate entries. Micrometer's registry dedupes internally; only this tracking collection was growing,
     * which was the memory leak (<a href="https://github.com/confluentinc/parallel-consumer/issues/859">confluentinc#859</a>).
     *
     * <p>All access is guarded by {@link #metersLock}: every add (via {@link #track(Meter.Id)}) and
     * every remove/iterate ({@link #close()} / {@link #removeMeter(Meter)} /
     * {@link #removeMetersByPrefixAndCommonTags(String)}) holds that lock - so a rebalance registering
     * meters (broker-poll thread) cannot corrupt the set against a concurrent shutdown (control thread).
     */
    private final Set<Meter.Id> registeredMeters = new LinkedHashSet<>();

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
        track(counter.getId());
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
        track(timer.getId());
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
        track(gauge.getId());
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
        track(distributionSummary.getId());
        return distributionSummary;
    }

    /**
     * Records a freshly-registered meter for cleanup on shutdown - or, if a concurrent {@link #close()}
     * has already run, removes this late registration itself so it isn't orphaned in the registry.
     *
     * <p>Narrowly synchronized on {@link #metersLock} so the slow Micrometer {@code register()} call in
     * the caller stays outside the lock. Because {@code register()} runs before this, a meter can be
     * added to the registry on the broker-poll thread (a rebalance/commit) at the moment the control
     * thread runs {@code close()} on a timeout/exception shutdown (the one path where the poll thread
     * isn't joined first). Deciding the meter's fate here under the lock keeps register+track atomic
     * w.r.t. close: either it's tracked (and close removes it), or close already ran and we undo the
     * registration now. Without this the late meter would leak in the (often user-supplied) registry.
     */
    @Synchronized("metersLock")
    private void track(Meter.Id meterId) {
        if (this.isClosed.get()) {
            // Racing a concurrent close(): close() has already swept the registry and won't run again,
            // and register() ran outside this lock - so undo our late registration rather than orphan it.
            removeQuietly(meterId, "undoing a late registration");
            log.debug("Metrics subsystem closed; removed late-registered meter {}", meterId);
            return;
        }
        registeredMeters.add(meterId);
    }

    /**
     * Closes PCMetrics object and cleans up all meters from registry - should be recreated before using it again.
     */
    @Synchronized("metersLock")
    public void close() {
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
        // removeQuietly, not removeAndUntrack: this iterates registeredMeters, and clear() below empties it.
        this.registeredMeters.forEach(meterId -> removeQuietly(meterId, "closing the metrics subsystem"));
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
     * Removes the metric from the singleton's meter registry. Delegates to the {@code metersLock}-guarded
     * {@link #removeMeter(Meter.Id)}, which serialises removal against {@link #close()} to avoid a
     * concurrent-modification race on shutdown (partition-meter removal on revocation vs closing the
     * metrics subsystem).
     *
     * @param meter to remove.
     */
    public void removeMeter(Meter meter) {
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
    // Self-locks on metersLock (via @Synchronized); do not rely on callers holding it.
    @Synchronized("metersLock")
    private void removeMeter(Meter.Id meterId) {
        if (this.isClosed.get()) {
            //Already closed metrics subsystem - ignore
            log.debug("Trying to remove meter when metrics subsystem is already closed. Meter Id {}", meterId);
            return;
        }
        log.debug("Removing meter: {}", meterId);
        removeAndUntrack(meterId, "deregistering a meter");
    }

    /**
     * Removes one meter from the registry and <b>never throws</b> - the class-level teardown contract.
     * Guarded here rather than at the eleven call sites across {@link bz.stub.parallelconsumer.state.PartitionState},
     * {@link bz.stub.parallelconsumer.state.PartitionStateManager} and
     * {@link bz.stub.parallelconsumer.state.WorkManager}, because one missed site reproduces the failure.
     *
     * @param context what the caller was doing, for the log line when the registry refuses.
     */
    private void removeQuietly(Meter.Id meterId, String context) {
        try {
            this.meterRegistry.remove(meterId);
        } catch (Exception e) {
            log.warn("Failed to remove meter {} from the registry while {} - it may be left behind there. " +
                    "Continuing: metrics teardown is reporting, and must not be able to break consuming or " +
                    "shutting down. Cause: {}", meterId, context, e.toString(), e);
        }
    }

    /**
     * {@link #removeQuietly} plus dropping the id from our own tracking set - done whatever the registry did,
     * so a failing registry cannot grow that collection without bound (the confluentinc#859 leak).
     */
    private void removeAndUntrack(Meter.Id meterId, String context) {
        removeQuietly(meterId, context);
        this.registeredMeters.remove(meterId);
    }

    @Synchronized("metersLock")
    public void removeMetersByPrefixAndCommonTags(String meterNamePrefix) {
        if (this.isClosed.get()) {
            //Already closed metrics subsystem - ignore
            log.debug("Trying to remove meters when metrics subsystem is already closed.");
            return;
        }
        // TWO guards, and both are load-bearing. The inner one is per meter, not around the loop, so one
        // hostile meter cannot stop the rest being untracked - the confluentinc#859 leak lives in
        // registeredMeters, and a loop-level guard would leave its tail un-pruned. The outer one covers
        // the parts removeAndUntrack cannot reach: enumerating the registry (getMeters() is the user's
        // code too) and Meter.getId(). Narrowing to per-meter alone would drop enumeration cover that the
        // whole-body try/catch this merged from did have.
        String context = "removing meters with prefix '" + meterNamePrefix + "'";
        try {
            Search.in(meterRegistry).name(name -> name.startsWith(meterNamePrefix))
                    .tags(commonTags).meters()
                    .forEach(meter -> removeAndUntrack(meter.getId(), context));
        } catch (Exception e) {
            log.warn("Failed to enumerate meters while {} - some may be left behind in the registry and " +
                    "tracked here. Continuing: metrics teardown must not be able to break shutting down. " +
                    "Cause: {}", context, e.toString(), e);
        }
    }

    /**
     * Number of currently-tracked meters (those this instance will remove from the registry on
     * {@link #close()}). Primarily visible for testing the <a href="https://github.com/confluentinc/parallel-consumer/issues/859">confluentinc#859</a> leak fix: the leak lived in this
     * tracking collection (Micrometer's own registry dedupes), so tests assert on this count rather
     * than reflecting into the private field.
     */
    @Synchronized("metersLock")
    public int registeredMeterCount() {
        return registeredMeters.size();
    }
}