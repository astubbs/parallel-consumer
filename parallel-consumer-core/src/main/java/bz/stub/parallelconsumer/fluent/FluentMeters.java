package bz.stub.parallelconsumer.fluent;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.metrics.PCMetrics;
import bz.stub.parallelconsumer.metrics.PCMetricsDef;
import bz.stub.parallelconsumer.state.WorkContainer;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Tag;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Supplier;

/**
 * The fluent API's meters: what each route did with its records, and what is parked right now (R19, KTD8).
 * <p>
 * They register through the {@link PCMetrics} of the {@code PCModule} the definition builds, so they land in the
 * user's own registry, carry the instance's common tags, and are swept by the same close as every engine meter -
 * rather than through a second registry the facade would have to own and tear down itself.
 *
 * <h2>Which meter answers which question</h2>
 * The counters are <b>outcome events</b>: they only ever go up, and they answer "how much of this topic succeeded,
 * was filtered, parked, or stopped the instance". The gauges are the <b>live parked set</b>: its size and the age
 * of its oldest member, per partition, which is the number an operator acts on. R19 asks for both and they are not
 * the same figure - a partition whose parked records were all resumed has a counter that stands still and a gauge
 * that has gone to zero.
 *
 * <h2>Why the counters are pre-registered and the gauges are not</h2>
 * The routed topics are known at start, so every topic-and-outcome counter exists from the first record - a meter
 * that appears only once something has gone wrong is a meter nobody has a dashboard for. Partitions are not known
 * until the assignment arrives, so the gauges are created and removed as partitions come and go, on the control
 * thread, from the same loop-end pass that keeps the partition gauges in line with the assignment.
 *
 * <h2>The gauges read containers, not parked records</h2>
 * They are handed the engine's own {@link WorkContainer}s rather than the {@link ParkedRecord} view built over
 * them, because the only three things they need - topic, partition, and when it parked - are on the container
 * already. Building the view instead cost a {@code RecordContext}, a {@code ParkedRecord} and a <b>full key
 * deserialisation</b> per parked record, per gauge, per scrape: two gauges per assigned partition, each walking the
 * whole retry queue, so a two-dozen-partition instance holding a thousand parked records decoded tens of thousands
 * of keys every time a dashboard refreshed. The gauge values are identical either way.
 */
@Slf4j
class FluentMeters {

    /**
     * The tag every meter here carries, because a route is named by its topic and that is how a user asks about it.
     */
    private static final String TOPIC_TAG = "topic";

    /**
     * Carried by the gauges only. A parked record is acted on per partition - its partition's committed offset is
     * the thing being held - so the live figures are cut that way and the outcome counts are not.
     */
    private static final String PARTITION_TAG = "partition";

    /**
     * Distinguishes the four counters that share one meter name, so a dashboard can sum them or split them without
     * knowing four names.
     */
    private static final String OUTCOME_TAG = "outcome";

    /**
     * Where the gauges read from: the engine's retry queue, through the handle - the same set the parked view
     * answers from, so a dashboard and a query never disagree.
     * <p>
     * Held as a field rather than captured in each gauge's lambda because Micrometer keeps only a weak reference to
     * the object a gauge reads, so a lambda nothing else holds would be collected and the gauge would go dead.
     */
    private final Supplier<List<WorkContainer<?, ?>>> parkedContainers;

    /**
     * The engine's own metrics facade, which is what puts these meters in the user's registry with the instance's
     * common tags rather than in a second registry this class would have to own.
     * <p>
     * <b>Null is the whole of what {@link #none()} means.</b> Every method here returns early on it, so the
     * no-op form needs no subclass and no call site needs a null check of its own.
     */
    private final PCMetrics metrics;

    /**
     * One topic's counters, by outcome tag.
     * <p>
     * Nested rather than keyed on a composed {@code topic + separator + outcome} string, which is what it was:
     * that built a fresh String for every terminal record, on the hottest path this library has, purely to look a
     * counter up. A hash lookup of an interned topic and an {@link EnumMap} index allocate nothing. (The separator
     * in that composed key was also
     * a raw NUL byte, which made this file read as binary to {@code grep} and to {@code file}, so every tree-wide
     * sweep silently skipped it.)
     * <p>
     * Populated entirely inside {@code registerFor} before this object is published, so the plain maps need no
     * synchronisation.
     */
    private final Map<String, Map<OutcomeTag, Counter>> countersByTopic = new LinkedHashMap<>();

    /**
     * The gauge pair registered for each assigned partition, kept so that a partition this instance loses can have
     * its gauges taken back out - a gauge left behind reports zero for a partition somebody else now owns.
     * <p>
     * Concurrent because it is written by the control thread, from the loop-end pass, and swept by whichever thread
     * closes the instance.
     */
    private final ConcurrentMap<TopicPartition, List<Meter>> gaugesByPartition = new ConcurrentHashMap<>();

    /**
     * Latched by {@link #deregister()} so that nothing registers a meter after the sweep has walked the maps.
     * Volatile because the sweep runs on the closing thread and the registrations on the control thread.
     */
    private volatile boolean deregistered;

    /**
     * Private: an instance arrives either through {@link #registerFor}, with its counters already in place, or
     * through {@link #none()}, with nothing at all.
     */
    private FluentMeters(PCMetrics metrics, Supplier<List<WorkContainer<?, ?>>> parkedContainers) {
        this.metrics = metrics;
        this.parkedContainers = parkedContainers;
    }

    /**
     * Registers one counter per routed topic per outcome, and returns the handle the dispatch wrapper reports to.
     */
    static FluentMeters registerFor(PCMetrics metrics, Collection<String> topics,
                                    Supplier<List<WorkContainer<?, ?>>> parkedContainers) {
        FluentMeters meters = new FluentMeters(metrics, parkedContainers);
        for (String topic : topics) {
            Map<OutcomeTag, Counter> counters = new EnumMap<>(OutcomeTag.class);
            for (OutcomeTag outcome : OutcomeTag.values()) {
                counters.put(outcome, metrics.getCounterFromMetricDef(PCMetricsDef.ROUTE_RECORDS,
                        Tag.of(TOPIC_TAG, topic), Tag.of(OUTCOME_TAG, outcome.tagValue())));
            }
            meters.countersByTopic.put(topic, counters);
        }
        return meters;
    }

    /**
     * What the dispatch wrapper reports to before it is wired to a running instance, and in a test that drives the
     * wrapper directly: counts nothing, registers nothing, and is never null so no call site needs a guard.
     */
    static FluentMeters none() {
        return new FluentMeters(null, null);
    }

    /**
     * One record on this topic reached this outcome.
     * <p>
     * Called from a worker thread, in the outcome path of a record. A topic that is not there - one this instance
     * does not route, which cannot happen - is a missing count, never a failed record.
     */
    void recordOutcome(String topic, OutcomeTag outcome) {
        if (metrics == null) {
            return;
        }
        Map<OutcomeTag, Counter> counters = countersByTopic.get(topic);
        Counter counter = counters == null ? null : counters.get(outcome);
        if (counter == null) {
            log.debug("No {} counter for topic {} - not counting it", outcome, topic);
            return;
        }
        counter.increment();
    }

    /**
     * Bring the per-partition parked gauges into line with the assignment: a gauge pair for every partition this
     * instance owns, and none for a partition it has lost.
     * <p>
     * Runs on the control thread, from the loop-end hook. It never throws: the hook that calls it is contained, and
     * a meter that could stop a control loop would be a reporting fault taking down consuming.
     */
    void syncPartitionGauges(Set<TopicPartition> assigned) {
        if (metrics == null || deregistered) {
            return;
        }
        if (gaugesByPartition.keySet().equals(assigned)) {
            // The common case by a wide margin - the assignment only moves on a rebalance, and this runs on every
            // pass of the control loop.
            return;
        }
        for (TopicPartition partition : assigned) {
            gaugesByPartition.computeIfAbsent(partition, this::registerGaugesFor);
        }
        List<TopicPartition> gone = new ArrayList<>();
        for (TopicPartition partition : gaugesByPartition.keySet()) {
            if (!assigned.contains(partition)) {
                gone.add(partition);
            }
        }
        for (TopicPartition partition : gone) {
            removeGaugesFor(partition);
        }
    }

    /**
     * The pair of gauges one partition gets: how many records are parked on it, and how long its oldest parked
     * record has been parked. Both read the live set through {@link #parkedContainers} rather than a value cached
     * here, so a scrape sees the set as it is at the moment of the scrape.
     *
     * @return the meters to remember for removal, or an empty list when this instance has already been swept - in
     * which case nothing was registered either
     */
    private List<Meter> registerGaugesFor(TopicPartition partition) {
        if (deregistered) {
            // Re-checked inside the mapping function, not only at the top of syncPartitionGauges: deregister()
            // runs on the closing thread and sweeps the map, so a control thread that passed the outer check
            // before the sweep would otherwise register gauges nothing here will ever remove.
            return Collections.emptyList();
        }
        Tag[] tags = {Tag.of(TOPIC_TAG, partition.topic()),
                Tag.of(PARTITION_TAG, String.valueOf(partition.partition()))};
        Gauge parkedNow = metrics.gaugeFromMetricDef(PCMetricsDef.ROUTE_PARKED_RECORDS, parkedContainers,
                parked -> countParked(parked, partition), tags);
        Gauge oldest = metrics.gaugeFromMetricDef(PCMetricsDef.ROUTE_PARKED_OLDEST_AGE, parkedContainers,
                parked -> oldestParkedAgeSeconds(parked, partition), tags);
        return Arrays.<Meter>asList(parkedNow, oldest);
    }

    /**
     * Counts this partition's parked records by walking the engine's containers, because the engine owns the parked
     * set and keeps no per-partition tally beside it. Walking on every scrape is what keeps the gauge live: a
     * cached number would go stale the moment a record resumed, was exported, or the partition was revoked.
     *
     * @return how many of the instance's parked records are on this partition - the live size of the set an
     * operator can act on, which is the figure the parked counter deliberately is not
     */
    private static double countParked(Supplier<List<WorkContainer<?, ?>>> parkedContainers,
                                      TopicPartition partition) {
        int count = 0;
        for (WorkContainer<?, ?> container : parkedContainers.get()) {
            if (isOn(container, partition)) {
                count++;
            }
        }
        return count;
    }

    /**
     * The head-of-line age an operator alerts on: how long this partition's longest-parked record has been parked,
     * which is also how far back its committed offset is being held while later records complete.
     *
     * @return the age in seconds of the oldest parked record on this partition, or zero when nothing is parked -
     * zero rather than NaN, because a gauge that disappears from a dashboard when the good news arrives reads as a
     * broken exporter
     */
    private static double oldestParkedAgeSeconds(Supplier<List<WorkContainer<?, ?>>> parkedContainers,
                                                 TopicPartition partition) {
        Instant oldest = null;
        for (WorkContainer<?, ?> container : parkedContainers.get()) {
            if (!isOn(container, partition)) {
                continue;
            }
            // The same instant ParkedRecord.parkedSince() reports: the moment of the failure that parked it.
            Instant parkedSince = container.getLastFailedAt().orElse(Instant.EPOCH);
            if (oldest == null || parkedSince.isBefore(oldest)) {
                oldest = parkedSince;
            }
        }
        if (oldest == null) {
            return 0d;
        }
        return Duration.between(oldest, Instant.now()).toMillis() / 1000d;
    }

    /**
     * Whether this parked container belongs to the partition a gauge is reporting on. Compared field by field
     * rather than through {@code getTopicPartition()}, which builds a {@link TopicPartition} per call and would
     * allocate one per parked record per gauge per scrape.
     */
    private static boolean isOn(WorkContainer<?, ?> container, TopicPartition partition) {
        ConsumerRecord<?, ?> record = container.getCr();
        return record.partition() == partition.partition() && record.topic().equals(partition.topic());
    }

    /**
     * Takes one partition's gauges out of the user's registry. The map entry is claimed first, so two threads
     * arriving together - a rebalance pass and the close - cannot both deregister the same meters; whichever loses
     * finds nothing and returns, which is why an absent entry is not an error.
     */
    private void removeGaugesFor(TopicPartition partition) {
        List<Meter> meters = gaugesByPartition.remove(partition);
        if (meters == null) {
            return;
        }
        for (Meter meter : meters) {
            metrics.removeMeter(meter);
        }
    }

    /**
     * Take every meter this instance registered back out of the user's registry (R19).
     * <p>
     * The engine's own close sweeps them too, since they were registered through its {@link PCMetrics} - this is
     * what makes the sweep happen at a moment the handle chooses rather than only inside the engine's shutdown, and
     * what makes it true for a handle whose engine never got as far as closing cleanly.
     */
    void deregister() {
        if (metrics == null || deregistered) {
            return;
        }
        deregistered = true;
        for (Map<OutcomeTag, Counter> counters : countersByTopic.values()) {
            for (Counter counter : counters.values()) {
                metrics.removeMeter(counter);
            }
        }
        for (TopicPartition partition : new ArrayList<>(gaugesByPartition.keySet())) {
            removeGaugesFor(partition);
        }
    }
}
