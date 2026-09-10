package bz.stub.parallelconsumer.fluent;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.metrics.PCMetrics;
import bz.stub.parallelconsumer.metrics.PCMetricsDef;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Tag;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

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
 * thread, from the same loop-end pass that takes the parked snapshot.
 */
@Slf4j
class FluentMeters {

    /**
     * The values of the {@code outcome} tag. They are the terminal outcomes of R7 plus the stop request of R24,
     * which is not a terminal outcome of the record but is counted beside them.
     */
    static final String SUCCEEDED = "succeeded";

    static final String FILTERED = "filtered";

    static final String PARKED = "parked";

    static final String STOPPED = "stopped";

    private static final String[] OUTCOMES = {SUCCEEDED, FILTERED, PARKED, STOPPED};

    private static final String TOPIC_TAG = "topic";

    private static final String PARTITION_TAG = "partition";

    private static final String OUTCOME_TAG = "outcome";

    /**
     * Where the gauges read from: the same snapshot the parked view answers from, so a dashboard and a query never
     * disagree.
     */
    private final ParkedSnapshots snapshots;

    private final PCMetrics metrics;

    private final Map<String, Counter> countersByTopicAndOutcome = new LinkedHashMap<>();

    private final ConcurrentMap<TopicPartition, List<Meter>> gaugesByPartition = new ConcurrentHashMap<>();

    private volatile boolean deregistered;

    private FluentMeters(PCMetrics metrics, ParkedSnapshots snapshots) {
        this.metrics = metrics;
        this.snapshots = snapshots;
    }

    /**
     * Registers one counter per routed topic per outcome, and returns the handle the dispatch wrapper reports to.
     */
    static FluentMeters registerFor(PCMetrics metrics, Collection<String> topics, ParkedSnapshots snapshots) {
        FluentMeters meters = new FluentMeters(metrics, snapshots);
        for (String topic : topics) {
            for (String outcome : OUTCOMES) {
                meters.countersByTopicAndOutcome.put(key(topic, outcome),
                        metrics.getCounterFromMetricDef(PCMetricsDef.ROUTE_RECORDS,
                                Tag.of(TOPIC_TAG, topic), Tag.of(OUTCOME_TAG, outcome)));
            }
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
     * Called from a worker thread, in the outcome path of a record. A meter that is not there - an outcome for a
     * topic this instance does not route, which cannot happen - is a missing count, never a failed record.
     */
    void recordOutcome(String topic, String outcome) {
        if (metrics == null) {
            return;
        }
        Counter counter = countersByTopicAndOutcome.get(key(topic, outcome));
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

    private List<Meter> registerGaugesFor(TopicPartition partition) {
        Tag[] tags = {Tag.of(TOPIC_TAG, partition.topic()),
                Tag.of(PARTITION_TAG, String.valueOf(partition.partition()))};
        Gauge parkedNow = metrics.gaugeFromMetricDef(PCMetricsDef.ROUTE_PARKED_RECORDS, snapshots,
                taken -> countParked(taken, partition), tags);
        Gauge oldest = metrics.gaugeFromMetricDef(PCMetricsDef.ROUTE_PARKED_OLDEST_AGE, snapshots,
                taken -> oldestParkedAgeSeconds(taken, partition), tags);
        List<Meter> registered = new ArrayList<>(2);
        registered.add(parkedNow);
        registered.add(oldest);
        return registered;
    }

    private static double countParked(ParkedSnapshots snapshots, TopicPartition partition) {
        int count = 0;
        for (ParkedRecord parked : snapshots.current()) {
            if (parked.partition() == partition.partition() && parked.topic().equals(partition.topic())) {
                count++;
            }
        }
        return count;
    }

    /**
     * @return the age in seconds of the oldest parked record on this partition, or zero when nothing is parked -
     * zero rather than NaN, because a gauge that disappears from a dashboard when the good news arrives reads as a
     * broken exporter
     */
    private static double oldestParkedAgeSeconds(ParkedSnapshots snapshots, TopicPartition partition) {
        Instant oldest = null;
        for (ParkedRecord parked : snapshots.current()) {
            if (parked.partition() == partition.partition() && parked.topic().equals(partition.topic())
                    && (oldest == null || parked.parkedSince().isBefore(oldest))) {
                oldest = parked.parkedSince();
            }
        }
        if (oldest == null) {
            return 0d;
        }
        return Duration.between(oldest, Instant.now()).toMillis() / 1000d;
    }

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
        for (Counter counter : countersByTopicAndOutcome.values()) {
            metrics.removeMeter(counter);
        }
        for (TopicPartition partition : new ArrayList<>(gaugesByPartition.keySet())) {
            removeGaugesFor(partition);
        }
    }

    private static String key(String topic, String outcome) {
        return topic + ' ' + outcome;
    }
}
