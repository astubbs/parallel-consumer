package bz.stub.parallelconsumer.fluent;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.metrics.PCMetrics;
import bz.stub.parallelconsumer.metrics.PCMetricsDef;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Tag;
import lombok.extern.slf4j.Slf4j;

import java.util.Collection;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * The fluent API's meters: what each route did with its records (R19, KTD8).
 * <p>
 * They register through the {@link PCMetrics} of the {@code PCModule} the definition builds, so they land in the
 * user's own registry, carry the instance's common tags, and are swept by the same close as every engine meter -
 * rather than through a second registry the facade would have to own and tear down itself.
 *
 * <h2>What these meters answer, and what answers the rest</h2>
 * The counters here are <b>outcome events</b>: they only ever go up, and they answer "how much of this topic
 * succeeded, was filtered, parked, or stopped the instance". The other half of R19 - the <b>live parked set</b>, its
 * size and the age of its oldest member per partition - is the engine's, published from the partition that owns the
 * records as {@code pc.partition.parked.records} and {@code pc.partition.parked.oldest.age}. The two are not the
 * same figure and neither substitutes for the other: a partition whose parked records were all resumed has a counter
 * standing still and a gauge that has gone to zero.
 *
 * <h2>Why the counters are pre-registered</h2>
 * The routed topics are known at start, so every topic-and-outcome counter exists from the first record - a meter
 * that appears only once something has gone wrong is a meter nobody has a dashboard for.
 */
@Slf4j
class FluentMeters {

    /**
     * The tag every meter here carries, because a route is named by its topic and that is how a user asks about it.
     */
    private static final String TOPIC_TAG = "topic";

    /**
     * Distinguishes the four counters that share one meter name, so a dashboard can sum them or split them without
     * knowing four names.
     */
    private static final String OUTCOME_TAG = "outcome";

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
     * Private: an instance arrives either through {@link #registerFor}, with its counters already in place, or
     * through {@link #none()}, with nothing at all.
     */
    private FluentMeters(PCMetrics metrics) {
        this.metrics = metrics;
    }

    /**
     * Registers one counter per routed topic per outcome, and returns the handle the dispatch wrapper reports to.
     */
    static FluentMeters registerFor(PCMetrics metrics, Collection<String> topics) {
        FluentMeters meters = new FluentMeters(metrics);
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
        return new FluentMeters(null);
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
     * Take every meter this instance registered back out of the user's registry (R19).
     * <p>
     * The engine's own close sweeps them too, since they were registered through its {@link PCMetrics} - this is
     * what makes the sweep happen at a moment the handle chooses rather than only inside the engine's shutdown, and
     * what makes it true for a handle whose engine never got as far as closing cleanly.
     * <p>
     * Safe to call twice, without a flag to say it has run: the counters are registered once, at start, on the
     * thread that started the instance, so there is nothing that can register behind the sweep and a second removal
     * of an already-removed meter is a no-op in the registry. The flag this used to hold existed for the parked
     * gauges, which the control thread registered as partitions arrived and which are the engine's now.
     */
    void deregister() {
        if (metrics == null) {
            return;
        }
        for (Map<OutcomeTag, Counter> counters : countersByTopic.values()) {
            for (Counter counter : counters.values()) {
                metrics.removeMeter(counter);
            }
        }
    }
}
