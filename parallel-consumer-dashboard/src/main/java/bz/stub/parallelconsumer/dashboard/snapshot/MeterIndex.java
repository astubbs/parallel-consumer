package bz.stub.parallelconsumer.dashboard.snapshot;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.metrics.PCMetricsDef;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.search.Search;
import org.apache.kafka.common.annotation.InterfaceStability;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Parallel Consumer's own meters, pulled out of a {@link MeterRegistry} in ONE pass and keyed by name.
 * <p>
 * <strong>Why this exists.</strong> {@link Search} looks inviting - {@code Search.in(registry).name(x).gauge()} reads
 * like a map lookup - but it is a linear scan of a fresh {@code ArrayList} copy of the <em>entire</em> registry, every
 * single time: {@code Search.meters()/.gauges()/.counters()/.gauge()/.timer()/.summary()} all funnel through
 * {@link MeterRegistry#getMeters()}, which copies the whole meter map before anything is filtered. A snapshot needs
 * roughly two dozen of those lookups, and the registry it is handed is commonly the host application's shared one,
 * carrying JVM, HTTP, connection-pool and Kafka-client meters. That made the cost of one sample proportional to the
 * host application's whole metric surface, two dozen times over - on the control thread, which
 * {@link MeterSource} exists to keep cheap.
 * <p>
 * One pass, over {@link MeterRegistry#forEachMeter} rather than {@code getMeters()} so not even a single copy of the
 * registry is allocated, and every subsequent lookup is a hash lookup over PC's own handful of names.
 * <p>
 * <strong>Built per sample, never cached.</strong> Meters come and go at runtime - a rebalance adds and removes a
 * whole partition family - so an index that outlived a sample would go stale, and staleness here reads as "that
 * partition vanished". A per-sample index cannot: it is built at the top of {@link StateSampler#sample()} and
 * discarded with the snapshot. Within a sample it is also a small bonus, in that every value read comes from one
 * consistent view of which meters existed.
 * <p>
 * Only PC's own meters are retained - lookups are keyed by {@link PCMetricsDef}, so it is not possible to ask this
 * index for a name it deliberately dropped.
 * <p>
 * Note this indexes meters, not readings. A {@link io.micrometer.core.instrument.Gauge} held here is still evaluated
 * on the thread that calls {@code value()}, which is why {@link MeterSource}'s control-thread rule is unchanged.
 * <p>
 * Experimental: the dashboard module is opt-in and its API may change without notice.
 */
@InterfaceStability.Unstable
public final class MeterIndex {

    private static final Set<String> PC_METER_NAMES = pcMeterNames();

    /** Shared by every empty case - a null registry, and a registry with none of PC's meters in it. */
    private static final MeterIndex EMPTY = new MeterIndex(Collections.<String, List<Meter>>emptyMap());

    private final Map<String, List<Meter>> byName;

    private MeterIndex(Map<String, List<Meter>> byName) {
        this.byName = byName;
    }

    /**
     * Indexes {@code registry}'s Parallel Consumer meters. A null registry - the dashboard was wired up without one -
     * yields an index in which every meter is absent, which is the honest answer rather than a special case every
     * caller has to remember.
     */
    static MeterIndex of(MeterRegistry registry) {
        if (registry == null) {
            return EMPTY;
        }
        Map<String, List<Meter>> byName = new HashMap<>();
        registry.forEachMeter(meter -> {
            String name = meter.getId().getName();
            if (PC_METER_NAMES.contains(name)) {
                // a family such as pc.partition.highest.seen.offset has one meter per assigned partition
                byName.computeIfAbsent(name, key -> new ArrayList<>(2)).add(meter);
            }
        });
        return byName.isEmpty() ? EMPTY : new MeterIndex(byName);
    }

    /**
     * Whether any of Parallel Consumer's own meters were found. Distinguishes "PC has not bound its meters yet, or no
     * registry was supplied" from "PC is genuinely idle", which the page must not conflate.
     */
    boolean hasPcMeters() {
        return !byName.isEmpty();
    }

    /**
     * Every meter of {@code def}'s family that is a {@code type} - the whole per-partition family, or empty when the
     * meter is not registered. Mirrors {@code Search.gauges()}/{@code Search.counters()}, which likewise match on the
     * concrete meter type as well as the name.
     */
    <M extends Meter> List<M> all(PCMetricsDef def, Class<M> type) {
        List<Meter> meters = byName.get(def.getName());
        if (meters == null) {
            return Collections.emptyList();
        }
        List<M> matched = new ArrayList<>(meters.size());
        for (Meter meter : meters) {
            if (type.isInstance(meter)) {
                matched.add(type.cast(meter));
            }
        }
        return matched;
    }

    /**
     * The single meter registered under {@code def}, or null when it is absent. Null means absent and must stay
     * distinguishable from a reading of zero all the way to the page.
     */
    <M extends Meter> M first(PCMetricsDef def, Class<M> type) {
        List<Meter> meters = byName.get(def.getName());
        if (meters == null) {
            return null;
        }
        for (Meter meter : meters) {
            if (type.isInstance(meter)) {
                return type.cast(meter);
            }
        }
        return null;
    }

    private static Set<String> pcMeterNames() {
        Set<String> names = new HashSet<>();
        for (PCMetricsDef def : PCMetricsDef.values()) {
            names.add(def.getName());
        }
        return names;
    }
}
