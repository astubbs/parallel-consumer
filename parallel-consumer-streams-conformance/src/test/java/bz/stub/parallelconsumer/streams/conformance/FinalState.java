package bz.stub.parallelconsumer.streams.conformance;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * The close-deterministic outcome of one {@link ConformanceCase}: what every state store held and what every sink
 * carried, snapshotted inside the open {@link org.apache.kafka.streams.TopologyTestDriver} scope (R3, KTD3).
 * <p>
 * <b>Per observable, on purpose.</b> The two maps are keyed by store name and by sink topic, so a comparison can
 * name the store or the sink that diverged rather than reporting "the outcome differs" (R11). {@link Oracle} is the
 * only thing that builds one, and U4's differ is the only thing that compares two.
 * <p>
 * <b>Everything is already a value.</b> Each observable is a {@code List<String>} of rendered entries, and the
 * rendering happens in {@link Oracle} <em>before</em> any map or sort is built, because byte arrays compare by
 * identity: two sink records under one logical key would otherwise stay two entries and the determinism proof (R7)
 * would red for a Java reason with nothing to do with Kafka (KTD3). {@code Bytes} was the alternative; a rendered
 * string is value-equal in the same way and is additionally readable in a failure message, which is the whole point
 * of R11.
 *
 * <h2>The rendering, which is part of this type's contract</h2>
 *
 * A driver at a later rung has to produce these same strings, so they are fixed here rather than left to whatever
 * {@code toString} happened to be convenient. {@link Oracle#render} owns the per-value half:
 *
 * <ul>
 *     <li><b>A key-value store entry</b> - {@code <key> -> <value>}, sorted.</li>
 *     <li><b>A window store entry</b> - {@code <key>@[<windowStart>,<windowEnd>) -> <value>}, with absolute epoch
 *     milliseconds, sorted. The window bounds are in the rendering because a windowed store's whole content
 *     <em>is</em> the (key, window) pairs; a rendering that dropped them would let a window-arithmetic bug pass.</li>
 *     <li><b>A sink entry</b> - {@code <key> -> <value> @<timestampMs>}. A sink fed by a non-windowed handle folds
 *     to the last record per key, sorted by key; a sink fed by a windowed handle keeps its FULL ordered record list,
 *     because {@code to-stream} drops the window and last-per-key would keep one record of many (R3).</li>
 * </ul>
 *
 * The store rendering deliberately does <em>not</em> carry the record timestamp a Kafka timestamped store keeps
 * beside each value. That timestamp is an artefact of the store wrapper Kafka materialises an aggregation into, not
 * something the wrapper's builder surface exposes, so a foreign binding could not be expected to reproduce it and
 * comparing it would red on an implementation detail.
 */
public final class FinalState {

    private final Map<String, List<String>> stores;

    private final Map<String, List<String>> sinks;

    private final String topologyDescription;

    FinalState(Map<String, List<String>> stores, Map<String, List<String>> sinks, String topologyDescription) {
        this.stores = deepUnmodifiableCopy(stores);
        this.sinks = deepUnmodifiableCopy(sinks);
        this.topologyDescription = topologyDescription;
    }

    /**
     * Every state store the case's topology created, by store name, each as its sorted entry renderings.
     *
     * @return an immutable map; empty only for a topology with no store-creating operation, which the loader lets
     *         through only when the case has a sink (R3, AE8)
     */
    public Map<String, List<String>> stores() {
        return stores;
    }

    /**
     * Every sink the case's topology wrote to, by topic name, each folded to the observable R3 defines - the last
     * record per key for a sink fed by a non-windowed handle, the full ordered record list for one fed by a windowed
     * handle.
     *
     * @return an immutable map; a sink that received nothing is present with an empty list, so "the sink was silent"
     *         and "there is no such sink" stay distinguishable
     */
    public Map<String, List<String>> sinks() {
        return sinks;
    }

    /**
     * {@code Topology.describe().toString()} for the topology the oracle actually built.
     * <p>
     * Kept on the outcome because the coverage gate credits an operation only when the <em>translated</em> topology
     * contains a node for it, so coverage measures what the oracle built rather than what the YAML said (KTD8). It
     * is deliberately not part of {@link #equals(Object)}: two runs of one case build the same topology by
     * construction, and the node names Kafka mints carry positional indices that would make an unrelated edit to a
     * case's operation order read as a divergence.
     */
    public String topologyDescription() {
        return topologyDescription;
    }

    /**
     * Whether the update stream was captured. Always {@code false} on this rung: the loader refuses
     * {@code final-state+updates}, and the update-stream observable, its capture and its differ path are the driver
     * rung's work (R3, KTD5, AE6). It is a method rather than an absent one so a caller can assert the absence.
     */
    public boolean hasUpdateStream() {
        return false;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof FinalState)) {
            return false;
        }
        FinalState that = (FinalState) other;
        // The topology description is out: see topologyDescription().
        return stores.equals(that.stores) && sinks.equals(that.sinks);
    }

    @Override
    public int hashCode() {
        return Objects.hash(stores, sinks);
    }

    /**
     * Multi-line, one observable per block, so a failed comparison is readable in a JUnit message without a
     * maintainer having to re-run anything (R11).
     */
    @Override
    public String toString() {
        StringBuilder rendered = new StringBuilder("final state:");
        appendObservables(rendered, "store", stores);
        appendObservables(rendered, "sink", sinks);
        if (stores.isEmpty() && sinks.isEmpty()) {
            rendered.append(System.lineSeparator()).append("  (no observables)");
        }
        return rendered.toString();
    }

    private static void appendObservables(StringBuilder rendered, String what, Map<String, List<String>> observables) {
        for (Map.Entry<String, List<String>> observable : observables.entrySet()) {
            rendered.append(System.lineSeparator()).append("  ").append(what).append(' ')
                    .append(observable.getKey()).append(':');
            if (observable.getValue().isEmpty()) {
                rendered.append(" (empty)");
            }
            for (String entry : observable.getValue()) {
                rendered.append(System.lineSeparator()).append("    ").append(entry);
            }
        }
    }

    private static Map<String, List<String>> deepUnmodifiableCopy(Map<String, List<String>> source) {
        Map<String, List<String>> copy = new LinkedHashMap<>();
        for (Map.Entry<String, List<String>> entry : source.entrySet()) {
            List<String> previous = copy.put(entry.getKey(),
                    Collections.unmodifiableList(new java.util.ArrayList<>(entry.getValue())));
            // One entry per observable name, so there is never a previous - named rather than dropped, because a
            // silent overwrite here would hide two stores sharing a name.
            if (previous != null) {
                throw new IllegalArgumentException("two observables named " + entry.getKey());
            }
        }
        return Collections.unmodifiableMap(copy);
    }
}
