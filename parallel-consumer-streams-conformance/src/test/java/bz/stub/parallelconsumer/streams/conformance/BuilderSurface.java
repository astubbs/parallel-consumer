package bz.stub.parallelconsumer.streams.conformance;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.streams.conformance.ConformanceCase.OperationKind;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * The ten builder operations the coverage gate holds the corpus against (R16), and - for each - the node names a
 * translated topology has to contain before that operation counts as covered (KTD8).
 *
 * <h2>This is the module's copy of an unfrozen proto</h2>
 *
 * The operations are the {@code v1alpha1} Streams builder surface on the
 * {@code research/kafka-streams-foreign-wrappers} branch, which is <b>not frozen</b>. This module holds its own
 * copy - {@link OperationKind}, which this class simply orders and annotates - so the gate's claim is bounded to
 * what this module knows: it can say "no operation <em>this module knows about</em> ships uncovered", never "the
 * corpus covers the wrapper". Reconciling the two lists is a driver-rung obligation (R14, U7), named in that rung's
 * obligations note, and nothing here can make that claim on its behalf.
 *
 * <h2>Crediting: what the oracle BUILT, not what the YAML said (KTD8)</h2>
 *
 * An operation is credited only when {@link FinalState#topologyDescription()} - {@code Topology.describe()} for the
 * topology the oracle actually built - contains a node whose name starts with one of this class's prefixes. That is
 * what stops an oracle which silently dropped an operation from passing a gate that only read the case file.
 *
 * <p>The prefixes below were derived <b>empirically</b>, by printing {@code describe()} for a topology exercising
 * each operation against Kafka 3.9.2, not from the documentation. The table, and its three honest limits:</p>
 *
 * <table border="1">
 *     <caption>What credits what</caption>
 *     <tr><th>Operation</th><th>Crediting node prefix</th><th>Kind of credit</th></tr>
 *     <tr><td>{@code source}</td><td>{@code KSTREAM-SOURCE-}</td><td>its own node</td></tr>
 *     <tr><td>{@code map-values}</td><td>{@code KSTREAM-MAPVALUES-}, {@code KTABLE-MAPVALUES-}</td>
 *         <td>its own node - which of the two depends on whether the handle it read was a stream or a table</td></tr>
 *     <tr><td>{@code group-by-key}</td><td>{@code KSTREAM-AGGREGATE-}, {@code KSTREAM-REDUCE-}</td>
 *         <td><b>no node of its own</b> - see below</td></tr>
 *     <tr><td>{@code count}</td><td>{@code KSTREAM-AGGREGATE-}</td><td><b>shared with aggregate</b></td></tr>
 *     <tr><td>{@code reduce}</td><td>{@code KSTREAM-REDUCE-}</td>
 *         <td>its own node, windowed or not</td></tr>
 *     <tr><td>{@code join}</td><td>{@code KSTREAM-JOIN-}</td><td>its own node</td></tr>
 *     <tr><td>{@code windowed-by}</td><td>{@code KSTREAM-KEY-SELECT-}, {@code KTABLE-SUPPRESS-}</td>
 *         <td><b>no node of its own</b> - see below</td></tr>
 *     <tr><td>{@code aggregate}</td><td>{@code KSTREAM-AGGREGATE-}</td><td><b>shared with count</b></td></tr>
 *     <tr><td>{@code to-stream}</td><td>{@code KTABLE-TOSTREAM-}</td><td>its own node</td></tr>
 *     <tr><td>{@code sink}</td><td>{@code KSTREAM-SINK-}</td><td>its own node</td></tr>
 * </table>
 *
 * <h3>Limit 1: count and aggregate share one node name</h3>
 *
 * Kafka names both {@code count} and {@code aggregate} {@code KSTREAM-AGGREGATE-<n>} - a count <em>is</em> an
 * aggregation with a fixed aggregator - and the description carries nothing that tells them apart. So the credit
 * proves an aggregation node was built, not <em>which</em> aggregation: an oracle that translated a {@code count}
 * into an {@code aggregate} would still be credited here. What that mistranslation does redden is the outcome
 * itself, in {@code OracleTest}'s hand-derived expectations - {@code count} is Long-valued and an aggregate is not.
 *
 * <h3>Limit 2: group-by-key and windowed-by mint no node at all</h3>
 *
 * Both are builder steps rather than processors: {@code groupByKey()} returns a {@code KGroupedStream} and
 * {@code windowedBy()} a {@code TimeWindowedKStream}, and neither adds anything to the topology until an
 * aggregation is called on it. They are therefore credited by a <b>witness</b> - a node that cannot exist without
 * them:
 *
 * <ul>
 *     <li><b>{@code group-by-key}</b> is witnessed by any aggregation node, because an aggregation is reachable
 *     only through a grouping. This credit is <em>structural</em>: no sabotage can remove the grouping while
 *     leaving the aggregation, so this cell cannot fire independently and it is not a translation check.</li>
 *     <li><b>{@code windowed-by}</b> is witnessed by {@code KSTREAM-KEY-SELECT-}, the key-selecting node the
 *     oracle's own {@code to-stream} mints when it drops a window (a non-windowed {@code toStream()} mints none), or
 *     by {@code KTABLE-SUPPRESS-}, which only a pinned-emit case builds. That credit witnesses the oracle's
 *     <em>windowed path</em> rather than Kafka's {@code windowedBy}, and it does fire: drop the {@code windowedBy}
 *     call and the key-select disappears with it. The cost is that <b>a windowed case with neither a
 *     {@code to-stream} nor an emit rule has no witness in the description at all</b>, and the gate will report its
 *     {@code windowed-by} as named-but-not-built. That is a conservative red rather than a silent pass, and the fix
 *     is to give the case a {@code to-stream}.</li>
 * </ul>
 *
 * <h3>Limit 3: only the joins the surface has</h3>
 *
 * Kafka mints {@code KSTREAM-LEFTJOIN-} and {@code KSTREAM-OUTERJOIN-} for the other join flavours. Neither is
 * listed, because the builder surface has one {@code join} and the oracle translates it to an inner join only - a
 * prefix no operation on this surface can produce would be a credit that can never fire, which is indistinguishable
 * from a credit nobody thought about.
 *
 * @see CorpusCoverageTest
 */
public final class BuilderSurface {

    /**
     * Per operation, the node-name prefixes that credit it. Guava's immutables rather than wrapped mutable maps:
     * these are constants, and an unmodifiable view of a mutable map is not one.
     */
    private static final ImmutableMap<OperationKind, ImmutableList<String>> CREDITING_NODE_PREFIXES =
            ImmutableMap.<OperationKind, ImmutableList<String>>builder()
                    .put(OperationKind.SOURCE, ImmutableList.of("KSTREAM-SOURCE-"))
                    .put(OperationKind.MAP_VALUES, ImmutableList.of("KSTREAM-MAPVALUES-", "KTABLE-MAPVALUES-"))
                    .put(OperationKind.GROUP_BY_KEY, ImmutableList.of("KSTREAM-AGGREGATE-", "KSTREAM-REDUCE-"))
                    .put(OperationKind.COUNT, ImmutableList.of("KSTREAM-AGGREGATE-"))
                    .put(OperationKind.REDUCE, ImmutableList.of("KSTREAM-REDUCE-"))
                    .put(OperationKind.JOIN, ImmutableList.of("KSTREAM-JOIN-"))
                    .put(OperationKind.WINDOWED_BY, ImmutableList.of("KSTREAM-KEY-SELECT-", "KTABLE-SUPPRESS-"))
                    .put(OperationKind.AGGREGATE, ImmutableList.of("KSTREAM-AGGREGATE-"))
                    .put(OperationKind.TO_STREAM, ImmutableList.of("KTABLE-TOSTREAM-"))
                    .put(OperationKind.SINK, ImmutableList.of("KSTREAM-SINK-"))
                    .build();

    /** The two operations that add nothing to a topology on their own - see the class javadoc's Limit 2. */
    private static final ImmutableSet<OperationKind> MINT_NO_NODE_OF_THEIR_OWN =
            ImmutableSet.of(OperationKind.GROUP_BY_KEY, OperationKind.WINDOWED_BY);

    /** The two operations Kafka gives one node name - see the class javadoc's Limit 1. */
    private static final ImmutableSet<OperationKind> SHARE_A_NODE_NAME =
            ImmutableSet.of(OperationKind.COUNT, OperationKind.AGGREGATE);

    private BuilderSurface() {
    }

    /** The ten operations, in the order {@link OperationKind} declares them, which is builder order. */
    public static List<OperationKind> all() {
        return ImmutableList.copyOf(Arrays.asList(OperationKind.values()));
    }

    /** The node-name prefixes that credit {@code kind}; never empty. */
    public static List<String> creditingNodePrefixes(OperationKind kind) {
        List<String> prefixes = CREDITING_NODE_PREFIXES.get(kind);
        if (prefixes == null) {
            // The surface is derived from the enum, so a missing entry is this class drifting from it rather than
            // a caller mistake - named here so it cannot be read as "this operation is simply never credited".
            throw new IllegalStateException("the builder surface knows no crediting node for " + kind.spelling()
                    + "; ConformanceCase.OperationKind has gained an operation this class was not updated for, and "
                    + "an uncredited operation would silently never be coverable");
        }
        return prefixes;
    }

    /**
     * Whether {@code kind} mints a node of its own, or is credited by a witness node it cannot exist without.
     * A witness credit is a weaker claim, and the gate's message says so rather than implying a translation check
     * that is not happening.
     */
    public static boolean mintsItsOwnNode(OperationKind kind) {
        return !MINT_NO_NODE_OF_THEIR_OWN.contains(kind);
    }

    /**
     * Whether another operation on this surface is credited by the same node name - {@code count} and
     * {@code aggregate}, which Kafka names identically. Where this is true the credit proves an operation of that
     * <em>family</em> was built, not which one.
     */
    public static boolean sharesItsNodeName(OperationKind kind) {
        return SHARE_A_NODE_NAME.contains(kind);
    }

    /**
     * Whether a topology description contains a node crediting {@code kind}.
     *
     * @param topologyDescription {@link FinalState#topologyDescription()} for a case the oracle executed
     */
    static boolean creditedBy(OperationKind kind, String topologyDescription) {
        return creditingNodePrefixes(kind).stream().anyMatch(topologyDescription::contains);
    }

    /** One line naming what credits {@code kind} and how strong that credit is - for a red a maintainer must act on. */
    static String creditExplanation(OperationKind kind) {
        String prefixes = String.join(" or ", creditingNodePrefixes(kind));
        if (!mintsItsOwnNode(kind)) {
            return kind.spelling() + " mints no topology node of its own and is credited by the witness node "
                    + prefixes + ", which cannot exist without it";
        }
        if (sharesItsNodeName(kind)) {
            return kind.spelling() + " is credited by " + prefixes + ", a node name it SHARES with another "
                    + "operation on this surface - the credit proves an operation of that family was built, not "
                    + "which one";
        }
        return kind.spelling() + " is credited by its own node, " + prefixes;
    }

    /** The whole table, for a message that has to show a maintainer what the gate was looking for. */
    static Map<OperationKind, ImmutableList<String>> table() {
        return CREDITING_NODE_PREFIXES;
    }
}
