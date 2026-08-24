package bz.stub.parallelconsumer.streams;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.streams.protocol.v1alpha1.Node;
import bz.stub.parallelconsumer.streams.protocol.v1alpha1.NodeKind;
import bz.stub.parallelconsumer.streams.protocol.v1alpha1.TopologyDescription;
import org.apache.kafka.streams.Topology;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Stream;

import static com.google.common.truth.Truth.assertThat;

/**
 * The description a host receives of a topology it described but has never seen assembled.
 *
 * <p>The point of the class under test is that the host does <em>not</em> already know this. It issued builder calls
 * and holds opaque handles; the engine is the only side that has seen the assembled graph, including the nodes Kafka
 * Streams generated and the sub-topology split it chose.
 */
class TopologyDescriberTest {

    private final TopologyAssembler.MapperFactory echo = token -> (key, value) -> value;

    /** Concatenates aggregate and value, so a reduction's result depends on BOTH arguments crossing correctly. */
    private final TopologyAssembler.ReducerFactory concat = token -> (aggregate, value) -> {
        byte[] joined = new byte[aggregate.length + value.length];
        System.arraycopy(aggregate, 0, joined, 0, aggregate.length);
        System.arraycopy(value, 0, joined, aggregate.length, value.length);
        return joined;
    };

    /** The five-call chain the demo runs, which is also the widest topology this protocol can currently express. */
    private Topology countingTopology() {
        TopologyAssembler assembler = new TopologyAssembler(echo, concat);
        long source = assembler.source("input");
        long mapped = assembler.mapValues(source, 1L);
        long grouped = assembler.groupByKey(mapped);
        long counted = assembler.count(grouped, "counts-store");
        assembler.sink(counted, "output");
        return assembler.build();
    }

    private static Stream<Node> nodes(TopologyDescription description) {
        return description.getSubtopologiesList().stream().flatMap(sub -> sub.getNodesList().stream());
    }

    @Test
    void theTextIsExactlyWhatKafkaStreamsPrints() {
        Topology topology = countingTopology();

        TopologyDescription described = TopologyDescriber.describe(topology);

        // Byte-identical, not merely similar: the value of this field is that existing visualisers parse it, and
        // they parse Kafka's format rather than ours. Any reformatting here would break every one of them.
        assertThat(described.getText()).isEqualTo(topology.describe().toString());
    }

    @Test
    void theSourceAndSinkNodesCarryTheirTopics() {
        TopologyDescription described = TopologyDescriber.describe(countingTopology());

        List<Node> sources = nodes(described).filter(n -> n.getKind() == NodeKind.NODE_KIND_SOURCE).toList();
        List<Node> sinks = nodes(described).filter(n -> n.getKind() == NodeKind.NODE_KIND_SINK).toList();

        assertThat(sources).hasSize(1);
        assertThat(sources.get(0).getTopicsList()).containsExactly("input");
        assertThat(sinks).hasSize(1);
        assertThat(sinks.get(0).getTopicsList()).containsExactly("output");
    }

    @Test
    void theCountingProcessorReportsItsStateStore() {
        TopologyDescription described = TopologyDescriber.describe(countingTopology());

        List<String> stores = nodes(described)
                .filter(n -> n.getKind() == NodeKind.NODE_KIND_PROCESSOR)
                .flatMap(n -> n.getStoresList().stream())
                .toList();

        assertThat(stores).contains("counts-store");
    }

    @Test
    void theGraphIsConnectedFromSourceToSink() {
        TopologyDescription described = TopologyDescriber.describe(countingTopology());

        // Every node except a source has a predecessor, and every node except a sink has a successor. This is what
        // makes the structured form usable for drawing rather than merely a list of names.
        nodes(described).forEach(node -> {
            if (node.getKind() != NodeKind.NODE_KIND_SOURCE) {
                assertThat(node.getPredecessorsList()).isNotEmpty();
            }
            if (node.getKind() != NodeKind.NODE_KIND_SINK) {
                assertThat(node.getSuccessorsList()).isNotEmpty();
            }
        });
    }

    @Test
    void theAggregationNeedsNoRepartitionAndTheHostCanSeeThat() {
        TopologyDescription described = TopologyDescriber.describe(countingTopology());

        // Written expecting the opposite - that grouping before an aggregation forces a repartition - and the
        // topology says otherwise. mapValues cannot change the key, so Kafka Streams knows the existing partitioning
        // is still correct and skips the repartition entirely. One sub-topology, no extra topic, no extra hop.
        //
        // That is exactly the kind of decision the host cannot infer from its own five calls, which is the case for
        // describing at all. It is also a regression test worth having: widening mapValues into a general map, which
        // CAN change the key, would silently introduce a repartition topic and an extra broker round trip per record.
        // This fires when that happens.
        assertThat(described.getSubtopologiesCount()).isEqualTo(1);
        assertThat(described.getText()).doesNotContain("repartition");
    }

    @Test
    void thereAreNoGlobalStores() {
        // Not reachable through the builder methods this protocol exposes. Asserted rather than assumed, so that
        // adding one later fails here instead of silently vanishing from the structured form.
        assertThat(TopologyDescriber.describe(countingTopology()).getGlobalStoreNamesList()).isEmpty();
    }

    @Test
    void describingTwiceGivesTheSameAnswer() {
        // Kafka Streams returns nodes in a Set, so without the sort this class applies the order is undefined and
        // a diff between two descriptions of one topology would be unreadable.
        Topology topology = countingTopology();

        assertThat(TopologyDescriber.describe(topology)).isEqualTo(TopologyDescriber.describe(topology));
    }

    @Test
    void describingATopologyDoesNotConsumeTheRightToStartIt() {
        TopologyAssembler assembler = new TopologyAssembler(echo, concat);
        long source = assembler.source("input");
        assembler.sink(source, "output");

        Topology described = assembler.build();
        TopologyDescriber.describe(described);

        // The same instance, not merely an equal one: a second build would produce a second topology, and the
        // session would then start something other than what it just showed the host.
        assertThat(assembler.build()).isSameInstanceAs(described);
    }
}
