// Copyright (C) 2026 Antony Stubbs and contributors
package bz.stub.parallelconsumer.streams;

import bz.stub.parallelconsumer.streams.protocol.v1alpha1.Node;
import bz.stub.parallelconsumer.streams.protocol.v1alpha1.NodeKind;
import bz.stub.parallelconsumer.streams.protocol.v1alpha1.Subtopology;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyDescription;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

/**
 * Turns an assembled {@link Topology} into the wire's description of it.
 *
 * <p>Both forms are produced from the same walk: the text Kafka Streams prints, and the graph
 * already parsed. The text exists because every Kafka Streams visualiser consumes it, so emitting
 * it hands a non-JVM host the whole existing tooling ecosystem. The structured form exists so this
 * project's own tooling never has to parse that text back - it is a human-readable rendering with
 * no stability guarantee, and each of those visualisers has had to reverse-engineer its shape.
 *
 * <p>Node names are sorted rather than left in iteration order. Kafka Streams returns nodes in a
 * {@link Set}, so the order is not defined and can differ between runs of the same topology; a
 * description that reorders itself would make a diff between two runs unreadable and would make
 * any test of this class flaky for a reason that has nothing to do with the topology.
 */
final class TopologyDescriber {

    private TopologyDescriber() {
    }

    static bz.stub.parallelconsumer.streams.protocol.v1alpha1.TopologyDescription describe(Topology topology) {
        TopologyDescription description = topology.describe();
        var message = bz.stub.parallelconsumer.streams.protocol.v1alpha1.TopologyDescription.newBuilder()
                .setText(description.toString());

        List<TopologyDescription.Subtopology> ordered = new ArrayList<>(description.subtopologies());
        ordered.sort((left, right) -> Integer.compare(left.id(), right.id()));
        for (TopologyDescription.Subtopology subtopology : ordered) {
            message.addSubtopologies(convert(subtopology));
        }

        // Cannot arise from the builder methods this protocol exposes - reported rather than
        // dropped, so that the day one can, the structured form does not quietly lose it.
        for (TopologyDescription.GlobalStore store : description.globalStores()) {
            message.addGlobalStoreNames(store.processor().name());
        }
        return message.build();
    }

    private static Subtopology convert(TopologyDescription.Subtopology subtopology) {
        Subtopology.Builder builder = Subtopology.newBuilder().setId(subtopology.id());
        for (TopologyDescription.Node node : sortedByName(subtopology.nodes())) {
            builder.addNodes(convert(node));
        }
        return builder.build();
    }

    private static Node convert(TopologyDescription.Node node) {
        Node.Builder builder = Node.newBuilder().setName(node.name());
        for (TopologyDescription.Node predecessor : sortedByName(node.predecessors())) {
            builder.addPredecessors(predecessor.name());
        }
        for (TopologyDescription.Node successor : sortedByName(node.successors())) {
            builder.addSuccessors(successor.name());
        }

        if (node instanceof TopologyDescription.Source source) {
            builder.setKind(NodeKind.NODE_KIND_SOURCE);
            if (source.topicSet() != null) {
                builder.addAllTopics(new TreeSet<>(source.topicSet()));
            }
            if (source.topicPattern() != null) {
                builder.setTopicPattern(source.topicPattern().pattern());
            }
        } else if (node instanceof TopologyDescription.Sink sink) {
            builder.setKind(NodeKind.NODE_KIND_SINK);
            // Null when the sink routes by a TopicNameExtractor rather than a fixed name. There is
            // no name to report in that case, and inventing one would be worse than saying nothing.
            if (sink.topic() != null) {
                builder.addTopics(sink.topic());
            }
        } else if (node instanceof TopologyDescription.Processor processor) {
            builder.setKind(NodeKind.NODE_KIND_PROCESSOR);
            builder.addAllStores(new TreeSet<>(processor.stores()));
        } else {
            builder.setKind(NodeKind.NODE_KIND_UNSPECIFIED);
        }
        return builder.build();
    }

    private static List<TopologyDescription.Node> sortedByName(Set<TopologyDescription.Node> nodes) {
        List<TopologyDescription.Node> ordered = new ArrayList<>(nodes);
        ordered.sort((left, right) -> left.name().compareTo(right.name()));
        return ordered;
    }
}
