package bz.stub.parallelconsumer.truth;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 * Modifications Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.internal.utils.PodamUtils;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import pl.tlinkowski.unij.api.UniMaps;

import static bz.stub.parallelconsumer.ManagedTruth.assertTruth;

/**
 * Basic tests of simple usage of the Truth Generator maven plugin
 *
 * @author Antony Stubbs
 */
class TruthGeneratorTests {

    @Test
    void generate() {
        // todo check legacy's also contribute to subject graph
        assertTruth(new ConsumerRecords<>(UniMaps.of())).getPartitions().isEmpty();

        assertTruth(PodamUtils.createInstance(OffsetAndMetadata.class)).getOffset().isNotNull();

        assertTruth(PodamUtils.createInstance(TopicPartition.class)).hasTopic().isNotEmpty();

        assertTruth(PodamUtils.createInstance(RecordMetadata.class)).ishasTimestamp();

        assertTruth(PodamUtils.createInstance(ProducerRecord.class, String.class, String.class)).getHeaders().isEmpty();
    }

}
