package bz.stub.parallelconsumer.state;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 * Modifications Copyright (C) 2026 Antony Stubbs and contributors
 */

import lombok.Value;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

/**
 * Useful identifier for a {@link ConsumerRecord}.
 */
@Value
public class ConsumerRecordId {
    TopicPartition tp;
    long offset;
}
