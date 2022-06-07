package io.confluent.parallelconsumer.internal;

import lombok.Value;
import org.apache.kafka.common.TopicPartition;

/**
 * todo docs
 */
@Value
public class ClusterTopicPartition {
    Cluster cluster;
    TopicPartition topicPartition;
}
