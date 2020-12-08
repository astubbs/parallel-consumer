package io.confluent.parallelconsumer;

import org.apache.kafka.common.TopicPartition;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

public class PartitionMonitor {


    private int numberOfAssignedPartitions;

    Map<TopicPartition, PartitionState> states = new HashMap<>();



}
