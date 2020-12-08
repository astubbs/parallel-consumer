package io.confluent.parallelconsumer;

import org.apache.kafka.common.TopicPartition;

import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentHashMap;

public class ShardManagement {
    // todo performance: disable/remove if using partition order
    /**
     * Map of Object keys to Map of offset to WorkUnits
     * <p>
     * Object is either the K key type, or it is a {@link TopicPartition}
     * <p>
     * Used to collate together a queue of work units for each unique key consumed
     *
     * @see K
     * @see #maybeGetWork()
     */
    private final Map<Object, NavigableMap<Long, WorkContainer<K, V>>> processingShards = new ConcurrentHashMap<>();
}
