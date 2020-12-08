package io.confluent.parallelconsumer;

import org.apache.kafka.common.TopicPartition;

import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

public class PartitionState {


    // visible for testing
    /**
     * A subset of Offsets, beyond the highest committable offset, which haven't been totally completed.
     * <p>
     * We only need to know the full incompletes when we do the {@link #findCompletedEligibleOffsetsAndRemove} scan, so
     * find the full sent only then, and discard. Otherwise, for continuous encoding, the encoders track it them
     * selves.
     * <p>
     * We work with incompletes, instead of completes, because it's a bet that most of the time the storage space for
     * storing the incompletes in memory will be smaller.
     *
     * @see #findCompletedEligibleOffsetsAndRemove(boolean)
     * @see #encodeWorkResult(boolean, WorkContainer)
     * @see #onSuccess(WorkContainer)
     * @see #onFailure(WorkContainer)
     */
    Set<Long> partitionOffsetsIncompleteMetadataPayloads;

    /**
     * Highest committable offset - the end offset of the highest (from the lowest seen) continuous set of completed
     * offsets. AKA low water mark.
     */
    Long partitionOffsetHighestContinuousSucceeded;

    /**
     * Continuous offset encodings
     */
    private final Map<TopicPartition, OffsetSimultaneousEncoder> partitionContinuousOffsetEncoders = new ConcurrentHashMap<>();

    /**
     * Highest offset which has completed
     */
    private long OffsetHighestSucceeded;

    /**
     * The highest seen offset for a partition
     */
    Long partitionOffsetHighestSeen;


    /**
     * If true, more messages are allowed to process for this partition.
     * <p>
     * If false, we have calculated that we can't record any more offsets for this partition, as our best performing
     * encoder requires nearly as much space is available for this partitions allocation of the maximum offset metadata
     * size.
     * <p>
     * Default (missing elements) is true - more messages can be processed.
     *
     * @see #manageOffsetEncoderSpaceRequirements()
     * @see OffsetMapCodecManager#DefaultMaxMetadataSize
     */
    Map<TopicPartition, Boolean> partitionMoreRecordsAllowedToProcess = new ConcurrentHashMap<>();


    /**
     * Map of partitions to Map of offsets to WorkUnits
     * <p>
     * Need to record globally consumed records, to ensure correct offset order committal. Cannot rely on incrementally
     * advancing offsets, as this isn't a guarantee of kafka's.
     * <p>
     * Concurrent because either the broker poller thread or the control thread may be requesting offset to commit
     * ({@link #findCompletedEligibleOffsetsAndRemove})
     *
     * @see #findCompletedEligibleOffsetsAndRemove
     */
    private final Map<TopicPartition, NavigableMap<Long, WorkContainer<K, V>>> partitionCommitQueues = new ConcurrentHashMap<>();
    
}
