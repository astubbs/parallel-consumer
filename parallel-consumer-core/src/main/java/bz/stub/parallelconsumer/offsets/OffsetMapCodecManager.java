package bz.stub.parallelconsumer.offsets;

/*-
 * Copyright (C) 2020-2023 Confluent, Inc.
 * Modifications Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.ParallelConsumerOptions.InvalidOffsetMetadataHandlingPolicy;
import bz.stub.parallelconsumer.internal.InternalRuntimeException;
import bz.stub.parallelconsumer.internal.PCModule;
import bz.stub.parallelconsumer.metrics.PCMetrics;
import bz.stub.parallelconsumer.metrics.PCMetricsDef;
import io.micrometer.core.instrument.Tag;
import bz.stub.parallelconsumer.state.PartitionState;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.*;

import static bz.stub.parallelconsumer.internal.utils.StringUtils.msg;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Uses multiple encodings to compare, when decided, can refactor other options out for analysis only -
 * {@link #encodeOffsetsCompressed}
 * <p>
 * TODO: consider IO exception management - question sneaky throws usage?
 * <p>
 * TODO: enforce max uncommitted {@literal <} encoding length (Short.MAX)
 * <p>
 * Bitset serialisation format:
 * <ul>
 * <li>byte1: magic
 * <li>byte2-3: Short: bitset size
 * <li>byte4-n: serialised {@link BitSet}
 * </ul>
 *
 * @author Antony Stubbs
 */
// metrics: avg time spend encoding, number of times each encoding used
@Slf4j
public class OffsetMapCodecManager<K, V> {

    /**
     * Used to prevent tests running in parallel that depends on setting static state in this class. Manipulation of
     * static state in tests needs to be removed to this isn't necessary.
     * <p>
     * todo remove static state manipulation from tests (make non static)
     */
    public static final String METADATA_DATA_SIZE_RESOURCE_LOCK = "Value doesn't matter, just needs a constant";

    /**
     * Maximum size of the commit offset metadata
     *
     * @see <a
     *         href="https://github.com/apache/kafka/blob/9bc9a37e50e403a356a4f10d6df12e9f808d4fba/core/src/main/scala/kafka/coordinator/group/OffsetConfig.scala#L52">OffsetConfig#DefaultMaxMetadataSize</a>
     * @see "kafka.coordinator.group.OffsetConfig#DefaultMaxMetadataSize"
     */
    // todo refactored to constant in the remove statics branch
    public static int DefaultMaxMetadataSize = 4096;

    public static final Charset CHARSET_TO_USE = UTF_8;

    private final PCModule module;

    private Timer offsetEncodingTimer;
    private final Map<OffsetEncoding, Counter> encodingCounters = new HashMap<>();

    private final PCMetrics pcMetrics;

    /**
     * What the decode path does with commit metadata this build cannot read. Read from the module's options (the DI
     * system) per instance - it used to be a mutable static written by this constructor, which meant the last
     * {@link OffsetMapCodecManager} constructed in the JVM decided the policy for every other one.
     */
    private final InvalidOffsetMetadataHandlingPolicy errorPolicy;

    /**
     * Decoding result for encoded offsets
     */
    @Value
    public static class HighestOffsetAndIncompletes {

        /**
         * The highest represented offset in this result.
         */
        Optional<Long> highestSeenOffset;

        /**
         * Of the offsets encoded, the incomplete ones.
         */
        // todo change to List as Sets have no order
        SortedSet<Long> incompleteOffsets;

        public static HighestOffsetAndIncompletes of(long highestSeenOffset) {
            return new HighestOffsetAndIncompletes(Optional.of(highestSeenOffset), new TreeSet<>());
        }

        public static HighestOffsetAndIncompletes of(long highestSeenOffset, SortedSet<Long> incompleteOffsets) {
            return new HighestOffsetAndIncompletes(Optional.of(highestSeenOffset), incompleteOffsets);
        }

        public static HighestOffsetAndIncompletes of() {
            return new HighestOffsetAndIncompletes(Optional.empty(), new TreeSet<>());
        }
    }

    /**
     * Forces the use of a specific codec, instead of choosing the most efficient one. Useful for testing.
     */
    public static Optional<OffsetEncoding> forcedCodec = Optional.empty();

    // todo remove consumer - confluentinc#233
    public OffsetMapCodecManager(PCModule<K, V> module) {
        this.module = module;
        this.errorPolicy = module.options().getInvalidOffsetMetadataPolicy();
        pcMetrics = module.pcMetrics();
        initMeters();
    }

    private void initMeters() {
        offsetEncodingTimer = pcMetrics.getTimerFromMetricDef(PCMetricsDef.OFFSETS_ENCODING_TIME);
    }

    /**
     * Load all the previously completed offsets that were not committed
     */
    // todo this is the only method that needs the consumer - offset encoding is being conflated with decoding upon assignment - confluentinc#233
    // todo make package private?
    // todo rename
    public Map<TopicPartition, PartitionState<K, V>> loadPartitionStateForAssignment(final Collection<TopicPartition> assignment) {
        // load last committed state / metadata from consumer
        // todo this should be controlled for - improve consumer management so that this can't happen
        Map<TopicPartition, OffsetAndMetadata> partitionLastCommittedOffsets = null;
        int attempts = 0;
        while (partitionLastCommittedOffsets == null) {
            WakeupException lastWakeupException = null;
            try {
                partitionLastCommittedOffsets = module.consumer().committed(new HashSet<>(assignment));
            } catch (WakeupException exception) {
                log.debug("Woken up trying to get assignment", exception);
                lastWakeupException = exception;
            }
            attempts++;
            if (attempts > 10) // shouldn't need more than 1 ever
                throw new InternalRuntimeException("Failed to get partition assignment - continuously woken up.", lastWakeupException);
        }

        var partitionStates = new HashMap<TopicPartition, PartitionState<K, V>>();
        partitionLastCommittedOffsets.forEach((tp, offsetAndMeta) -> {
            if (offsetAndMeta != null) {
                try {
                    PartitionState<K, V> state = decodePartitionState(tp, offsetAndMeta);
                    partitionStates.put(tp, state);
                } catch (OffsetDecodingError offsetDecodingError) {
                    log.error("Error decoding offsets from assigned partition, dropping offset map (will replay previously completed messages - partition: {}, data: {})",
                            tp, offsetAndMeta, offsetDecodingError);
                }
            }

        });

        // assigned partitions for which there has never been a commit
        // for each assignment with no commit history, enter a default entry. Catches multiple other cases.
        assignment.stream()
                .filter(topicPartition -> !partitionStates.containsKey(topicPartition))
                .forEach(topicPartition -> {
                    var psm = module.workManager().getPm();
                    var epoch = psm.getEpochOfPartition(topicPartition);
                    PartitionState<K, V> defaultEntry = new PartitionState<>(epoch, module, topicPartition, HighestOffsetAndIncompletes.of());
                    partitionStates.put(topicPartition, defaultEntry);
                });

        return partitionStates;
    }

    private HighestOffsetAndIncompletes deserialiseIncompleteOffsetMapFromBase64(TopicPartition tp, OffsetAndMetadata offsetData) throws OffsetDecodingError {
        return deserialiseIncompleteOffsetMapFromBase64(offsetData.offset(), offsetData.metadata(), errorPolicy, tp);
    }

    /**
     * Decodes with the strict {@link InvalidOffsetMetadataHandlingPolicy#FAIL} policy - for callers with no configured
     * consumer to take a policy from.
     */
    public static HighestOffsetAndIncompletes deserialiseIncompleteOffsetMapFromBase64(long committedOffsetForPartition, String base64EncodedOffsetPayload) throws OffsetDecodingError {
        return deserialiseIncompleteOffsetMapFromBase64(committedOffsetForPartition, base64EncodedOffsetPayload, InvalidOffsetMetadataHandlingPolicy.FAIL, null);
    }

    /**
     * @param errorPolicy what to do with metadata this build cannot read - see
     *                    {@link EncodedOffsetPair#decodeToIncompletes}
     * @param tp          the partition the metadata was committed against, for diagnosis - may be null when unknown
     */
    public static HighestOffsetAndIncompletes deserialiseIncompleteOffsetMapFromBase64(long committedOffsetForPartition,
                                                                                       String base64EncodedOffsetPayload,
                                                                                       InvalidOffsetMetadataHandlingPolicy errorPolicy,
                                                                                       TopicPartition tp) throws OffsetDecodingError {
        byte[] decodedBytes;
        try {
            decodedBytes = OffsetSimpleSerialisation.decodeBase64(base64EncodedOffsetPayload);
        } catch (IllegalArgumentException a) {
            throw new OffsetDecodingError(msg("Error decoding offset metadata, input was: {}", base64EncodedOffsetPayload), a);
        }
        return decodeCompressedOffsets(committedOffsetForPartition, decodedBytes, errorPolicy, tp);
    }

    PartitionState<K, V> decodePartitionState(TopicPartition tp, OffsetAndMetadata offsetData) throws OffsetDecodingError {
        HighestOffsetAndIncompletes incompletes = deserialiseIncompleteOffsetMapFromBase64(tp, offsetData);
        log.debug("Loaded incomplete offsets from offset payload {}", incompletes);
        var epoch = module.workManager().getPm().getEpochOfPartition(tp);
        return new PartitionState<>(epoch, module, tp, incompletes);
    }

    public String makeOffsetMetadataPayload(long baseOffsetForPartition, PartitionState<K, V> state) throws NoEncodingPossibleException {
        String offsetMap = serialiseIncompleteOffsetMapToBase64(baseOffsetForPartition, state);
        return offsetMap;
    }

    String serialiseIncompleteOffsetMapToBase64(long baseOffsetForPartition, PartitionState<K, V> state) throws NoEncodingPossibleException {
        byte[] compressedEncoding = encodeOffsetsCompressed(baseOffsetForPartition, state);
        String b64 = OffsetSimpleSerialisation.base64(compressedEncoding);
        return b64;
    }

    /**
     * Print out all the offset status into a String, and use X to effectively do run length encoding compression on the
     * string.
     * <p>
     * Include the magic byte in the returned array.
     * <p>
     * Can remove string encoding in favour of the boolean array for the `BitSet` if that's how things settle.
     */
    byte[] encodeOffsetsCompressed(long baseOffsetForPartition, PartitionState<K, V> partitionState) throws NoEncodingPossibleException {
        var incompleteOffsets = partitionState.getIncompleteOffsetsBelowHighestSucceeded();
        long highestSucceeded = partitionState.getOffsetHighestSucceeded();
        if (log.isDebugEnabled()) {
            log.debug("Encoding partition {}, highest succeeded {}, incomplete offsets to encode {}",
                    partitionState.getTp(),
                    highestSucceeded,
                    incompleteOffsets);
        }


        OffsetSimultaneousEncoder simultaneousEncoder = null;
        try {
            simultaneousEncoder = new OffsetSimultaneousEncoder(baseOffsetForPartition, highestSucceeded, incompleteOffsets);
            offsetEncodingTimer.recordCallable(simultaneousEncoder::invoke);
        } catch (Exception e) {
            throw new InternalRuntimeException("Error encoding offsets", e);
        }

        //
        if (forcedCodec.isPresent()) {
            var forcedOffsetEncoding = forcedCodec.get();
            log.debug("Forcing use of {}, for testing", forcedOffsetEncoding);
            getCounterMeterForEncoding(forcedOffsetEncoding).increment();

            Map<OffsetEncoding, byte[]> encodingMap = simultaneousEncoder.getEncodingMap();
            byte[] bytes = encodingMap.get(forcedOffsetEncoding);
            if (bytes == null)
                throw new NoEncodingPossibleException(msg("Can't force an encoding that hasn't been run: {}", forcedOffsetEncoding));
            return simultaneousEncoder.packEncoding(new EncodedOffsetPair(forcedOffsetEncoding, ByteBuffer.wrap(bytes)));
        } else {
            getCounterMeterForEncoding(simultaneousEncoder.sortedEncodings.first().getEncoding()).increment();
            return simultaneousEncoder.packSmallest();
        }
    }

    private Counter getCounterMeterForEncoding(OffsetEncoding encoding) {
        Counter counter = encodingCounters.get(encoding);
        if (counter == null) {
            counter = pcMetrics.getCounterFromMetricDef(PCMetricsDef.OFFSETS_ENCODING_USAGE,
                    Tag.of("encoding", encoding.name()));
            encodingCounters.put(encoding, counter);
        }
        return counter;
    }

    /**
     * Print out all the offset status into a String, and potentially use zstd to effectively do run length encoding
     * compression
     *
     * @return Set of offsets which are not complete, and the highest offset encoded.
     */
    static HighestOffsetAndIncompletes decodeCompressedOffsets(long nextExpectedOffset, byte[] decodedBytes) {
        return decodeCompressedOffsets(nextExpectedOffset, decodedBytes, InvalidOffsetMetadataHandlingPolicy.FAIL, null);
    }

    /**
     * @param errorPolicy what to do with metadata this build cannot read - see
     *                    {@link EncodedOffsetPair#decodeToIncompletes}
     * @param tp          the partition the metadata was committed against, for diagnosis - may be null when unknown
     */
    static HighestOffsetAndIncompletes decodeCompressedOffsets(long nextExpectedOffset,
                                                               byte[] decodedBytes,
                                                               InvalidOffsetMetadataHandlingPolicy errorPolicy,
                                                               TopicPartition tp) {

        // if no offset bitmap data
        if (decodedBytes.length == 0) {
            // in this case, as there is no encoded offset data in the matadata, the highest we previously saw must be
            // the offset before the committed offset
            long highestSeenOffsetIsThen = nextExpectedOffset - 1;
            return HighestOffsetAndIncompletes.of(highestSeenOffsetIsThen);
        } else {
            return EncodedOffsetPair.decodeToIncompletes(decodedBytes, nextExpectedOffset, errorPolicy, tp);
        }
    }

}
