package bz.stub.parallelconsumer.offsets;

/*-
 * Copyright (C) 2020-2023 Confluent, Inc.
 * Modifications Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions;
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
     * Per-instance: two {@link bz.stub.parallelconsumer.ParallelConsumer}s in one JVM may be configured with
     * different policies, and previously the last one constructed silently set it for all of them.
     */
    private ParallelConsumerOptions.InvalidOffsetMetadataHandlingPolicy errorPolicy = ParallelConsumerOptions.InvalidOffsetMetadataHandlingPolicy.FAIL;

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
        if (module != null){
            this.errorPolicy = module.options().getInvalidOffsetMetadataPolicy();
        }
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
                    log.error("Error decoding offsets from assigned partition, dropping offset map (will replay previously completed messages - partition: {}, data: {}). " +
                                    "If this consumer group was previously used by another application, its offset metadata isn't ours to read - " +
                                    "processing resumes from the committed offset, and PC will write its own metadata from now on.",
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

    private HighestOffsetAndIncompletes deserialiseIncompleteOffsetMapFromString(OffsetAndMetadata offsetData) throws OffsetDecodingError {
        return deserialiseIncompleteOffsetMapFromString(offsetData.offset(), offsetData.metadata(), errorPolicy);
    }

    /**
     * Decodes an offset payload under the default {@link ParallelConsumerOptions.InvalidOffsetMetadataHandlingPolicy#FAIL}
     * policy.
     * <p>
     * The policy only decides what happens for metadata identifiable as Kafka Streams'. Metadata that cannot be decoded
     * at all raises {@link OffsetDecodingError} under either policy, so callers must handle it regardless of which they
     * pass.
     *
     * @throws OffsetDecodingError if the payload is in neither of the string codecs, or holds an encoding this version
     *                             cannot read
     * @see #deserialiseIncompleteOffsetMapFromString(long, String, ParallelConsumerOptions.InvalidOffsetMetadataHandlingPolicy)
     */
    public static HighestOffsetAndIncompletes deserialiseIncompleteOffsetMapFromString(long committedOffsetForPartition, String stringEncodedOffsetPayload) throws OffsetDecodingError {
        return deserialiseIncompleteOffsetMapFromString(committedOffsetForPartition, stringEncodedOffsetPayload,
                ParallelConsumerOptions.InvalidOffsetMetadataHandlingPolicy.FAIL);
    }

    /**
     * Decodes the string offset payload committed against a partition, into the highest offset seen and the set of
     * incomplete offsets below it.
     *
     * <h2>Which string codec</h2>
     * Two are read, and both always will be: a payload starting with {@code '%'} is
     * {@link Z85Codec Z85} after the sentinel, and anything else is Base64. The sentinel character is not in the Base64
     * alphabet, so no payload written by any earlier release of PC can start with it - which is what makes bare Base64
     * safe to keep reading forever, and why blank or foreign strings still take the Base64 path and its existing
     * error handling. The writer picks per payload: it emits whichever form is shorter, which is Base64 below 22
     * payload bytes (the 12-to-21 ties included) and sentinel+Z85 from 22 bytes up, where the denser codec starts
     * winning characters against the metadata cap.
     *
     * @param committedOffsetForPartition the committed offset the payload is relative to - incompletes are encoded as
     *                                    offsets from this base
     * @param stringEncodedOffsetPayload  the {@code metadata} field of the committed offset
     * @param errorPolicy                 what to do about metadata recognisable as Kafka Streams'. Does <em>not</em>
     *                                    govern metadata that cannot be decoded at all, which always raises
     *                                    {@link OffsetDecodingError}
     * @throws OffsetDecodingError if the payload decodes as neither of the two string codecs, or its leading magic byte
     *                             matches no encoding this version knows - all of which callers are expected to recover
     *                             from by dropping the offset map, not by failing
     * @see #loadPartitionStateForAssignment
     * @see OffsetSimpleSerialisation#decodeBase64OrZ85(String)
     */
    public static HighestOffsetAndIncompletes deserialiseIncompleteOffsetMapFromString(long committedOffsetForPartition,
                                                                                       String stringEncodedOffsetPayload,
                                                                                       ParallelConsumerOptions.InvalidOffsetMetadataHandlingPolicy errorPolicy) throws OffsetDecodingError {
        byte[] decodedBytes;
        try {
            decodedBytes = OffsetSimpleSerialisation.decodeBase64OrZ85(stringEncodedOffsetPayload);
        } catch (IllegalArgumentException | Z85DecodingException a) {
            throw new OffsetDecodingError(msg("Error decoding offset metadata, input was: {}", stringEncodedOffsetPayload), a);
        }
        return decodeCompressedOffsets(committedOffsetForPartition, decodedBytes, errorPolicy);
    }

    PartitionState<K, V> decodePartitionState(TopicPartition tp, OffsetAndMetadata offsetData) throws OffsetDecodingError {
        HighestOffsetAndIncompletes incompletes = deserialiseIncompleteOffsetMapFromString(offsetData);
        log.debug("Loaded incomplete offsets from offset payload {}", incompletes);
        var epoch = module.workManager().getPm().getEpochOfPartition(tp);
        return new PartitionState<>(epoch, module, tp, incompletes);
    }

    public String makeOffsetMetadataPayload(long baseOffsetForPartition, PartitionState<K, V> state) throws NoEncodingPossibleException {
        String offsetMap = serialiseIncompleteOffsetMapToString(baseOffsetForPartition, state);
        return offsetMap;
    }

    /**
     * Encodes the offset map, then string-encodes it with whichever outer codec yields the shorter string - the length
     * being what {@link PartitionState} measures against the metadata cap.
     *
     * @see OffsetSimpleSerialisation#encodeShorterOfBase64OrZ85(byte[])
     * @see #deserialiseIncompleteOffsetMapFromString(long, String, ParallelConsumerOptions.InvalidOffsetMetadataHandlingPolicy)
     */
    String serialiseIncompleteOffsetMapToString(long baseOffsetForPartition, PartitionState<K, V> state) throws NoEncodingPossibleException {
        byte[] compressedEncoding = encodeOffsetsCompressed(baseOffsetForPartition, state);
        return OffsetSimpleSerialisation.encodeShorterOfBase64OrZ85(compressedEncoding);
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
     * Decodes an offset map under the default {@link ParallelConsumerOptions.InvalidOffsetMetadataHandlingPolicy#FAIL}
     * policy.
     *
     * @return Set of offsets which are not complete, and the highest offset encoded.
     * @throws OffsetDecodingError if the bytes hold an encoding this version cannot read
     * @see #decodeCompressedOffsets(long, byte[], ParallelConsumerOptions.InvalidOffsetMetadataHandlingPolicy)
     */
    static HighestOffsetAndIncompletes decodeCompressedOffsets(long nextExpectedOffset, byte[] decodedBytes) throws OffsetDecodingError {
        return decodeCompressedOffsets(nextExpectedOffset, decodedBytes, ParallelConsumerOptions.InvalidOffsetMetadataHandlingPolicy.FAIL);
    }

    /**
     * Decodes the offset map out of bytes the outer string codec has already been stripped from, whose leading byte is
     * the {@link OffsetEncoding} magic number.
     * <p>
     * Empty input is not an error: it means the commit carried no offset map, so nothing was incomplete below the
     * committed offset.
     *
     * @param nextExpectedOffset the committed offset the map is relative to
     * @param decodedBytes       the payload, magic byte first
     * @param errorPolicy        what to do about metadata recognisable as Kafka Streams'. Does <em>not</em> govern an
     *                           unreadable magic byte, which always raises {@link OffsetDecodingError} so the caller
     *                           can drop the offset map rather than die - see {@link OffsetEncoding#decode(byte)}
     * @return Set of offsets which are not complete, and the highest offset encoded.
     * @throws OffsetDecodingError if the magic byte matches no encoding this version knows
     */
    static HighestOffsetAndIncompletes decodeCompressedOffsets(long nextExpectedOffset,
                                                               byte[] decodedBytes,
                                                               ParallelConsumerOptions.InvalidOffsetMetadataHandlingPolicy errorPolicy) throws OffsetDecodingError {

        // if no offset bitmap data
        if (decodedBytes.length == 0) {
            // in this case, as there is no encoded offset data in the matadata, the highest we previously saw must be
            // the offset before the committed offset
            long highestSeenOffsetIsThen = nextExpectedOffset - 1;
            return HighestOffsetAndIncompletes.of(highestSeenOffsetIsThen);
        } else {
            var result = EncodedOffsetPair.unwrap(decodedBytes);
            return result.getDecodedIncompletes(nextExpectedOffset, errorPolicy);
        }
    }

}
