package bz.stub.parallelconsumer.offsets;

/*-
 * Copyright (C) 2020-2023 Confluent, Inc.
 * Modifications Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.ParallelConsumerOptions.InvalidOffsetMetadataHandlingPolicy;
import bz.stub.parallelconsumer.internal.PCInternalRuntimeException;
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
import java.util.concurrent.ConcurrentHashMap;

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

    /**
     * What a caught-up partition has to encode: nothing.
     */
    private static final byte[] NO_INNER_BYTES = new byte[0];

    private final PCModule module;

    private Timer offsetEncodingTimer;

    /**
     * Which encoding each commit chose, one counter per {@link OffsetEncoding}, populated lazily by
     * {@link #getCounterMeterForEncoding(OffsetEncoding)} on the encode path.
     *
     * <p>Concurrent, and populated with a single {@code computeIfAbsent} rather than a
     * {@code get}-then-{@code put}, because the map is a cache whose miss handler <em>registers a meter</em> -
     * so the check and the act have to be one step. Two encoders interleaving inside that window both see the
     * miss and both register, and that second registration is the cost: one redundant trip through
     * {@code PCMetrics.track} under {@code metersLock} - the monitor {@code close()} and every rebalance's
     * meter registration also contend for it - paid once. Both threads {@code put} the <em>same</em> key, so an
     * entry exists whichever write wins and every later encode hits the cache; micrometer returns the same
     * {@code Counter} for the same id, so the reported value is right too. An entry that never appears needs
     * two <em>different</em> encodings, and a plain {@link HashMap} loses one of them two ways without ever
     * growing: on the very first {@code put}, where JDK 17's {@code HashMap.putVal} allocates the null table by
     * calling {@code resize()}, so two first-time callers on an empty cache each allocate a table and the one
     * whose {@code table} assignment lands second drops the other's, entry and all, with no collision needed;
     * or, once the table exists, two keys in one bucket where one thread's write of the chain head drops the
     * other's node. The last of the four unguarded metrics collections named by
     * {@code docs/solutions/logic-errors/the-metrics-counter-maps-were-plain-hashmaps-2026-09-05.md};
     * astubbs#267 made the other three concurrent and missed this one because it sits on the encode path
     * rather than in the rebalance callbacks.
     *
     * <p><b>Cleared suspicion, 2026-09-05: nothing interleaves here today.</b> The suspicion the next reader
     * will form is that two threads encode at once, because this repo has demonstrated exactly that on the
     * commit path. The discriminator is {@code AbstractParallelEoSStreamProcessor.tryCommitOffsetsOnRevoke},
     * which takes {@code commitLock} with {@code tryLock} and <em>declines</em> rather than blocking, so the
     * broker-poll thread's revoke commit and the control thread's commit are mutually exclusive; and
     * {@code ConsumerOffsetCommitter.commit} routes a non-owner caller through the request queue instead of
     * encoding on the calling thread. One encoder at a time, per instance, in every commit mode. This is
     * therefore a latent defect made unreachable by the scheduler - not a live one - and it is fixed anyway
     * because that is a property of the commit scheduler rather than of this class, which is the rule
     * {@code docs/solutions/architecture-patterns/a-query-must-never-mutate-derive-thread-safety-from-callers.md}
     * states as "prefer the guarantee you own".
     *
     * <p><b>What would reopen it</b>: confluentinc#233 splitting encode from decode, or confluentinc#200
     * parallelising encoding - and nothing would go red to tell you, because no gate reasons about which
     * thread reaches this field. {@code EncodingCounterRegistrationIsAtomicTest} pins the atomicity instead,
     * by driving the interleaving through a seam rather than waiting for the scheduler to supply one.
     *
     * <p><b>What is NOT the argument</b>, so nobody re-derives it: table corruption on a <em>growth</em> resize.
     * There are twelve {@link OffsetEncoding} constants and a default {@link HashMap} grows above twelve entries,
     * so this map never grew and never could - which rules out the rehash race and nothing else. {@code resize()}
     * is also how the table is first allocated, so the initial-allocation race above is reachable on the very
     * first commit, and the dropped bucket node needs no resize at all.
     */
    private final Map<OffsetEncoding, Counter> encodingCounters = new ConcurrentHashMap<>();

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
     * Everything one committed metadata payload said: the offsets, and what its rider slot held.
     * <p>
     * A sibling of {@link HighestOffsetAndIncompletes} rather than a field on it, deliberately. That type is a public
     * {@code @Value}: a fifth field would change its generated constructor's arity, and an array field would make its
     * {@code equals} identity-based. It is also the return type of four public methods that must keep it - the
     * {@code NoSuchMethodError} this class's javadoc records came from replacing one of those rather than adding to
     * it.
     * <p>
     * So the rider travels here instead, on a package-private decode family the public overloads delegate to and
     * project down from.
     *
     * @see EncodedOffsetPair#decodeToRiderAndIncompletes
     */
    @Value
    public static class DecodedMetadata {

        /**
         * Exactly what the public decode overloads return - this value adds to it and never alters it.
         */
        HighestOffsetAndIncompletes offsets;

        /**
         * What the payload said about the rider slot: none, dropped, present with bytes, or unreadable when the
         * policy discarded the metadata.
         */
        OffsetRiderEnvelope.Rider rider;

        public static DecodedMetadata of(HighestOffsetAndIncompletes offsets, OffsetRiderEnvelope.Rider rider) {
            return new DecodedMetadata(offsets, rider);
        }

        /**
         * A payload that carried no envelope at all.
         */
        public static DecodedMetadata of(HighestOffsetAndIncompletes offsets) {
            return new DecodedMetadata(offsets, OffsetRiderEnvelope.Rider.none());
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
                throw new PCInternalRuntimeException("Failed to get partition assignment - continuously woken up.", lastWakeupException);
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

    /**
     * Decodes the offset map committed against one partition, under <em>this manager's</em> configured
     * {@link ParallelConsumerOptions#getInvalidOffsetMetadataPolicy()}.
     * <p>
     * Deliberately not named {@code deserialiseIncompleteOffsetMapFromBase64} like the statics it delegates to: an
     * instance method sharing a name with static overloads reads at the call site as though the policy argument were
     * optional, when in fact the instance form is the only one that consults the user's configuration. SpotBugs flags
     * the shape as {@code MOM_MISLEADING_OVERLOAD_MODEL}.
     *
     * @param tp         the partition, carried purely so an unreadable payload can name itself in the log
     * @param offsetData the committed offset and its free-form metadata field
     * @throws OffsetDecodingError if the metadata is not valid base64
     */
    private HighestOffsetAndIncompletes decodeOffsetMapForPartition(TopicPartition tp, OffsetAndMetadata offsetData) throws OffsetDecodingError {
        return deserialiseIncompleteOffsetMapFromBase64(offsetData.offset(), offsetData.metadata(), errorPolicy, tp);
    }

    /**
     * Decodes an offset payload under the strict {@link InvalidOffsetMetadataHandlingPolicy#FAIL} policy - for callers
     * with no configured consumer to take a policy from, which in practice means tests.
     * <p>
     * {@code FAIL} is chosen here rather than inherited: this overload has no user to ask, and silently discarding an
     * offset map is not a decision a helper should make on a caller's behalf. Note this is the opposite of the
     * <em>runtime</em> default, which is {@link InvalidOffsetMetadataHandlingPolicy#IGNORE}.
     *
     * @param committedOffsetForPartition the committed offset the payload is relative to - incompletes are encoded as
     *                                    offsets from this base
     * @param base64EncodedOffsetPayload  the {@code metadata} field of the committed offset
     * @return the highest offset seen, and the incomplete offsets below it
     * @throws OffsetDecodingError if the payload is not valid base64
     * @see #deserialiseIncompleteOffsetMapFromBase64(long, String, InvalidOffsetMetadataHandlingPolicy, TopicPartition)
     */
    public static HighestOffsetAndIncompletes deserialiseIncompleteOffsetMapFromBase64(long committedOffsetForPartition, String base64EncodedOffsetPayload) throws OffsetDecodingError {
        return deserialiseIncompleteOffsetMapFromBase64(committedOffsetForPartition, base64EncodedOffsetPayload, InvalidOffsetMetadataHandlingPolicy.FAIL, null);
    }

    /**
     * Decodes an offset payload under an explicit policy, without a partition to name in diagnostics.
     * <p>
     * Retained at its original three-argument shape: this is public API, and an earlier revision of this change
     * replaced it with the four-argument form below rather than adding to it. That broke already-compiled callers
     * with {@code NoSuchMethodError} and forced source callers to pass a {@link TopicPartition} they had no use for.
     * The default-policy change this PR makes never required removing it.
     *
     * @see #deserialiseIncompleteOffsetMapFromBase64(long, String, InvalidOffsetMetadataHandlingPolicy, TopicPartition)
     */
    public static HighestOffsetAndIncompletes deserialiseIncompleteOffsetMapFromBase64(long committedOffsetForPartition,
                                                                                       String base64EncodedOffsetPayload,
                                                                                       InvalidOffsetMetadataHandlingPolicy errorPolicy) throws OffsetDecodingError {
        return deserialiseIncompleteOffsetMapFromBase64(committedOffsetForPartition, base64EncodedOffsetPayload, errorPolicy, null);
    }

    /**
     * Decodes the base64 offset payload committed against a partition, into the highest offset seen and the set of
     * incomplete offsets below it.
     *
     * @param committedOffsetForPartition the committed offset the payload is relative to - incompletes are encoded as
     *                                    offsets from this base
     * @param base64EncodedOffsetPayload  the {@code metadata} field of the committed offset
     * @param errorPolicy                 what to do with a payload this build cannot read - every such case, not only
     *                                    metadata recognisable as Kafka Streams'. See
     *                                    {@link EncodedOffsetPair#decodeToIncompletes}
     * @param tp                          the partition the metadata was committed against, for diagnosis - may be
     *                                    {@code null} when the caller does not know it
     * @return the highest offset seen, and the incomplete offsets below it
     * @throws OffsetDecodingError if the payload is not valid base64. An unreadable <em>payload</em> does not arrive
     *                             here: it is settled by {@code errorPolicy} further in
     */
    public static HighestOffsetAndIncompletes deserialiseIncompleteOffsetMapFromBase64(long committedOffsetForPartition,
                                                                                       String base64EncodedOffsetPayload,
                                                                                       InvalidOffsetMetadataHandlingPolicy errorPolicy,
                                                                                       TopicPartition tp) throws OffsetDecodingError {
        return deserialiseMetadataFromBase64(committedOffsetForPartition, base64EncodedOffsetPayload, errorPolicy, tp)
                .getOffsets();
    }

    /**
     * The same decode as {@link #deserialiseIncompleteOffsetMapFromBase64(long, String,
     * InvalidOffsetMetadataHandlingPolicy, TopicPartition)}, carrying the rider slot as well as the offsets.
     * <p>
     * Package-private on purpose: the public overloads above are the API, and they project this down. The rider is
     * reached from outside the package through the read-back entry point, not through here.
     */
    // TODO(refactor): this class is a flagged hotspot (docs/refactoring.md) - the decode family and the encode
    //  entry points both want extracting into their own types rather than growing a fifth and sixth static here.
    static DecodedMetadata deserialiseMetadataFromBase64(long committedOffsetForPartition,
                                                         String base64EncodedOffsetPayload,
                                                         InvalidOffsetMetadataHandlingPolicy errorPolicy,
                                                         TopicPartition tp) throws OffsetDecodingError {
        byte[] decodedBytes;
        try {
            decodedBytes = OffsetSimpleSerialisation.decodeBase64(base64EncodedOffsetPayload);
        } catch (IllegalArgumentException a) {
            // Metadata that is not even base64 is unreadable in exactly the sense the policy governs, so it goes
            // through the same handler as every other case. It used to throw OffsetDecodingError, which
            // loadPartitionStateForAssignment catches unconditionally - so a deployment that chose FAIL silently
            // dropped the offset map and replayed completed records instead of stopping. Arbitrary bytes left by
            // another framework take this path readily, which made it the widest hole in the policy's coverage.
            // Nothing is known about the rider slot of a payload that never decoded, so it reports UNREADABLE rather
            // than "no rider was configured" - see DecodedMetadata.
            return DecodedMetadata.of(EncodedOffsetPair.handleUnreadableMetadata(committedOffsetForPartition,
                    errorPolicy,
                    msg("the metadata is not valid base64"),
                    () -> new CorruptOffsetMetadataException("metadata is not valid base64",
                            EncodedOffsetPair.describeSource(tp, committedOffsetForPartition)),
                    tp), OffsetRiderEnvelope.Rider.unreadable());
        }
        return decodeCompressedMetadata(committedOffsetForPartition, decodedBytes, errorPolicy, tp);
    }

    /**
     * Reads the opaque rider back out of one partition's committed offset metadata - the public half of the embedder
     * API whose write half is {@link ParallelConsumerOptions#getRiderSupplier()}.
     * <p>
     * <b>This is the route for an embedder that does not own Parallel Consumer's consumer.</b> The rider is
     * partition-scoped and durable, so an embedder that wants it before (or without) handing the partition to PC
     * fetches the committed offset and its metadata field with its own consumer or admin client - {@code
     * Consumer#committed} or {@code AdminClient#listConsumerGroupOffsets} - and hands the two here. PC's own
     * assignment path does not go through this method and does not keep what it reads: it builds
     * {@link PartitionState} from the incompletes and discards the rider.
     * <p>
     * <b>What the four answers mean to an embedder</b>, which are four different situations and not degrees of one:
     * <ul>
     *     <li>{@link OffsetRiderEnvelope.RiderState#PRESENT} - these are the bytes the supplier returned for this
     *     partition. Decode them.</li>
     *     <li>{@link OffsetRiderEnvelope.RiderState#NONE} - no rider was configured when this was committed, or the
     *     budget ladder had to shed the envelope itself to keep the offset map (R9). Start from nothing.</li>
     *     <li>{@link OffsetRiderEnvelope.RiderState#DROPPED} - a rider existed and was too big for the commit it
     *     would have ridden on. Also start from nothing, but the supplier is producing more bytes than PC can carry
     *     and the dropped-rider counter says how often.</li>
     *     <li>{@link OffsetRiderEnvelope.RiderState#UNREADABLE} - this build could not read the metadata at all and
     *     {@code IGNORE} discarded it, so what the rider slot held is <em>unknown</em> rather than absent. Never
     *     returned under {@code FAIL}, which throws instead.</li>
     * </ul>
     * An envelope that parsed keeps its rider even when the offset map inside it did not: the two are structurally
     * independent, so a corrupt inner body still answers {@code PRESENT} under {@code IGNORE}.
     * <p>
     * <b>The policy is a required parameter, and no overload without one is ever added.</b> There is no default that
     * is not a decision on the caller's behalf: {@code IGNORE} is PC's runtime default and silently discards an
     * offset map, while {@link #deserialiseIncompleteOffsetMapFromBase64(long, String)} - the policy-less helper in
     * this class - deliberately picks the opposite, {@code FAIL}, on the grounds that a helper with no user to ask
     * must not discard on one. A convenience overload here would have to pick one of those two and would read at the
     * call site as though the choice did not matter. It does: it decides whether an embedder that cannot read the
     * metadata restarts from an unknown state or refuses to start.
     * <p>
     * <b>The committed offset is not decoration.</b> It is the base the payload's offsets are relative to, and it is
     * the only thing that locates this metadata in the diagnostics of the failure path - see
     * {@link EncodedOffsetPair#describeSource}, which renders it into the exception {@code FAIL} throws and the
     * warning {@code IGNORE} logs. This overload has no {@link TopicPartition} to name, so under {@code FAIL} the
     * offset is the whole of the operator's clue about which commit holds the bad payload.
     *
     * @param committedOffset the committed offset the metadata was written against - the NEXT offset to be polled,
     *                        exactly as {@code OffsetAndMetadata#offset()} reports it
     * @param metadata        the {@code metadata} field of that committed offset; empty means nothing was written,
     *                        which is {@link OffsetRiderEnvelope.RiderState#NONE}
     * @param policy          what to do with metadata this build cannot read - required, see above
     * @return the rider slot's state, and a copy of its bytes when it has any
     * @throws OffsetDecodingError    declared like every other entry point in this family. The base64 failure it
     *                                once named is now settled by {@code policy} instead, but the declaration stays:
     *                                dropping it would be a source-incompatible change to a public signature for no
     *                                gain, and the outer string codec is where a future checked failure would arise
     * @throws CorruptOffsetMetadataException      under {@code FAIL}, when the payload is not readable metadata
     * @throws EncodingNotSupportedException      under {@code FAIL}, when the payload is readable but this build cannot
     *                                             decode what it names: {@link UnknownOffsetMetadataMagicException}
     *                                             for a magic byte belonging to no encoding this build knows,
     *                                             {@link KafkaStreamsEncodingNotSupported} for Kafka Streams' own
     *                                             metadata, and {@link UnsupportedOffsetEncodingException} for an
     *                                             encoding in the enum that has no decoder ({@code ByteArray}). All
     *                                             three are deliberately NOT an {@link OffsetDecodingError}, so they
     *                                             escape the rebalance callback rather than being swallowed. They
     *                                             are checked, and the family is <b>declared</b> here even though
     *                                             they arrive through the same sneaky-throw path every other entry
     *                                             point in this family uses ({@code EncodedOffsetPair}'s policy
     *                                             handler): a checked type that is thrown but not declared cannot
     *                                             be caught by name - javac rejects the {@code catch} as unreachable
     *                                             - so without the declaration a {@code FAIL} caller could not
     *                                             write the handling this javadoc describes. Declaring the parent
     *                                             lets a caller catch the family in one clause or any member by
     *                                             its own type
     * @see ParallelConsumerOptions#getRiderSupplier()
     */
    public static OffsetRiderEnvelope.Rider decodeRider(long committedOffset,
                                                        String metadata,
                                                        InvalidOffsetMetadataHandlingPolicy policy)
            throws OffsetDecodingError, CorruptOffsetMetadataException, EncodingNotSupportedException {
        // Straight through the string-level entry point rather than round the outer codec: decodeCompressedMetadata
        // stays the single decode choke point, so this answers with whatever the assignment path would have seen for
        // the same string, including the policy's fallback. The Rider it returns copies its bytes out on every
        // getBytes() call, so nothing this method allocated is shared with the caller.
        return deserialiseMetadataFromBase64(committedOffset, metadata, policy, null).getRider();
    }

    PartitionState<K, V> decodePartitionState(TopicPartition tp, OffsetAndMetadata offsetData) throws OffsetDecodingError {
        HighestOffsetAndIncompletes incompletes = decodeOffsetMapForPartition(tp, offsetData);
        log.debug("Loaded incomplete offsets from offset payload {}", incompletes);
        var epoch = module.workManager().getPm().getEpochOfPartition(tp);
        return new PartitionState<>(epoch, module, tp, incompletes);
    }

    /**
     * The whole write side in one call, with no rider: encode the offset map once and turn it into the metadata
     * string. Byte for byte what this method produced before the rider existed.
     */
    public String makeOffsetMetadataPayload(long baseOffsetForPartition, PartitionState<K, V> state) throws NoEncodingPossibleException {
        byte[] innerBytes = encodeOffsetsToInnerBytes(baseOffsetForPartition, state);
        return assembleMetadataPayload(innerBytes, OffsetRiderEnvelope.Rider.none());
    }

    /**
     * Step one of the write side, for a caller that needs the encoded offset map <em>before</em> it decides what to
     * wrap around it: the budget ladder repacks these same bytes rather than encoding again.
     * <p>
     * <b>The encoder competition runs exactly once per call</b>, and so do its meters
     * ({@link PCMetricsDef#OFFSETS_ENCODING_USAGE}, {@link PCMetricsDef#OFFSETS_ENCODING_TIME}). A second pass would
     * snapshot a later offset map - the confluentinc#894 tear class - and double-count both.
     *
     * <b>This method never asks whether there is anything to encode - the caller already did, once, on the read it
     * commits against.</b> {@code tryToEncodeOffsets} decides the caught-up case from the same sample of the
     * partition that fixes its commit offset, and a caught-up partition never reaches here. An earlier draft
     * re-read emptiness at this point, and that second read was the confluentinc#893/894 tear class in a new
     * coat: the control thread can complete the last incomplete record between the caller's decision and this
     * call, and a re-check then answered "nothing to encode" for a commit already pinned to the older offset - so
     * the commit carried no map and a crash before the next one replayed records this build had recorded as
     * complete. Encoding whatever the partition holds at the encoder's own single sample is what the pre-rider
     * path always did: a map that emptied in between encodes as a complete map, which resumes correctly.
     *
     * @return the offset map, its magic byte first. Offsets in flight above the high-water mark still get the
     *         encoding this build has always written for them
     * @throws NoEncodingPossibleException as {@link #encodeOffsetsCompressed} does, and for the same reason
     */
    public byte[] encodeOffsetsToInnerBytes(long baseOffsetForPartition, PartitionState<K, V> state)
            throws NoEncodingPossibleException {
        return encodeOffsetsCompressed(baseOffsetForPartition, state);
    }

    /**
     * Step two of the write side: inner bytes plus a rider slot become the string that goes in the commit's metadata
     * field.
     * <p>
     * With {@link OffsetRiderEnvelope.RiderState#NONE} there is no envelope at all - the inner bytes go through the
     * outer codec unchanged, which is what makes a payload written with no rider byte-identical to one written by a
     * build that had never heard of riders. {@code PRESENT} and {@code DROPPED} are wrapped;
     * {@link OffsetRiderEnvelope.RiderState#UNREADABLE} is a read-side answer and reaching here with one is a
     * programming error, which {@link OffsetRiderEnvelope#wrap} rejects.
     *
     * @param innerBytes the encoded offset map, or empty for a caught-up partition
     * @param rider      what to put in the rider slot - never {@code null}; {@link OffsetRiderEnvelope.Rider#none()}
     *                   is how "no rider" is spelled
     */
    public String assembleMetadataPayload(byte[] innerBytes, OffsetRiderEnvelope.Rider rider) {
        if (rider.getState() == OffsetRiderEnvelope.RiderState.NONE) {
            return OffsetSimpleSerialisation.base64(innerBytes);
        }
        return OffsetSimpleSerialisation.base64(OffsetRiderEnvelope.wrap(innerBytes, rider));
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
        // Sample the high-water mark ONCE and derive both the incomplete-offsets snapshot and the encoder's range
        // top from that single sample, so the two cannot disagree by construction. Two separate reads here raced
        // concurrent completions into silent record loss - the full mechanism is on
        // PartitionState#getIncompleteOffsetsBelow; guarded by OffsetEncoderWidenedRangeRaceTest.
        long highestSucceeded = partitionState.getOffsetHighestSucceeded();
        var incompleteOffsets = partitionState.getIncompleteOffsetsBelow(highestSucceeded);
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
            throw new PCInternalRuntimeException("Error encoding offsets", e);
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

    /**
     * @see #encodingCounters for why this is one {@code computeIfAbsent} and not a {@code get}-then-{@code put}
     */
    private Counter getCounterMeterForEncoding(OffsetEncoding encoding) {
        return encodingCounters.computeIfAbsent(encoding, enc ->
                pcMetrics.getCounterFromMetricDef(PCMetricsDef.OFFSETS_ENCODING_USAGE,
                        Tag.of("encoding", enc.name())));
    }

    /**
     * Decodes an offset map under the strict {@link InvalidOffsetMetadataHandlingPolicy#FAIL} policy - see the sibling
     * of {@link #deserialiseIncompleteOffsetMapFromBase64(long, String)} for why a policy-less overload picks the
     * strict one rather than the runtime default.
     *
     * @param nextExpectedOffset the committed offset the map is relative to
     * @param decodedBytes       the payload, magic byte first
     * @return the highest offset seen, and the incomplete offsets below it
     * @see #decodeCompressedOffsets(long, byte[], InvalidOffsetMetadataHandlingPolicy, TopicPartition)
     */
    static HighestOffsetAndIncompletes decodeCompressedOffsets(long nextExpectedOffset, byte[] decodedBytes) {
        return decodeCompressedOffsets(nextExpectedOffset, decodedBytes, InvalidOffsetMetadataHandlingPolicy.FAIL, null);
    }

    /**
     * Decodes the offset map out of already-base64-decoded bytes, whose leading byte is the {@link OffsetEncoding}
     * magic number.
     * <p>
     * Empty input is not an error and never reaches the decoders: it means the commit carried no offset map, so
     * nothing was incomplete below the committed offset. That branch and the {@code IGNORE} branch of
     * {@link EncodedOffsetPair#decodeToIncompletes} must agree, and both answer {@code nextExpectedOffset - 1} - the
     * committed offset is the next one to be POLLED, so the highest we can claim to have seen is the one below it.
     *
     * @param nextExpectedOffset the committed offset the map is relative to
     * @param decodedBytes       the payload, magic byte first; empty means no map was committed
     * @param errorPolicy        what to do with a payload this build cannot read - every such case, not only metadata
     *                           recognisable as Kafka Streams'. See {@link EncodedOffsetPair#decodeToIncompletes}
     * @param tp                 the partition the metadata was committed against, for diagnosis - may be {@code null}
     *                           when the caller does not know it
     * @return the highest offset seen, and the incomplete offsets below it
     */
    static HighestOffsetAndIncompletes decodeCompressedOffsets(long nextExpectedOffset,
                                                               byte[] decodedBytes,
                                                               InvalidOffsetMetadataHandlingPolicy errorPolicy,
                                                               TopicPartition tp) {
        return decodeCompressedMetadata(nextExpectedOffset, decodedBytes, errorPolicy, tp).getOffsets();
    }

    /**
     * The single decode choke point, carrying the rider slot as well as the offsets.
     * <p>
     * Every decode entry point in this class funnels through here, which is what keeps the empty-payload branch, the
     * rider-only branch inside {@link EncodedOffsetPair#decodeToRiderAndIncompletes} and
     * {@link EncodedOffsetPair#handleUnreadableMetadata} all answering {@code nextExpectedOffset - 1}: the three must
     * agree, because one higher marks the committed record itself as done.
     *
     * @see #decodeCompressedOffsets(long, byte[], InvalidOffsetMetadataHandlingPolicy, TopicPartition)
     */
    static DecodedMetadata decodeCompressedMetadata(long nextExpectedOffset,
                                                    byte[] decodedBytes,
                                                    InvalidOffsetMetadataHandlingPolicy errorPolicy,
                                                    TopicPartition tp) {

        // if no offset bitmap data
        if (decodedBytes.length == 0) {
            // in this case, as there is no encoded offset data in the matadata, the highest we previously saw must be
            // the offset before the committed offset
            long highestSeenOffsetIsThen = nextExpectedOffset - 1;
            // no envelope was written, so no rider was configured when this was committed
            return DecodedMetadata.of(HighestOffsetAndIncompletes.of(highestSeenOffsetIsThen));
        } else {
            return EncodedOffsetPair.decodeToRiderAndIncompletes(decodedBytes, nextExpectedOffset, errorPolicy, tp);
        }
    }

}
