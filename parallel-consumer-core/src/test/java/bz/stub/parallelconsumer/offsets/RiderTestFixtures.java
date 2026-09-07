package bz.stub.parallelconsumer.offsets;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.RiderContext;
import bz.stub.parallelconsumer.internal.PCModuleTestEnv;
import bz.stub.parallelconsumer.offsets.OffsetMapCodecManager.HighestOffsetAndIncompletes;
import bz.stub.parallelconsumer.offsets.OffsetRiderEnvelope.RiderState;
import bz.stub.parallelconsumer.offsets.OffsetRiderEnvelope.UnwrappedEnvelope;
import bz.stub.parallelconsumer.state.PartitionState;
import bz.stub.parallelconsumer.state.PartitionStateManager;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Base64;
import java.util.Optional;
import java.util.Random;
import java.util.SortedSet;
import java.util.function.Function;

/**
 * The fixtures the opaque-rider test classes share: the partition shapes they commit, the modules they commit
 * through, and the readers that take a committed metadata string apart again.
 * <p>
 * <b>This is a test restatement, and that is the point.</b> Several of the classes using it claim that their
 * arithmetic is stated <em>independently of production</em> - {@link #base64Characters} is checked against
 * {@code RiderBudgetRung}'s own copy, and the budget the ladder offers a supplier is checked against a
 * restatement of {@code PartitionState.maxRiderBytes}. That property is about not borrowing from the production
 * side, never about each test class writing its own copy: one shared restatement here is still an independent
 * statement of the arithmetic, and it is the <em>only</em> place a drifting copy could hide.
 * <p>
 * Everything here is deliberately parameterised on the caller's partition, record count and seed rather than
 * holding constants of its own, so a class keeps its own fixture tuning where its scenarios can be read against
 * it.
 *
 * @author Antony Stubbs
 * @see OffsetRiderEnvelope
 */
@Slf4j
public final class RiderTestFixtures {

    private RiderTestFixtures() {
        // fixtures only
    }

    /**
     * How many encoded characters {@code rawBytes} occupies: Base64's closed form, which is exact for the padding
     * encoder {@code OffsetSimpleSerialisation.base64} uses.
     * <p>
     * Written out here rather than borrowed from {@code RiderBudgetRung} - which is package-private to
     * {@code bz.stub.parallelconsumer.state} in any case - so that a prediction is checked against an independent
     * statement of the arithmetic rather than against itself.
     */
    public static int base64Characters(int rawBytes) {
        return 4 * ((rawBytes + 2) / 3);
    }

    /**
     * Rider bytes that do not compress, so a fixture's arithmetic about their length survives the outer codec.
     *
     * @param seed the calling class's own corpus seed, so two classes asking for the same length still get their
     *             own bytes
     */
    public static byte[] riderOf(long seed, int length) {
        byte[] bytes = new byte[length];
        new Random(seed + length).nextBytes(bytes);
        return bytes;
    }

    /**
     * A module wired to {@code supplier}, with its own meter registry so that nothing here perturbs another
     * suite's meters - or is perturbed by them, which is what an encodes-once assertion depends on.
     */
    public static PCModuleTestEnv moduleWith(Function<RiderContext, byte[]> supplier) {
        return moduleWith(supplier, new SimpleMeterRegistry());
    }

    /**
     * A module with no supplier configured at all - the build that has never heard of riders, which is the
     * baseline every codec and read-back test compares against. Its own registry, for the same reason as
     * {@link #moduleWith}: an encodes-once assertion counts this test's encodes and nobody else's.
     */
    public static PCModuleTestEnv moduleWithNoSupplier() {
        return moduleWith(null);
    }

    /**
     * A partition state built directly from a decoded shape rather than by polling records: the highest succeeded
     * offset and the incomplete set as a reader would hand them back.
     */
    public static PartitionState<String, String> stateOver(PCModuleTestEnv module,
                                                           TopicPartition tp,
                                                           long highestSucceeded,
                                                           SortedSet<Long> incompleteOffsets) {
        return new PartitionState<>(0, module, tp,
                new HighestOffsetAndIncompletes(Optional.of(highestSucceeded), incompleteOffsets));
    }

    public static PCModuleTestEnv moduleWith(Function<RiderContext, byte[]> supplier, MeterRegistry meterRegistry) {
        return new PCModuleTestEnv(ParallelConsumerOptions.<String, String>builder()
                .riderSupplier(supplier)
                .meterRegistry(meterRegistry)
                .build());
    }

    public static ConsumerRecord<String, String> record(TopicPartition tp, long offset) {
        return new ConsumerRecord<>(tp.topic(), tp.partition(), offset, "key", "value");
    }

    /**
     * A partition with {@code records} offsets polled and a pseudorandom half of them still outstanding - an
     * ordinary out-of-order completion pattern, chosen over an alternating one because random holes do not
     * compress and so the encoded length stays a function of the range rather than of gzip.
     * <p>
     * Offset zero is always incomplete, which pins the commit offset at zero and so the encoder's base; the top
     * offset always succeeds, which pins the range.
     */
    public static PartitionState<String, String> stateWithHoles(PCModuleTestEnv module,
                                                                TopicPartition tp,
                                                                int records,
                                                                long holeSeed) {
        var state = new PartitionState<String, String>(0, module, tp, HighestOffsetAndIncompletes.of());
        populateWithHoles(state, tp, records, holeSeed);
        return state;
    }

    /**
     * @see #stateWithHoles
     */
    public static void populateWithHoles(PartitionState<String, String> state,
                                         TopicPartition tp,
                                         int records,
                                         long holeSeed) {
        var holes = new Random(holeSeed);
        for (long offset = 0; offset < records; offset++) {
            state.addNewIncompleteRecord(record(tp, offset));
        }
        for (long offset = 1; offset < records; offset++) {
            if (offset == records - 1 || holes.nextBoolean()) {
                state.onSuccess(offset);
            }
        }
    }

    /**
     * A partition with nothing outstanding - the steady state, which committed no metadata at all before the
     * rider existed.
     */
    public static PartitionState<String, String> caughtUpState(PCModuleTestEnv module, TopicPartition tp) {
        var state = new PartitionState<String, String>(0, module, tp, HighestOffsetAndIncompletes.of());
        state.addNewIncompleteRecord(record(tp, 0));
        state.onSuccess(0);
        return state;
    }

    /**
     * The encoded length of {@link #stateWithHoles}'s offset map, measured by running the same encoder the commit
     * will run against a throwaway state. Derive a fixture's caps from this rather than hard-coding them, so a
     * change to the encodings retunes the fixtures instead of silently reclassifying a scenario.
     */
    public static int measureInnerEncodingLength(TopicPartition tp, int records, long holeSeed)
            throws NoEncodingPossibleException {
        var module = moduleWith(context -> null);
        var state = stateWithHoles(module, tp, records, holeSeed);
        byte[] inner = new OffsetMapCodecManager<String, String>(module).encodeOffsetsToInnerBytes(0, state);
        log.debug("Fixture offset map encodes to {} bytes ({} characters)", inner.length,
                base64Characters(inner.length));
        return inner.length;
    }

    public static byte[] decoded(OffsetAndMetadata committed) {
        return Base64.getDecoder().decode(committed.metadata());
    }

    public static UnwrappedEnvelope unwrap(OffsetAndMetadata committed) throws CorruptOffsetMetadataException {
        return OffsetRiderEnvelope.unwrap(decoded(committed));
    }

    /**
     * Unwraps a payload the caller has just committed, so a corruption here is a broken test rather than an
     * expected outcome - which is why it does not hand the caller a checked exception to declare.
     */
    public static UnwrappedEnvelope unwrapOrFail(byte[] raw) {
        try {
            return OffsetRiderEnvelope.unwrap(raw);
        } catch (CorruptOffsetMetadataException corrupt) {
            throw new AssertionError("a payload this test just committed did not unwrap", corrupt);
        }
    }

    /**
     * What a committed string says about the rider slot - {@link RiderState#NONE} when there is no envelope at
     * all, which is both "never configured" and the ladder's bottom envelope rung.
     */
    public static RiderState riderStateOf(String metadata) {
        if (metadata == null || metadata.isEmpty()) {
            return RiderState.NONE;
        }
        byte[] raw = Base64.getDecoder().decode(metadata);
        if (raw.length == 0 || raw[0] != OffsetRiderEnvelope.MAGIC_BYTE) {
            return RiderState.NONE;
        }
        return unwrapOrFail(raw).getRider().getState();
    }

    /**
     * @see #riderStateOf(String)
     */
    public static RiderState riderStateOf(OffsetAndMetadata committed) {
        return riderStateOf(committed.metadata());
    }

    /**
     * The two mutable statics the rider's derived cap is a function of, remembered so a scenario can move them
     * and put them back.
     * <p>
     * Both are JVM-global and this module runs test methods in parallel, so a class touching them carries
     * {@code @ResourceLock(OffsetMapCodecManager.METADATA_DATA_SIZE_RESOURCE_LOCK)}. That annotation is JUnit
     * metadata and stays on the test class; only the remember/restore pair lives here.
     */
    public static final class MetadataSizeStatics {

        private final int maxMetadataSize;

        private final double thresholdMultiplier;

        private MetadataSizeStatics() {
            this.maxMetadataSize = OffsetMapCodecManager.DefaultMaxMetadataSize;
            this.thresholdMultiplier = PartitionStateManager.getUSED_PAYLOAD_THRESHOLD_MULTIPLIER();
        }

        public static MetadataSizeStatics remember() {
            return new MetadataSizeStatics();
        }

        public void restore() {
            OffsetMapCodecManager.DefaultMaxMetadataSize = maxMetadataSize;
            PartitionStateManager.setUSED_PAYLOAD_THRESHOLD_MULTIPLIER(thresholdMultiplier);
        }
    }

    /**
     * The pair of statics that pin which encoding wins the competition, remembered so a scenario can force one
     * and put them back. Guarded by {@code OffsetSimultaneousEncoder.COMPRESSION_FORCED_RESOURCE_LOCK} on the
     * test class, for the same reason as {@link MetadataSizeStatics}.
     */
    public static final class CodecForcingStatics {

        private final Optional<OffsetEncoding> forcedCodec;

        private final boolean compressionForced;

        private CodecForcingStatics() {
            this.forcedCodec = OffsetMapCodecManager.forcedCodec;
            this.compressionForced = OffsetSimultaneousEncoder.compressionForced;
        }

        public static CodecForcingStatics remember() {
            return new CodecForcingStatics();
        }

        public void restore() {
            OffsetMapCodecManager.forcedCodec = forcedCodec;
            OffsetSimultaneousEncoder.compressionForced = compressionForced;
        }
    }

}
