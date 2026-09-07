package bz.stub.parallelconsumer.offsets;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import lombok.extern.slf4j.Slf4j;
import bz.stub.parallelconsumer.ParallelConsumerOptions.InvalidOffsetMetadataHandlingPolicy;
import bz.stub.parallelconsumer.internal.PCModuleTestEnv;
import bz.stub.parallelconsumer.offsets.OffsetRiderEnvelope.Rider;
import bz.stub.parallelconsumer.offsets.OffsetRiderEnvelope.RiderState;
import bz.stub.parallelconsumer.state.PartitionState;
import lombok.SneakyThrows;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import pl.tlinkowski.unij.api.UniLists;
import java.util.Arrays;
import java.util.TreeSet;

import static bz.stub.parallelconsumer.offsets.RiderTestFixtures.moduleWithNoSupplier;
import static bz.stub.parallelconsumer.offsets.RiderTestFixtures.stateOver;
import static bz.stub.parallelconsumer.ParallelConsumerOptions.InvalidOffsetMetadataHandlingPolicy.FAIL;
import static bz.stub.parallelconsumer.ParallelConsumerOptions.InvalidOffsetMetadataHandlingPolicy.IGNORE;
import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * The read-back seam an embedder that does <b>not</b> own Parallel Consumer's consumer uses:
 * {@link OffsetMapCodecManager#decodeRider(long, String, InvalidOffsetMetadataHandlingPolicy)}, given a committed
 * metadata string it fetched itself.
 * <p>
 * {@link OffsetRiderCodecTest} owns the codec seams either side of the envelope and already covers what each payload
 * shape decodes to through the package-private {@code deserialiseMetadataFromBase64}; this class owns the
 * <b>public</b> entry point on top of it - that it exists with the shape R14 requires, that it projects the same four
 * states out, and that nothing about it leaks a shared array to an embedder.
 * <p>
 * The payloads here are built by {@link OffsetMapCodecManager}'s own write side rather than typed out as literals, so
 * that a change to the wire format fails this class instead of silently making it test a shape PC no longer writes.
 *
 * @author Antony Stubbs
 * @see OffsetRiderEnvelope
 * @see ForeignOffsetMetadataOnAssignmentTest for the same degradation at the rebalance frame (AE6)
 */
@Slf4j
class OffsetRiderReadBackTest {

    static final TopicPartition TP = new TopicPartition("myTopic", 0);

    /**
     * The committed offset the payloads are relative to: the NEXT offset to be polled, so a payload carrying no
     * offset map reads back as having seen {@code COMMITTED_OFFSET - 1}.
     */
    static final long COMMITTED_OFFSET = 100L;

    static final long HIGHEST_SUCCEEDED = 104L;

    static final byte[] RIDER_BYTES = {1, 2, 3, 4, 5, 6, 7, 8};

    PCModuleTestEnv module;

    OffsetMapCodecManager<String, String> codec;

    PartitionState<String, String> state;

    TreeSet<Long> incompleteOffsets;

    @BeforeEach
    void setup() {
        incompleteOffsets = new TreeSet<>(UniLists.of(100L, 102L, 103L));
        module = moduleWithNoSupplier();
        state = stateOver(module, TP, HIGHEST_SUCCEEDED, incompleteOffsets);
        codec = new OffsetMapCodecManager<>(module);
    }

    /**
     * Every payload this build wrote before the rider existed - and every payload it still writes when no rider is
     * configured. It has no envelope, so the answer is "never configured", which is a different fact from both
     * {@link RiderState#DROPPED} and {@link RiderState#UNREADABLE}.
     */
    @SneakyThrows
    @Test
    void todaysPayloadReadsBackAsNoRider() {
        String payload = codec.makeOffsetMetadataPayload(COMMITTED_OFFSET, state);

        assertWithMessage("a payload with no envelope says no rider was configured when it was committed")
                .that(OffsetMapCodecManager.decodeRider(COMMITTED_OFFSET, payload, FAIL).getState())
                .isEqualTo(RiderState.NONE);
    }

    /**
     * The ladder's third rung: the drop marker itself did not fit, so the envelope was shed and the offset map
     * committed bare (R9). Byte for byte that is today's payload, and R6 says so out loud - the rider reads back as
     * never configured, and the dropped-rider counter is the only remaining signal.
     */
    @SneakyThrows
    @Test
    void aStrippedEnvelopeIsIndistinguishableFromNoRiderEverConfigured() {
        byte[] inner = codec.encodeOffsetsToInnerBytes(COMMITTED_OFFSET, state);
        String thirdRung = codec.assembleMetadataPayload(inner, Rider.none());

        assertWithMessage("shedding the envelope leaves exactly the payload a build with no rider would have written")
                .that(thirdRung)
                .isEqualTo(codec.makeOffsetMetadataPayload(COMMITTED_OFFSET, state));
        assertWithMessage("R6: once the envelope itself is shed, the payload reads as never configured")
                .that(OffsetMapCodecManager.decodeRider(COMMITTED_OFFSET, thirdRung, FAIL).getState())
                .isEqualTo(RiderState.NONE);
    }

    @SneakyThrows
    @Test
    void anEnvelopeAroundHolesReadsBackAsPresentWithItsBytes() {
        byte[] inner = codec.encodeOffsetsToInnerBytes(COMMITTED_OFFSET, state);
        String payload = codec.assembleMetadataPayload(inner, Rider.present(RIDER_BYTES));

        Rider rider = OffsetMapCodecManager.decodeRider(COMMITTED_OFFSET, payload, FAIL);

        assertThat(rider.getState()).isEqualTo(RiderState.PRESENT);
        assertWithMessage("the rider must come back exactly as it was supplied")
                .that(rider.getBytes())
                .isEqualTo(RIDER_BYTES);
        assertWithMessage("reading the rider must not cost the caller the hole map - the same string still decodes to it")
                .that(OffsetMapCodecManager.deserialiseIncompleteOffsetMapFromBase64(COMMITTED_OFFSET, payload)
                        .getIncompleteOffsets())
                .containsExactlyElementsIn(incompleteOffsets)
                .inOrder();
    }

    /**
     * What a caught-up partition commits: a rider and no offset map at all.
     */
    @SneakyThrows
    @Test
    void aRiderOnlyEnvelopeReadsBackAsPresent() {
        String payload = codec.assembleMetadataPayload(new byte[0], Rider.present(RIDER_BYTES));

        Rider rider = OffsetMapCodecManager.decodeRider(COMMITTED_OFFSET, payload, FAIL);

        assertThat(rider.getState()).isEqualTo(RiderState.PRESENT);
        assertThat(rider.getBytes()).isEqualTo(RIDER_BYTES);
    }

    @SneakyThrows
    @Test
    void theDroppedMarkerReadsBackAsDropped() {
        byte[] inner = codec.encodeOffsetsToInnerBytes(COMMITTED_OFFSET, state);
        String payload = codec.assembleMetadataPayload(inner, Rider.dropped());

        Rider rider = OffsetMapCodecManager.decodeRider(COMMITTED_OFFSET, payload, FAIL);

        assertWithMessage("a rider shed for size must not read as one that was never configured")
                .that(rider.getState())
                .isEqualTo(RiderState.DROPPED);
        assertThrows(IllegalStateException.class, rider::getBytes,
                "a dropped rider has no bytes to hand back");
    }

    /**
     * An empty metadata field is what a partition with nothing to say commits, and it is also what an embedder gets
     * from {@code OffsetAndMetadata#metadata()} when nobody has written any. No envelope, so no rider.
     */
    @Test
    void anEmptyMetadataStringReadsBackAsNoRider() throws Exception {
        assertThat(OffsetMapCodecManager.decodeRider(COMMITTED_OFFSET, "", FAIL).getState())
                .isEqualTo(RiderState.NONE);
    }

    /**
     * Metadata this build cannot read at all reports {@link RiderState#UNREADABLE}: "the payload was thrown away"
     * and "no rider was ever configured" lead an embedder to different repairs, so they must not read the same (R6).
     */
    @Test
    void garbageUnderIgnoreReadsBackAsUnreadable() throws Exception {
        assertThat(OffsetMapCodecManager.decodeRider(COMMITTED_OFFSET, "not-valid-base64!!", IGNORE).getState())
                .isEqualTo(RiderState.UNREADABLE);
    }

    /**
     * The other half of the policy. The typed exception is deliberately <b>not</b> an {@link OffsetDecodingError}:
     * that one is swallowed by {@code loadPartitionStateForAssignment} even under {@code FAIL}, which is astubbs#207's
     * defect one layer down.
     */
    @Test
    void garbageUnderFailThrowsTheTypedExceptionRatherThanAnOffsetDecodingError() {
        var thrown = assertThrows(CorruptOffsetMetadataException.class,
                () -> OffsetMapCodecManager.decodeRider(COMMITTED_OFFSET, "not-valid-base64!!", FAIL));
        assertThat(thrown).isNotInstanceOf(OffsetDecodingError.class);
    }

    /**
     * AE7. The envelope parsed and the offset map inside it did not - the two are structurally independent, and the
     * rider was read before the body was looked at. So {@code IGNORE} still answers with the rider, and {@code FAIL}
     * still stops.
     */
    @Test
    void anIntactEnvelopeWithATruncatedInnerBodyStillYieldsItsRiderUnderIgnore() throws Exception {
        String payload = codec.assembleMetadataPayload(truncatedInnerBody(), Rider.present(RIDER_BYTES));

        Rider rider = OffsetMapCodecManager.decodeRider(COMMITTED_OFFSET, payload, IGNORE);

        assertWithMessage("losing the body must not lose the rider - AE7")
                .that(rider.getState())
                .isEqualTo(RiderState.PRESENT);
        assertThat(rider.getBytes()).isEqualTo(RIDER_BYTES);
    }

    @Test
    void anIntactEnvelopeWithATruncatedInnerBodyThrowsUnderFail() {
        String payload = codec.assembleMetadataPayload(truncatedInnerBody(), Rider.present(RIDER_BYTES));

        var thrown = assertThrows(CorruptOffsetMetadataException.class,
                () -> OffsetMapCodecManager.decodeRider(COMMITTED_OFFSET, payload, FAIL));
        assertThat(thrown).isNotInstanceOf(OffsetDecodingError.class);
    }

    /**
     * A {@link OffsetEncoding#BitSet} header declaring 32767 bits with no body behind it - the first fabricated map
     * {@link CorruptOffsetMetadataTest} pins, reused here as an inner encoding that is intact enough to be
     * recognised and truncated enough not to decode.
     */
    private static byte[] truncatedInnerBody() {
        return new byte[]{OffsetEncoding.BitSet.magicByte, (byte) 0x7F, (byte) 0xFF};
    }

    /**
     * The copy-out half of KTD7's "PC copies the array on the way in and on the way out". An embedder that decodes
     * its blob in place must not be able to corrupt what the next call returns.
     */
    @Test
    void mutatingTheReturnedBytesCannotChangeWhatASecondCallReturns() throws Exception {
        String payload = codec.assembleMetadataPayload(new byte[0], Rider.present(RIDER_BYTES));

        byte[] first = OffsetMapCodecManager.decodeRider(COMMITTED_OFFSET, payload, FAIL).getBytes();
        Arrays.fill(first, (byte) 0);

        assertWithMessage("no array the read path allocated may be shared with an embedder")
                .that(OffsetMapCodecManager.decodeRider(COMMITTED_OFFSET, payload, FAIL).getBytes())
                .isEqualTo(RIDER_BYTES);

        Rider rider = OffsetMapCodecManager.decodeRider(COMMITTED_OFFSET, payload, FAIL);
        byte[] once = rider.getBytes();
        Arrays.fill(once, (byte) 0);
        assertWithMessage("nor may two reads of the same Rider share one")
                .that(rider.getBytes())
                .isEqualTo(RIDER_BYTES);
    }

    /**
     * The half of R14 that {@code assertThrows} cannot pin. Its callable is declared {@code throws Throwable},
     * so the compiler never asks whether the checked types the javadoc promises are actually <em>declared</em> - and
     * a checked type that is thrown but not declared cannot be caught by name: javac rejects the {@code catch} as
     * unreachable. This test is the caller the javadoc describes, written the way an embedder would write it, and
     * it compiles only while the declaration holds.
     */
    @Test
    void aFailCallerCanCatchTheDocumentedCheckedExceptionsByType() throws Exception {
        String corrupt = "not-valid-base64!!";
        String unknownMagic = codec.assembleMetadataPayload(
                new byte[]{OffsetCodecTestUtils.magicByteOfAnEncodingThatDoesNotExistYet(), 1, 2, 3}, Rider.none());

        String caughtForCorrupt = "nothing";
        try {
            OffsetMapCodecManager.decodeRider(COMMITTED_OFFSET, corrupt, FAIL);
        } catch (CorruptOffsetMetadataException e) {
            caughtForCorrupt = "corrupt";
        } catch (UnknownOffsetMetadataMagicException e) {
            caughtForCorrupt = "unknown magic";
        }
        assertWithMessage("the corrupt payload lands in the catch the javadoc names for it")
                .that(caughtForCorrupt)
                .isEqualTo("corrupt");

        String caughtForUnknownMagic = "nothing";
        try {
            OffsetMapCodecManager.decodeRider(COMMITTED_OFFSET, unknownMagic, FAIL);
        } catch (CorruptOffsetMetadataException e) {
            caughtForUnknownMagic = "corrupt";
        } catch (UnknownOffsetMetadataMagicException e) {
            caughtForUnknownMagic = "unknown magic";
        }
        assertWithMessage("the unknown magic byte lands in ITS catch, not the corrupt one")
                .that(caughtForUnknownMagic)
                .isEqualTo("unknown magic");
    }

    /**
     * R14's shape, pinned rather than described: the offset leads, like every entry point in this family; the policy
     * is a parameter; and the checked {@link OffsetDecodingError} is declared, so a caller cannot forget the base64
     * failure mode - along with the two typed {@code FAIL} outcomes, so a caller can catch them by name.
     */
    @Test
    void theEntryPointLeadsWithTheOffsetAndDeclaresTheCheckedDecodingError() throws Exception {
        Method decodeRider = OffsetMapCodecManager.class.getMethod("decodeRider", long.class, String.class,
                InvalidOffsetMetadataHandlingPolicy.class);

        assertThat(Modifier.isPublic(decodeRider.getModifiers())).isTrue();
        assertWithMessage("an embedder reading a string it fetched itself has no codec instance to reach this through")
                .that(Modifier.isStatic(decodeRider.getModifiers()))
                .isTrue();
        assertWithMessage("the committed offset leads, like the deserialiseIncompleteOffsetMapFromBase64 family")
                .that(decodeRider.getParameterTypes()[0])
                .isEqualTo(long.class);
        assertThat(decodeRider.getReturnType()).isEqualTo(Rider.class);
        assertWithMessage("the checked FAIL outcomes are declared as a family, so a caller can catch the parent in one "
                        + "clause or any member - unknown magic, Kafka Streams metadata, an undecodable enum "
                        + "member - by its own type")
                .that(decodeRider.getExceptionTypes())
                .asList()
                .containsAtLeast(OffsetDecodingError.class,
                        CorruptOffsetMetadataException.class,
                        EncodingNotSupportedException.class);
    }

    /**
     * R14, the half a javadoc alone cannot hold: <b>there is no overload without a policy</b>. A convenience overload
     * would have to pick a policy on the caller's behalf, and the two candidate defaults disagree - {@code IGNORE} is
     * the runtime default, {@code FAIL} is what the policy-less {@code deserialiseIncompleteOffsetMapFromBase64}
     * helper picks precisely because discarding an offset map is not a helper's decision. The Streams rung will want
     * one; this is the assertion that makes adding it a deliberate act rather than a convenience.
     */
    @Test
    void thereIsNoOverloadOfTheEntryPointWithoutAPolicy() {
        assertWithMessage("no overload of decodeRider may omit the policy - R14")
                .that(Arrays.stream(OffsetMapCodecManager.class.getMethods())
                        .filter(m -> "decodeRider".equals(m.getName()))
                        .map(m -> Arrays.toString(m.getParameterTypes()))
                        .toArray())
                .asList()
                .containsExactly(Arrays.toString(new Class<?>[]{long.class, String.class,
                        InvalidOffsetMetadataHandlingPolicy.class}));
    }
}
