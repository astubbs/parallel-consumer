package bz.stub.parallelconsumer.state;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.RiderContext;
import bz.stub.parallelconsumer.internal.PCModuleTestEnv;
import bz.stub.parallelconsumer.metrics.PCMetricsDef;
import bz.stub.parallelconsumer.offsets.CorruptOffsetMetadataException;
import bz.stub.parallelconsumer.offsets.NoEncodingPossibleException;
import bz.stub.parallelconsumer.offsets.OffsetDecodingError;
import bz.stub.parallelconsumer.offsets.OffsetMapCodecManager;
import bz.stub.parallelconsumer.offsets.OffsetMapCodecManager.HighestOffsetAndIncompletes;
import bz.stub.parallelconsumer.offsets.OffsetRiderEnvelope;
import bz.stub.parallelconsumer.offsets.OffsetRiderEnvelope.RiderState;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceAccessMode;
import org.junit.jupiter.api.parallel.ResourceLock;

import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Random;
import java.util.function.Function;

import static bz.stub.parallelconsumer.state.PartitionStateManager.USED_PAYLOAD_THRESHOLD_MULTIPLIER_DEFAULT;
import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;

/**
 * The budget ladder (U2 of the opaque-rider plan): what a commit does when the payload it wants to write does not
 * fit the metadata field.
 * <p>
 * <b>The two measurements, and why they are different numbers</b> (KTD4/R7). Back pressure exists so that a
 * payload can <em>shrink</em> as work completes. Rider bytes do not shrink - the embedder hands over the same
 * blob whatever the hole map is doing - so charging them against the back-pressure threshold would make a rider a
 * floor that back pressure can never relieve, and on a caught-up partition a permanent block. So the hole
 * encoding alone is measured against the threshold, and the assembled string, rider included, against the hard
 * metadata cap.
 * <p>
 * <b>The ladder</b> (R9). When the assembled string will not fit the cap, PC sheds the rider, then the drop
 * marker, and only then the hole map - so configuring a rider can never cost a partition metadata it would
 * otherwise have committed. Each rung is chosen by <em>predicted</em> encoded length, from Base64's closed form
 * {@code 4*ceil(n/3)}, so the outer codec runs once on the winning rung rather than once per rung; every scenario
 * here asserts the prediction against the string that was actually produced.
 * <p>
 * <b>The fixtures name their statics explicitly.</b> {@link OffsetMapCodecManager#DefaultMaxMetadataSize} and
 * {@link PartitionStateManager#getUSED_PAYLOAD_THRESHOLD_MULTIPLIER()} are mutable statics, and the rider cap is
 * derived from <em>both</em>. A scenario that needs the ladder reachable has to keep the multiplier below 1 (the
 * 0.75 default) and set a cap large enough that the derived rider cap clears the envelope's own three bytes: at
 * the back-pressure harnesses' multipliers of 30 and 2 the derived cap is zero for every cap, so the write-time
 * guard fires first and nothing here would be about the ladder at all. The lock is taken in WRITE mode because
 * these tests move the cap, and both statics are restored per test and again after the class.
 * <p>
 * <b>Sizes are measured, never assumed.</b> Every scenario encodes its own hole map first, reads the byte length
 * the encoder competition actually produced, and derives the cap it needs from that - so a change to the
 * encodings moves the fixture rather than silently turning a ladder scenario into a guard scenario. Each one
 * asserts its own preconditions for that reason.
 *
 * @author Antony Stubbs
 * @see RiderSupplierGuardTest the write-time guard above this - which rider reaches the ladder at all
 * @see OffsetRiderEnvelope
 */
@Slf4j
@ResourceLock(value = OffsetMapCodecManager.METADATA_DATA_SIZE_RESOURCE_LOCK, mode = ResourceAccessMode.READ_WRITE)
class PartitionStateRiderBudgetTest {

    private static final TopicPartition TP = new TopicPartition("rider-budget", 0);

    /**
     * The envelope's own cost in bytes, restated so a scenario's arithmetic reads without a lookup.
     */
    private static final int HEADER = OffsetRiderEnvelope.HEADER_BYTES;

    /**
     * Enough records for the hole map to encode to a couple of hundred bytes, which leaves the derived rider cap
     * room to be interesting. Randomised holes rather than a pattern, because an alternating one compresses to
     * almost nothing and the fixture would then be measuring gzip.
     */
    private static final int RECORDS = 1700;

    private static final long HOLE_SEED = 20260906L;

    private int realMaxMetadataSize;

    private double realThresholdMultiplier;

    @BeforeEach
    void rememberStatics() {
        realMaxMetadataSize = OffsetMapCodecManager.DefaultMaxMetadataSize;
        realThresholdMultiplier = PartitionStateManager.getUSED_PAYLOAD_THRESHOLD_MULTIPLIER();
    }

    @AfterEach
    void restoreStatics() {
        OffsetMapCodecManager.DefaultMaxMetadataSize = realMaxMetadataSize;
        PartitionStateManager.setUSED_PAYLOAD_THRESHOLD_MULTIPLIER(realThresholdMultiplier);
    }

    @AfterAll
    static void restoreDefaults() {
        OffsetMapCodecManager.DefaultMaxMetadataSize = 4096;
        PartitionStateManager.setUSED_PAYLOAD_THRESHOLD_MULTIPLIER(USED_PAYLOAD_THRESHOLD_MULTIPLIER_DEFAULT);
    }

    // ---- the ladder's rungs ---------------------------------------------------------------------------------

    /**
     * AE3, covering R7. The hole encoding sits under the back-pressure threshold; the rider lifts the assembled
     * string over it but leaves it under the cap. The rider is committed and the partition stays unblocked -
     * because only the hole encoding is measured against the threshold.
     * <p>
     * On the pre-change rule (assembled string against both limits) this commit blocks the partition, and nothing
     * would ever unblock it: the rider does not shrink when work completes.
     */
    @Test
    void aRiderOverTheThresholdIsCommittedAndDoesNotBlockThePartition() throws Exception {
        int innerBytes = measureInnerEncodingLength();
        int innerCharacters = base64Characters(innerBytes);

        // 1.8x the hole encoding: comfortably above it (so the assembled string clears the 75% threshold once the
        // rider is on) and comfortably below 1/0.75 of it (so the hole encoding alone is still under that
        // threshold). Both are asserted below rather than trusted.
        int cap = roundUpToFour(innerCharacters * 9 / 5);
        OffsetMapCodecManager.DefaultMaxMetadataSize = cap;
        PartitionStateManager.setUSED_PAYLOAD_THRESHOLD_MULTIPLIER(USED_PAYLOAD_THRESHOLD_MULTIPLIER_DEFAULT);

        int riderLength = PartitionState.maxRiderBytes(innerBytes, cap, USED_PAYLOAD_THRESHOLD_MULTIPLIER_DEFAULT);
        byte[] rider = riderOf(riderLength);
        double threshold = cap * USED_PAYLOAD_THRESHOLD_MULTIPLIER_DEFAULT;
        int predicted = base64Characters(HEADER + riderLength + innerBytes);

        assertWithMessage("fixture: the hole encoding alone must sit UNDER the back-pressure threshold, or this "
                + "scenario is not about the rider at all")
                .that((double) innerCharacters)
                .isAtMost(threshold);
        assertWithMessage("fixture: the rider must lift the assembled string OVER the threshold, or the pre-change "
                + "rule would pass this test too")
                .that((double) predicted)
                .isGreaterThan(threshold);
        assertWithMessage("fixture: and must leave it under the hard cap, or this is the ladder's next rung")
                .that(predicted)
                .isAtMost(cap);

        var state = stateWithHoles(context -> rider);
        OffsetAndMetadata committed = state.createOffsetAndMetadata();

        assertWithMessage("R7: the rider is charged against the cap, never against the back-pressure threshold - "
                + "rider bytes do not shrink as work completes, so charging them there is a floor back pressure "
                + "can never relieve")
                .that(state.isAllowedMoreRecords())
                .isTrue();
        assertThat(riderStateOf(committed)).isEqualTo(RiderState.PRESENT);
        assertThat(unwrap(committed).getRider().getBytes()).isEqualTo(rider);
        assertWithMessage("R9: the hole map rides with it, untouched")
                .that(incompletesOf(committed))
                .isNotEmpty();
        assertWithMessage("the rung is chosen by predicted length, so the prediction has to be exact")
                .that(committed.metadata().length())
                .isEqualTo(predicted);
    }

    /**
     * AE4, covering R6 and R9. The hole map fits the cap with room for the drop marker, but not with the rider on
     * top. The committed payload is the envelope carrying the zero-length marker around the hole map - which is
     * how a reader tells a rider that was shed for size from one that was never configured - and the block
     * follows the <em>inner</em> length, not the assembled one.
     */
    @Test
    void whenTheRiderWillNotFitTheMarkerRidesAroundTheHoleMapInstead() throws Exception {
        int innerBytes = measureInnerEncodingLength();
        int innerCharacters = base64Characters(innerBytes);

        // exactly enough for the marker form and no more: 4*ceil((3+inner)/3) is the smallest cap that fits it
        int cap = base64Characters(HEADER + innerBytes);
        OffsetMapCodecManager.DefaultMaxMetadataSize = cap;
        PartitionStateManager.setUSED_PAYLOAD_THRESHOLD_MULTIPLIER(USED_PAYLOAD_THRESHOLD_MULTIPLIER_DEFAULT);

        byte[] rider = riderOf(32);
        int predicted = base64Characters(HEADER + innerBytes);

        assertWithMessage("fixture: the marker form must fit the cap")
                .that(predicted)
                .isAtMost(cap);
        assertWithMessage("fixture: the rider form must not")
                .that(base64Characters(HEADER + rider.length + innerBytes))
                .isGreaterThan(cap);

        var state = stateWithHoles(context -> rider);
        OffsetAndMetadata committed = state.createOffsetAndMetadata();

        assertWithMessage("R6: the envelope survives carrying the zero-length marker, so a reader can tell a "
                + "rider shed for size from one that was never configured")
                .that(riderStateOf(committed))
                .isEqualTo(RiderState.DROPPED);
        assertWithMessage("R9: shedding the rider must not cost the hole map")
                .that(incompletesOf(committed))
                .isNotEmpty();
        assertWithMessage("KTD4: the block follows the hole encoding's own length, not the assembled string's")
                .that(state.isAllowedMoreRecords())
                .isEqualTo(innerCharacters <= cap * USED_PAYLOAD_THRESHOLD_MULTIPLIER_DEFAULT);
        assertThat(committed.metadata().length()).isEqualTo(predicted);
    }

    /**
     * AE5, covering R9 - the rung that exists because the marker is not free. The hole map's string sits within
     * the marker's own cost of the cap, so an envelope of any kind would push it over. PC writes today's bare
     * hole map instead: a payload that reads back as "no rider was ever configured", which is the price of R9's
     * guarantee that configuring a rider never costs the partition metadata it would otherwise have committed.
     */
    @Test
    void whenEvenTheMarkerWillNotFitTheEnvelopeGoesAndTheHoleMapStays() throws Exception {
        int innerBytes = measureInnerEncodingLength();

        // exactly the bare form's length: the marker's three bytes are always four more characters than this
        int cap = base64Characters(innerBytes);
        OffsetMapCodecManager.DefaultMaxMetadataSize = cap;
        PartitionStateManager.setUSED_PAYLOAD_THRESHOLD_MULTIPLIER(USED_PAYLOAD_THRESHOLD_MULTIPLIER_DEFAULT);

        byte[] rider = riderOf(32);

        assertWithMessage("fixture: the bare hole map must fit")
                .that(cap)
                .isAtLeast(base64Characters(innerBytes));
        assertWithMessage("fixture: and the marker form must not - that is the whole point of this rung")
                .that(base64Characters(HEADER + innerBytes))
                .isGreaterThan(cap);

        var state = stateWithHoles(context -> rider);
        OffsetAndMetadata committed = state.createOffsetAndMetadata();

        assertWithMessage("R9: a hole map within the marker's cost of the cap must still commit, so the envelope "
                + "itself is what goes")
                .that((int) decoded(committed)[0])
                .isNotEqualTo((int) OffsetRiderEnvelope.MAGIC_BYTE);
        assertWithMessage("R6: with the envelope gone the payload reads as one written with no rider configured")
                .that(riderStateOf(committed))
                .isEqualTo(RiderState.NONE);
        assertWithMessage("R9: and the hole map is retained")
                .that(incompletesOf(committed))
                .isNotEmpty();
        assertThat(committed.metadata().length()).isEqualTo(base64Characters(innerBytes));
    }

    /**
     * The bottom of the ladder, and the one rung that predates the rider: the hole map alone does not fit, so the
     * payload is stripped and a bare offset is committed. Asserted with a rider configured <em>and</em> without
     * one, because the outcome has to be identical - that is R9 stated from the other end.
     */
    @Test
    void holesAloneOverTheCapStripThePayloadWithOrWithoutARider() throws Exception {
        int innerBytes = measureInnerEncodingLength();

        // one Base64 quantum below the bare form: nothing on the ladder can fit
        int cap = base64Characters(innerBytes) - 4;
        OffsetMapCodecManager.DefaultMaxMetadataSize = cap;
        PartitionStateManager.setUSED_PAYLOAD_THRESHOLD_MULTIPLIER(USED_PAYLOAD_THRESHOLD_MULTIPLIER_DEFAULT);

        assertWithMessage("fixture: not even the bare hole map may fit")
                .that(base64Characters(innerBytes))
                .isGreaterThan(cap);

        for (Function<RiderContext, byte[]> supplier : suppliers(null, riderOf(32))) {
            var state = stateWithHoles(supplier);
            OffsetAndMetadata committed = state.createOffsetAndMetadata();

            assertWithMessage("the payload is stripped and the bare offset committed, exactly as before the "
                    + "rider existed")
                    .that(committed.metadata())
                    .isEmpty();
            assertThat(state.isAllowedMoreRecords()).isFalse();
        }
    }

    /**
     * The ladder's arithmetic on its own, at every boundary: a rung is taken when its predicted payload fits and
     * given up one character below that. Asserted directly because two of the descents are unreachable through a
     * supplier - the write-time guard's own cap already refuses a rider that would not fit beside its offset map,
     * so the ladder is the defence for when that arithmetic changes, not a path a well-behaved supplier takes.
     */
    @Test
    void theLadderTakesTheFirstRungThatFitsAndGivesItUpOneCharacterBelow() {
        int inner = 100;
        int rider = 40;
        int riderForm = base64Characters(HEADER + rider + inner);
        int markerForm = base64Characters(HEADER + inner);
        int bareForm = base64Characters(inner);

        assertWithMessage("the three rungs cost strictly less as the ladder descends")
                .that(bareForm)
                .isLessThan(markerForm);
        assertThat(markerForm).isLessThan(riderForm);

        assertThat(RiderBudgetRung.choose(RiderState.PRESENT, rider, inner, riderForm))
                .isEqualTo(RiderBudgetRung.RIDER);
        assertWithMessage("one character short of the rider form, the rider is shed for the marker")
                .that(RiderBudgetRung.choose(RiderState.PRESENT, rider, inner, riderForm - 1))
                .isEqualTo(RiderBudgetRung.MARKER);
        assertThat(RiderBudgetRung.choose(RiderState.PRESENT, rider, inner, markerForm))
                .isEqualTo(RiderBudgetRung.MARKER);
        assertWithMessage("one character short of the marker form, the envelope itself goes - R9, because the "
                + "offset map still fits")
                .that(RiderBudgetRung.choose(RiderState.PRESENT, rider, inner, markerForm - 1))
                .isEqualTo(RiderBudgetRung.NO_ENVELOPE);

        assertWithMessage("a rider the guard already dropped never climbs back onto the wire, however much room "
                + "the ladder finds")
                .that(RiderBudgetRung.choose(RiderState.DROPPED, 0, inner, riderForm))
                .isEqualTo(RiderBudgetRung.MARKER);
        assertThat(RiderBudgetRung.choose(RiderState.DROPPED, 0, inner, markerForm - 1))
                .isEqualTo(RiderBudgetRung.NO_ENVELOPE);

        for (int cap : new int[]{0, bareForm - 1, bareForm, markerForm, riderForm}) {
            assertWithMessage("R2: with no rider configured there is never an envelope, at a cap of %s", cap)
                    .that(RiderBudgetRung.choose(RiderState.NONE, 0, inner, cap))
                    .isEqualTo(RiderBudgetRung.NO_ENVELOPE);
        }
        assertWithMessage("below the bare form nothing fits, and the answer is still NO_ENVELOPE - stripping is "
                + "what the size check does with the result, not a rung of the envelope ladder")
                .that(RiderBudgetRung.choose(RiderState.PRESENT, rider, inner, bareForm - 1))
                .isEqualTo(RiderBudgetRung.NO_ENVELOPE);
    }

    /**
     * The closed form the ladder predicts with, checked against the encoder itself across its whole working
     * domain rather than sampled - a size claim tested only on friendly inputs is the defect class
     * {@code docs/solutions/logic-errors/boundary-claim-tested-only-on-friendly-samples.md} records. If this ever
     * disagrees, every rung choice above is measuring a payload that will not be the one produced.
     */
    @Test
    void thePredictedLengthIsTheLengthTheEncoderProduces() {
        for (int rawBytes = 0; rawBytes <= 600; rawBytes++) {
            assertWithMessage("Base64 closed form at %s raw bytes", rawBytes)
                    .that(RiderBudgetRung.base64Characters(rawBytes))
                    .isEqualTo(Base64.getEncoder().encodeToString(new byte[rawBytes]).length());
        }
    }

    // ---- the derived cap ------------------------------------------------------------------------------------

    /**
     * KTD4's arithmetic, asserted directly rather than through a commit: the rider cap is a function of the two
     * mutable statics and of the inner encoding, and each of the three inputs moves it for its own reason.
     */
    @Test
    void theRiderCapTracksBothStaticsAndTheInnerLength() {
        assertWithMessage("the quarter of the metadata field back pressure never uses: 4096 characters at the "
                + "0.75 multiplier leaves 1024 characters, which is 768 raw bytes")
                .that(PartitionState.maxRiderBytes(0, 4096, USED_PAYLOAD_THRESHOLD_MULTIPLIER_DEFAULT))
                .isEqualTo(768);
        assertWithMessage("a multiplier at or above 1 leaves no slice for a rider at all, whatever the cap - "
                + "which is why the back-pressure harnesses' multipliers of 30 and 2 cannot reach the ladder")
                .that(PartitionState.maxRiderBytes(0, 4096, 1.5))
                .isEqualTo(0);
        assertThat(PartitionState.maxRiderBytes(0, 4096, 1.0)).isEqualTo(0);

        int inner = 2500;
        int remaining = (4096 / 4) * 3 - HEADER - inner;
        assertWithMessage("with a hole map to share the payload with, the answer is the smaller of the cap and "
                + "what is actually left")
                .that(PartitionState.maxRiderBytes(inner, 4096, USED_PAYLOAD_THRESHOLD_MULTIPLIER_DEFAULT))
                .isEqualTo(Math.min(768, remaining));
        assertWithMessage("and never negative when the hole map has already used everything")
                .that(PartitionState.maxRiderBytes(4000, 4096, USED_PAYLOAD_THRESHOLD_MULTIPLIER_DEFAULT))
                .isEqualTo(0);
    }

    // ---- what must not change -------------------------------------------------------------------------------

    /**
     * R2's behavioural half, over a corpus rather than one sample: with no rider configured, the point at which
     * back pressure engages is byte for byte the one this build had before the ladder existed. Today's rule is
     * computed here from the pre-change definition - the assembled string's length against the threshold - which
     * with no rider is the same string the inner-only rule measures.
     */
    @Test
    void withNoRiderTheThresholdEngagementPointIsUnchanged() throws Exception {
        int cap = 400;
        OffsetMapCodecManager.DefaultMaxMetadataSize = cap;
        PartitionStateManager.setUSED_PAYLOAD_THRESHOLD_MULTIPLIER(USED_PAYLOAD_THRESHOLD_MULTIPLIER_DEFAULT);
        double threshold = cap * USED_PAYLOAD_THRESHOLD_MULTIPLIER_DEFAULT;

        // a corpus of hole densities, spanning payloads well under the threshold to well over the cap
        int[] recordCounts = {5, 25, 50, 100, 400, 1000, 1700, 2200, 2600, 3200, 4000};
        for (int records : recordCounts) {
            var state = stateWithHoles(context -> null, records);
            String expected = todaysPayload(records);
            OffsetAndMetadata committed = state.createOffsetAndMetadata();

            boolean expectedStrip = expected.length() > cap;
            assertWithMessage("R2: with no rider the payload is byte for byte today's, over %s records", records)
                    .that(committed.metadata())
                    .isEqualTo(expectedStrip ? "" : expected);
            assertWithMessage("the engagement point must be today's: %s characters against a threshold of %s, "
                    + "over %s records", expected.length(), threshold, records)
                    .that(state.isAllowedMoreRecords())
                    .isEqualTo(expected.length() <= threshold);
        }
    }

    /**
     * KTD3: a zero-length envelope is the ladder's marker and nothing else may write one, or an embedder could
     * forge the signal that says "your rider was dropped". Every unhelpful thing a supplier can return has to
     * come out as a payload with no envelope at all.
     */
    @Test
    void noSupplierReturnValueProducesAnEnvelopeWithAZeroLengthRider() throws Exception {
        OffsetMapCodecManager.DefaultMaxMetadataSize = 4096;
        PartitionStateManager.setUSED_PAYLOAD_THRESHOLD_MULTIPLIER(USED_PAYLOAD_THRESHOLD_MULTIPLIER_DEFAULT);

        for (Function<RiderContext, byte[]> supplier : suppliers(null, new byte[0])) {
            var state = stateWithHoles(supplier, 5);
            OffsetAndMetadata committed = state.createOffsetAndMetadata();
            assertWithMessage("null and empty are normalised into no rider at all, above the envelope")
                    .that((int) decoded(committed)[0])
                    .isNotEqualTo((int) OffsetRiderEnvelope.MAGIC_BYTE);
        }

        // and the caught-up partition whose rider is over its cap writes nothing rather than a bare marker
        var caughtUp = caughtUpState(context -> riderOf(4000));
        assertThat(caughtUp.createOffsetAndMetadata().metadata()).isEmpty();
        assertThat(caughtUp.isAllowedMoreRecords()).isTrue();
    }

    /**
     * KTD6, restated here so the ladder cannot quietly take it away: the caught-up early return is the only place
     * a partition blocked by back pressure unblocks, and all three of its outcomes leave it unblocked.
     */
    @Test
    void aCaughtUpPartitionIsUnblockedWhateverItsRiderDoes() throws Exception {
        OffsetMapCodecManager.DefaultMaxMetadataSize = 4096;
        PartitionStateManager.setUSED_PAYLOAD_THRESHOLD_MULTIPLIER(USED_PAYLOAD_THRESHOLD_MULTIPLIER_DEFAULT);

        var noRider = caughtUpState(context -> null);
        assertThat(noRider.createOffsetAndMetadata().metadata()).isEmpty();
        assertThat(noRider.isAllowedMoreRecords()).isTrue();

        byte[] rider = riderOf(8);
        var withRider = caughtUpState(context -> rider);
        OffsetAndMetadata committed = withRider.createOffsetAndMetadata();
        assertThat(unwrap(committed).getRider().getBytes()).isEqualTo(rider);
        assertThat(unwrap(committed).getInnerBytes()).isEqualTo(new byte[0]);
        assertThat(withRider.isAllowedMoreRecords()).isTrue();

        var oversized = caughtUpState(context -> riderOf(4000));
        assertThat(oversized.createOffsetAndMetadata().metadata()).isEmpty();
        assertThat(oversized.isAllowedMoreRecords()).isTrue();
    }

    // ---- one encode, one snapshot ---------------------------------------------------------------------------

    /**
     * KTD9: the encoder competition runs exactly once per commit, whichever rung the ladder lands on. A ladder
     * that encoded per rung would snapshot a later hole map on the second pass - the confluentinc#894 tear class
     * - and double-count both encoding meters.
     */
    @Test
    void theEncoderRunsOncePerCommitWhicheverRungIsChosen() throws Exception {
        int innerBytes = measureInnerEncodingLength();
        PartitionStateManager.setUSED_PAYLOAD_THRESHOLD_MULTIPLIER(USED_PAYLOAD_THRESHOLD_MULTIPLIER_DEFAULT);

        // one cap per rung: the rider fits, only the marker fits, only the bare map fits, nothing fits
        int[] capPerRung = {
                base64Characters(HEADER + 32 + innerBytes),
                base64Characters(HEADER + innerBytes),
                base64Characters(innerBytes),
                base64Characters(innerBytes) - 4};
        byte[] rider = riderOf(32);

        for (int cap : capPerRung) {
            OffsetMapCodecManager.DefaultMaxMetadataSize = cap;
            var module = moduleWith(context -> rider);
            var state = stateWithHoles(module, RECORDS);
            var timer = module.pcMetrics().getTimerFromMetricDef(PCMetricsDef.OFFSETS_ENCODING_TIME);
            long before = timer.count();

            var ignoredCommitted = state.createOffsetAndMetadata(); // the payload is asserted by the rung tests

            assertWithMessage("KTD9: one encoder competition per commit, at a cap of %s", cap)
                    .that(timer.count() - before)
                    .isEqualTo(1);
        }
    }

    /**
     * The perturb-between-the-reads arm, the shape {@link PartitionStateCommitEncodeShift894Test} established:
     * a completion landing between the encoder's snapshot and the assemble must change neither the offset that is
     * committed nor the payload's base. The ladder repacks the bytes the snapshot produced, so it must be
     * incapable of noticing.
     */
    @Test
    void completionsLandingInsideTheCommitCycleChangeNeitherTheOffsetNorThePayload() throws Exception {
        OffsetMapCodecManager.DefaultMaxMetadataSize = 4096;
        PartitionStateManager.setUSED_PAYLOAD_THRESHOLD_MULTIPLIER(USED_PAYLOAD_THRESHOLD_MULTIPLIER_DEFAULT);
        byte[] rider = riderOf(16);

        var state = new RacingCommitCycleState(moduleWith(context -> rider), TP, HighestOffsetAndIncompletes.of());
        for (long offset = 0; offset <= 2; offset++) {
            state.addNewIncompleteRecord(record(offset));
        }
        state.onSuccess(0);
        state.onSuccess(2);
        state.armRaceOn(1);

        OffsetAndMetadata committed = state.createOffsetAndMetadata();

        assertWithMessage("precondition: the injected race must actually have fired, or this proves nothing")
                .that(state.raceHasFired())
                .isTrue();
        long encodeBase = state.firstOffsetToCommitRead();
        assertWithMessage("confluentinc#894: the committed offset is the decode base for the metadata stored with "
                + "it, and the rider must not have introduced a second read of it")
                .that(committed.offset())
                .isEqualTo(encodeBase);
        assertWithMessage("the payload still describes the state the snapshot saw")
                .that(OffsetMapCodecManager
                        .deserialiseIncompleteOffsetMapFromBase64(encodeBase, committed.metadata())
                        .getIncompleteOffsets())
                .containsExactly(1L);
        assertWithMessage("and the rider rode with it")
                .that(unwrap(committed).getRider().getBytes())
                .isEqualTo(rider);
    }

    // ---- fixtures -------------------------------------------------------------------------------------------

    /**
     * The Base64 closed form, written out here rather than borrowed from the production side: it is what the
     * ladder's predictions are checked <em>against</em>, so a shared implementation would agree with itself.
     * {@code OffsetSimpleSerialisation.base64} uses the padding encoder, for which this is exact.
     */
    private static int base64Characters(int rawBytes) {
        return 4 * ((rawBytes + 2) / 3);
    }

    private static int roundUpToFour(int characters) {
        return ((characters + 3) / 4) * 4;
    }

    /**
     * Rider bytes that do not compress, so a fixture's arithmetic about their length survives the outer codec.
     */
    private static byte[] riderOf(int length) {
        byte[] bytes = new byte[length];
        new Random(HOLE_SEED + length).nextBytes(bytes);
        return bytes;
    }

    private List<Function<RiderContext, byte[]>> suppliers(byte[]... returnValues) {
        List<Function<RiderContext, byte[]>> out = new ArrayList<>();
        for (byte[] value : returnValues) {
            out.add(context -> value);
        }
        return out;
    }

    private PCModuleTestEnv moduleWith(Function<RiderContext, byte[]> supplier) {
        return new PCModuleTestEnv(ParallelConsumerOptions.<String, String>builder()
                .riderSupplier(supplier)
                // its own registry, so the encoder-runs-once assertion counts this test's encodes and nobody else's
                .meterRegistry(new SimpleMeterRegistry())
                .build());
    }

    /**
     * A partition with {@link #RECORDS} offsets polled and a pseudorandom half of them still outstanding - an
     * ordinary out-of-order completion pattern, chosen over an alternating one because random holes do not
     * compress and so the encoded length stays a function of the range rather than of gzip.
     * <p>
     * Offset zero is always a hole, which pins the commit offset at zero and so the encoder's base; the top
     * offset always succeeds, which pins the range.
     */
    private PartitionState<String, String> stateWithHoles(Function<RiderContext, byte[]> supplier) {
        return stateWithHoles(supplier, RECORDS);
    }

    private PartitionState<String, String> stateWithHoles(Function<RiderContext, byte[]> supplier, int records) {
        return stateWithHoles(moduleWith(supplier), records);
    }

    private PartitionState<String, String> stateWithHoles(PCModuleTestEnv module, int records) {
        var state = new PartitionState<String, String>(0, module, TP, HighestOffsetAndIncompletes.of());
        populateWithHoles(state, records);
        return state;
    }

    private void populateWithHoles(PartitionState<String, String> state, int records) {
        var holes = new Random(HOLE_SEED);
        for (long offset = 0; offset < records; offset++) {
            state.addNewIncompleteRecord(record(offset));
        }
        for (long offset = 1; offset < records; offset++) {
            if (offset == records - 1 || holes.nextBoolean()) {
                state.onSuccess(offset);
            }
        }
    }

    private PartitionState<String, String> caughtUpState(Function<RiderContext, byte[]> supplier) {
        var state = new PartitionState<String, String>(0, moduleWith(supplier), TP, HighestOffsetAndIncompletes.of());
        state.addNewIncompleteRecord(record(0));
        state.onSuccess(0);
        return state;
    }

    /**
     * The encoded length of the fixture's hole map, measured by running the same encoder the commit will run,
     * against a throwaway state. Every cap in this class is derived from this number rather than hard-coded, so a
     * change to the encodings retunes the fixtures instead of silently reclassifying a scenario.
     */
    private int measureInnerEncodingLength() throws NoEncodingPossibleException {
        var module = moduleWith(context -> null);
        var state = stateWithHoles(module, RECORDS);
        byte[] inner = new OffsetMapCodecManager<String, String>(module).encodeOffsetsToInnerBytes(0, state);
        log.debug("Fixture hole map encodes to {} bytes ({} characters)", inner.length, base64Characters(inner.length));
        return inner.length;
    }

    /**
     * What this build writes for the same holes with no rider configured - the baseline the no-rider scenarios
     * compare against, produced through the codec's own no-rider entry point.
     */
    private String todaysPayload(int records) throws NoEncodingPossibleException {
        var module = moduleWith(context -> null);
        var state = stateWithHoles(module, records);
        return new OffsetMapCodecManager<String, String>(module).makeOffsetMetadataPayload(0, state);
    }

    private ConsumerRecord<String, String> record(long offset) {
        return new ConsumerRecord<>(TP.topic(), TP.partition(), offset, "key", "value");
    }

    private static byte[] decoded(OffsetAndMetadata committed) {
        return Base64.getDecoder().decode(committed.metadata());
    }

    private static OffsetRiderEnvelope.UnwrappedEnvelope unwrap(OffsetAndMetadata committed)
            throws CorruptOffsetMetadataException {
        return OffsetRiderEnvelope.unwrap(decoded(committed));
    }

    /**
     * What the committed string says about the rider slot - {@link RiderState#NONE} when there is no envelope at
     * all, which is both "never configured" and the ladder's bottom envelope rung.
     */
    private static RiderState riderStateOf(OffsetAndMetadata committed) throws CorruptOffsetMetadataException {
        byte[] raw = decoded(committed);
        if (raw.length == 0 || raw[0] != OffsetRiderEnvelope.MAGIC_BYTE) {
            return RiderState.NONE;
        }
        return OffsetRiderEnvelope.unwrap(raw).getRider().getState();
    }

    private static Iterable<Long> incompletesOf(OffsetAndMetadata committed) throws OffsetDecodingError {
        return OffsetMapCodecManager
                .deserialiseIncompleteOffsetMapFromBase64(committed.offset(), committed.metadata())
                .getIncompleteOffsets();
    }

}
