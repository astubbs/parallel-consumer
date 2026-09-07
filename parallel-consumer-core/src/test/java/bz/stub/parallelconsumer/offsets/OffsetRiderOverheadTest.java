package bz.stub.parallelconsumer.offsets;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumer;
import bz.stub.parallelconsumer.RiderContext;
import bz.stub.parallelconsumer.internal.PCModuleTestEnv;
import bz.stub.parallelconsumer.offsets.OffsetMapCodecManager.HighestOffsetAndIncompletes;
import bz.stub.parallelconsumer.offsets.OffsetRiderEnvelope.Rider;
import bz.stub.parallelconsumer.offsets.OffsetRiderEnvelope.RiderState;
import bz.stub.parallelconsumer.state.PartitionState;
import bz.stub.parallelconsumer.state.PartitionStateManager;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceAccessMode;
import org.junit.jupiter.api.parallel.ResourceLock;

import java.util.Arrays;
import java.util.Base64;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.IntPredicate;

import static bz.stub.parallelconsumer.offsets.RiderTestFixtures.base64Characters;
import static bz.stub.parallelconsumer.offsets.RiderTestFixtures.moduleWith;
import static bz.stub.parallelconsumer.offsets.RiderTestFixtures.riderStateOf;
import static bz.stub.parallelconsumer.offsets.RiderTestFixtures.unwrapOrFail;
import static bz.stub.parallelconsumer.state.PartitionStateManager.USED_PAYLOAD_THRESHOLD_MULTIPLIER_DEFAULT;
import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;

/**
 * What the rider actually costs, measured (U6 of the opaque-rider plan): the overhead table over the whole rider
 * domain, and the two engagement points - back pressure and the metadata cap - located with a rider and without
 * one.
 * <p>
 * <b>Why a measurement rather than an argument.</b> The payload is a forever format: it lives in
 * {@code __consumer_offsets} and is read back by whichever group member takes the partition next, possibly a
 * different build, so a size claim about it cannot be revised later.
 * {@code docs/solutions/best-practices/benchmark-first-wire-format-decisions.md} is the method - settle a wire
 * format from measured strings, not from reasoning about them - and
 * {@code docs/solutions/logic-errors/boundary-claim-tested-only-on-friendly-samples.md} is why the first part of
 * this class walks its whole domain rather than sampling it: a boundary claim checked only on the inputs its
 * author had in mind is not a boundary claim.
 * <p>
 * <b>Base64 only, deliberately, and it says so.</b> Every closed form here is {@code 4*ceil(n/3)}, which is exact
 * for the padding encoder {@link OffsetSimpleSerialisation#base64} uses. astubbs/parallel-consumer#306 adds a Z85
 * outer codec chosen per payload from a 22-byte floor upward, at which point the assembled length stops being one
 * closed form and becomes a step function of the assembled payload - so this table gains a column there rather
 * than being rewritten. R10 is unaffected either way: it is scoped to the offset map encoding, which no outer codec can
 * see.
 * <p>
 * <b>Why {@code PartitionStateRiderBudgetTest}'s exhaustive check is not enough.</b> That one walks 0 to 600 raw
 * bytes and compares the ladder's closed form against {@code java.util.Base64} alone, which establishes that the
 * arithmetic is right about Base64. It says nothing about the <em>envelope</em>: that the header really is three
 * bytes on the wire, that {@link OffsetMapCodecManager#assembleMetadataPayload} adds nothing of its own, and that
 * the predicted length is the length of a string a commit actually produces. This class asserts the assembled
 * string, through the real assemble path, for every rider length the derived cap allows.
 * <p>
 * <b>What is asserted about codec selection, and what cannot be</b> (AE8, R10). The encoder competition orders
 * equal-sized candidates by nothing at all - {@code OffsetSimultaneousEncoder} feeds a {@code ConcurrentHashMap}
 * key set into a {@code TreeSet} ordered by encoded size - so when two encodings tie, which one survives varies
 * from call to call. {@code RiderSupplierGuardTest}'s {@code assertPayloadIsTodays} javadoc records the
 * measurement: the same three-offset state encoded as two different magic bytes on consecutive calls, both five
 * bytes. A magic byte is therefore not a legal expectation and this class never asserts on one. What R10
 * promises, and what the whole budget arithmetic rests on, is the <b>size</b>, which is deterministic - so the
 * AE8 arms compare the encoded offset map's byte length with and without a rider, both under a forced codec and under
 * the free competition.
 * <p>
 * <b>Wall-clock floor.</b> States are built from {@link HighestOffsetAndIncompletes} - the shape the production
 * decode path builds them in - rather than by adding a {@code ConsumerRecord} per offset, each corpus shape is one
 * seeded set the range sizes take prefixes of, and every encode is memoised per corpus point.
 * {@code RunLengthEncoderTest}'s rule is what is being kept: measure the encoding, never walk the offset space to
 * get to it.
 * <p>
 * <b>No type nests inside this class, and that is not a style choice.</b> The
 * {@code truth-generator-maven-plugin} generates a Truth subject for types it finds in the test tree, and for one
 * nested inside a test class it writes the outer class's name into an {@code import} as though it were a package -
 * which compiles on the build that generates it and fails on the next. A corpus shape is therefore a
 * {@code String} constant and a commit's result a {@link ParallelConsumer.Tuple}, neither of which the generator
 * has anything to say about.
 * <p>
 * <b>NOT excluded from the mutation lane, and the plan expected it to be.</b> U6's verification line says to
 * exclude this class the way astubbs/parallel-consumer#306 excludes its density benchmark
 * ({@code bin/ci-mutation-test.sh}, anchor {@code excludedTestClasses}). Two things argue the other way, and the
 * decision is recorded here rather than silently taken. The benchmark's exclusion buys a real thing - it asserts
 * aggregates and a committed report, so its kills are incidental, by way of a size number moving, and they hide
 * gaps instead of closing them. Every assertion here is an exact value: a string's length against a closed form,
 * a byte length against the encoder's own output, a rung against its prediction. A mutant this class kills is a
 * mutant a size claim about the wire format catches, which is the kind the lane exists for, so excluding it would
 * lower the score by removing real kills. And it costs seconds rather than the benchmark's minutes. The exclusion
 * is one line in {@code bin/ci-mutation-test.sh} plus the argv {@code bin/test-ci-mutation-test.sh} pins, if a
 * later measurement of the lane says otherwise.
 *
 * @author Antony Stubbs
 * @see OffsetRiderEnvelope
 */
@Slf4j
@ResourceLock(value = OffsetMapCodecManager.METADATA_DATA_SIZE_RESOURCE_LOCK, mode = ResourceAccessMode.READ_WRITE)
@ResourceLock(value = OffsetSimultaneousEncoder.COMPRESSION_FORCED_RESOURCE_LOCK, mode = ResourceAccessMode.READ_WRITE)
class OffsetRiderOverheadTest {

    private static final TopicPartition TP = new TopicPartition("rider-overhead", 0);

    /**
     * The envelope's own cost in bytes, restated so the arithmetic below reads without a lookup.
     */
    private static final int HEADER = OffsetRiderEnvelope.HEADER_BYTES;

    /**
     * The metadata limit every measurement here is taken at - the broker's documented default. A mutable static,
     * which is what the write-mode lock above is for.
     */
    private static final int CAP = 4096;

    /**
     * A rider the size the slot's first customer's will be - a stream time is a long - so the overhead measured is
     * the one an embedder actually pays rather than a worst case.
     */
    private static final int RIDER_BYTES = 8;

    /**
     * The most characters a rider of {@value #RIDER_BYTES} bytes can add to a payload: its whole envelope, encoded
     * on its own. The real overhead is that or one Base64 quantum less, depending on where the encoded offset map's
     * length sits modulo three, and this is the bound the cap-engagement arm asserts against.
     */
    private static final int RIDER_FOOTPRINT_CHARACTERS = base64Characters(HEADER + RIDER_BYTES);

    private static final long CORPUS_SEED = 20260907L;

    /**
     * The smallest and largest ranges the engagement-point search may answer with. The top stays below
     * {@code Short.MAX_VALUE} so that the v1 encodings remain expressible over the whole domain - past it they
     * deregister themselves and a forced run answers {@link NoEncodingPossibleException}, which would make a
     * search boundary mean two different things.
     */
    private static final int MIN_RANGE = 8;

    private static final int MAX_RANGE = 30_000;

    /**
     * What {@link #engagementRangeFor} answers when the whole domain fails the predicate: this encoding and shape
     * never reach that engagement point at any range a commit could hold.
     */
    private static final int NEVER_ENGAGES = -1;

    /**
     * How many evenly spaced points {@link #firstEngagedGridPoint} measures before believing the top of the domain:
     * 64 over 30,000 is a step of 468 ranges, which is wide enough to keep the fallback cheap and narrow enough
     * that a compressed length has to rise and fall again inside one step to hide from it.
     */
    private static final int NEVER_ENGAGES_GRID_STEPS = 64;

    /**
     * The corpus shapes, in the form {@code OffsetEncodingDensityBenchmarkTest} uses on
     * astubbs/parallel-consumer#306 so that the two describe the same world: uniform-random incompletes at a
     * couple of densities, bursts of a few slow keys, and nothing succeeded at all.
     */
    private static final String SHAPE_UNIFORM_ONE_PERCENT = "uniform-random 1%";

    private static final String SHAPE_UNIFORM_TWENTY_PERCENT = "uniform-random 20%";

    private static final String SHAPE_CLUSTERED_BURSTS = "clustered-bursts";

    private static final String SHAPE_ALL_INCOMPLETE = "all-incomplete";

    private static final String[] SHAPES = {
            SHAPE_UNIFORM_ONE_PERCENT,
            SHAPE_UNIFORM_TWENTY_PERCENT,
            SHAPE_CLUSTERED_BURSTS,
            SHAPE_ALL_INCOMPLETE,
    };

    /**
     * The incumbent encodings: every constant {@code OffsetEncodingTests} does not exclude. {@code ByteArray} and
     * its compressed twin have no encoder at all
     * ({@code docs/inflight/core-bytearray-encodings-have-no-codec.md}), the Kafka Streams pair is recognised
     * rather than written, and {@code RiderEnvelope} names no encoder - it is the wrapper whose cost this class is
     * measuring, put <em>around</em> whichever of these wins.
     */
    private static final OffsetEncoding[] INCUMBENTS = {
            OffsetEncoding.BitSet,
            OffsetEncoding.BitSetCompressed,
            OffsetEncoding.BitSetV2,
            OffsetEncoding.BitSetV2Compressed,
            OffsetEncoding.RunLength,
            OffsetEncoding.RunLengthCompressed,
            OffsetEncoding.RunLengthV2,
            OffsetEncoding.RunLengthV2Compressed,
    };

    /**
     * Each shape's incomplete offsets over the whole domain, generated once; a range size takes the prefix below
     * it.
     */
    private final Map<String, SortedSet<Long>> wholeDomainPerShape = new HashMap<>();

    /**
     * Every encode this class performs, keyed by the corpus point that produced it, so the three binary searches
     * over one shape share their probes instead of re-running the competition on the same offsets.
     */
    private final Map<OffsetEncoding, Map<String, Map<Integer, Integer>>> innerLengthCache =
            new EnumMap<>(OffsetEncoding.class);

    private RiderTestFixtures.MetadataSizeStatics realSizeStatics;

    private RiderTestFixtures.CodecForcingStatics realCodecStatics;

    @BeforeEach
    void rememberStatics() {
        realSizeStatics = RiderTestFixtures.MetadataSizeStatics.remember();
        realCodecStatics = RiderTestFixtures.CodecForcingStatics.remember();

        OffsetMapCodecManager.DefaultMaxMetadataSize = CAP;
        PartitionStateManager.setUSED_PAYLOAD_THRESHOLD_MULTIPLIER(USED_PAYLOAD_THRESHOLD_MULTIPLIER_DEFAULT);
    }

    @AfterEach
    void restoreStatics() {
        realSizeStatics.restore();
        realCodecStatics.restore();
    }

    @AfterAll
    static void restoreDefaults() {
        OffsetMapCodecManager.DefaultMaxMetadataSize = CAP;
        PartitionStateManager.setUSED_PAYLOAD_THRESHOLD_MULTIPLIER(USED_PAYLOAD_THRESHOLD_MULTIPLIER_DEFAULT);
        OffsetMapCodecManager.forcedCodec = Optional.empty();
        OffsetSimultaneousEncoder.compressionForced = false;
    }

    // ---- (a) the overhead table -------------------------------------------------------------------------------

    /**
     * The overhead table, over the whole rider domain rather than a sample of it: for every rider length the
     * derived cap allows, and for a spread of offset-map lengths including both ends, the length of the string
     * {@link OffsetMapCodecManager#assembleMetadataPayload} actually produces is
     * {@code 4*ceil((3 + rider + inner)/3)}.
     * <p>
     * A rider length of zero is the ladder's dropped marker rather than a rider - a zero-length rider is not a
     * representable value, which is what keeps the marker unforgeable from above - so the domain's bottom rung is
     * measured through {@link Rider#dropped()}, and it costs the same three header bytes.
     * <p>
     * The cap the domain runs to is not restated here: it is read off the {@link RiderContext} a real commit hands
     * a supplier, so this table cannot walk a different domain from the one an embedder is given. (The production
     * arithmetic behind it, {@code PartitionState.maxRiderBytes}, is package-private to
     * {@code bz.stub.parallelconsumer.state} and unreachable from this package; {@code
     * PartitionStateRiderBudgetTest} asserts it directly from there.)
     */
    @Test
    void theAssembledStringLengthIsTheClosedFormForEveryRiderLengthInTheDomain() {
        int riderCap = riderCapOfferedToASupplier();
        assertWithMessage("fixture: the derived cap must leave room for a rider at all, or this walks an empty "
                + "domain")
                .that(riderCap)
                .isGreaterThan(0);

        // the largest offset map encoding that still leaves room for a single rider byte, from the same
        // inversion of the closed form the production cap uses: n bytes cost 4*ceil(n/3) characters, so c
        // characters hold 3*floor(c/4) bytes
        int capacityInBytes = (CAP / 4) * 3;
        int largestInnerWithRoomForOneRiderByte = capacityInBytes - HEADER - 1;
        int[] innerLengths = {0, 1, 2, 3, 4, 100, largestInnerWithRoomForOneRiderByte};

        var om = new OffsetMapCodecManager<String, String>(moduleWith(null));
        byte[] riderPool = new byte[riderCap];
        new Random(CORPUS_SEED).nextBytes(riderPool);

        for (int innerLength : innerLengths) {
            byte[] inner = innerBytesOf(innerLength);

            assertWithMessage("R2: with no rider there is no envelope at all, so the assembled string is the hole "
                            + "encoding's own, at an inner length of %s", innerLength)
                    .that(om.assembleMetadataPayload(inner, Rider.none()).length())
                    .isEqualTo(base64Characters(innerLength));

            for (int riderLength = 0; riderLength <= riderCap; riderLength++) {
                Rider rider = riderLength == 0
                        ? Rider.dropped()
                        : Rider.present(Arrays.copyOf(riderPool, riderLength));

                assertWithMessage("the assembled envelope is 4*ceil((%s + %s + %s)/3) characters",
                                HEADER, riderLength, innerLength)
                        .that(om.assembleMetadataPayload(inner, rider).length())
                        .isEqualTo(base64Characters(HEADER + riderLength + innerLength));
            }
        }
    }

    // ---- (b) the engagement points ----------------------------------------------------------------------------

    /**
     * The finding this unit exists for, over every incumbent encoding and a corpus of offset-map shapes: the
     * <b>back-pressure engagement point does not move at all</b> when a rider is configured (R7, KTD4 - back
     * pressure is judged on the offset map encoding alone, because rider bytes do not shrink as work completes and
     * charging them there would be a floor the mechanism could never relieve), and the <b>cap engagement point
     * moves earlier by at most the rider's Base64 footprint</b>, never later.
     * <p>
     * Both points are located by binary search over the range size, which is the incompletes count for the
     * all-incomplete shape and proportional to it for the others - and then the two neighbouring corpus points are
     * committed for real, in both arms, so the located boundary is a measurement of the production path rather
     * than of this class's arithmetic.
     * <p>
     * <b>The bound is asserted in characters, not in incompletes.</b> A count is a property of the shape and the
     * encoding and would have to be re-derived whenever either moves; the character bound is the actual claim, and
     * it is exact: at the range where a rider first fails to fit, the <em>bare</em> offset map is already within one
     * rider footprint of the cap. Below that point the ladder sheds the rider rather than the offset map, which is
     * R9 and {@code PartitionStateRiderBudgetTest}'s subject.
     */
    @Test
    void theThresholdPointDoesNotMoveAndTheCapPointShiftsByAtMostTheRidersFootprint() {
        int combinationsMeasured = 0;
        int shiftsObserved = 0;

        for (OffsetEncoding encoding : INCUMBENTS) {
            for (String shape : SHAPES) {
                int capPointNoRider = engagementRangeFor(encoding, shape, inner -> base64Characters(inner) > CAP);
                int capPointWithRider = engagementRangeFor(encoding, shape, this::aRiderOfEightWouldNotFit);
                int thresholdPoint = engagementRangeFor(encoding, shape,
                        inner -> base64Characters(inner) > CAP * USED_PAYLOAD_THRESHOLD_MULTIPLIER_DEFAULT);

                String where = String.format(Locale.ROOT, "%s over %s", encoding, shape);
                log.debug("{}: back pressure engages at range {}, the cap at {} with an {}-byte rider and {} "
                                + "without one", where, thresholdPoint, capPointWithRider, RIDER_BYTES,
                        capPointNoRider);

                if (capPointNoRider != NEVER_ENGAGES) {
                    assertWithMessage("a rider cannot make a hole map that was already over the cap fit - %s",
                                    where)
                            .that(capPointWithRider)
                            .isNotEqualTo(NEVER_ENGAGES);
                    assertWithMessage("the shift is non-negative: a rider can only bring the cap forward, never "
                                    + "push it back - %s", where)
                            .that(capPointWithRider)
                            .isAtMost(capPointNoRider);
                }
                if (capPointWithRider != NEVER_ENGAGES) {
                    combinationsMeasured++;
                    int innerAtTheShiftedPoint = innerEncodingLength(encoding, shape, capPointWithRider);
                    assertWithMessage("the whole shift is the rider's Base64 footprint: where a rider first fails "
                                    + "to fit, the BARE hole map is already within %s characters of the cap - %s",
                                    RIDER_FOOTPRINT_CHARACTERS, where)
                            .that(base64Characters(innerAtTheShiftedPoint))
                            .isGreaterThan(CAP - RIDER_FOOTPRINT_CHARACTERS);
                    if (capPointWithRider != capPointNoRider) {
                        shiftsObserved++;
                    }
                }

                for (int rangeSize : neighbourhoodOf(thresholdPoint, capPointWithRider, capPointNoRider)) {
                    assertBothArmsAgreeAt(encoding, shape, rangeSize);
                }
            }
        }

        assertWithMessage("fixture: some encoding and shape must reach the cap at all, or the bound above is "
                + "vacuous")
                .that(combinationsMeasured)
                .isGreaterThan(0);
        assertWithMessage("fixture: and at least one must sit in the band where the RIDER is what crosses the "
                + "cap, or the shift was never exercised")
                .that(shiftsObserved)
                .isGreaterThan(0);
    }

    /**
     * One corpus point, committed for real in both arms: with an {@value #RIDER_BYTES}-byte rider configured and
     * with no supplier at all.
     * <p>
     * Three claims at once. <b>AE8/R10</b>: the encoded offset map's byte length is identical, so rider bytes changed
     * neither which encoding won nor whether it was compressed. <b>R7</b>: the partition's back-pressure state is
     * identical, and is what the encoded offset map's own length alone predicts. <b>R9</b>: whichever rung the ladder
     * landed on, the string is the predicted length, and a payload is stripped only when the offset map alone was
     * already over the cap.
     */
    private void assertBothArmsAgreeAt(OffsetEncoding encoding, String shape, int rangeSize) {
        int innerLength = innerEncodingLength(encoding, shape, rangeSize);
        byte[] rider = riderOf(RIDER_BYTES);

        ParallelConsumer.Tuple<String, Boolean> noRider = commit(encoding, shape, rangeSize, null);
        ParallelConsumer.Tuple<String, Boolean> withRider = commit(encoding, shape, rangeSize, context -> rider);

        String where = String.format(Locale.ROOT, "%s over %s at range %s (hole map %s bytes)",
                encoding, shape, rangeSize, innerLength);

        assertWithMessage("AE8/R10: the winning encoding's SIZE is identical with and without a rider - %s", where)
                .that(innerLengthOf(withRider.getLeft()))
                .isEqualTo(innerLengthOf(noRider.getLeft()));

        assertWithMessage("R7/KTD4: back pressure engages at the same corpus point with a rider as without one - "
                        + "%s", where)
                .that(withRider.getRight())
                .isEqualTo(noRider.getRight());
        assertWithMessage("and it is the hole encoding's own length that decided it, never the assembled string - "
                        + "%s", where)
                .that(noRider.getRight())
                .isEqualTo(base64Characters(innerLength) > CAP * USED_PAYLOAD_THRESHOLD_MULTIPLIER_DEFAULT);

        if (base64Characters(innerLength) > CAP) {
            assertWithMessage("R9: the hole map alone is over the cap, so it is stripped and the rider was never "
                            + "the reason - %s", where)
                    .that(withRider.getLeft())
                    .isEmpty();
            assertThat(noRider.getLeft()).isEmpty();
            return;
        }

        assertWithMessage("with no rider the string is the hole encoding's own, byte for byte today's - %s", where)
                .that(noRider.getLeft().length())
                .isEqualTo(base64Characters(innerLength));
        assertWithMessage("and the hole encoding read back out of the committed string is the one the encoder "
                        + "produced - %s", where)
                .that(innerLengthOf(noRider.getLeft()))
                .isEqualTo(innerLength);

        if (aRiderOfEightWouldNotFit(innerLength)) {
            assertWithMessage("R6/R9: the rider is shed but the hole map is not, so the payload is the marker "
                            + "form or the bare form - never empty - %s", where)
                    .that(withRider.getLeft())
                    .isNotEmpty();
            assertWithMessage("and whichever of the two it is, it never costs more than the marker form - %s",
                            where)
                    .that(withRider.getLeft().length())
                    .isAtMost(base64Characters(HEADER + innerLength));
        } else {
            assertWithMessage("the rider rides, and the string is exactly the predicted length - %s", where)
                    .that(withRider.getLeft().length())
                    .isEqualTo(base64Characters(HEADER + RIDER_BYTES + innerLength));
            assertThat(riderStateOf(withRider.getLeft())).isEqualTo(RiderState.PRESENT);
        }
    }

    /**
     * Whether an {@value #RIDER_BYTES}-byte rider fails to reach the wire beside an offset map encoding of this many
     * bytes - which is the cap engaging on the rider, by either of the two mechanisms that can refuse it: the
     * write-time guard's derived budget, and the ladder's own arithmetic. The two answer at the same byte, which
     * is KTD4's claim that the guard's cap and the ladder's rungs are the same arithmetic seen from two sides.
     */
    private boolean aRiderOfEightWouldNotFit(int innerLength) {
        return riderBudgetFor(innerLength) < RIDER_BYTES
                || base64Characters(HEADER + RIDER_BYTES + innerLength) > CAP;
    }

    // ---- (c) AE8 under the free competition -------------------------------------------------------------------

    /**
     * AE8 again, without a forced codec: when {@link OffsetSimultaneousEncoder} is left to choose for itself, the
     * <b>size</b> of the encoding it ships is identical with and without a rider, over the whole corpus. That is
     * the structural claim KTD5 makes - the envelope lives outside the encoder, which never sees a rider - stated
     * from the outside.
     * <p>
     * Size, not identity: equal-sized candidates tie and the tie is broken by iteration order, so the magic byte
     * is not stable from one call to the next (the class javadoc records the measurement). Asserting on it would
     * be a flake rather than a stronger test.
     */
    @Test
    void theFreeCompetitionShipsTheSameSizedEncodingWithOrWithoutARider() {
        byte[] rider = riderOf(RIDER_BYTES);

        for (String shape : SHAPES) {
            for (int rangeSize : new int[]{MIN_RANGE, 200, 3_200, 12_800}) {
                ParallelConsumer.Tuple<String, Boolean> noRider = commit(null, shape, rangeSize, null);
                ParallelConsumer.Tuple<String, Boolean> withRider = commit(null, shape, rangeSize,
                        context -> rider);

                String where = String.format(Locale.ROOT, "%s at range %s", shape, rangeSize);
                int shipped = innerLengthOf(noRider.getLeft());
                assertWithMessage("AE8/R10: rider bytes change neither the winning encoding's size nor whether "
                                + "it was compressed - %s", where)
                        .that(innerLengthOf(withRider.getLeft()))
                        .isEqualTo(shipped);
                assertWithMessage("and the envelope is the only difference between the two strings - %s", where)
                        .that(withRider.getLeft().length())
                        .isEqualTo(base64Characters(HEADER + RIDER_BYTES + shipped));
            }
        }
    }

    // ---- the engagement-point search --------------------------------------------------------------------------

    /**
     * A range size at which {@code engaged} holds of the encoded offset map's byte length and at which the range one
     * below does not - the engagement point - by binary search over {@link #MIN_RANGE}..{@link #MAX_RANGE}.
     * <p>
     * <b>It is a boundary by construction rather than by assuming the encoded length rises with the range.</b> The
     * loop's invariant is that the predicate is false at {@code below} and true at {@code atOrAbove}, so whatever
     * the shape of the encoding the pair it converges on is adjacent and differs - which is what the two
     * assertions record. A compressed encoding's length is not monotone in the range, and this must not need it to
     * be.
     * <p>
     * <b>Why comparing two of these answers is still sound.</b> The rider's predicate is a strict superset of the
     * no-rider one (an offset map over the cap is over it with a rider on top too), and both searches walk the
     * identical probe sequence until they first disagree - at which point the rider's takes the left half and the
     * other the right. So the rider's engagement point is never above the no-rider one, however the encoded length
     * behaves in between.
     *
     * <b>What the top of the domain cannot tell you.</b> The predicate being false at {@link #MAX_RANGE} does not
     * mean it is false everywhere below: a compressed length can rise over an interior range and fall again by the
     * top, and inferring "never engages" from the endpoint alone would skip every boundary assertion for exactly
     * the shape this test exists to measure. So when the top says no, the search walks a fixed grid of
     * {@link #NEVER_ENGAGES_GRID_STEPS} points across the domain first; the first grid point where the predicate
     * holds becomes the upper end of the search, and only a grid with no engaged point at all answers
     * {@link #NEVER_ENGAGES}. The residual is a transition narrower than one grid step that reverses before the
     * next point - bounded and named, rather than the whole interior being unobserved (Codex review on
     * astubbs#460).
     *
     * @return {@link #NEVER_ENGAGES} when the predicate is false at the top of the domain and at every grid point
     */
    private int engagementRangeFor(OffsetEncoding encoding, String shape, IntPredicate engaged) {
        int atOrAbove = MAX_RANGE;
        if (!engaged.test(innerEncodingLength(encoding, shape, MAX_RANGE))) {
            atOrAbove = firstEngagedGridPoint(encoding, shape, engaged);
            if (atOrAbove == NEVER_ENGAGES) {
                return NEVER_ENGAGES;
            }
        }
        if (engaged.test(innerEncodingLength(encoding, shape, MIN_RANGE))) {
            return MIN_RANGE;
        }

        int below = MIN_RANGE;
        while (atOrAbove - below > 1) {
            int middle = below + (atOrAbove - below) / 2;
            if (engaged.test(innerEncodingLength(encoding, shape, middle))) {
                atOrAbove = middle;
            } else {
                below = middle;
            }
        }

        assertWithMessage("the answer must be a boundary: %s over %s engages at range %s",
                        encoding, shape, atOrAbove)
                .that(engaged.test(innerEncodingLength(encoding, shape, atOrAbove)))
                .isTrue();
        assertWithMessage("and must not engage one range below it: %s over %s at range %s", encoding, shape, below)
                .that(engaged.test(innerEncodingLength(encoding, shape, below)))
                .isFalse();
        return atOrAbove;
    }

    /**
     * The grid {@link #engagementRangeFor} falls back to when the top of the domain is not engaged: evenly spaced
     * range sizes from {@link #MIN_RANGE} up to but excluding {@link #MAX_RANGE} (already measured by the caller),
     * walked upward so the answer is the lowest engaged point and the binary search that follows has a true
     * boundary below it.
     *
     * @return the first grid point at which {@code engaged} holds, or {@link #NEVER_ENGAGES}
     */
    private int firstEngagedGridPoint(OffsetEncoding encoding, String shape, IntPredicate engaged) {
        int step = Math.max(1, (MAX_RANGE - MIN_RANGE) / NEVER_ENGAGES_GRID_STEPS);
        for (int point = MIN_RANGE; point < MAX_RANGE; point += step) {
            if (engaged.test(innerEncodingLength(encoding, shape, point))) {
                return point;
            }
        }
        return NEVER_ENGAGES;
    }

    /**
     * The corpus points worth committing for real: each located engagement point and the range immediately below
     * it, plus the bottom of the domain as a control arm well under every limit. Deduplicated and ordered, and
     * points the search never reached are simply absent.
     */
    private static Set<Integer> neighbourhoodOf(int... engagementPoints) {
        var points = new LinkedHashSet<Integer>();
        // the set is the deduplication: two searches converging on the same range, or a point next to the control
        // arm, are one commit to make, so an add that finds its point already there is the expected case
        boolean ignoredControlArmWasNew = points.add(MIN_RANGE);
        for (int point : engagementPoints) {
            if (point != NEVER_ENGAGES) {
                boolean ignoredPointWasNew = points.add(point);
                if (point - 1 >= MIN_RANGE) {
                    boolean ignoredNeighbourWasNew = points.add(point - 1);
                }
            }
        }
        return points;
    }

    // ---- the corpus -------------------------------------------------------------------------------------------

    /**
     * A shape's incomplete offsets over a range, as the prefix below {@code rangeSize} of one set generated over
     * the whole domain from a seeded {@link Random}.
     * <p>
     * A prefix of a uniform sample is a uniform sample of the same density, and a prefix of a burst corpus is a
     * burst corpus, so nothing about a shape changes with the range - which is what lets the binary search above
     * probe a hundred range sizes without generating a hundred corpora. It also makes the encoded length rise with
     * the range for the uncompressed encodings, because a prefix's run structure is untouched by what follows it.
     * <p>
     * Offset zero is always incomplete, which pins the commit offset - and so the encoder's base - at zero, the
     * way {@code PartitionStateRiderBudgetTest}'s fixture does.
     */
    private SortedSet<Long> incompletesOver(String shape, int rangeSize) {
        return wholeDomainPerShape
                .computeIfAbsent(shape, OffsetRiderOverheadTest::generateWholeDomain)
                .headSet((long) rangeSize);
    }

    private static SortedSet<Long> generateWholeDomain(String shape) {
        var random = new Random(CORPUS_SEED * 31 + Arrays.asList(SHAPES).indexOf(shape) * 1_000_003L);
        var out = new TreeSet<Long>();
        if (SHAPE_ALL_INCOMPLETE.equals(shape)) {
            // nothing succeeded yet - the cheapest possible run-length encoding and the most expensive bitset
            for (int offset = 0; offset < MAX_RANGE; offset++) {
                boolean ignoredWasNew = out.add((long) offset); // a counting loop never repeats an offset
            }
        } else if (SHAPE_CLUSTERED_BURSTS.equals(shape)) {
            // bursts of ten to a hundred: a few slow keys, or a poison-pill cluster
            int bursts = Math.max(1, MAX_RANGE / 1_000);
            for (int burst = 0; burst < bursts; burst++) {
                int length = 10 + random.nextInt(91); // 10..100 inclusive
                int start = random.nextInt(MAX_RANGE);
                for (int offset = start; offset < Math.min(start + length, MAX_RANGE); offset++) {
                    boolean ignoredWasNew = out.add((long) offset); // bursts may overlap: an offset in two is one hole
                }
            }
        } else {
            // a scatter of slow records: light enough to be an ordinary steady state, or heavy enough to make
            // run-length pay for every flip
            double density = SHAPE_UNIFORM_TWENTY_PERCENT.equals(shape) ? 0.20d : 0.01d;
            int target = Math.max(1, (int) Math.round(MAX_RANGE * density));
            while (out.size() < target) {
                // the loop condition is the size, so a repeat draw simply costs another draw
                boolean ignoredWasNew = out.add((long) random.nextInt(MAX_RANGE));
            }
        }
        // every shape keeps offset zero incomplete, and the all-incomplete and scatter shapes may have put it there
        boolean ignoredZeroWasNew = out.add(0L);
        return out;
    }

    // ---- fixtures ---------------------------------------------------------------------------------------------

    /**
     * One real commit through {@link PartitionState}.
     *
     * @param encoding the encoding to force, so a shape is measured against a named incumbent rather than against
     *                 whichever candidate happened to win the tie; {@code null} leaves the competition free
     * @param supplier the rider supplier, or {@code null} for a commit with no rider configured at all - which is
     *                 the byte-identical-to-today path, not an envelope with nothing in it
     * @return the committed metadata string, and whether the partition came out blocked
     */
    private ParallelConsumer.Tuple<String, Boolean> commit(OffsetEncoding encoding, String shape, int rangeSize,
                                                           Function<RiderContext, byte[]> supplier) {
        return commitWith(encoding, shape, rangeSize, moduleWith(supplier));
    }

    /**
     * The back-pressure flag is a {@code @Getter(PACKAGE)} on {@link PartitionState}, so it is read here through
     * the public {@code isBlocked()} convenience rather than through the getter - and read while the state that
     * produced the commit is still in scope, which is why the payload and the flag come back together.
     */
    private ParallelConsumer.Tuple<String, Boolean> commitWith(OffsetEncoding encoding, String shape,
                                                               int rangeSize, PCModuleTestEnv module) {
        forceCodec(encoding);
        try {
            PartitionState<String, String> state = stateOver(module, shape, rangeSize);
            Optional<OffsetAndMetadata> committed = state.getCommitDataIfDirty();
            assertWithMessage("precondition: the state must be dirty, or nothing was measured")
                    .that(committed.isPresent())
                    .isTrue();
            String metadata = committed.get().metadata() == null ? "" : committed.get().metadata();
            return ParallelConsumer.Tuple.pairOf(metadata, state.isBlocked());
        } finally {
            forceCodec(null);
        }
    }

    /**
     * The encoded offset map's own byte length, read back out of a committed string - through the envelope when there
     * is one.
     *
     * @return negative when nothing was committed, so there is no string to read it from
     */
    private static int innerLengthOf(String metadata) {
        if (metadata.isEmpty()) {
            return -1;
        }
        byte[] raw = Base64.getDecoder().decode(metadata);
        if (raw.length > 0 && raw[0] == OffsetRiderEnvelope.MAGIC_BYTE) {
            return unwrapOrFail(raw).getInnerBytes().length;
        }
        return raw.length;
    }

    /**
     * The encoded offset map's byte length for one corpus point, memoised - the three binary searches over a shape
     * revisit each other's probes, and an encode is the expensive thing this class does.
     */
    private int innerEncodingLength(OffsetEncoding encoding, String shape, int rangeSize) {
        Map<Integer, Integer> perRange = innerLengthCache
                .computeIfAbsent(encoding, ignoredKey -> new HashMap<>())
                .computeIfAbsent(shape, ignoredKey -> new HashMap<>());
        Integer cached = perRange.get(rangeSize);
        if (cached != null) {
            return cached;
        }

        var module = moduleWith(null);
        forceCodec(encoding);
        try {
            PartitionState<String, String> state = stateOver(module, shape, rangeSize);
            // every shape keeps offset zero incomplete, so the frontier - and the encoder's base - is zero;
            // PartitionState#getOffsetToCommit is protected and not reachable from this package
            int length = new OffsetMapCodecManager<String, String>(module)
                    .encodeOffsetsToInnerBytes(0, state)
                    .length;
            Integer ignoredPreviousLength = perRange.put(rangeSize, length); // a re-measurement of one point is the
            // same encode of the same corpus, so the previous value is the same value
            return length;
        } catch (NoEncodingPossibleException notExpressible) {
            // MAX_RANGE is deliberately under Short.MAX_VALUE so that no incumbent deregisters itself over this
            // domain: if one does, the engagement points would mean two different things and the measurement is
            // not safe to report
            throw new AssertionError(String.format(Locale.ROOT,
                    "%s cannot express a range of %s, so this corpus no longer measures what it says it does",
                    encoding, rangeSize), notExpressible);
        } finally {
            forceCodec(null);
        }
    }

    /**
     * The compressed twins are only registered when nothing is small enough <em>or</em> compression is forced, so
     * a measurement of one has to ask for them; forcing it for the others would only cost wall clock.
     *
     * @param encoding the encoding to force, or {@code null} to leave the competition free
     */
    private static void forceCodec(OffsetEncoding encoding) {
        OffsetMapCodecManager.forcedCodec = Optional.ofNullable(encoding);
        OffsetSimultaneousEncoder.compressionForced = encoding != null && encoding.name().endsWith("Compressed");
    }

    /**
     * A partition holding exactly the shape's incompletes, built the way the production decode path builds one
     * rather than by adding a record per offset - which is what keeps a 30,000-offset corpus point cheap enough to
     * live in a unit suite.
     * <p>
     * The top of the range is seeded as incomplete and then completed, which is what makes the state dirty and so
     * commit-eligible; it leaves the incomplete set exactly as the shape described it.
     */
    private PartitionState<String, String> stateOver(PCModuleTestEnv module, String shape, int rangeSize) {
        long top = rangeSize;
        var seeded = new TreeSet<Long>(incompletesOver(shape, rangeSize));
        boolean addedTheTop = seeded.add(top);
        assertWithMessage("fixture: the top of the range is outside the shape, so it is ours to complete")
                .that(addedTheTop)
                .isTrue();

        var state = new PartitionState<String, String>(0, module, TP, HighestOffsetAndIncompletes.of(top, seeded));
        state.onSuccess(top);
        return state;
    }

    /**
     * The rider budget a commit with an offset map encoding of {@code innerLength} bytes offers its supplier, restated
     * from KTD4's two limits - the quarter of the field back pressure never uses, and what is actually left once
     * the offset map and the envelope's header are accounted for.
     * <p>
     * Restated rather than called: {@code PartitionState.maxRiderBytes} is package-private to
     * {@code bz.stub.parallelconsumer.state}. {@link #theSupplierIsToldTheBudgetThisTestPredicts()} asserts this
     * restatement against the number a real commit hands a supplier, so the two cannot drift apart silently.
     */
    private static int riderBudgetFor(int innerLength) {
        int riderCapInCharacters = (int) Math.floor(CAP * (1 - USED_PAYLOAD_THRESHOLD_MULTIPLIER_DEFAULT));
        int riderCap = (riderCapInCharacters / 4) * 3;
        int remaining = (CAP / 4) * 3 - HEADER - innerLength;
        return Math.max(0, Math.min(Math.min(riderCap, remaining), OffsetRiderEnvelope.MAX_RIDER_BYTES));
    }

    /**
     * The guard on the restatement above: what a real commit tells a supplier it may return, at a spread of hole
     * encoding lengths, is what {@link #riderBudgetFor} says. Without this the cap-engagement arm could be
     * measuring a budget the production path does not use.
     */
    @Test
    void theSupplierIsToldTheBudgetThisTestPredicts() {
        for (int rangeSize : new int[]{200, 3_200, 12_800}) {
            int innerLength = innerEncodingLength(OffsetEncoding.BitSetV2, SHAPE_UNIFORM_TWENTY_PERCENT, rangeSize);

            var offered = new AtomicInteger(-1);
            var module = moduleWith(context -> {
                offered.set(context.getMaxRiderBytes());
                return null;
            });
            var ignoredCommit = commitWith(OffsetEncoding.BitSetV2, SHAPE_UNIFORM_TWENTY_PERCENT, rangeSize,
                    module); // the payload is asserted by the engagement-point test

            assertWithMessage("the budget a supplier is offered beside a %s-byte hole map", innerLength)
                    .that(offered.get())
                    .isEqualTo(riderBudgetFor(innerLength));
        }
    }

    /**
     * The rider budget a caught-up commit offers, which is the derived cap with no offset map encoding to share the
     * field with - the top of the domain the overhead table walks. Read off a real {@link RiderContext} rather
     * than restated, so the table's domain is the budget an embedder is actually given.
     */
    private int riderCapOfferedToASupplier() {
        var offered = new AtomicInteger(-1);
        var module = moduleWith(context -> {
            offered.set(context.getMaxRiderBytes());
            return null;
        });
        var state = RiderTestFixtures.caughtUpState(module, TP);

        // a caught-up commit: no offset map to write, but the supplier still runs - that commit is the one a
        // restart reads back
        var ignoredCommit = state.getCommitDataIfDirty();
        assertWithMessage("precondition: the supplier must have been asked, or there is no measured cap")
                .that(offered.get())
                .isAtLeast(0);
        assertWithMessage("and it must agree with the restated arithmetic for an absent hole map")
                .that(offered.get())
                .isEqualTo(riderBudgetFor(0));
        return offered.get();
    }

    private static byte[] riderOf(int length) {
        return RiderTestFixtures.riderOf(CORPUS_SEED, length);
    }

    /**
     * Synthetic offset-map bytes for the overhead table: the content is irrelevant, because Base64's length
     * depends only on the byte count - but the first byte may not be the envelope's own, which never nests.
     */
    private static byte[] innerBytesOf(int length) {
        byte[] inner = new byte[length];
        if (length > 0) {
            inner[0] = OffsetEncoding.BitSetV2.magicByte;
        }
        return inner;
    }

}
