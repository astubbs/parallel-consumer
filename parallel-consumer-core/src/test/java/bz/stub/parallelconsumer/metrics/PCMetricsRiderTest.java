package bz.stub.parallelconsumer.metrics;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.RiderContext;
import bz.stub.parallelconsumer.internal.PCModuleTestEnv;
import bz.stub.parallelconsumer.offsets.NoEncodingPossibleException;
import bz.stub.parallelconsumer.offsets.OffsetEncoding;
import bz.stub.parallelconsumer.offsets.OffsetMapCodecManager;
import bz.stub.parallelconsumer.offsets.OffsetRiderEnvelope;
import bz.stub.parallelconsumer.offsets.RiderTestFixtures;
import bz.stub.parallelconsumer.state.PartitionState;
import bz.stub.parallelconsumer.state.PartitionStateManager;
import bz.stub.parallelconsumer.state.ShardManager;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
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
import org.mockito.Mockito;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static bz.stub.parallelconsumer.offsets.RiderTestFixtures.base64Characters;
import static bz.stub.parallelconsumer.offsets.RiderTestFixtures.moduleWith;
import static bz.stub.parallelconsumer.state.PartitionStateManager.USED_PAYLOAD_THRESHOLD_MULTIPLIER_DEFAULT;
import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;

/**
 * The rider's metrics (U5 of the opaque-rider plan): what an operator can see about a commit that carried a
 * rider, dropped one, or wrote no payload at all.
 * <p>
 * <b>Why every one of these is a counter and not a log line.</b> The rider is opaque to PC, so PC cannot tell
 * whether an embedder's feature is working - only whether the bytes it was handed reached the wire. Each of the
 * four series is one way they do not: shed for size by the ladder ({@link PCMetricsDef#OFFSETS_RIDER_DROPPED}),
 * stripped along with the offset map because the offset map alone would not fit
 * ({@link PCMetricsDef#OFFSETS_PAYLOAD_STRIPPED}), or never produced because the supplier threw
 * ({@link PCMetricsDef#OFFSETS_RIDER_SUPPLIER_FAILED}). The last of those is the feature's only health signal -
 * KTD8 makes a broken supplier silent by design, so a rider-based feature that has stopped working looks exactly
 * like one that was never configured.
 * <p>
 * <b>The two pre-existing ratios keep answering different questions</b> (KTD14).
 * {@link PCMetricsDef#PAYLOAD_RATIO_USED} is <em>density</em> - how many characters the offset map spends per
 * offset it describes - so it records the encoded offset map's own length and is unmoved by a rider.
 * {@link PCMetricsDef#METADATA_SPACE_USED} is <em>headroom</em> against the broker's metadata limit, so it
 * records the assembled string, rider included. A caught-up commit records neither: its offset range is zero or
 * negative, and Micrometer takes {@code -0.0} and {@code 0.0} as samples (a positive numerator over a zero range
 * is {@code Infinity}), so leaving the divisor unguarded would drag both distributions off their meaning on
 * every steady-state commit.
 * <p>
 * <b>Static state.</b> {@link OffsetMapCodecManager#DefaultMaxMetadataSize},
 * {@link PartitionStateManager#getUSED_PAYLOAD_THRESHOLD_MULTIPLIER()} and
 * {@link OffsetMapCodecManager#forcedCodec} are mutable statics shared with the offsets suites; the ladder
 * scenarios move the first two and the {@code NoEncodingPossibleException} scenario moves the third, so the lock
 * is taken in WRITE mode and all three are restored per test and again after the class.
 * <p>
 * <b>Meters are read out of a registry this class owns</b>, one per test, so a count is this test's commits and
 * nobody else's - the shape {@link PCMetrics859Test} uses for the confluentinc#859 leak regression, whose
 * assign/revoke assertion is mirrored at the bottom of this file for the four new meters.
 *
 * @author Antony Stubbs
 * @see PCMetrics859Test the meter-leak regression these four meters have to keep passing
 */
@Slf4j
@ResourceLock(value = OffsetMapCodecManager.METADATA_DATA_SIZE_RESOURCE_LOCK, mode = ResourceAccessMode.READ_WRITE)
class PCMetricsRiderTest {

    private static final TopicPartition TP = new TopicPartition("rider-metrics", 0);

    /**
     * The envelope's own cost in bytes, restated so a scenario's arithmetic reads without a lookup.
     */
    private static final int HEADER = OffsetRiderEnvelope.HEADER_BYTES;

    /**
     * Enough records for the offset map to encode to a couple of hundred bytes, so a cap derived from it leaves the
     * ladder's rungs distinguishable. Randomised holes, because an alternating pattern compresses to almost
     * nothing and the fixture would then be measuring gzip.
     */
    private static final int RECORDS = 1700;

    private static final long HOLE_SEED = 20260906L;

    /**
     * Over the rider cap the defaults derive - 4096 metadata characters at the 0.75 multiplier leaves 1024
     * characters, which is 768 raw bytes - and well under {@link OffsetRiderEnvelope#MAX_RIDER_BYTES}, so what
     * refuses it is the derived cap rather than the format's own ceiling.
     */
    private static final int OVERSIZED_RIDER_BYTES = 2000;

    private SimpleMeterRegistry registry;

    private RiderTestFixtures.MetadataSizeStatics realSizeStatics;

    @BeforeEach
    void setUp() {
        registry = new SimpleMeterRegistry();
        realSizeStatics = RiderTestFixtures.MetadataSizeStatics.remember();
        PartitionStateManager.setUSED_PAYLOAD_THRESHOLD_MULTIPLIER(USED_PAYLOAD_THRESHOLD_MULTIPLIER_DEFAULT);
    }

    @AfterEach
    void restoreStatics() {
        realSizeStatics.restore();
        OffsetMapCodecManager.forcedCodec = Optional.empty();
        registry.close();
    }

    @AfterAll
    static void restoreDefaults() {
        OffsetMapCodecManager.DefaultMaxMetadataSize = 4096;
        PartitionStateManager.setUSED_PAYLOAD_THRESHOLD_MULTIPLIER(USED_PAYLOAD_THRESHOLD_MULTIPLIER_DEFAULT);
        OffsetMapCodecManager.forcedCodec = Optional.empty();
    }

    // ---- the rider that gets written ------------------------------------------------------------------------

    /**
     * R20's first half: a rider that fits is measured, in the bytes the embedder handed over rather than the
     * characters they cost on the wire - bytes are the unit {@link RiderContext#getMaxRiderBytes()} gives the
     * supplier its budget in, so a size distribution in any other unit could not be compared against it.
     */
    @Test
    void aWrittenRiderIsRecordedAtItsByteLength() {
        byte[] rider = riderOf(16);

        var state = stateWithHoles(context -> rider, RECORDS);
        OffsetAndMetadata committed = commit(state);

        assertWithMessage("precondition: this scenario is about a rider that was actually written")
                .that(committed.metadata())
                .isNotEmpty();
        assertThat(summaryCount(PCMetricsDef.OFFSETS_RIDER_SIZE)).isEqualTo(1);
        assertThat(summaryTotal(PCMetricsDef.OFFSETS_RIDER_SIZE)).isEqualTo(16d);
        assertWithMessage("nothing was dropped, stripped or failed on a commit that carried its rider")
                .that(counterValue(PCMetricsDef.OFFSETS_RIDER_DROPPED))
                .isEqualTo(0d);
        assertThat(counterValue(PCMetricsDef.OFFSETS_PAYLOAD_STRIPPED)).isEqualTo(0d);
        assertThat(counterValue(PCMetricsDef.OFFSETS_RIDER_SUPPLIER_FAILED)).isEqualTo(0d);
    }

    // ---- the ladder's drops ---------------------------------------------------------------------------------

    /**
     * The ladder's first descent, counted: the offset map fits the cap with room for the drop marker but not with
     * the rider on top, so the rider is shed. One increment, and no size sample - the bytes never reached the
     * wire.
     */
    @Test
    void sheddingTheRiderForTheMarkerCountsOneDrop() throws Exception {
        int innerBytes = measureInnerEncodingLength();
        // exactly enough for the marker form and no more, which is the rung this scenario is about
        int cap = base64Characters(HEADER + innerBytes);
        OffsetMapCodecManager.DefaultMaxMetadataSize = cap;
        byte[] rider = riderOf(32);

        assertWithMessage("fixture: the rider form must not fit, or the ladder never descends")
                .that(base64Characters(HEADER + rider.length + innerBytes))
                .isGreaterThan(cap);

        var state = stateWithHoles(context -> rider, RECORDS);
        OffsetAndMetadata committed = commit(state);

        assertWithMessage("R9: the hole map is committed regardless - only the rider was shed")
                .that(committed.metadata())
                .isNotEmpty();
        assertThat(counterValue(PCMetricsDef.OFFSETS_RIDER_DROPPED)).isEqualTo(1d);
        assertWithMessage("a rider that never reached the wire is not a size sample")
                .that(summaryCount(PCMetricsDef.OFFSETS_RIDER_SIZE))
                .isEqualTo(0);
        assertThat(counterValue(PCMetricsDef.OFFSETS_PAYLOAD_STRIPPED)).isEqualTo(0d);
    }

    /**
     * The ladder's deepest envelope rung, and the double-count question KTD14 asks: the marker itself does not
     * fit, so the whole envelope goes. <b>The ladder takes exactly one rung per commit</b> - it chooses by
     * predicted length and jumps straight to the rung that fits - so a commit that loses its rider this way
     * passes through {@code shedEnvelopeForSize} and never through {@code shedRiderForSize}. The counter is
     * per-commit, so this must read one, not two.
     */
    @Test
    void sheddingTheWholeEnvelopeStillCountsExactlyOneDrop() throws Exception {
        int innerBytes = measureInnerEncodingLength();
        // exactly the bare form's length: the marker's three bytes are always four more characters than this
        int cap = base64Characters(innerBytes);
        OffsetMapCodecManager.DefaultMaxMetadataSize = cap;
        byte[] rider = riderOf(32);

        assertWithMessage("fixture: not even the marker form may fit, or this is the rung above")
                .that(base64Characters(HEADER + innerBytes))
                .isGreaterThan(cap);

        var state = stateWithHoles(context -> rider, RECORDS);
        OffsetAndMetadata committed = commit(state);

        assertWithMessage("fixture: the rung really was the one where the envelope goes - the committed string "
                + "is the bare hole map, not the marker form")
                .that(committed.metadata().length())
                .isEqualTo(base64Characters(innerBytes));
        assertWithMessage("KTD14: one drop per commit, however far down the ladder the commit fell")
                .that(counterValue(PCMetricsDef.OFFSETS_RIDER_DROPPED))
                .isEqualTo(1d);
        assertThat(counterValue(PCMetricsDef.OFFSETS_PAYLOAD_STRIPPED)).isEqualTo(0d);
        assertThat(summaryCount(PCMetricsDef.OFFSETS_RIDER_SIZE)).isEqualTo(0);
    }

    /**
     * The one commit that passes two shedding sites, and the reason the ladder's second descent counts
     * conditionally: a cap this tight leaves no allowance at all, so the write-time guard refuses the rider and
     * hands the ladder the drop marker - which then does not fit either, so the envelope goes as well. Two places
     * saw a loss; there was one rider, so the series must read one.
     * <p>
     * Counting unconditionally at both sites is the plausible implementation, and every other scenario here
     * passes with it.
     */
    @Test
    void aRiderRefusedByTheGuardAndThenByTheLadderIsCountedOnce() throws Exception {
        int innerBytes = measureInnerEncodingLength();
        int cap = base64Characters(innerBytes);
        OffsetMapCodecManager.DefaultMaxMetadataSize = cap;

        var state = stateWithHoles(context -> {
            assertWithMessage("fixture: the offset map has used the whole field, so there is no allowance left "
                    + "and whatever the supplier returns is over cap")
                    .that(context.getMaxRiderBytes())
                    .isEqualTo(0);
            return riderOf(32);
        }, RECORDS);
        OffsetAndMetadata committed = commit(state);

        assertWithMessage("fixture: and the marker did not fit either, so the envelope went as well")
                .that(committed.metadata().length())
                .isEqualTo(base64Characters(innerBytes));
        assertWithMessage("KTD14: one rider, one drop - the guard counted it, and the ladder must not count the "
                + "marker it was already turned into")
                .that(counterValue(PCMetricsDef.OFFSETS_RIDER_DROPPED))
                .isEqualTo(1d);
    }

    /**
     * The bottom of the ladder: the offset map alone is over the cap, so the payload is stripped and a bare offset
     * is committed. Two events on one commit, and they are separately counted - the rider was lost as well as the
     * offset map, and an operator reading only the stripped counter would not know a rider had been configured at
     * all.
     */
    @Test
    void strippingThePayloadCountsAStripAndTheDropThatWentWithIt() throws Exception {
        int innerBytes = measureInnerEncodingLength();
        // one Base64 quantum below the bare form: nothing on the ladder can fit
        int cap = base64Characters(innerBytes) - 4;
        OffsetMapCodecManager.DefaultMaxMetadataSize = cap;

        var withRider = stateWithHoles(context -> riderOf(32), RECORDS);
        OffsetAndMetadata committed = commit(withRider);

        assertWithMessage("fixture: the payload really was stripped")
                .that(committed.metadata())
                .isEmpty();
        assertThat(counterValue(PCMetricsDef.OFFSETS_PAYLOAD_STRIPPED)).isEqualTo(1d);
        assertWithMessage("the rider was lost too, and says so in its own series")
                .that(counterValue(PCMetricsDef.OFFSETS_RIDER_DROPPED))
                .isEqualTo(1d);
    }

    /**
     * The same strip with no rider configured: this is the pre-rider behaviour, and the drop counter has to stay
     * at zero for it or the series would report a loss that never happened.
     */
    @Test
    void aStripWithNoRiderConfiguredCountsNoDrop() throws Exception {
        int innerBytes = measureInnerEncodingLength();
        int cap = base64Characters(innerBytes) - 4;
        OffsetMapCodecManager.DefaultMaxMetadataSize = cap;

        var state = stateWithHoles(context -> null, RECORDS);
        OffsetAndMetadata committed = commit(state);

        assertThat(committed.metadata()).isEmpty();
        assertThat(counterValue(PCMetricsDef.OFFSETS_PAYLOAD_STRIPPED)).isEqualTo(1d);
        assertWithMessage("nothing was configured, so nothing was dropped")
                .that(counterValue(PCMetricsDef.OFFSETS_RIDER_DROPPED))
                .isEqualTo(0d);
    }

    // ---- the guard above the ladder -------------------------------------------------------------------------

    /**
     * AE9's counter half. KTD8 makes a throwing supplier silent - the commit proceeds with today's payload and
     * one rate-limited warning - so this counter is the only continuous signal that an embedder's feature has
     * stopped working. A throw is not a size sample either.
     */
    @Test
    void aThrowingSupplierIsCountedAndRecordsNoSize() {
        var state = stateWithHoles(context -> {
            throw new IllegalStateException("the embedder is half deployed");
        }, RECORDS);

        OffsetAndMetadata committed = commit(state);

        assertWithMessage("KTD8: the commit still happens - only the rider is lost")
                .that(committed.metadata())
                .isNotEmpty();
        assertThat(counterValue(PCMetricsDef.OFFSETS_RIDER_SUPPLIER_FAILED)).isEqualTo(1d);
        assertThat(summaryCount(PCMetricsDef.OFFSETS_RIDER_SIZE)).isEqualTo(0);
        assertWithMessage("a supplier that produced nothing dropped nothing - the two faults have different "
                + "fixes and must not share a series")
                .that(counterValue(PCMetricsDef.OFFSETS_RIDER_DROPPED))
                .isEqualTo(0d);
    }

    /**
     * R8: a rider over the cap {@link RiderContext#getMaxRiderBytes()} handed the supplier is refused at the
     * write side, before the ladder ever sees it. That is a dropped rider like any other - the embedder's bytes
     * did not reach the wire - so it lands in the same series, once.
     */
    @Test
    void anOversizedRiderCountsOneDrop() {
        OffsetMapCodecManager.DefaultMaxMetadataSize = 4096;

        var state = stateWithHoles(context -> riderOf(OVERSIZED_RIDER_BYTES), RECORDS);
        OffsetAndMetadata committed = commit(state);

        assertWithMessage("R9: the offset map is still committed")
                .that(committed.metadata())
                .isNotEmpty();
        assertThat(counterValue(PCMetricsDef.OFFSETS_RIDER_DROPPED)).isEqualTo(1d);
        assertThat(summaryCount(PCMetricsDef.OFFSETS_RIDER_SIZE)).isEqualTo(0);
        assertWithMessage("returning too much is not the same fault as throwing")
                .that(counterValue(PCMetricsDef.OFFSETS_RIDER_SUPPLIER_FAILED))
                .isEqualTo(0d);
    }

    // ---- the two ratios -------------------------------------------------------------------------------------

    /**
     * KTD14, the discriminating case: the same holes committed with and without a rider. Density is a property
     * of the offset map, so {@link PCMetricsDef#PAYLOAD_RATIO_USED} must record the <em>same</em> value both times;
     * headroom is a property of what goes to the broker, so {@link PCMetricsDef#METADATA_SPACE_USED} must record
     * the assembled string - a larger share of the cap when a rider rides along.
     * <p>
     * Recording the assembled string in both, which is what this build did before the rider existed, would make
     * the density series report an offset map that got denser because somebody configured a rider.
     */
    @Test
    void densityIgnoresTheRiderAndHeadroomIncludesIt() {
        OffsetMapCodecManager.DefaultMaxMetadataSize = 4096;
        var bareRegistry = new SimpleMeterRegistry();
        try {
            var bare = stateWithHoles(moduleWith(context -> null, bareRegistry), RECORDS);
            OffsetAndMetadata bareCommit = commit(bare);

            var withRider = stateWithHoles(context -> riderOf(64), RECORDS);
            OffsetAndMetadata riderCommit = commit(withRider);

            assertWithMessage("fixture: the rider must actually have cost characters, or this proves nothing")
                    .that(riderCommit.metadata().length())
                    .isGreaterThan(bareCommit.metadata().length());

            assertWithMessage("KTD14: density is the hole encoding's own length against the offsets it "
                    + "describes, so a rider cannot move it")
                    .that(summaryTotal(PCMetricsDef.PAYLOAD_RATIO_USED))
                    .isEqualTo(summaryTotal(bareRegistry, PCMetricsDef.PAYLOAD_RATIO_USED));

            assertWithMessage("KTD14: headroom is what actually goes to the broker, rider included")
                    .that(summaryTotal(PCMetricsDef.METADATA_SPACE_USED))
                    .isEqualTo(riderCommit.metadata().length() / (double) OffsetMapCodecManager.DefaultMaxMetadataSize);
            assertThat(summaryTotal(bareRegistry, PCMetricsDef.METADATA_SPACE_USED))
                    .isEqualTo(bareCommit.metadata().length() / (double) OffsetMapCodecManager.DefaultMaxMetadataSize);
        } finally {
            bareRegistry.close();
        }
    }

    /**
     * The steady-state path, which is the one that runs on every commit of a healthy consumer once a rider is
     * configured: a caught-up partition has no offset map, so it has no density and no headroom worth reporting,
     * and its offset range is zero or negative. Neither ratio may take a sample - not {@code 0.0}, not
     * {@code -0.0}, and above all not the {@code Infinity} a positive numerator over a zero range produces.
     * Micrometer drops {@code NaN} but records the other three, so its own sign check cannot be relied on.
     * <p>
     * The rider itself is still written and still measured: a caught-up commit is exactly the one a restart reads
     * back, so its size matters more than any other.
     */
    @Test
    void aCaughtUpCommitWithARiderRecordsNeitherRatio() {
        OffsetMapCodecManager.DefaultMaxMetadataSize = 4096;
        byte[] rider = riderOf(8);

        var state = caughtUpState(context -> rider);
        OffsetAndMetadata committed = commit(state);

        assertWithMessage("precondition: the rider rode alone on a caught-up commit")
                .that(committed.metadata())
                .isNotEmpty();
        assertThat(summaryCount(PCMetricsDef.OFFSETS_RIDER_SIZE)).isEqualTo(1);
        assertThat(summaryTotal(PCMetricsDef.OFFSETS_RIDER_SIZE)).isEqualTo(8d);

        assertWithMessage("KTD14: a caught-up commit is not a density sample")
                .that(summaryCount(PCMetricsDef.PAYLOAD_RATIO_USED))
                .isEqualTo(0);
        assertWithMessage("nor a headroom sample - and the guard is on the divisor, not on the result")
                .that(summaryCount(PCMetricsDef.METADATA_SPACE_USED))
                .isEqualTo(0);
        assertWithMessage("no Infinity, NaN or negative zero reached either distribution")
                .that(summaryTotal(PCMetricsDef.PAYLOAD_RATIO_USED))
                .isEqualTo(0d);
        assertThat(summaryTotal(PCMetricsDef.METADATA_SPACE_USED)).isEqualTo(0d);
    }

    // ---- the encoder that cannot encode ---------------------------------------------------------------------

    /**
     * KTD14's ordering claim, made falsifiable: when no encoding is possible the exception escapes the
     * inner-bytes step, which under KTD9 runs <em>before</em> the supplier is called. So there is no rider to
     * lose on that path and nothing to count as dropped - the commit is a stripped payload and nothing else.
     * <p>
     * The supplier's call count is asserted rather than inferred: it is the only thing that can tell a
     * deliberately-uncounted drop from an ordering that quietly changed.
     */
    @Test
    void noEncodingPossibleCountsAStripAndNeverADrop() {
        OffsetMapCodecManager.DefaultMaxMetadataSize = 4096;
        // an encoding that cannot describe this state: the v1 bitset's length field is a short, so it is not
        // among the encoders that ran for a range this wide, and forcing it is the fallback OffsetEncodingTests
        // provokes the same way
        OffsetMapCodecManager.forcedCodec = Optional.of(OffsetEncoding.BitSet);
        var supplierCalls = new AtomicInteger();

        var state = stateWithHoles(context -> {
            supplierCalls.incrementAndGet();
            return riderOf(16);
        }, 0);
        state.addNewIncompleteRecord(record(0));
        state.addNewIncompleteRecord(record(40_000));
        state.onSuccess(40_000);

        OffsetAndMetadata committed = commit(state);

        assertWithMessage("fixture: the forced encoding really was impossible")
                .that(committed.metadata())
                .isEmpty();
        assertThat(counterValue(PCMetricsDef.OFFSETS_PAYLOAD_STRIPPED)).isEqualTo(1d);
        assertWithMessage("KTD9: the supplier is called after the holes are encoded, so this path never reaches "
                + "it - there is no rider to drop and nothing to count")
                .that(supplierCalls.get())
                .isEqualTo(0);
        assertThat(counterValue(PCMetricsDef.OFFSETS_RIDER_DROPPED)).isEqualTo(0d);
        assertThat(counterValue(PCMetricsDef.OFFSETS_RIDER_SUPPLIER_FAILED)).isEqualTo(0d);
        assertThat(summaryCount(PCMetricsDef.OFFSETS_RIDER_SIZE)).isEqualTo(0);
    }

    // ---- the leak class -------------------------------------------------------------------------------------

    /**
     * The confluentinc#859 class, extended to the four new meters: a partition's meters are registered on
     * assignment and removed on revocation, so a consumer that rebalances for days must not accumulate them.
     * Four new per-partition series is four new ways to leak, and the removal has to go through the guarded path
     * for the reason
     * {@code docs/solutions/runtime-errors/a-throwing-meter-registry-kills-the-poll-thread-and-strands-close.md}
     * records - revocation runs on the broker-poll thread, where a throw stops every commit.
     * <p>
     * The steady state is taken after the first full cycle rather than before it, because the encoder's own
     * meters are registered lazily on the first commit and are not per-partition, so they are not the subject
     * here.
     */
    @Test
    void assignAndRevokeCyclesLeaveNoRiderMetersBehind() {
        OffsetMapCodecManager.DefaultMaxMetadataSize = 4096;
        var module = moduleWith(context -> riderOf(16), registry);
        @SuppressWarnings("unchecked")
        ShardManager<String, String> shardManager = Mockito.mock(ShardManager.class);

        long steadyState = -1;
        for (int cycle = 0; cycle < 3; cycle++) {
            var state = stateWithHoles(module, RECORDS);
            var ignoredCommit = commit(state); // the payload is asserted elsewhere; this cycle is about meters

            state.onPartitionsRemoved(shardManager);

            long registered = module.pcMetrics().registeredMeterCount();
            if (cycle == 0) {
                steadyState = registered;
            } else {
                assertWithMessage("confluentinc#859: the tracking set returns to its steady state on cycle %s",
                        cycle)
                        .that(registered)
                        .isEqualTo(steadyState);
            }
            assertWithMessage("and the partition's rider meters are gone from the registry itself, not merely "
                    + "untracked, on cycle %s", cycle)
                    .that(registry.find(PCMetricsDef.OFFSETS_RIDER_SIZE.getName()).summary())
                    .isNull();
            assertThat(registry.find(PCMetricsDef.OFFSETS_RIDER_DROPPED.getName()).counter()).isNull();
            assertThat(registry.find(PCMetricsDef.OFFSETS_PAYLOAD_STRIPPED.getName()).counter()).isNull();
            assertThat(registry.find(PCMetricsDef.OFFSETS_RIDER_SUPPLIER_FAILED.getName()).counter()).isNull();
        }
    }

    // ---- fixtures -------------------------------------------------------------------------------------------

    private static byte[] riderOf(int length) {
        return RiderTestFixtures.riderOf(HOLE_SEED, length);
    }

    /**
     * A partition with {@code records} offsets polled and a pseudorandom half of them still outstanding.
     *
     * @see RiderTestFixtures#stateWithHoles
     */
    private PartitionState<String, String> stateWithHoles(Function<RiderContext, byte[]> supplier, int records) {
        return stateWithHoles(moduleWith(supplier, registry), records);
    }

    private PartitionState<String, String> stateWithHoles(PCModuleTestEnv module, int records) {
        return RiderTestFixtures.stateWithHoles(module, TP, records, HOLE_SEED);
    }

    private PartitionState<String, String> caughtUpState(Function<RiderContext, byte[]> supplier) {
        return RiderTestFixtures.caughtUpState(moduleWith(supplier, registry), TP);
    }

    /**
     * The commit itself, through the public entry point the controller uses - {@code createOffsetAndMetadata} is
     * package-private to the state package and this test lives with the meters it asserts on.
     */
    private OffsetAndMetadata commit(PartitionState<String, String> state) {
        Optional<OffsetAndMetadata> committed = state.getCommitDataIfDirty();
        assertWithMessage("fixture: the partition must be dirty, or no commit happened and nothing was recorded")
                .that(committed.isPresent())
                .isTrue();
        return committed.get();
    }

    /**
     * The encoded length of the fixture's offset map, so a change to the encodings retunes every cap here instead
     * of silently reclassifying a scenario.
     */
    private int measureInnerEncodingLength() throws NoEncodingPossibleException {
        return RiderTestFixtures.measureInnerEncodingLength(TP, RECORDS, HOLE_SEED);
    }

    private ConsumerRecord<String, String> record(long offset) {
        return RiderTestFixtures.record(TP, offset);
    }

    private double counterValue(PCMetricsDef def) {
        Counter counter = registry.find(def.getName())
                .tags("topic", TP.topic(), "partition", String.valueOf(TP.partition()))
                .counter();
        return counter == null ? 0d : counter.count();
    }

    private long summaryCount(PCMetricsDef def) {
        DistributionSummary summary = findSummary(registry, def);
        return summary == null ? 0L : summary.count();
    }

    private double summaryTotal(PCMetricsDef def) {
        return summaryTotal(registry, def);
    }

    private double summaryTotal(MeterRegistry meterRegistry, PCMetricsDef def) {
        DistributionSummary summary = findSummary(meterRegistry, def);
        return summary == null ? 0d : summary.totalAmount();
    }

    private DistributionSummary findSummary(MeterRegistry meterRegistry, PCMetricsDef def) {
        return meterRegistry.find(def.getName()).summary();
    }

}
