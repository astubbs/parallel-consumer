package bz.stub.parallelconsumer.state;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.internal.PCModuleTestEnv;
import bz.stub.parallelconsumer.offsets.OffsetMapCodecManager;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.jetbrains.lincheck.datastructures.IntGen;
import org.jetbrains.lincheck.datastructures.Operation;
import org.jetbrains.lincheck.datastructures.Param;
import org.jetbrains.lincheck.datastructures.StressOptions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static bz.stub.parallelconsumer.offsets.OffsetMapCodecManager.HighestOffsetAndIncompletes;
import static com.google.common.truth.Truth.assertThat;

/**
 * CALIBRATION for the two tears that live in the commit path: the confluentinc#894 two-read of the
 * offset-to-commit in {@code createOffsetAndMetadata}, and the encoder's snapshot/range tear in
 * {@code OffsetMapCodecManager.encodeOffsetsCompressed}. Both are reached through one operation here -
 * {@link #commit()} - because in production they are one call.
 * <p>
 * <b>The oracle is the reader's view, not the writer's.</b> {@link #commit()} does not return the
 * {@link OffsetAndMetadata} it produced; it returns what a broker-side consumer would <em>reconstruct</em> from
 * it - the committed offset, plus the incomplete offsets obtained by decoding the payload against that same
 * committed offset, which is what the real bootstrap path does. That is deliberate. Comparing raw payload
 * strings would flag any encoding difference; decoding first states the actual correctness property, so a
 * violation reads as "this commit tells the broker to replay the wrong records" rather than "these two base64
 * blobs differ". Both known tears corrupt exactly this value:
 * <ul>
 *   <li>the offset-to-commit tear ships a payload encoded relative to base B with the committed offset B+n, so
 *       every decoded incomplete is shifted;</li>
 *   <li>the encoder tear filters the incompletes against one read of {@code offsetHighestSucceeded} and sizes
 *       the encoded range from a second, so an offset between the two reads vanishes from the payload while
 *       remaining inside its range - and decodes as complete.</li>
 * </ul>
 * Nothing here names either seam. The operations are "a record succeeded" and "collect commit data", which is
 * all the two real threads do, and the verifier is Lincheck's default linearizability check: a returned value
 * that no sequential order of the same operations could have produced.
 * <p>
 * <b>Expected on this tree</b>: a violation, because master carries both defects unfixed. When they are fixed
 * these tests go red and must be inverted - astubbs#337 carries the offset-to-commit fix and astubbs#344
 * the encoder one; see {@link ShardManagerLincheckTest} for the same note.
 * <p>
 * <b>STRESS only, and this is the PoC's most expensive negative result.</b> The model checker is the strategy
 * that can put a thread switch between two named reads, and on one run it did exactly that here. It is not in
 * this lane because it could not be made to do so repeatably at any affordable bound: the commit path spends
 * its budget on "Cannot reproduce the interleaving", because replaying an interleaving requires the code under
 * test to be deterministic and this path is not - micrometer meter updates on every encode, and two
 * {@code PartitionState} accessors built on {@code parallelStream()}, whose ForkJoinPool threads the model
 * checker did not start and cannot schedule. Every configuration tried, what each cost, and the one run that
 * did find it are in {@code docs/plans/2026-08-25-001-test-lincheck-poc-plan.md}. A stress arm that finds the
 * tear in seconds is worth more in a lane than a model-checking arm that finds it once in seven attempts.
 *
 * @author Antony Stubbs
 */
@Tag("lincheck")
@Param(name = "offset", gen = IntGen.class, conf = "0:2")
public class PartitionStateLincheckTest {

    private static final String TOPIC = "lincheck-topic";

    private static final TopicPartition TP = new TopicPartition(TOPIC, 0);

    /**
     * Offsets 0..{@link #HIGHEST_POLLED_OFFSET} are polled and tracked, and this one is completed during setup
     * so that there is a genuine gap - an incomplete offset below the highest succeeded one - which is the only
     * condition under which PC encodes a payload at all. Without the gap {@code tryToEncodeOffsets} returns
     * empty and the commit carries no metadata, so neither tear has anything to corrupt.
     */
    private static final long PRE_COMPLETED_OFFSET = 3L;

    /**
     * The highest offset polled, and it is the SAME as the pre-completed one on purpose - which costs this
     * harness one of the two tears, so the reasoning is recorded rather than left as a number.
     * <p>
     * The encoder's second defect is that it filters the incomplete set against one read of
     * {@code offsetHighestSucceeded} and then takes a SECOND read as the encoded range's top; it is only
     * observable if that value can MOVE during the window, which needs a completable offset above it. Tracking
     * one (and widening the generator to match) makes it expressible - and made this harness's stress arm find
     * a violation in one run out of three instead of every run, because every extra value the generator can
     * produce dilutes the chance a randomly built scenario contains the pair that tears.
     * <p>
     * The trade was worth taking only while a model-checking arm existed to exploit the wider range - it is
     * the strategy that places a switch between two specific reads rather than hoping to land there - and no
     * such arm survived (see the class javadoc). So the range is back to its narrow, reliable form, and the
     * range-top leg of the encoder tear is NOT EXPRESSIBLE in this lane today. Widen both this and the
     * generator together if a model-checking arm ever returns.
     */
    private static final long HIGHEST_POLLED_OFFSET = 3L;

    private final PCModuleTestEnv module = new PCModuleTestEnv(ParallelConsumerOptions.<String, String>builder().build());

    private final PartitionState<String, String> state = newState();

    private PartitionState<String, String> newState() {
        var fresh = new PartitionState<String, String>(0L, module, TP, HighestOffsetAndIncompletes.of());
        for (long offset = 0; offset <= HIGHEST_POLLED_OFFSET; offset++) {
            fresh.addNewIncompleteRecord(new ConsumerRecord<>(TOPIC, 0, offset, "k" + offset, "v" + offset));
        }
        fresh.onSuccess(PRE_COMPLETED_OFFSET);
        return fresh;
    }

    /**
     * Control thread: the user function for this offset returned successfully.
     */
    @Operation
    public void succeed(@Param(name = "offset") int offset) {
        state.onSuccess(offset);
    }

    /**
     * Broker-poll or control thread, depending on commit mode: produce this partition's commit data.
     *
     * @return what the broker would hand back on the next bootstrap, decoded the way PC itself decodes it
     */
    @Operation
    public String commit() {
        OffsetAndMetadata committed = state.createOffsetAndMetadata();
        return asBrokerWouldReplayIt(committed);
    }

    /**
     * The validity oracle, spelled out: a commit is only meaningful as the pair (offset to resume from, set of
     * offsets below it still to replay), and the payload is decoded against the offset it was committed WITH -
     * because that is the only number the broker gives back.
     */
    private static String asBrokerWouldReplayIt(OffsetAndMetadata committed) {
        String payload = committed.metadata();
        if (payload == null || payload.isEmpty()) {
            return "resumeFrom=" + committed.offset() + " replay=[] (no payload)";
        }
        try {
            HighestOffsetAndIncompletes decoded =
                    OffsetMapCodecManager.deserialiseIncompleteOffsetMapFromBase64(committed.offset(), payload);
            return "resumeFrom=" + committed.offset() + " replay=" + decoded.getIncompleteOffsets();
        } catch (Exception e) {
            // A payload that cannot be decoded at the offset it shipped with is itself the failure, and
            // saying so is more useful than letting the exception be compared as an opaque result.
            return "resumeFrom=" + committed.offset() + " replay=UNDECODABLE(" + e.getClass().getSimpleName() + ")";
        }
    }

    @Test
    void stressRediscoversTheCommitTear() {
        var options = new StressOptions()
                .threads(2)
                .actorsPerThread(2)
                .actorsBefore(0)
                .actorsAfter(0)
                .iterations(300)
                .invocationsPerIteration(5_000);
        String report = LincheckHarness.runExpectingViolation("PartitionState / stress", options, getClass());
        assertThat(report).contains("commit()");
    }
}
