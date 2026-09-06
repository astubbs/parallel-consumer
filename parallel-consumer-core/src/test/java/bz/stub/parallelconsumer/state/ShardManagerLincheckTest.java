package bz.stub.parallelconsumer.state;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.ParallelConsumerOptions;
import bz.stub.parallelconsumer.internal.PCModuleTestEnv;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.jetbrains.lincheck.datastructures.IntGen;
import org.jetbrains.lincheck.datastructures.Operation;
import org.jetbrains.lincheck.datastructures.Param;
import org.jetbrains.lincheck.datastructures.StressOptions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import pl.tlinkowski.unij.api.UniLists;

import java.util.List;
import java.util.Optional;

import static bz.stub.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.KEY;

/**
 * CALIBRATION, not a regression test: treats {@link ShardManager} as a concurrent data structure, declares its
 * operations, and asks whether Lincheck rediscovers a bug we already know is here <em>without being told where
 * it is</em>.
 * <p>
 * Nothing below names the defect. The operations are the four things the two real threads do to the shard
 * map - the control thread adds polled work and completes or fails it, the broker-poll thread sweeps a
 * revoked partition's records out - and the offsets they act on come from Lincheck's own parameter
 * generator. Which pair races, and at which instruction the switch has to happen, is Lincheck's to find.
 * <p>
 * <b>The calibration target, and what happened to it.</b> Master carried an unfixed
 * {@code containsKey}-then-{@code get} pair in {@code removeWorkFromShardFor} whose {@code get} was
 * dereferenced unconditionally; under {@link ParallelConsumerOptions.ProcessingOrder#KEY} a concurrent
 * {@code removeShardIfEmpty} could drop the shard between the two reads. Lincheck refound it unaided, in
 * seconds, by a route nobody had written down (the sweep racing <em>itself</em> rather than
 * {@code onSuccess}). astubbs#345 fixed it with the single-read {@code getShard} idiom, and this harness is
 * now that fix's regression detector: the assertion is that <b>no Lincheck report over these operations
 * mentions {@code NullPointerException} again</b>.
 * <p>
 * <b>It is now an assert-no-violation harness, and getting there took three failed controls.</b> The lane's
 * inversion contract (docs/testing.md, "Every harness currently asserts that a bug EXISTS") says to flip a
 * harness once its fix lands. That flip was RED for a while after astubbs#345, because a SECOND violation was
 * reachable through the same operations and Lincheck stops at the first thing it finds: {@code revokeSweep(0)}
 * in the prefix, then {@code addWork(0)} against {@code addWork(0)} in parallel, landing on the
 * read-then-decide-then-write in {@code ProcessingShard#addWorkContainer} - the lane's own
 * "concurrent collection plus a derived counter" signature.
 * <p>
 * <b>astubbs#336 removed it</b>, by admitting to the population before the put and reading the outcome from
 * the map rather than from the earlier read ("ADMIT FIRST, then let the map itself say what happened - never
 * the read above"). Bisected rather than assumed, with a FRESH worktree per commit so no two trees could
 * share a {@code target/}: this harness, unchanged, fires 5 of 5 at astubbs#345, 5 of 5 at
 * confluentinc#905's hot-shard metric and 5 of 5 at astubbs#373's claim compare-and-set, and misses 5 of 5
 * at astubbs#336 - the sole commit touching core's main sources between the last hit and the first miss.
 * So the flip is this harness's assertion now, and astubbs#336 is what it regression-tests.
 * <p>
 * <b>astubbs#336's own commit message says the opposite, and measurement wins.</b> Its verification
 * paragraph reads "the Lincheck lane green - the latter still finding the violation it is calibrated to
 * find". Run on that exact tree the lane is RED, on this arm alone and with the other five green. The
 * likely mechanism is in the same message: astubbs#336 is an "adapted cherry-pick" of {@code fa4d1cf251}
 * from another branch, so the lane it reports on is plausibly the pre-adaptation one. Recorded rather than
 * explained away - if that line is ever shown to describe the merged tree, this flip is wrong.
 * <p>
 * <b>The miss is confirmed on a second machine.</b> Before the flip, this arm also found nothing on
 * {@code ubuntu-latest} in CI - four cores against the 32 the rest of this evidence was taken on - while
 * {@link LincheckToolchainProbeTest}'s stress arm fired on that same runner. That is what rules out the
 * lane's known 3.4x cross-machine hit-rate variance as the explanation: a rate effect does not turn 15 of
 * 15 hits into 0 across one commit on one box, and does not agree across two machines this far apart.
 * <p>
 * <b>Reverting HALF of astubbs#336 does not restore the counterexample, and three separate attempts proved
 * it.</b> Restoring the map's check-then-act alone leaves it green; restoring the counter's bare increment
 * alone leaves it green; keeping the put atomic never reintroduced a check-then-act at all. The defect was
 * deciding the accounting from the pre-put read, which astubbs#336 removed wholesale, so no single half
 * carries it. To ask whether a commit removed a violation, check out its PARENT and run - do not hand-rebuild
 * its before-state from the diff.
 * <p>
 * <b>Whether that violation was ever a product defect is still open, and the flip does not settle it.</b>
 * Production registers work from the broker-poll thread alone, so two concurrent {@code addWork} calls are not
 * an interleaving the library can take; the operation set is declared wider than the real thread model. The
 * same artefact-or-defect question stands over {@code PartitionStateLincheckTest}'s parallel {@code succeed},
 * and {@code docs/inflight/test-lincheck-lane-open-items.md} owns taking it for both harnesses at once.
 * <p>
 * Bounds are stated in the test methods rather than hidden in a base class, because "how long did it take and
 * at what bounds" is half of the adoption question.
 *
 * @author Antony Stubbs
 * @see LincheckToolchainProbeTest for the red control that proves the toolchain is actually instrumenting
 */
@Tag("lincheck")
@Param(name = "offset", gen = IntGen.class, conf = "0:1")
public class ShardManagerLincheckTest {

    /**
     * Any epoch will do - staleness is not what is under test, and every container here shares one, so the
     * shard's stale-replacement path is never entered.
     */
    private static final long EPOCH = 0L;

    /**
     * One key for every record, so that KEY ordering puts them all in ONE shard. Two shards would be two
     * independent maps and the interesting removal could not collide.
     */
    private static final String SHARED_KEY = "one-key";

    private static final String TOPIC = "lincheck-topic";

    private final PCModuleTestEnv module = new PCModuleTestEnv(ParallelConsumerOptions.<String, String>builder()
            .ordering(KEY)
            .build());

    private final ShardManager<String, String> sm = module.workManager().getSm();

    private final ConsumerRecord<String, String>[] records = newRecords();

    private final WorkContainer<String, String>[] containers = newContainers();

    /**
     * The revoke sweep's argument, built ONCE per instance rather than inside the operation.
     * <p>
     * Not cosmetic. {@code UniLists.of(..)} resolves its factory through {@link java.util.ServiceLoader} on
     * first use, and the model checker - which cannot tell one-time class initialisation from a spin loop -
     * reported "All unfinished threads are in livelock due to non-terminating loops" instead of the real bug.
     * A model-checked operation must do no first-time classloading; that is a general rule for writing these
     * harnesses, not a quirk of this one.
     */
    private final List<List<Optional<ConsumerRecord<String, String>>>> sweepArguments = newSweepArguments();

    private List<List<Optional<ConsumerRecord<String, String>>>> newSweepArguments() {
        return UniLists.of(UniLists.of(Optional.of(records[0])), UniLists.of(Optional.of(records[1])));
    }

    @SuppressWarnings("unchecked")
    private ConsumerRecord<String, String>[] newRecords() {
        return new ConsumerRecord[]{
                new ConsumerRecord<>(TOPIC, 0, 0L, SHARED_KEY, "v0"),
                new ConsumerRecord<>(TOPIC, 0, 1L, SHARED_KEY, "v1")
        };
    }

    @SuppressWarnings("unchecked")
    private WorkContainer<String, String>[] newContainers() {
        return new WorkContainer[]{
                new WorkContainer<>(EPOCH, records[0], module),
                new WorkContainer<>(EPOCH, records[1], module)
        };
    }

    public ShardManagerLincheckTest() {
        // Initial state: one record already tracked, i.e. the shard exists and is not empty. This is the
        // ordinary steady state of a running consumer, not a state contrived to expose anything.
        sm.addWorkContainer(EPOCH, records[0]);
    }

    /**
     * Control thread: a freshly polled record is registered as work.
     */
    @Operation
    public void addWork(@Param(name = "offset") int offset) {
        sm.addWorkContainer(EPOCH, records[offset]);
    }

    /**
     * Control thread: the user function returned successfully, so the work leaves the shard.
     */
    @Operation
    public void success(@Param(name = "offset") int offset) {
        sm.onSuccess(containers[offset]);
    }

    /**
     * Control thread: the user function threw, so the work goes back for retry.
     */
    @Operation
    public void failure(@Param(name = "offset") int offset) {
        sm.onFailure(containers[offset]);
    }

    /**
     * Broker-poll thread: the partition was revoked, so its records are swept out of the shards. In production
     * this runs inside the {@code ConsumerRebalanceListener}, i.e. inside {@code consumer.poll}.
     */
    @Operation
    public void revokeSweep(@Param(name = "offset") int offset) {
        sm.removeAnyShardEntriesReferencedFrom(sweepArguments.get(offset));
    }

    /**
     * STRESS only, and that is a finding rather than a preference.
     * <p>
     * The model-checking arm cannot run here at all: KEY ordering keys the shard map with
     * {@code ShardKey.KeyOrderedKey}, a Lombok {@code @EqualsAndHashCode(callSuper = true)} value type, and
     * Lincheck 3.7 rewrites its {@code super.hashCode()} into a virtual self-call that recurses to
     * StackOverflowError before any interleaving is explored. {@link LincheckSuperHashCodeProbeTest} settles
     * that with a control arm and fires when it is fixed. And the bug is KEY-only - {@code removeShardIfEmpty}
     * returns early under PARTITION and UNORDERED - so there is no ordering mode that both reaches this defect
     * and avoids the blocker.
     * <p>
     * Bounds. Every iteration now runs to completion - a run that finds nothing cannot stop early - so this is
     * a straight cost with no hit rate left to price. 50 x 5,000 is kept because it is the bound the
     * counterexample was FOUND at before astubbs#336 removed it, which makes the zero worth more than a bound
     * chosen for cost: the same budget that used to hit now reports 0 in 250,000 invocations, and 0 in
     * 2,500,000 at ten times it.
     * <p>
     * The assertion is Lincheck's own: {@code check} throws and fails this test, carrying the interleaving,
     * the moment a result appears that no sequential order of the same operations could produce. A red here
     * is a finding about the shard map, not a bound that needs widening - do not reprice it and do not add a
     * retry.
     */
    @Test
    void stressFindsNoWayToBreakTheShardMap() {
        new StressOptions()
                .threads(2)
                .actorsPerThread(2)
                .actorsBefore(0)
                .actorsAfter(0)
                .iterations(50)
                .invocationsPerIteration(5_000)
                .check(getClass());
    }
}
