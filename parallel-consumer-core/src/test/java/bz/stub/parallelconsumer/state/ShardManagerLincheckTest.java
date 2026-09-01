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
import static com.google.common.truth.Truth.assertWithMessage;

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
 * <b>It is NOT yet an assert-no-violation harness, and that is a finding rather than an oversight.</b> The
 * lane's inversion contract (docs/testing.md, "Every harness currently asserts that a bug EXISTS") says to
 * flip a harness to assert-no-failure once its fix lands. Measured on the fixed tree, that flip would be
 * RED: Lincheck still reports {@code = Invalid execution results =}, but for a different, non-NPE
 * interleaving - {@code revokeSweep(0)} in the prefix, then {@code addWork(0)} against {@code addWork(0)} in
 * parallel. That pair is the {@code entries.get} / {@code entries.put} + {@code incrementAndGet}
 * check-then-act in {@link ProcessingShard#addWorkContainer}, i.e. the lane's own
 * "concurrent collection plus a derived counter" signature - but production registers work from the
 * broker-poll thread alone, so two concurrent {@code addWork} calls are not an interleaving the library can
 * take, and nothing here establishes it as a product defect. Diagnosing it is tracked in
 * {@code docs/inflight/test-lincheck-lane-open-items.md}; until it is diagnosed, asserting the NPE's
 * absence is the strongest claim the evidence supports.
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
     */
    @Test
    void stressMustNotRediscoverTheShardTear() {
        var options = new StressOptions()
                .threads(2)
                .actorsPerThread(2)
                .actorsBefore(0)
                .actorsAfter(0)
                .iterations(50)
                .invocationsPerIteration(5_000);
        String report = LincheckHarness.runExpectingViolation("ShardManager / stress", options, getClass());
        assertWithMessage("astubbs#345 removed the torn containsKey/get pair, so a revoke sweep must never "
                + "dereference a shard that left the map - an NPE anywhere in a Lincheck report over these "
                + "operations is that defect back. Report was:\n%s", report)
                .that(report)
                .doesNotContain("NullPointerException");
    }
}
