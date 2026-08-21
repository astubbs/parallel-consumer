package bz.stub.parallelconsumer.client.demo.integrationTests;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.client.demo.ArmResult;
import bz.stub.parallelconsumer.client.demo.DemoBroker;
import bz.stub.parallelconsumer.client.demo.DemoOptions;
import bz.stub.parallelconsumer.client.demo.ReferenceDemo;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * <b>Runs the whole demo, small, against a real broker.</b>
 *
 * <h2>Why this exists</h2>
 *
 * This branch family has been bitten four times by code that was committed without being executed -
 * a documented run command that never worked, a required lane broken by a shared-helper refactor
 * that nobody saw, a runner that aborted given no arguments, and an arm silently configured
 * differently from its comparators. Unit tests did not catch any of them, because none of them was
 * a logic error: they were all "the thing does not actually run" errors.
 *
 * So the demo gets an automated execution path. It runs at a volume chosen to prove the machinery
 * rather than to measure anything - twenty records, one partition-light topic, no big replay - and
 * it spawns real sidecar child processes over a real broker, because those are precisely the parts
 * a unit test cannot reach.
 *
 * <h2>What "it returned" is worth, and why that is not a vacuous assertion</h2>
 *
 * Every arm now refuses to produce a result it did not earn: an arm whose latch opens before its
 * target is reached throws rather than reporting a partial count, and the serial arm has a stall
 * budget. So a completed run is already a statement that all five arms processed every record. The
 * assertions below make that explicit rather than leaving it implied, and they are what fails if a
 * future change makes an arm silently do less.
 *
 * <h2>Cost</h2>
 *
 * It needs Docker and it lives in the integration lane, which is where this repo puts anything
 * broker-backed - {@code bin/ci-integration-test.sh} is the only place failsafe runs, so this adds
 * nothing to the unit lane's time.
 *
 * @author Antony Stubbs
 */
@Slf4j
class ReferenceDemoIT {

    /** Small enough to be quick, big enough that every arm has to poll more than once. */
    private static final int RECORDS = 20;

    private static final List<String> EVERY_ARM = List.of(
            "AK core", "pc-core", "java-direct", "java-grpc", "java-raw-grpc");

    @Test
    void everyArmRunsEndToEndAgainstARealBrokerAndProcessesEveryRecord() throws Exception {
        var options = DemoOptions.parse(new String[]{
                "--records", String.valueOf(RECORDS),
                "--delay-ms", "1",
                "--concurrency", "4",
                "--partitions", "2",
                // no big replay: this test proves the machinery runs, it does not measure anything,
                // and a second replay would only buy wall-clock
                "--replay-factor", "1"},
                Collections.emptyMap());

        try (DemoBroker broker = DemoBroker.resolve(null)) {
            String topic = "reference-demo-it-" + System.nanoTime();
            List<ArmResult> results = ReferenceDemo.runFor(options, broker, topic);

            assertThat(results)
                    .withFailMessage("every arm must report, or the comparison has a hole in it")
                    .extracting(ArmResult::arm)
                    .containsExactlyElementsOf(EVERY_ARM);

            assertThat(results)
                    .allSatisfy(result -> {
                        assertThat(result.processed())
                                .withFailMessage("%s processed %d of %d records",
                                        result.arm(), result.processed(), RECORDS)
                                .isGreaterThanOrEqualTo(RECORDS);
                        assertThat(result.ratePerSecond())
                                .withFailMessage("%s reported no throughput at all", result.arm())
                                .isGreaterThan(0d);
                    });
        }
    }
}
