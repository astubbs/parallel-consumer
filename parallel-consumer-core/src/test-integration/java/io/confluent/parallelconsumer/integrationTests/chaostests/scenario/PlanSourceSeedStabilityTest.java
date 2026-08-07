package io.confluent.parallelconsumer.integrationTests.chaostests.scenario;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;

/**
 * <b>The most important test in the scenario framework.</b> Guards the seed-stability invariant across
 * BUILDS, not merely within one: for a fixed seed, the generalised plan source must produce a
 * draw-for-draw identical plan to the implementation that existed before {@code ChaosConductor} was
 * generalised into a scenario driver.
 * <p>
 * Why it matters, and why nothing else would catch it: every chaos run logs its seed and a replay
 * command, and the Chaos Pain Suite's probes are calibrated against a real historical drain-zombie defect
 * with thresholds sitting in measured gaps. If a refactor changes the order or count of random draws -
 * or the total weight or iteration order of a weight map - then every previously recorded seed silently
 * stops reproducing the schedule it used to, and the calibration is quietly void. <b>That failure looks
 * like nothing at all: no test goes red.</b> Hence golden values.
 * <p>
 * The goldens below were captured by running the PRE-generalisation {@code ChaosConductor.planTicks}
 * (the {@code TickDraws}-returning static, W1 and W4 default weights) and recording its output verbatim.
 * They are not derived from the current code, so they cannot drift with it. Two layers:
 * <ul>
 *   <li>readable per-seed strings, so a divergence names the exact tick and field that moved;</li>
 *   <li>a digest over a much broader corpus (7 seeds x 200 ticks x both weight maps), so a divergence
 *       that only shows up deep into a run is still caught.</li>
 * </ul>
 * <b>If this test fails, do not update the goldens.</b> The goldens are the historical record; a failure
 * means the change under test broke replay, and the change is what has to move.
 * <p>
 * Not tagged {@code chaos}: a pure function with no broker, it gates every default integration build.
 */
class PlanSourceSeedStabilityTest {

    private static final Duration W1_MIN = Duration.ofMillis(500);
    private static final Duration W1_MAX = Duration.ofMillis(1500);
    private static final Duration W4_MIN = Duration.ofMillis(300);
    private static final Duration W4_MAX = Duration.ofMillis(1000);

    /** Seed corpus for the digest. Includes the seed from the W4 calibration diagnosis in the suite docs. */
    private static final long[] DIGEST_SEEDS = {0L, 1L, 42L, 43L, 999L, -1L, 7612284256787897904L};

    // --- goldens captured from the pre-generalisation ChaosConductor.planTicks ---

    private static final String W1_SEED_42 =
            "630|4588038915657817584|STOP_NO_DRAIN|662969970 "
                    + "1025|4598663022256506966|JOIN_NEW|196118093 "
                    + "682|4601800747299277032|STOP_DRAIN|592164476 "
                    + "532|4602024060202878324|STOP_DRAIN|2143572209 "
                    + "900|4594642893183368168|STOP_DRAIN|1610411243 "
                    + "741|4600635389643180080|RESTART|323298246";

    private static final String W1_SEED_43 =
            "1256|4599502835504443372|RESTART|1887674357 "
                    + "876|4588715284028846880|STOP_DRAIN|44086986 "
                    + "1174|4599998587498668674|STOP_DRAIN|495578942 "
                    + "922|4602964040790252954|RESTART|664193430 "
                    + "1485|4604957506911710025|STOP_NO_DRAIN|1927069265 "
                    + "1440|4606009792824768678|STOP_DRAIN|1461539324";

    private static final String W1_SEED_CALIBRATION =
            "652|4590533751855079560|RESTART|1700594725 "
                    + "673|4604607687982548817|STOP_DRAIN|1277566315 "
                    + "586|4605484467584065724|STOP_DRAIN|803597843 "
                    + "666|4606499671782035663|STOP_DRAIN|1714715908 "
                    + "1317|4586142031462700208|STOP_NO_DRAIN|708989607 "
                    + "1253|4600805384250569442|STOP_NO_DRAIN|1834725774";

    private static final String W4_SEED_42 =
            "391|4588038915657817584|STOP_NO_DRAIN|662969970 "
                    + "667|4598663022256506966|RESTART|196118093 "
                    + "427|4601800747299277032|RESTART|592164476 "
                    + "322|4602024060202878324|JOIN_NEW|2143572209 "
                    + "580|4594642893183368168|RESTART|1610411243 "
                    + "468|4600635389643180080|STOP_NO_DRAIN|323298246";

    private static final String W4_SEED_CALIBRATION =
            "406|4590533751855079560|STOP_NO_DRAIN|1700594725 "
                    + "421|4604607687982548817|JOIN_NEW|1277566315 "
                    + "360|4605484467584065724|JOIN_NEW|803597843 "
                    + "416|4606499671782035663|JOIN_NEW|1714715908 "
                    + "871|4586142031462700208|STOP_NO_DRAIN|708989607 "
                    + "827|4600805384250569442|RESTART|1834725774";

    /** SHA-256 over DIGEST_SEEDS x 200 ticks x {W1, W4}, rendered by {@link #render}. */
    private static final String BROAD_DIGEST =
            "6747d498504949de590ac2e8bdb66f01475bb0970161da1d6c44d41dd8a1a8e4";

    @Test
    void w1PlanMatchesThePreGeneralisationGoldens() {
        assertGolden("W1 seed 42", W1_SEED_42, 42L, W1_MIN, W1_MAX, ChaosScenarios.w1Weights());
        assertGolden("W1 seed 43", W1_SEED_43, 43L, W1_MIN, W1_MAX, ChaosScenarios.w1Weights());
        assertGolden("W1 calibration seed", W1_SEED_CALIBRATION, 7612284256787897904L,
                W1_MIN, W1_MAX, ChaosScenarios.w1Weights());
    }

    @Test
    void w4PlanMatchesThePreGeneralisationGoldens() {
        assertGolden("W4 seed 42", W4_SEED_42, 42L, W4_MIN, W4_MAX, ChaosScenarios.w4Weights());
        assertGolden("W4 calibration seed", W4_SEED_CALIBRATION, 7612284256787897904L,
                W4_MIN, W4_MAX, ChaosScenarios.w4Weights());
    }

    @Test
    void broadCorpusDigestMatchesThePreGeneralisationImplementation() {
        StringBuilder all = new StringBuilder();
        for (long seed : DIGEST_SEEDS) {
            all.append(render(SeededPlanSource.plan(seed, 200, W1_MIN, W1_MAX, ChaosScenarios.w1Weights())));
            all.append(render(SeededPlanSource.plan(seed, 200, W4_MIN, W4_MAX, ChaosScenarios.w4Weights())));
        }
        assertWithMessage("the seeded draw stream has changed somewhere in %s seeds x 200 ticks x {W1,W4}. "
                + "Every recorded chaos seed now reproduces a DIFFERENT schedule and the probe calibration is "
                + "void. Do not update this digest - revert whatever changed the draw order, count, bounds, "
                + "weight totals or weight-map iteration order.", DIGEST_SEEDS.length)
                .that(sha256(all.toString())).isEqualTo(BROAD_DIGEST);
    }

    /**
     * The draw CONTRACT, stated independently of the goldens: exactly four draws per tick, in a fixed
     * order, from bounds that must not move. Counted against a Random that records every call.
     */
    @Test
    void everyTickConsumesExactlyFourDrawsInTheContractedOrder() {
        RecordingRandom recording = new RecordingRandom(42L);
        SeededPlanSource.drawTick(recording, W1_MIN, W1_MAX, ChaosScenarios.w1Weights());
        assertThat(recording.calls).containsExactly(
                "nextInt(1000)",       // tick length
                "nextDouble()",        // follow-on bias roll
                "nextInt(10)",         // weighted action pick - W1 weights total 4+2+3+1
                "nextInt(2147483647)") // target roll
                .inOrder();
    }

    /**
     * The other half of the seed contract: iteration order. An unordered map would draw the same NUMBER
     * of times but resolve the pick to a different action, which is exactly the silent failure mode.
     */
    @Test
    void weightMapIterationOrderIsPartOfTheContract() {
        Map<ScenarioAction, Integer> reversed = new LinkedHashMap<>();
        reversed.put(MembershipAction.JOIN_NEW, 1);
        reversed.put(MembershipAction.RESTART, 3);
        reversed.put(MembershipAction.STOP_NO_DRAIN, 2);
        reversed.put(MembershipAction.STOP_DRAIN, 4);

        List<ScenarioTick> canonical = SeededPlanSource.plan(42L, 50, W1_MIN, W1_MAX, ChaosScenarios.w1Weights());
        List<ScenarioTick> reordered = SeededPlanSource.plan(42L, 50, W1_MIN, W1_MAX, reversed);

        assertWithMessage("same weights in a different iteration order must produce the same tick lengths "
                + "and rolls (the draw stream is untouched) but a different action mapping - if this ever "
                + "stops holding, weight-map ordering has silently stopped mattering, and so has the "
                + "guarantee that it does")
                .that(reordered).isNotEqualTo(canonical);
        for (int i = 0; i < canonical.size(); i++) {
            assertThat(reordered.get(i).getTickMs()).isEqualTo(canonical.get(i).getTickMs());
            assertThat(reordered.get(i).getTargetRoll()).isEqualTo(canonical.get(i).getTargetRoll());
        }
    }

    /** The declaration order the EnumMap-backed weight maps depend on. */
    @Test
    void membershipActionDeclarationOrderIsUnchanged() {
        assertThat(MembershipAction.values()).asList().containsExactly(
                MembershipAction.STOP_DRAIN,
                MembershipAction.STOP_NO_DRAIN,
                MembershipAction.RESTART,
                MembershipAction.JOIN_NEW).inOrder();
    }

    /** A phase must not reorder the weights it was handed - it copies into a LinkedHashMap for that reason. */
    @Test
    void phaseWeightCopyPreservesIterationOrder() {
        ScenarioPhase phase = ScenarioPhase.builder()
                .description("order check")
                .minTick(W1_MIN)
                .maxTick(W1_MAX)
                .weights(ChaosScenarios.w1Weights())
                .build();
        assertThat(phase.getWeights().keySet()).containsExactly(
                MembershipAction.STOP_DRAIN,
                MembershipAction.STOP_NO_DRAIN,
                MembershipAction.RESTART,
                MembershipAction.JOIN_NEW).inOrder();

        // and the phase's own plan source draws the same stream as the raw static
        List<ScenarioTick> viaPhase = drainTicks(phase, 20, 42L);
        List<ScenarioTick> viaStatic = SeededPlanSource.plan(42L, 20, W1_MIN, W1_MAX, ChaosScenarios.w1Weights());
        assertThat(viaPhase).isEqualTo(viaStatic);
    }

    /** Phases of one scenario share the RNG, so a run is one continuous stream rather than a per-phase restart. */
    @Test
    void phasesShareOneContinuousDrawStream() {
        ScenarioPhase phase = ScenarioPhase.builder()
                .description("shared stream")
                .minTick(W1_MIN)
                .maxTick(W1_MAX)
                .weights(ChaosScenarios.w1Weights())
                .build();
        Random shared = new Random(42L);
        PlanSource first = phase.newPlanSource(shared);
        PlanSource second = phase.newPlanSource(shared);

        List<ScenarioTick> reference = SeededPlanSource.plan(42L, 4, W1_MIN, W1_MAX, ChaosScenarios.w1Weights());
        assertThat(first.nextTick()).isEqualTo(reference.get(0));
        assertThat(first.nextTick()).isEqualTo(reference.get(1));
        assertThat(second.nextTick()).isEqualTo(reference.get(2));
        assertThat(second.nextTick()).isEqualTo(reference.get(3));
    }

    // --- helpers ---

    private static void assertGolden(String label, String golden, long seed,
                                     Duration min, Duration max,
                                     Map<? extends ScenarioAction, Integer> weights) {
        int ticks = golden.split(" ").length;
        assertWithMessage("%s: the seeded draw stream no longer matches the pre-generalisation "
                + "implementation. Every chaos seed recorded before this change now replays a DIFFERENT "
                + "schedule, and the probe calibration is void. Do not update the golden.", label)
                .that(render(SeededPlanSource.plan(seed, ticks, min, max, weights)))
                .isEqualTo(golden);
    }

    private static List<ScenarioTick> drainTicks(ScenarioPhase phase, int count, long seed) {
        PlanSource source = phase.newPlanSource(new Random(seed));
        List<ScenarioTick> ticks = new java.util.ArrayList<>();
        for (int i = 0; i < count; i++) {
            ticks.add(source.nextTick());
        }
        return ticks;
    }

    /** Raw bits for the double, so the comparison is exact rather than formatted. */
    static String render(List<ScenarioTick> plan) {
        StringBuilder sb = new StringBuilder();
        for (ScenarioTick d : plan) {
            if (sb.length() > 0) sb.append(' ');
            sb.append(d.getTickMs()).append('|')
                    .append(Double.doubleToRawLongBits(d.getBiasRoll())).append('|')
                    .append(d.getAction().name()).append('|')
                    .append(d.getTargetRoll());
        }
        return sb.toString();
    }

    private static String sha256(String input) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(input.getBytes(StandardCharsets.UTF_8));
            StringBuilder hex = new StringBuilder();
            for (byte b : digest) {
                hex.append(String.format("%02x", b));
            }
            return hex.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is required by every JRE", e);
        }
    }

    /** Records which RNG methods were called, with their bounds, in order. */
    private static class RecordingRandom extends Random {
        private final List<String> calls = new java.util.ArrayList<>();

        RecordingRandom(long seed) {
            super(seed);
        }

        @Override
        public int nextInt(int bound) {
            calls.add("nextInt(" + bound + ")");
            return super.nextInt(bound);
        }

        @Override
        public double nextDouble() {
            calls.add("nextDouble()");
            return super.nextDouble();
        }
    }
}
