package io.confluent.parallelconsumer.integrationTests.chaostests;

/*-
 * Copyright (C) 2020-2026 Antony Stubbs and contributors
 */

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import pl.tlinkowski.unij.api.UniSets;

import static com.google.common.truth.Truth.assertThat;
import static pl.tlinkowski.unij.api.UniLists.of;

/**
 * Boundary regression for {@link ProgressProbe#ledger}: the end-of-run correctness arithmetic must be
 * provably able to fire (a bound that cannot fire is decoration). Pure function, no broker - and
 * deliberately NOT tagged {@code chaos}, so it gates every default integration build.
 */
class ProgressProbeLedgerIT {

    @Test
    void lossDetected() {
        List<String> problems = ProgressProbe.ledger(UniSets.of("a", "b", "c"), of("a", "b"), 1, 10);
        assertThat(problems).hasSize(1);
        assertThat(problems.get(0)).contains("LEDGER_LOSS");
        assertThat(problems.get(0)).contains("1 produced records never consumed");
    }

    @Test
    void balancedLedgerIsClean() {
        List<String> problems = ProgressProbe.ledger(UniSets.of("a", "b", "c"), of("a", "b", "c"), 3, 10);
        assertThat(problems).isEmpty();
    }

    @Test
    void duplicatesAtAllowancePass() {
        // 1 duplicate, allowance = 1 disturbance x 1 = 1 -> at the bound, passes
        List<String> problems = ProgressProbe.ledger(UniSets.of("a", "b"), of("a", "a", "b"), 1, 1);
        assertThat(problems).isEmpty();
    }

    @Test
    void duplicatesOverAllowanceFail() {
        // 2 duplicates, allowance = 1 disturbance x 1 = 1 -> over the bound
        List<String> problems = ProgressProbe.ledger(UniSets.of("a", "b"), of("a", "a", "b", "b"), 1, 1);
        assertThat(problems).hasSize(1);
        assertThat(problems.get(0)).contains("LEDGER_DUPLICATES");
    }

    @Test
    void allowanceCappedAtHalfOfExpected() {
        // stormy-run guard: 100 disturbances x 5000 = 500k raw allowance would be vacuous against
        // 10 expected records - the cap (half of expected = 5) must keep the bound able to fire
        Set<String> expected = UniSets.of("k0", "k1", "k2", "k3", "k4", "k5", "k6", "k7", "k8", "k9");
        List<String> consumed = of("k0", "k1", "k2", "k3", "k4", "k5", "k6", "k7", "k8", "k9",
                "k0", "k1", "k2", "k3", "k4", "k5"); // 6 duplicates > capped allowance of 5
        List<String> problems = ProgressProbe.ledger(expected, consumed, 100, 5_000);
        assertThat(problems).hasSize(1);
        assertThat(problems.get(0)).contains("LEDGER_DUPLICATES");
        assertThat(problems.get(0)).contains("capped");
    }
}
