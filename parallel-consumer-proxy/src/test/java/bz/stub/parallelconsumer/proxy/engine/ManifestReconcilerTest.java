package bz.stub.parallelconsumer.proxy.engine;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.proxy.protocol.v1.Token;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;

/**
 * R43's three-way reconciliation as a table: every combination of what the proxy holds and what a reconnecting
 * client claims, decided without an engine.
 *
 * @author Antony Stubbs
 */
class ManifestReconcilerTest {

    private static Token token(String recordId, long epoch) {
        return Token.newBuilder().setRecordId(recordId).setEpoch(epoch).build();
    }

    /** AE19 exactly: A current, B superseded, C unnamed. */
    @Test
    void theThreeArmsInOneManifest() {
        var held = Map.of("t/0/0", 3L, "t/0/1", 7L, "t/0/2", 1L);

        var outcome = ManifestReconciler.reconcile(held,
                List.of(token("t/0/0", 3), token("t/0/1", 6)));

        assertThat(outcome.kept()).containsExactly("t/0/0");
        assertThat(outcome.drops()).containsExactly(token("t/0/1", 6));
        assertThat(outcome.unmanifested()).containsExactly("t/0/2");
        assertThat(outcome.unissued()).isEmpty();
    }

    /**
     * A token the proxy never issued is rejected, and - the load-bearing half - nothing held moves because of
     * it: the held record it does not name is still returned, and the one it does is still kept.
     */
    @Test
    void aTokenTheProxyNeverIssuedIsRejectedWithoutDisturbingAnythingHeld() {
        var held = Map.of("t/0/0", 1L);

        var outcome = ManifestReconciler.reconcile(held,
                List.of(token("t/0/0", 1), token("fabricated/9/9", 42)));

        assertThat(outcome.unissued()).containsExactly(token("fabricated/9/9", 42));
        assertThat(outcome.kept()).containsExactly("t/0/0");
        assertWithMessage("a rejected token must not push a held record out")
                .that(outcome.unmanifested()).isEmpty();
        assertThat(outcome.drops()).isEmpty();
    }

    /** An empty manifest is the honest report of a client whose every worker died: everything comes back. */
    @Test
    void anEmptyManifestReturnsEverythingHeld() {
        var held = Map.of("t/0/0", 1L, "t/0/1", 1L);

        var outcome = ManifestReconciler.reconcile(held, List.of());

        assertThat(outcome.unmanifested()).containsExactly("t/0/0", "t/0/1");
        assertThat(outcome.kept()).isEmpty();
        assertThat(outcome.drops()).isEmpty();
    }

    /** Nothing held: every token is unissued, and the empty result is not mistaken for "return everything". */
    @Test
    void aManifestAgainstAnEmptyHeldSetIsAllRejection() {
        var outcome = ManifestReconciler.reconcile(Map.of(), List.of(token("t/0/0", 1)));

        assertThat(outcome.unissued()).containsExactly(token("t/0/0", 1));
        assertThat(outcome.kept()).isEmpty();
        assertThat(outcome.unmanifested()).isEmpty();
        assertThat(outcome.drops()).isEmpty();
    }

    /**
     * A client naming one record twice - a worker holding the live delivery and a dead one holding the
     * superseded epoch - keeps the record and still gets the drop order for the stale delivery.
     */
    @Test
    void oneRecordNamedAtTwoEpochsIsBothKeptAndDropped() {
        var held = Map.of("t/0/0", 2L);

        var outcome = ManifestReconciler.reconcile(held,
                List.of(token("t/0/0", 2), token("t/0/0", 1)));

        assertThat(outcome.kept()).containsExactly("t/0/0");
        assertThat(outcome.drops()).containsExactly(token("t/0/0", 1));
        assertThat(outcome.unmanifested()).isEmpty();
    }
}
