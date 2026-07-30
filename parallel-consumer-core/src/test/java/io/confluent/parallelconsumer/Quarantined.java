package io.confluent.parallelconsumer;
/*-
 * Copyright (C) 2020-2026 Antony Stubbs and contributors
 */

import org.junit.jupiter.api.Tag;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Quarantine lane for tests that are known to fail on master's gating CI.
 * <p>
 * A quarantined test is excluded from the required (gating) suites so green checks mean "mergeable",
 * but it keeps RUNNING on every PR in the non-gating "Quarantined Tests" CI job - so we still see when
 * it starts passing (its fix landed), when it gets worse, and it can still surprise us. This is a
 * quarantine, not a kill switch: {@code @Disabled} loses the signal entirely and, as the drain-zombie
 * investigation proved, a "known flake" can be a real product bug (write-up lands with PR #80 in
 * {@code docs/solutions/test-flakiness/}).
 * <p>
 * Discipline (enforced by the required fields + reviewed via the CI job's audit summary):
 * <ol>
 *     <li><b>No quarantine without diagnosis.</b> A test is only tagged after its failure signature is
 *     understood and rostered ({@link #reason()}, {@link #tracking()}). Undiagnosed red stays red and
 *     blocks, on purpose.</li>
 *     <li><b>Quarantine is master-state, not PR-state.</b> Only tests failing on master (or on every PR
 *     regardless of content) qualify. A test red on only one PR is that PR's problem.</li>
 *     <li><b>Re-enabling = deleting this annotation AND its entry in {@code docs/QUARANTINED_TESTS.md}</b>
 *     (the CI-enforced live registry - {@code bin/check-quarantine-registry.sh} fails on drift), done by
 *     the owning fix PR ({@link #fixedBy()}) after it merges master - which atomically moves the test
 *     back into the gating lane.</li>
 * </ol>
 */
@Target({ElementType.TYPE, ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
@Tag(Quarantined.TAG)
public @interface Quarantined {

    String TAG = "quarantined";

    /**
     * The diagnosed failure signature - what fails, how, and why. Never "it's red sometimes".
     */
    String reason();

    /**
     * Where the diagnosis is rostered - typically a {@code docs/inflight.md} entry or a
     * {@code docs/solutions/} write-up.
     */
    String tracking();

    /**
     * The open PR that fixes the failure and will delete this annotation on merge. Empty means
     * diagnosed-but-unowned - the CI audit summary flags these so they can't fall through the cracks.
     */
    String fixedBy() default "";
}
