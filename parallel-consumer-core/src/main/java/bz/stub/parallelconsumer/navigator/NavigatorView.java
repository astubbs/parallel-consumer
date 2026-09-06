package bz.stub.parallelconsumer.navigator;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.internal.navigator.ParticipantBackedNavigatorView;
import bz.stub.parallelconsumer.state.ShardKey;

import java.util.List;
import java.util.Map;
import java.util.OptionalDouble;

/**
 * The navigator's observed-state surface (U5, R18): the narrow read-only view a user function reaches through
 * {@code PollContext#getNavigatorView()}, and the surface the test harness and the later web GUI assert against.
 * It answers "what is currently resource-ineligible, why, and what rate is available" - nothing else.
 * <p>
 * <b>Side-effect-free by contract (AE6, KTD9's query discipline).</b> Every method is a pure read: no lazy
 * initialisation, no registration, no lease renewal, no allocation of navigator state - reading this view is
 * observably inert, so the allocator's conservation counters are byte-identical before and after any number of
 * reads. Implementations delegate only to the pure-read half of {@link ResourceAllocator}'s split (see that
 * interface's javadoc for which calls are which).
 * <p>
 * <b>Never null, never throwing.</b> An untagged instance ({@link #isActive()} false) answers every query with
 * the empty/unconstrained shape: empty counts, an empty deferral list, and empty ({@literal =} unconstrained)
 * rates - the same answers a tagged instance gives for a resource it does not tag. Unknown resource names get
 * the unconstrained shape too, never an exception (AE6).
 * <p>
 * <b>Weakly consistent, deliberately.</b> The per-shard counts are maintained on the controller thread at the
 * defer/undefer transitions and read here as a snapshot from any thread; a read concurrent with a transition may
 * see the count from just before or just after it, but never a torn value, never an exception, and never the
 * controller-owned shard map itself. The blocking-resource list and the rates are evaluated at read time against
 * the one canonical clock (KTD4), so two consecutive reads across a quantum boundary may legitimately differ.
 *
 * @author Antony Stubbs
 * @see bz.stub.parallelconsumer.internal.navigator.NavigatorParticipant the state this view reads
 */
public interface NavigatorView {

    /**
     * Whether this instance participates in the navigator at all (R3). False for an untagged instance, whose
     * every other query returns the empty/unconstrained shape.
     */
    boolean isActive();

    /**
     * The resource names this instance's function requires (R2). Immutable; empty when {@link #isActive()} is
     * false.
     */
    List<String> resourceTags();

    /**
     * How many records are currently resource-ineligible (deferred waiting for a credit) across all of this
     * instance's ordering shards - the sum of {@link #resourceIneligibleCountByShard()}'s values. Zero when
     * nothing is deferred or the instance is untagged.
     */
    long resourceIneligibleCount();

    /**
     * How many records are currently resource-ineligible PER ORDERING SHARD (R18) - an immutable,
     * weakly-consistent snapshot keyed by the engine's own {@link ShardKey}. Shards with nothing deferred are
     * absent, so an idle or untagged instance reads an empty map - never a map of zeroes, never null.
     */
    Map<ShardKey, Long> resourceIneligibleCountByShard();

    /**
     * Every tagged resource currently withholding work, each paired with its own next-credit time (R18's "for
     * which resource, their {@code availableAt}"; R9's all-binding-predicates shape) - the same unreduced set
     * the defer-moment attribution logs. A deferred record's own {@code availableAt} is the LATEST of these
     * ({@link bz.stub.parallelconsumer.internal.navigator.NavigatorParticipant#availableAt}); a projection,
     * not a promise (KD10). Empty when nothing is blocking right now, or when the instance is untagged.
     */
    List<ResourceDeferral> blockingResourceDeferrals();

    /**
     * The rate currently available to THIS instance against {@code resourceName}, in credits per second - its
     * equal share of the declared policy rate under current allocator membership (R18's instance-local view).
     * Empty means the navigator does not constrain this instance on that name: the instance is untagged, or does
     * not tag {@code resourceName} (AE6's unconstrained shape). {@code 0.0} is a real answer - tagged, but not
     * currently a member (before the running transition, or after close).
     */
    OptionalDouble localRatePerSecond(String resourceName);

    /**
     * The resource's globally declared rate in credits per second - the whole policy rate all members share
     * (R18's global view). Empty means unconstrained here, exactly as {@link #localRatePerSecond} defines it.
     */
    OptionalDouble globalRatePerSecond(String resourceName);

    /**
     * This instance's current share of {@code resourceName}'s declared rate, as a fraction in {@code [0, 1]}
     * (the partition-share plan's R9): {@link #localRatePerSecond} over {@link #globalRatePerSecond}, so under
     * partition-share it is the fraction of the subscription's partitions this instance holds - three
     * quarters for a holder of three partitions out of four (AE3). {@code 0.0} is a real answer: no partition
     * held, or the partition total still unresolved (R5), or a shut valve. Empty means unconstrained here,
     * exactly as {@link #localRatePerSecond} defines it.
     */
    OptionalDouble shareFraction(String resourceName);

    /**
     * What this instance's current share of {@code resourceName} is worth in credits per quantum (R9):
     * {@link #localRatePerSecond} times the resource's quantum length in seconds - the "why am I at 1Hz"
     * number, read from one instance. Fractional values are real: a share below one credit per quantum accrues
     * through the allocator's remainder rotation rather than rounding to zero (AE3). Empty means unconstrained
     * here, exactly as {@link #localRatePerSecond} defines it.
     */
    OptionalDouble creditsPerQuantum(String resourceName);

    /**
     * The untagged instance's view (R3, AE6): {@link #isActive()} false and every query the empty/unconstrained
     * shape, at zero cost - it holds no allocator and can touch nothing.
     */
    static NavigatorView inert() {
        return ParticipantBackedNavigatorView.INERT;
    }
}
