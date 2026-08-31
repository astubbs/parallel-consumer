package bz.stub.parallelconsumer.internal.navigator;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.navigator.NavigatorView;
import bz.stub.parallelconsumer.navigator.ResourceDeferral;
import bz.stub.parallelconsumer.state.ShardKey;

import java.time.Clock;
import java.util.List;
import java.util.Map;
import java.util.OptionalDouble;

/**
 * The one {@link NavigatorView} implementation: a stateless binding of a {@link NavigatorParticipant} to the one
 * canonical {@link Clock} (KTD4), so callers read "now" without holding a clock themselves. Every method
 * delegates to the participant's pure-read half - nothing here mutates, caches, or lazily initialises anything
 * (AE6); the per-shard counts come back as the participant's own immutable snapshot.
 * <p>
 * Both fields are final and set before publication, so this class needs no lock and no {@code @GuardedBy}
 * (KTD11's immutability shape) - thread-safety of the underlying reads is the participant's contract.
 *
 * @author Antony Stubbs
 */
public final class ParticipantBackedNavigatorView implements NavigatorView {

    /** The untagged instance's singleton (R3): backed by the inert participant, so every read short-circuits. */
    public static final NavigatorView INERT =
            new ParticipantBackedNavigatorView(NavigatorParticipant.inert(), Clock.systemUTC());

    private final NavigatorParticipant participant;

    private final Clock clock;

    private ParticipantBackedNavigatorView(NavigatorParticipant participant, Clock clock) {
        this.participant = participant;
        this.clock = clock;
    }

    /**
     * A view over {@code participant}, reading "now" from {@code clock} - the ONE canonical clock the allocator
     * and its members share (KTD4): the module clock in production, the shared {@code MutableClock} in the
     * virtual-clock test lane. An inert participant yields a view equivalent to {@link NavigatorView#inert()}.
     * <p>
     * Lives here rather than on {@link NavigatorView} so the public interface's signatures never name an
     * internal type - {@link NavigatorParticipant} is engine wiring, not user surface.
     */
    public static NavigatorView of(NavigatorParticipant participant, Clock clock) {
        return new ParticipantBackedNavigatorView(participant, clock);
    }

    @Override
    public boolean isActive() {
        return participant.isActive();
    }

    @Override
    public List<String> resourceTags() {
        return participant.resourceTags();
    }

    @Override
    public long resourceIneligibleCount() {
        return participant.currentlyDeferredCount();
    }

    @Override
    public Map<ShardKey, Long> resourceIneligibleCountByShard() {
        return participant.resourceIneligibleCountByShardSnapshot();
    }

    @Override
    public List<ResourceDeferral> blockingResourceDeferrals() {
        return participant.blockingResourceDeferrals(clock.instant());
    }

    @Override
    public OptionalDouble localRatePerSecond(String resourceName) {
        if (isUnconstrained(resourceName)) {
            return OptionalDouble.empty();
        }
        return OptionalDouble.of(participant.localRatePerSecond(resourceName, clock.instant()));
    }

    @Override
    public OptionalDouble globalRatePerSecond(String resourceName) {
        if (isUnconstrained(resourceName)) {
            return OptionalDouble.empty();
        }
        return OptionalDouble.of(participant.globalRatePerSecond(resourceName));
    }

    /**
     * AE6's unconstrained shape: the navigator constrains this instance on a name only when the instance is
     * active AND tags that name - anything else (untagged instance, un-tagged or unknown name, null) reads as
     * empty, never as an exception.
     */
    private boolean isUnconstrained(String resourceName) {
        return !participant.isActive() || resourceName == null || !participant.resourceTags().contains(resourceName);
    }

    @Override
    public String toString() {
        return "NavigatorView(" + participant + ")";
    }
}
