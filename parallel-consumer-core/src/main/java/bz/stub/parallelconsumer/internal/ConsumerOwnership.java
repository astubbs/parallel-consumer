package bz.stub.parallelconsumer.internal;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

/**
 * Who, if anyone, may currently call the Kafka consumer - the state {@link ThreadConfinedConsumer}
 * guards.
 * <p>
 * Three states, and the third is the point:
 * <ul>
 *   <li>{@link Phase#UNCLAIMED} - before the poll loop starts. Every thread is allowed, because
 *       init-time calls like {@code subscribe} happen on whatever thread builds PC.</li>
 *   <li>{@link Phase#OWNED} - only {@link #owner} may call.</li>
 *   <li>{@link Phase#RELEASED} - set by the poll loop as its last act. Admits <b>no direct use at
 *       all</b>; the only legal move out of it is a claim.</li>
 * </ul>
 * Collapsing RELEASED into UNCLAIMED would reopen the window between the poll loop finishing and the
 * closing thread taking over, during which a third thread's misuse would go undetected - the one
 * hole a two-state guard cannot see.
 * <p>
 * A top-level type rather than nested inside {@link ThreadConfinedConsumer} for a build reason worth
 * knowing: the ManagedTruth assertion generator treats that class as a
 * {@code org.apache.kafka.clients.consumer.Consumer} implementation (it is in the plugin's
 * {@code legacyClasses}) and emits a Truth Subject for each of its NESTED types into the parent
 * package, where the package-private outer class is not visible - so any nested type there fails the
 * build. Nothing reaches this class, because it is neither in the plugin's {@code classes} allow-list
 * nor a Consumer implementation.
 *
 * @see <a href="https://github.com/confluentinc/parallel-consumer/issues/857">#857</a>
 */
public final class ConsumerOwnership {

    public enum Phase {UNCLAIMED, OWNED, RELEASED}

    static final ConsumerOwnership UNCLAIMED = new ConsumerOwnership(Phase.UNCLAIMED, null);
    static final ConsumerOwnership RELEASED = new ConsumerOwnership(Phase.RELEASED, null);

    private final Phase phase;
    private final Thread owner;

    private ConsumerOwnership(Phase phase, Thread owner) {
        this.phase = phase;
        this.owner = owner;
    }

    static ConsumerOwnership ownedBy(Thread owner) {
        return new ConsumerOwnership(Phase.OWNED, owner);
    }

    boolean isOwnedBy(Thread thread) {
        return phase == Phase.OWNED && owner == thread;
    }

    boolean isOwnedBySomeoneOtherThan(Thread thread) {
        return phase == Phase.OWNED && owner != thread;
    }

    boolean isClaimable() {
        return phase != Phase.OWNED;
    }

    boolean isReleased() {
        return phase == Phase.RELEASED;
    }

    /** The owning thread, or null when not {@link Phase#OWNED}. */
    Thread owner() {
        return owner;
    }

    @Override
    public String toString() {
        switch (phase) {
            case OWNED:
                return "'" + owner.getName() + "' (id:" + owner.getId() + ", alive:" + owner.isAlive() + ")";
            case RELEASED:
                return "released-for-handoff";
            default:
                return "unclaimed";
        }
    }
}
