package bz.stub.parallelconsumer.internal.navigator;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import lombok.Value;

import java.time.Instant;
import java.util.Optional;

/**
 * One binding resource in a {@link NavigatorDecision} (U4, R9): the resource's name and when this member next
 * gains spendable credit against it. One instance per currently-blocking tagged resource - a record blocked on
 * several resources carries several of these, never a chosen one (R9's all-binding-predicates clause).
 * <p>
 * {@link #getNextCreditAt()} is empty exactly when {@link ResourceAllocator#nextCreditAt} is: the resource's
 * policy mints nothing, so there is no time to name (mirrors {@link NavigatorParticipant#availableAt}'s own
 * emptiness case).
 */
@Value
public class ResourceDeferral {

    /**
     * The blocking resource's name - matches {@link ResourceContract#getName()}.
     */
    String resourceName;

    /**
     * When this member next gains spendable credit against {@link #getResourceName()}, projected from current
     * membership (KD10's best-effort framing - a projection, not a promise).
     */
    Optional<Instant> nextCreditAt;
}
