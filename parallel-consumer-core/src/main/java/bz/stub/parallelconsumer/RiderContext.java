package bz.stub.parallelconsumer;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import lombok.Value;
import org.apache.kafka.common.TopicPartition;

/**
 * What a {@link ParallelConsumerOptions#getRiderSupplier() rider supplier} is told when Parallel Consumer asks it
 * for the opaque bytes to carry in one partition's committed offset metadata.
 * <p>
 * Every field is a fact about <em>this one commit</em>, sampled inside the same snapshot that produces the payload
 * the rider travels in, and none of them may be re-derived by the supplier: the offset in particular is the offset
 * this rider will be committed against, and reading it from anywhere else reintroduces the confluentinc#893 defect
 * class, where a payload was stored against a later offset than the one it described.
 *
 * @author Antony Stubbs
 * @see ParallelConsumerOptions#getRiderSupplier()
 */
@Value
public class RiderContext {

    /**
     * The partition whose commit this rider will ride on. The rider is partition-scoped: whichever member of the
     * group next owns this partition is the one that reads it back.
     */
    TopicPartition partition;

    /**
     * The offset that will be committed for {@link #partition} together with this rider - the next offset to be
     * polled, not the last one processed.
     * <p>
     * A rider describes the state of the partition <em>as at</em> this offset, so it must be built from this
     * number rather than from a fresh read of anything: the two are captured in one snapshot precisely so they
     * cannot disagree.
     */
    long offsetToCommit;

    /**
     * The most bytes this call may return. A longer array is dropped for this commit, with a rate-limited warning.
     * <p>
     * <b>Derived, and it moves.</b> It is the smaller of Parallel Consumer's own rider cap - a fixed fraction of
     * the broker's offset-metadata size limit, so that a rider at its cap can never be what pushes a payload over
     * that limit - and what is actually left in this particular commit once the partition's outstanding-offset map
     * has been encoded. A partition with a large offset map therefore offers less room than the same partition
     * caught up, and a supplier that wants to be carried on every commit sizes itself for the small case.
     * <p>
     * Bytes, not encoded characters: the outer string encoding is Parallel Consumer's business and leaking it into
     * this API would tie an embedder to it. Zero is a legal answer, and means there is no room for a rider at all
     * on this commit.
     */
    int maxRiderBytes;

}
