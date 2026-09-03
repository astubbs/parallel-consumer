package bz.stub.parallelconsumer;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Declares that a test method exercises one or more documented transactional guarantees.
 * <p>
 * {@link TransactionalClaimCoverageTest} reads these to answer "is every claim we say is covered actually
 * referenced by a test?". Annotate the method that does the proving, not the class - a class-level annotation
 * would keep claiming coverage after the one method that mattered was deleted.
 * <p>
 * Note what this buys and what it does not: the annotation records a <em>reference</em>. It says nothing about
 * whether the referenced test is strong enough to fail when the claim is false. That is what the per-claim
 * negative controls are for - see {@link TransactionalClaim}.
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface ProvesClaim {

    /**
     * @return the claims this test method exercises
     */
    TransactionalClaim[] value();
}
