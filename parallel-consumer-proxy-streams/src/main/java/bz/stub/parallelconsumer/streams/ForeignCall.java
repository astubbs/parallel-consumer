package bz.stub.parallelconsumer.streams;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.streams.protocol.v1alpha1.InvocationKind;
import com.github.bsideup.jabel.Desugar;

/**
 * One request for the host to run a function, and everything that request carries.
 *
 * <p>A value object rather than a longer parameter list, and the reason is arithmetic: each operator shape adds
 * an argument, so the sink's signature was heading for seven parameters of which most are null at any call. Worse,
 * they were all {@code byte[]}, so transposing two would compile and produce a plausible wrong answer.
 *
 * <p>The factory methods are the only way to build one, so a call cannot exist whose kind disagrees with the
 * fields it carries.
 */
@Desugar // Jabel requires the annotation on every record
public record ForeignCall(InvocationKind kind, byte[] key, byte[] value, byte[] aggregate, byte[] right) {

    /** A stateless transform: the record's key and value. */
    public static ForeignCall map(byte[] key, byte[] value) {
        return new ForeignCall(InvocationKind.INVOCATION_KIND_MAP, key, value, null, null);
    }

    /**
     * A combination: the stored aggregate and the new value.
     *
     * <p>No key, because Kafka's {@code Reducer} does not receive one.
     */
    public static ForeignCall reduce(byte[] aggregate, byte[] value) {
        return new ForeignCall(InvocationKind.INVOCATION_KIND_REDUCE, null, value, aggregate, null);
    }

    /**
     * A join: the stream-side value and the table-side value.
     *
     * <p>Takes two values like {@link #reduce}, which is exactly why the kind is carried explicitly - the two are
     * indistinguishable by shape and mean entirely different things.
     */
    public static ForeignCall join(byte[] value, byte[] right) {
        return new ForeignCall(InvocationKind.INVOCATION_KIND_JOIN, null, value, null, right);
    }
}
