package bz.stub.parallelconsumer.streams;
/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import bz.stub.parallelconsumer.streams.protocol.v1alpha1.InvocationKind;

/**
 * One request for the host to run a function, and everything that request carries.
 *
 * <p>A value object rather than a longer parameter list, and the reason is arithmetic: each operator shape adds
 * an argument, so the sink's signature was heading for seven parameters of which most are null at any call. Worse,
 * they were all {@code byte[]}, so transposing two would compile and produce a plausible wrong answer.
 *
 * <p>The factory methods are the only way to build one, so a call cannot exist whose kind disagrees with the
 * fields it carries.
 *
 * <p><b>A plain final class rather than a record, and not by preference.</b> Error Prone 2.42.0 - which this
 * module inherited when the language-proxy stack merged down - does not report on a Jabel-desugared record, it
 * CRASHES, taking the whole compilation with it. Neither term can move: the root pom pins Error Prone at
 * 2.42.0 because 2.43.0 needs a JVM this build cannot use. The proxy module converted five value types for the
 * same reason (astubbs/parallel-consumer#293); this is the sixth. The accessors keep their record names, so
 * nothing at a call site changed.
 */
public final class ForeignCall {

    private final InvocationKind kind;

    private final byte[] key;

    private final byte[] value;

    private final byte[] aggregate;

    private final byte[] right;

    private ForeignCall(InvocationKind kind, byte[] key, byte[] value, byte[] aggregate, byte[] right) {
        this.kind = kind;
        this.key = key;
        this.value = value;
        this.aggregate = aggregate;
        this.right = right;
    }

    public InvocationKind kind() {
        return kind;
    }

    public byte[] key() {
        return key;
    }

    public byte[] value() {
        return value;
    }

    public byte[] aggregate() {
        return aggregate;
    }

    public byte[] right() {
        return right;
    }

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

    /**
     * A windowed aggregation step: the record's key and value, and the current accumulator.
     *
     * <p>The first three-field shape. Unlike {@link #reduce} the key travels - Kafka's {@code Aggregator} receives
     * it - and the aggregate is present on a key's FIRST value too, because the engine hands the initializer's
     * bytes out itself rather than skipping the call the way a reduction does.
     */
    public static ForeignCall aggregate(byte[] key, byte[] value, byte[] aggregate) {
        return new ForeignCall(InvocationKind.INVOCATION_KIND_AGGREGATE, key, value, aggregate, null);
    }
}
