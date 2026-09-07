package bz.stub.parallelconsumer.internal;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.function.Supplier;

/**
 * Where a replacement producer comes from, handed to {@link ProducerManager} only on the path where PC built the
 * producer itself: each {@link #build()} builds a new producer from the same configuration, the same
 * {@code transactional.id} included. The id travels with it so a failure to build can name what was refused.
 */
@RequiredArgsConstructor
public class ReplacementProducerSource<K, V> {

    private final Supplier<ProducerWrapper<K, V>> builder;

    /**
     * The {@code transactional.id} every producer from this source carries.
     */
    @Getter
    private final String transactionalId;

    /**
     * @return a wrapper around a newly built producer, not yet initialised for transactions
     */
    public ProducerWrapper<K, V> build() {
        return builder.get();
    }
}
