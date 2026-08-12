package bz.stub.parallelconsumer.internal;

/*-
 * Copyright (C) 2020-2024 Confluent, Inc.
 * Modifications Copyright (C) 2026 Antony Stubbs and contributors
 */

public class LimitedDynamicExtraLoadFactor extends DynamicLoadFactor {
    public LimitedDynamicExtraLoadFactor() {
        super(2, 2);
    }
}
