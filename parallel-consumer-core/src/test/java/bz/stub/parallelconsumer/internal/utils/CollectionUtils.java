package bz.stub.parallelconsumer.internal.utils;

/*-
 * Copyright (C) 2020-2021 Confluent, Inc.
 * Modifications Copyright (C) 2026 Antony Stubbs and contributors
 */

import java.util.List;
import java.util.Optional;

public class CollectionUtils {

    public static <T> Optional<T> getLast(List<T> history) {
        return history.isEmpty() ? Optional.empty() : Optional.of(history.get(history.size() - 1));
    }

}
