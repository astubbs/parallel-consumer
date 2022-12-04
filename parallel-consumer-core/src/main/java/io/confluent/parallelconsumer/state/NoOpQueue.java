package io.confluent.parallelconsumer.state;

import lombok.experimental.Delegate;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Queue;

// todo delete
public class NoOpQueue<K, V> implements Queue<WorkContainer<K, V>> {

    // Use Lombok's dynamic delegate feature to implement the MyInterface interface
    @Delegate(excludes = MyExcludes.class)
    final Queue<WorkContainer<K, V>> delegate = new ArrayDeque<>();

    @Override
    public boolean containsAll(Collection<?> c) {
        return false;
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        return false;
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        return false;
    }

    private interface MyExcludes {

        boolean containsAll(Collection<?> c);

        boolean removeAll(Collection<?> c);

        boolean retainAll(Collection<?> c);

        WorkContainer toArray(WorkContainer[] a);

    }

}
