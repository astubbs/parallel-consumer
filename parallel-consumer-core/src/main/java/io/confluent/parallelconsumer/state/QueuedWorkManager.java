package io.confluent.parallelconsumer.state;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * todo docs
 *
 * @author Antony Stubbs
 */
@Slf4j
@RequiredArgsConstructor
public class QueuedWorkManager<K, V> implements BlockingQueue {

    private final WorkManager<K, V> wm;

    @Override
    public boolean add(Object o) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean offer(Object o) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void put(Object o) throws InterruptedException {
        throw new UnsupportedOperationException();

    }

    @Override
    public boolean offer(Object o, long timeout, TimeUnit unit) throws InterruptedException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object take() throws InterruptedException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object poll(long timeout, TimeUnit unit) throws InterruptedException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int remainingCapacity() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean remove(Object o) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean contains(Object o) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int drainTo(Collection c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int drainTo(Collection c, int maxElements) {
        throw new UnsupportedOperationException();
    }

    private void unsupportedOperationException() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object remove() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object poll() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object element() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object peek() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int size() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isEmpty() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterator iterator() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object[] toArray() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object[] toArray(Object[] a) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean containsAll(Collection c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean addAll(Collection c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean removeAll(Collection c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean retainAll(Collection c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException();

    }
}
