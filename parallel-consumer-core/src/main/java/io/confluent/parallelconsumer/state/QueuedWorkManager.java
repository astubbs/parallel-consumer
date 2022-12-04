package io.confluent.parallelconsumer.state;

import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
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
@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
public class QueuedWorkManager<K, V> implements BlockingQueue<Batch<WorkContainer<K, V>>> {

//    WorkManager<K, V> wm;
//
//    PCModule<K,V> module;

    QueuedShardManager<K, V> qsm;// = new QueuedShardManager<>(wm.getModule(), wm);


    @Override
    public boolean add(Batch<WorkContainer<K, V>> o) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean offer(Batch<WorkContainer<K, V>> o) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void put(Batch<WorkContainer<K, V>> o) throws InterruptedException {
        throw new UnsupportedOperationException();

    }

    @Override
    public boolean offer(Batch<WorkContainer<K, V>> o, long timeout, TimeUnit unit) throws InterruptedException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Batch<WorkContainer<K, V>> take() throws InterruptedException {
//        wm.getSm().getWorkThreadSafe();
        var take = qsm.take();

        return new Batch<>(take);
//        return internal();
    }

//    private synchronized Batch<WorkContainer<K, V>>internal() {
//        var work = wm.getWorkIfAvailable(1);
//        return new Batch<>(work);
//    }

    @Override
    public Batch<WorkContainer<K, V>> poll(long timeout, TimeUnit unit) throws InterruptedException {
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
    public Batch<WorkContainer<K, V>> remove() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Batch<WorkContainer<K, V>> poll() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Batch<WorkContainer<K, V>> element() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Batch<WorkContainer<K, V>> peek() {
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
