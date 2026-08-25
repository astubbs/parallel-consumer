package bz.stub.parallelconsumer.internal;

/*-
 * Copyright (C) 2026 Antony Stubbs and contributors
 */

import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A {@link LinkedTransferQueue} that keeps its own size, because the executor's queue depth is read on
 * a hot path and {@code LinkedTransferQueue.size()} is O(n).
 * <p>
 * WHY THIS EXISTS. Swapping the worker pool's {@link java.util.concurrent.LinkedBlockingQueue} for a
 * plain {@code LinkedTransferQueue} - to remove the single {@code takeLock} every worker contends on -
 * cost 69% of throughput at a zero-cost handler. That experiment changed TWO things at once: it removed
 * the lock, and it made {@code size()} a linear scan. {@code getNumberOfUserFunctionsQueued()} reads
 * that size every control loop, and at a zero-cost handler the queue holds on the order of a thousand
 * entries, so each loop paid for a walk.
 * <p>
 * This class separates the two so each can be measured on its own. It is a delegating wrapper rather
 * than a subclass on purpose: {@code LinkedTransferQueue} has many mutating entry points
 * ({@code add}, {@code offer}, {@code put}, {@code transfer}, {@code tryTransfer}, bulk operations),
 * and a subclass that overrode only the ones {@link java.util.concurrent.ThreadPoolExecutor} happens to
 * call today would drift out of true the moment it called a different one. Delegating means every path
 * in or out passes through the counter by construction.
 * <p>
 * The count is maintained ONLY where an element provably crossed the boundary - after a successful
 * {@code offer}, or when a {@code poll} actually returned something. A count maintained on intent
 * rather than on outcome is the drift this project already has one of, in
 * {@code ProcessingShard.availableWorkContainerCnt}, which carries a clamp resetting it to zero "in
 * case of possible race condition".
 *
 * @author Antony Stubbs
 */
public class CountedTransferQueue<E> implements BlockingQueue<E> {

    private final LinkedTransferQueue<E> delegate = new LinkedTransferQueue<>();
    private final AtomicInteger count = new AtomicInteger();

    @Override
    public int size() {
        return count.get();
    }

    @Override
    public boolean isEmpty() {
        return delegate.isEmpty();
    }

    @Override
    public boolean offer(E e) {
        boolean added = delegate.offer(e);
        if (added) count.incrementAndGet();
        return added;
    }

    @Override
    public boolean offer(E e, long timeout, TimeUnit unit) {
        return offer(e);
    }

    @Override
    public boolean add(E e) {
        return offer(e);
    }

    @Override
    public void put(E e) {
        offer(e);
    }

    @Override
    public E take() throws InterruptedException {
        E taken = delegate.take();
        count.decrementAndGet();
        return taken;
    }

    @Override
    public E poll(long timeout, TimeUnit unit) throws InterruptedException {
        E taken = delegate.poll(timeout, unit);
        if (taken != null) count.decrementAndGet();
        return taken;
    }

    @Override
    public E poll() {
        E taken = delegate.poll();
        if (taken != null) count.decrementAndGet();
        return taken;
    }

    @Override
    public boolean remove(Object o) {
        boolean removed = delegate.remove(o);
        if (removed) count.decrementAndGet();
        return removed;
    }

    @Override
    public int drainTo(Collection<? super E> c) {
        return drainTo(c, Integer.MAX_VALUE);
    }

    @Override
    public int drainTo(Collection<? super E> c, int maxElements) {
        int drained = delegate.drainTo(c, maxElements);
        count.addAndGet(-drained);
        return drained;
    }

    @Override
    public void clear() {
        // Drain rather than clear(), so the counter is corrected by what actually left.
        delegate.drainTo(new java.util.ArrayList<>());
        count.set(delegate.size());
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        boolean changed = delegate.removeAll(c);
        if (changed) count.set(delegate.size());
        return changed;
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        boolean changed = delegate.retainAll(c);
        if (changed) count.set(delegate.size());
        return changed;
    }

    @Override
    public boolean addAll(Collection<? extends E> c) {
        boolean changed = false;
        for (E e : c) changed |= offer(e);
        return changed;
    }

    // --- read-only delegation, no counting needed ---

    @Override public E peek() { return delegate.peek(); }
    @Override public E element() { return delegate.element(); }
    @Override public E remove() { E e = delegate.remove(); count.decrementAndGet(); return e; }
    @Override public boolean contains(Object o) { return delegate.contains(o); }
    @Override public boolean containsAll(Collection<?> c) { return delegate.containsAll(c); }
    @Override public Iterator<E> iterator() { return delegate.iterator(); }
    @Override public Object[] toArray() { return delegate.toArray(); }
    @Override public <T> T[] toArray(T[] a) { return delegate.toArray(a); }
    @Override public int remainingCapacity() { return delegate.remainingCapacity(); }
    @Override public String toString() { return delegate.toString(); }
}
