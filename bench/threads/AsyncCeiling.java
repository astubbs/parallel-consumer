import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The same control as ThreadCeiling, with the one change that matters: the work does not
 * hold a thread while it waits.
 *
 * Instead of a pool thread calling Thread.sleep, each unit of work registers a completion
 * to fire later on a small scheduler - which is what a non-blocking engine (Vert.x,
 * Reactor, Mutiny) does with an async call. Concurrency is decoupled from threading.
 *
 * If this reaches full concurrency on a handful of threads, then virtual threads are one
 * way to lift the ceiling and not the only one.
 */
public class AsyncCeiling {
    static final AtomicInteger inFlight = new AtomicInteger();
    static final AtomicInteger peak = new AtomicInteger();
    static final AtomicInteger done = new AtomicInteger();

    public static void main(String[] args) throws Exception {
        int concurrency = Integer.parseInt(args[0]);
        int delayMs = Integer.parseInt(args[1]);
        int target = Integer.parseInt(args[2]);
        int schedulerThreads = args.length > 3 ? Integer.parseInt(args[3]) : 4;

        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(schedulerThreads);
        Semaphore permits = new Semaphore(concurrency);

        long start = System.currentTimeMillis();
        int submitted = 0;
        while (submitted < target) {
            permits.acquire();
            submitted++;
            int now = inFlight.incrementAndGet();
            peak.accumulateAndGet(now, Math::max);
            // No thread is held for the duration - the completion is registered, not waited on.
            scheduler.schedule(() -> {
                inFlight.decrementAndGet();
                done.incrementAndGet();
                permits.release();
            }, delayMs, TimeUnit.MILLISECONDS);
        }
        while (done.get() < target) Thread.sleep(5);
        long ms = System.currentTimeMillis() - start;
        System.out.printf("concurrency=%d delay=%dms schedulerThreads=%d -> %.0f msg/s, peak in flight %d (of %d)%n",
                concurrency, delayMs, schedulerThreads, target * 1000.0 / ms, peak.get(), concurrency);
        scheduler.shutdownNow();
        System.exit(0);
    }
}
