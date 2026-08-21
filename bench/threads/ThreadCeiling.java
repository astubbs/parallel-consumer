import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The floor beneath every arm: no Kafka, no Parallel Consumer, no queueing decisions.
 * A fixed thread pool, an infinite supply of synthetic work, and a sleep - which is
 * exactly what both Java benchmark arms' handlers do.
 *
 * If this cannot hold N records in flight at a 100ms sleep, then no amount of work
 * available can make it, and every ceiling measured further up the stack is explained
 * here rather than in the client or the engine.
 */
public class ThreadCeiling {
    static final AtomicInteger inFlight = new AtomicInteger();
    static final AtomicInteger peak = new AtomicInteger();
    static final AtomicInteger done = new AtomicInteger();

    public static void main(String[] args) throws Exception {
        int concurrency = Integer.parseInt(args[0]);
        int delayMs = Integer.parseInt(args[1]);
        int target = Integer.parseInt(args[2]);
        boolean virtual = args.length > 3 && args[3].equals("virtual");

        ExecutorService pool;
        if (virtual) {
            // reflective, so this compiles and runs on any JDK; fails loudly on an old one
            pool = (ExecutorService) Executors.class
                    .getMethod("newVirtualThreadPerTaskExecutor").invoke(null);
        } else {
            pool = new ThreadPoolExecutor(concurrency, concurrency, 0L, TimeUnit.MILLISECONDS,
                    new LinkedBlockingQueue<>());
        }

        // Work is INFINITE and free to produce - a semaphore caps in-flight at `concurrency`,
        // exactly as the pool arm does. Nothing upstream can starve this.
        Semaphore permits = new Semaphore(concurrency);
        long start = System.currentTimeMillis();
        int submitted = 0;
        while (submitted < target) {
            permits.acquire();
            submitted++;
            pool.submit(() -> {
                int now = inFlight.incrementAndGet();
                peak.accumulateAndGet(now, Math::max);
                try { Thread.sleep(delayMs); } catch (InterruptedException ignored) { }
                inFlight.decrementAndGet();
                done.incrementAndGet();
                permits.release();
            });
        }
        while (done.get() < target) Thread.sleep(5);
        long ms = System.currentTimeMillis() - start;
        System.out.printf("concurrency=%d delay=%dms threads=%s -> %.0f msg/s, peak in flight %d (of %d), mean %.0f%n",
                concurrency, delayMs, virtual ? "virtual" : "platform",
                target * 1000.0 / ms, peak.get(), concurrency, target * 1000.0 / ms * delayMs / 1000.0);
        pool.shutdownNow();
        System.exit(0);
    }
}
