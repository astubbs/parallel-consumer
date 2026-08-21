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
    // Measures what the sleep ACTUALLY took, which nothing in this investigation had recorded.
    static final java.util.concurrent.ConcurrentLinkedQueue<Long> overshootNanos = new java.util.concurrent.ConcurrentLinkedQueue<>();

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
                long t0 = System.nanoTime();
                try { Thread.sleep(delayMs); } catch (InterruptedException ignored) { }
                overshootNanos.add((System.nanoTime() - t0) / 1_000_000 - delayMs);
                inFlight.decrementAndGet();
                done.incrementAndGet();
                permits.release();
            });
        }
        // The discriminating number: did the pool ever CREATE the threads? If poolSize reaches
        // `concurrency` but in-flight does not, they exist and cannot all run. If poolSize itself
        // stalls, the pool never made them and the ceiling is thread CREATION, not scheduling.
        int peakPoolSize = 0;
        while (done.get() < target) {
            if (pool instanceof ThreadPoolExecutor) {
                peakPoolSize = Math.max(peakPoolSize, ((ThreadPoolExecutor) pool).getPoolSize());
            }
            Thread.sleep(5);
        }
        System.out.printf("peak pool threads created: %d%n", peakPoolSize);
        long ms = System.currentTimeMillis() - start;
        var os = new java.util.ArrayList<>(overshootNanos);
        java.util.Collections.sort(os);
        String pct = os.isEmpty() ? "n/a" : String.format("p50=%dms p90=%dms p99=%dms",
                os.get(os.size()/2), os.get((int)(os.size()*0.9)), os.get((int)(os.size()*0.99)));
        System.out.printf("sleep overshoot: %s%n", pct);
        System.out.printf("concurrency=%d delay=%dms threads=%s -> %.0f msg/s, peak in flight %d (of %d), mean %.0f%n",
                concurrency, delayMs, virtual ? "virtual" : "platform",
                target * 1000.0 / ms, peak.get(), concurrency, target * 1000.0 / ms * delayMs / 1000.0);
        pool.shutdownNow();
        System.exit(0);
    }
}
