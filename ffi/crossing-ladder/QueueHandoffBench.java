/*
 * Copyright (C) 2026 Antony Stubbs and contributors
 *
 * Crossing-cost ladder arm (b): the in-process queue handoff - the embedded pull seam's
 * shape. Two threads; the caller offers a request through a SynchronousQueue and blocks
 * for the response on a second SynchronousQueue; the worker loops take -> fold -> put.
 * One round trip = one host-function crossing under the pull model. Measures no-op, the
 * 1KB fold, and the ~1us busy-wait instrument check, as ns/round-trip distributions.
 * See docs/inflight/perf-crossing-cost-ladder.md.
 */
import java.lang.management.ManagementFactory;
import java.util.Arrays;
import java.util.concurrent.SynchronousQueue;

public class QueueHandoffBench {

    record Req(byte[] key, byte[] val, byte[] acc, int mode) {} // mode 0=noop 1=fold 2=fold+spin

    static void fold(byte[] key, byte[] val, byte[] acc) {
        int n = Math.min(val.length, acc.length);
        byte k = key.length > 0 ? key[0] : 0;
        for (int i = 0; i < n; i++) {
            acc[i] = (byte) (acc[i] + val[i] + k);
        }
    }

    static void spin1us() {
        long start = System.nanoTime();
        while (System.nanoTime() - start < 1_000) { /* spin */ }
    }

    public static void main(String[] args) throws Exception {
        int warmup = args.length > 0 ? Integer.parseInt(args[0]) : 50_000;
        int batches = args.length > 1 ? Integer.parseInt(args[1]) : 30;
        int n = args.length > 2 ? Integer.parseInt(args[2]) : 10_000;

        SynchronousQueue<Req> reqQ = new SynchronousQueue<>();
        SynchronousQueue<Integer> respQ = new SynchronousQueue<>();
        Thread worker = new Thread(() -> {
            try {
                while (true) {
                    Req r = reqQ.take();
                    if (r.mode() >= 1) fold(r.key(), r.val(), r.acc());
                    if (r.mode() == 2) spin1us();
                    respQ.put(0);
                }
            } catch (InterruptedException e) { /* end */ }
        }, "ladder-worker");
        worker.setDaemon(true);
        worker.start();

        byte[] key = new byte[16];
        byte[] val = new byte[1024];
        byte[] acc = new byte[1024];
        Arrays.fill(key, (byte) 'k');
        Arrays.fill(val, (byte) 'v');

        System.out.printf("load: %.2f  warmup %d round trips/arm, %d batches x %d%n",
                ManagementFactory.getOperatingSystemMXBean().getSystemLoadAverage(), warmup, batches, n);
        double foldMedian = 0;
        for (int mode = 0; mode <= 2; mode++) {
            Req req = new Req(key, val, acc, mode);
            for (int i = 0; i < (mode == 2 ? warmup / 5 : warmup); i++) {
                reqQ.put(req);
                respQ.take();
            }
            double[] perBatch = new double[batches];
            for (int b = 0; b < batches; b++) {
                long t0 = System.nanoTime();
                for (int i = 0; i < n; i++) {
                    reqQ.put(req);
                    respQ.take();
                }
                perBatch[b] = (System.nanoTime() - t0) / (double) n;
            }
            Arrays.sort(perBatch);
            double median = perBatch[batches / 2];
            double p99 = perBatch[Math.max(0, (int) (batches * 0.99) - 1)];
            String label = switch (mode) {
                case 0 -> "(b) queue round trip no-op";
                case 1 -> "(b) queue round trip fold";
                default -> "(b) queue round trip fold+1us spin";
            };
            System.out.printf("%-36s median %10.1f ns/call  p99 %10.1f  batches %d%n", label, median, p99, batches);
            if (mode == 1) foldMedian = median;
            if (mode == 2) System.out.printf("(b) instrument-check delta: %.1f ns (expect ~1000)%n", median - foldMedian);
        }
    }
}
