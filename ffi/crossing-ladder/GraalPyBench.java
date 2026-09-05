/*
 * Copyright (C) 2026 Antony Stubbs and contributors
 *
 * Crossing-cost ladder arm (e): a Python function called through the GraalPy polyglot
 * API from a JVM host. Measures, post-warmup: no-op call, the 1KB fold over HOST byte
 * arrays (every element access crosses the interop boundary - the honest shape when the
 * engine owns the bytes), the fold over a GUEST-staged bytearray (pure call overhead +
 * guest compute, parallel to the wasm staged arm), and the calibrated ~1us spin
 * instrument check. See docs/inflight/perf-crossing-cost-ladder.md.
 *
 * Run on the GraalVM 25 JDK with the graal/target/deps2502 classpath (matching polyglot
 * version, so Truffle compiles rather than interprets).
 */
import java.lang.management.ManagementFactory;
import java.util.Arrays;

import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.Engine;
import org.graalvm.polyglot.Value;

public class GraalPyBench {

    static double median(double[] a) {
        double[] c = a.clone();
        Arrays.sort(c);
        return c[c.length / 2];
    }

    static double p99(double[] a) {
        double[] c = a.clone();
        Arrays.sort(c);
        return c[Math.max(0, (int) (c.length * 0.99) - 1)];
    }

    interface Callee { void call(); }

    static double[] bench(Callee c, int warmup, int batches, int n) {
        for (int i = 0; i < warmup; i++) c.call();
        double[] perBatch = new double[batches];
        for (int b = 0; b < batches; b++) {
            long t0 = System.nanoTime();
            for (int i = 0; i < n; i++) c.call();
            perBatch[b] = (System.nanoTime() - t0) / (double) n;
        }
        return perBatch;
    }

    static double report(String label, double[] perBatch) {
        System.out.printf("%-38s median %10.1f ns/call  p99 %10.1f  batches %d%n",
                label, median(perBatch), p99(perBatch), perBatch.length);
        return median(perBatch);
    }

    static final String PY = """
            def noop(key, val, acc):
                return 0

            def fold(key, val, acc):
                n = min(len(val), len(acc))
                k = key[0] if len(key) else 0
                for i in range(n):
                    v = (acc[i] + val[i] + k) & 0xFF
                    acc[i] = v - 256 if v > 127 else v  # host byte[] elements are signed
                return acc[n - 1]

            GUEST_VAL = bytearray(b'v' * 1024)
            GUEST_ACC = bytearray(1024)

            def fold_guest(key_byte):
                n = 1024
                for i in range(n):
                    GUEST_ACC[i] = (GUEST_ACC[i] + GUEST_VAL[i] + key_byte) & 0xFF
                return GUEST_ACC[n - 1]

            def fold_spin(key, val, acc, count):
                r = fold(key, val, acc)
                s = acc[0] & 0xFF
                for i in range(count):
                    s = (s * 31 + i) & 0xFF
                acc[0] = s - 256 if s > 127 else s
                return r
            """;

    public static void main(String[] args) {
        int warmup = 100_000, batches = 30, n = 5_000;
        Context ctx = Context.newBuilder("python").allowAllAccess(true).build();
        ctx.eval("python", PY);
        Value bind = ctx.getBindings("python");
        Value noop = bind.getMember("noop");
        Value fold = bind.getMember("fold");
        Value foldGuest = bind.getMember("fold_guest");
        Value foldSpin = bind.getMember("fold_spin");

        byte[] key = new byte[16];
        byte[] val = new byte[1024];
        byte[] acc = new byte[1024];
        Arrays.fill(key, (byte) 'k');
        Arrays.fill(val, (byte) 'v');

        System.out.printf("load: %.2f  warmup %d calls/arm (fold arms %d), %d batches x %d  (polyglot %s)%n",
                ManagementFactory.getOperatingSystemMXBean().getSystemLoadAverage(),
                warmup, warmup / 5, batches, n, Engine.create().getVersion());

        report("(e) graalpy no-op", bench(() -> noop.execute(key, val, acc), warmup, batches, n));
        double mFold = report("(e) graalpy fold (host byte[])",
                bench(() -> fold.execute(key, val, acc), warmup / 5, batches, n));
        report("(e) graalpy fold (guest bytearray)",
                bench(() -> foldGuest.execute(107), warmup / 5, batches, n));

        int count = 0;
        double delta = 0;
        for (int c : new int[]{4000, 8000, 16000, 32000, 64000, 128000}) {
            count = c;
            final int fc = c;
            double[] d = bench(() -> foldSpin.execute(key, val, acc, fc), 5_000, 10, n);
            delta = median(d) - mFold;
            if (delta >= 900) break;
        }
        System.out.printf("calibrated spin count %d -> ~%.0f ns extra%n", count, delta);
        final int fc = count;
        double mSpin = report("(e) graalpy fold+~1us spin",
                bench(() -> foldSpin.execute(key, val, acc, fc), 5_000, batches, n));
        System.out.printf("(e) instrument-check delta: %.1f ns (expect ~1000, spin calibrated to %.0f)%n",
                mSpin - mFold, delta);
        ctx.close();
    }
}
