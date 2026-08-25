/*
 * Copyright (C) 2026 Antony Stubbs and contributors
 *
 * Crossing-cost ladder arm (f), the PRIMARY candidate per owner direction: a WASM UDF
 * called through the GraalWasm polyglot API from a JVM host. Measures, post-warmup:
 * no-op export call, the 1KB fold with bytes STAGED in wasm linear memory (f1 - call
 * overhead + wasm compute), the fold with 1KB copied into wasm memory per call through
 * the polyglot buffer API (f2 - what unstaged data handoff costs), and the calibrated
 * ~1us spin instrument check. See docs/inflight/perf-crossing-cost-ladder.md.
 *
 * Run on the GraalVM 25 JDK with the graal/target/deps classpath (Truffle needs the
 * Graal compiler to reach peak; on a stock JDK it interprets).
 */
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.Source;
import org.graalvm.polyglot.Value;
import org.graalvm.polyglot.io.ByteSequence;

public class GraalWasmBench {

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
        System.out.printf("%-36s median %10.1f ns/call  p99 %10.1f  batches %d%n",
                label, median(perBatch), p99(perBatch), perBatch.length);
        return median(perBatch);
    }

    public static void main(String[] args) throws IOException {
        int warmup = 200_000, batches = 30, n = 20_000;
        byte[] wasmBytes = Files.readAllBytes(Path.of(args.length > 0 ? args[0] : "fold.wasm"));
        Context ctx = Context.newBuilder("wasm").allowAllAccess(true).build();
        Source src = Source.newBuilder("wasm", ByteSequence.create(wasmBytes), "fold").build();
        Value module = ctx.eval(src);
        Value exports;
        if (module.canInstantiate()) {
            exports = module.newInstance().getMember("exports");
        } else {
            exports = ctx.getBindings("wasm").getMember("fold"); // older API: instance in bindings
        }
        Value noop = exports.getMember("noop");
        Value fold = exports.getMember("fold");
        Value foldSpin = exports.getMember("fold_spin");
        Value memory = exports.getMember("memory");
        int bp = exports.getMember("buf_ptr").execute().asInt();
        int keyOff = bp, valOff = bp + 16, accOff = bp + 1040;

        // stage key + value bytes into wasm linear memory once (f1's premise)
        for (int i = 0; i < 16; i++) memory.writeBufferByte(keyOff + i, (byte) 'k');
        for (int i = 0; i < 1024; i++) memory.writeBufferByte(valOff + i, (byte) 'v');

        System.out.printf("load: %.2f  warmup %d calls/arm, %d batches x %d  (polyglot %s)%n",
                ManagementFactory.getOperatingSystemMXBean().getSystemLoadAverage(), warmup, batches, n, org.graalvm.polyglot.Engine.create().getVersion());

        double mNoop = report("(f)  graalwasm no-op",
                bench(() -> noop.execute(keyOff, 16, valOff, 1024, accOff, 1024), warmup, batches, n));
        double mFold = report("(f)  graalwasm fold (bytes staged)",
                bench(() -> fold.execute(keyOff, 16, valOff, 1024, accOff, 1024), warmup, batches, n));

        // calibrate the spin chain to ~1us
        int count = 0;
        double delta = 0;
        for (int c : new int[]{4000, 8000, 16000, 32000, 64000, 128000}) {
            count = c;
            final int fc = c;
            double[] d = bench(() -> foldSpin.execute(keyOff, 16, valOff, 1024, accOff, 1024, fc),
                    20_000, 10, n);
            delta = median(d) - mFold;
            if (delta >= 900) break;
        }
        System.out.printf("calibrated spin count %d -> ~%.0f ns extra%n", count, delta);
        final int fc = count;
        double mSpin = report("(f)  graalwasm fold+~1us spin",
                bench(() -> foldSpin.execute(keyOff, 16, valOff, 1024, accOff, 1024, fc), 20_000, batches, n));
        System.out.printf("(f)  instrument-check delta: %.1f ns (expect ~1000, spin calibrated to %.0f)%n",
                mSpin - mFold, delta);

        // f2: per-call 1KB copy through the polyglot buffer API, then the fold
        byte[] hostVal = new byte[1024];
        Arrays.fill(hostVal, (byte) 'v');
        double mF2 = report("(f2) graalwasm 1KB copy/call + fold",
                bench(() -> {
                    for (int i = 0; i < 1024; i++) memory.writeBufferByte(valOff + i, hostVal[i]);
                    fold.execute(keyOff, 16, valOff, 1024, accOff, 1024);
                }, 50_000, batches, n));
        System.out.printf("(f2) copy overhead over staged fold: %.1f ns%n", mF2 - mFold);
        ctx.close();
    }
}
