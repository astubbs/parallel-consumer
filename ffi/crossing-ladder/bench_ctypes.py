# Copyright (C) 2026 Antony Stubbs and contributors
#
# Crossing-cost ladder arm (c): what Python pays to call C through ctypes, and (c') what a
# native caller pays to call the same functions through a bare function pointer (the
# engine-side proxy). Measures no-op, the 1KB fold, and the ~1us busy-wait instrument
# check, as ns/call distributions (median + p99 over batches).
# See docs/inflight/perf-crossing-cost-ladder.md.
#
# Usage: python3 bench_ctypes.py [batches] [calls-per-batch]

import ctypes as ct
import os
import statistics
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
lib = ct.CDLL(os.path.join(HERE, "libfold.so"))

SIG = [ct.c_char_p, ct.c_int32, ct.c_char_p, ct.c_int32, ct.c_char_p, ct.c_int32]
for name in ("pc_noop", "pc_fold", "pc_fold_spin"):
    fn = getattr(lib, name)
    fn.argtypes = SIG
    fn.restype = ct.c_int32

FOLD_FN = ct.CFUNCTYPE(ct.c_int32, ct.c_char_p, ct.c_int32, ct.c_char_p, ct.c_int32,
                       ct.c_char_p, ct.c_int32)
lib.pc_drive_ptr.argtypes = [FOLD_FN, ct.c_uint64] + SIG + [ct.POINTER(ct.c_int32)]
lib.pc_drive_ptr.restype = ct.c_uint64

KEY = ct.create_string_buffer(b"k" * 16, 16)
VAL = ct.create_string_buffer(b"v" * 1024, 1024)
ACC = ct.create_string_buffer(b"\x00" * 1024, 1024)

def bench_python_side(fn, warmup, batches, n):
    """ns/call: Python loop calling into C; the Python-side marshalling is part of the cost."""
    for _ in range(warmup):
        fn(KEY, 16, VAL, 1024, ACC, 1024)
    per_batch = []
    for _ in range(batches):
        t0 = time.perf_counter_ns()
        for _ in range(n):
            fn(KEY, 16, VAL, 1024, ACC, 1024)
        per_batch.append((time.perf_counter_ns() - t0) / n)
    return per_batch

def bench_c_side(name, warmup, batches, n):
    """ns/call: the C driver loops over a function POINTER, timed inside C - engine-side proxy."""
    ptr = FOLD_FN((name, lib))
    sink = ct.c_int32(0)
    lib.pc_drive_ptr(ptr, warmup, KEY, 16, VAL, 1024, ACC, 1024, ct.byref(sink))
    per_batch = []
    for _ in range(batches):
        ns = lib.pc_drive_ptr(ptr, n, KEY, 16, VAL, 1024, ACC, 1024, ct.byref(sink))
        per_batch.append(ns / n)
    return per_batch

def report(label, per_batch):
    med = statistics.median(per_batch)
    p99 = sorted(per_batch)[max(0, int(len(per_batch) * 0.99) - 1)]
    print(f"{label:34s} median {med:10.1f} ns/call  p99 {p99:10.1f}  batches {len(per_batch)}")
    return med

if __name__ == "__main__":
    batches = int(sys.argv[1]) if len(sys.argv) > 1 else 50
    n = int(sys.argv[2]) if len(sys.argv) > 2 else 20000
    print(f"load: {os.getloadavg()[0]:.2f}  warmup 100000 calls/arm, {batches} batches x {n}")
    m_noop = report("(c)  ctypes py->c no-op", bench_python_side(lib.pc_noop, 100000, batches, n))
    m_fold = report("(c)  ctypes py->c fold", bench_python_side(lib.pc_fold, 100000, batches, n))
    m_spin = report("(c)  ctypes py->c fold+1us spin", bench_python_side(lib.pc_fold_spin, 10000, batches, n))
    print(f"(c)  instrument-check delta: {m_spin - m_fold:.1f} ns (expect ~1000)")
    c_noop = report("(c') C drive ptr    no-op", bench_c_side("pc_noop", 100000, batches, n))
    c_fold = report("(c') C drive ptr    fold", bench_c_side("pc_fold", 100000, batches, n))
    c_spin = report("(c') C drive ptr    fold+1us spin", bench_c_side("pc_fold_spin", 10000, batches, n))
    print(f"(c') instrument-check delta: {c_spin - c_fold:.1f} ns (expect ~1000)")
