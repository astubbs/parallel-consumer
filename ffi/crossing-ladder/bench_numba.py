# Copyright (C) 2026 Antony Stubbs and contributors
#
# Crossing-cost ladder arm (d): a Numba @cfunc-compiled fold, called through its raw
# function POINTER by the C driver in libfold.so - the compile-the-function shape, where
# the engine holds a registered pointer and Python is not in the call path at all.
# Timing is done inside C (pc_drive_ptr). The ~1us instrument check is a Numba-side
# clock-calibrated spin (numba can call ctypes-wrapped clock_gettime? no - it spins on a
# calibrated iteration count instead, and the calibration is printed).
# See docs/inflight/perf-crossing-cost-ladder.md.
#
# Run inside the numba venv: numba-venv/bin/python bench_numba.py [batches] [calls-per-batch]

import ctypes as ct
import os
import statistics
import sys
import time

from numba import cfunc, types, njit

HERE = os.path.dirname(os.path.abspath(__file__))
lib = ct.CDLL(os.path.join(HERE, "libfold.so"))

SIG_C = [ct.c_char_p, ct.c_int32, ct.c_char_p, ct.c_int32, ct.c_char_p, ct.c_int32]
FOLD_FN = ct.CFUNCTYPE(ct.c_int32, *SIG_C)
lib.pc_drive_ptr.argtypes = [FOLD_FN, ct.c_uint64] + SIG_C + [ct.POINTER(ct.c_int32)]
lib.pc_drive_ptr.restype = ct.c_uint64

# the fold, in the numba nopython subset: byte pointers as CPointer(uint8)
u8p = types.CPointer(types.uint8)
fold_sig = types.int32(u8p, types.int32, u8p, types.int32, u8p, types.int32)

@cfunc(fold_sig, nopython=True, cache=False)
def nb_noop(key, klen, val, vlen, acc, alen):
    return 0

@cfunc(fold_sig, nopython=True, cache=False)
def nb_fold(key, klen, val, vlen, acc, alen):
    n = vlen if vlen < alen else alen
    k = key[0] if klen > 0 else 0
    for i in range(n):
        acc[i] = (acc[i] + val[i] + k) & 0xFF
    return acc[n - 1] if n > 0 else 0

# instrument check: calibrated spin. SPIN_COUNT is patched below after calibration.
def make_spin(count):
    @cfunc(fold_sig, nopython=True, cache=False)
    def nb_fold_spin(key, klen, val, vlen, acc, alen):
        n = vlen if vlen < alen else alen
        k = key[0] if klen > 0 else 0
        for i in range(n):
            acc[i] = (acc[i] + val[i] + k) & 0xFF
        # serial data-dependent chain, result written to observable memory - not eliminable
        s = acc[0]
        for i in range(count):
            s = (s * 31 + i) & 0xFF
        acc[0] = s
        return acc[n - 1] if n > 0 else 0
    return nb_fold_spin

KEY = ct.create_string_buffer(b"k" * 16, 16)
VAL = ct.create_string_buffer(b"v" * 1024, 1024)
ACC = ct.create_string_buffer(b"\x00" * 1024, 1024)
SINK = ct.c_int32(0)

def drive(address, warmup, batches, n):
    ptr = ct.cast(address, FOLD_FN)
    lib.pc_drive_ptr(ptr, warmup, KEY, 16, VAL, 1024, ACC, 1024, ct.byref(SINK))
    per_batch = []
    for _ in range(batches):
        ns = lib.pc_drive_ptr(ptr, n, KEY, 16, VAL, 1024, ACC, 1024, ct.byref(SINK))
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

    # calibrate the spin count to ~1us by measuring the fold-with-spin against the fold
    base = drive(nb_fold.address, 100000, 10, n)
    count, delta = 0, 0.0
    for count in (4000, 8000, 16000, 32000, 64000):
        spin = make_spin(count)
        d = drive(spin.address, 10000, 10, n)
        delta = statistics.median(d) - statistics.median(base)
        if delta >= 900:
            break
    print(f"calibrated spin count {count} -> ~{delta:.0f} ns extra")

    m_noop = report("(d) numba cfunc ptr no-op", drive(nb_noop.address, 100000, batches, n))
    m_fold = report("(d) numba cfunc ptr fold", drive(nb_fold.address, 100000, batches, n))
    spin = make_spin(count)
    m_spin = report("(d) numba cfunc ptr fold+~1us spin", drive(spin.address, 10000, batches, n))
    print(f"(d) instrument-check delta: {m_spin - m_fold:.1f} ns (expect ~1000, spin calibrated to {delta:.0f})")
