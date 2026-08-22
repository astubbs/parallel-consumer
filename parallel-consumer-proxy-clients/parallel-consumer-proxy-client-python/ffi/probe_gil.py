# Copyright (C) 2026 Antony Stubbs and contributors

"""Does the pull model actually dissolve Python's GIL problem?

This is the load-bearing unproven claim behind binding Python at all. The parked FFI note ranks
Python "hard mechanically" because a callback-based FFI has to take the GIL per call. The pull
model has no callbacks - Python calls OUT and blocks - so the claim is that ``ctypes`` releases the
GIL for the duration and other Python threads keep running.

That is reasoning, not measurement, so this measures it.

THE CONTROL ARM IS THE POINT. ``CDLL`` releases the GIL around a foreign call; ``PyDLL`` does not.
Same library, same function, same blocking duration, one flag different - so if the counter runs
under CDLL and stalls under PyDLL, the cause is GIL release and not something incidental about the
call being slow.

Prediction, recorded before the first run: CDLL keeps the busy thread near its baseline rate;
PyDLL drops it to approximately zero.
"""

from __future__ import annotations

import ctypes
import os
import pathlib
import sys
import threading
import time

BLOCK_MS = 2000
ERR_TIMEOUT = -3


def library_path() -> pathlib.Path:
    override = os.environ.get("PC_EMBEDDED_LIBRARY")
    if override:
        return pathlib.Path(override)
    suffix = "dylib" if sys.platform == "darwin" else "so"
    here = pathlib.Path(__file__).resolve().parent
    return (here / ".." / ".." / "parallel-consumer-proxy-client-go" / "ffi" / "build"
            / f"libpc.{suffix}").resolve()


def declare(lib: ctypes.CDLL) -> None:
    """ctypes defaults every return type to int, which truncates a 64-bit handle on some ABIs."""
    lib.graal_create_isolate.restype = ctypes.c_int
    lib.graal_create_isolate.argtypes = [ctypes.c_void_p,
                                         ctypes.POINTER(ctypes.c_void_p),
                                         ctypes.POINTER(ctypes.c_void_p)]
    lib.graal_get_current_thread.restype = ctypes.c_void_p
    lib.graal_get_current_thread.argtypes = [ctypes.c_void_p]
    lib.graal_attach_thread.restype = ctypes.c_int
    lib.graal_attach_thread.argtypes = [ctypes.c_void_p, ctypes.POINTER(ctypes.c_void_p)]
    lib.pc_session_open.restype = ctypes.c_longlong
    lib.pc_session_open.argtypes = [ctypes.c_void_p]
    lib.pc_next.restype = ctypes.c_int
    lib.pc_next.argtypes = [ctypes.c_void_p, ctypes.c_longlong, ctypes.c_char_p, ctypes.c_int,
                            ctypes.POINTER(ctypes.c_int), ctypes.c_int]


def busy_count(stop: threading.Event) -> list[int]:
    """A pure-Python spinner. Its rate is the whole measurement."""
    counter = [0]

    def spin() -> None:
        while not stop.is_set():
            counter[0] += 1

    threading.Thread(target=spin, daemon=True).start()
    return counter


def measure(lib: ctypes.CDLL, label: str) -> int:
    """Blocks in pc_next for BLOCK_MS and reports how far a Python thread got meanwhile."""
    declare(lib)

    isolate = ctypes.c_void_p()
    thread = ctypes.c_void_p()
    if lib.graal_create_isolate(None, ctypes.byref(isolate), ctypes.byref(thread)) != 0:
        raise SystemExit(f"{label}: graal_create_isolate failed")

    handle = lib.pc_session_open(thread)
    if handle <= 0:
        raise SystemExit(f"{label}: pc_session_open returned {handle}")

    # Nothing is queued on this session, so pc_next is guaranteed to block for the full timeout and
    # return ERR_TIMEOUT. That makes the blocking duration a constant rather than a variable.
    buf = ctypes.create_string_buffer(4096)
    written = ctypes.c_int()

    stop = threading.Event()
    counter = busy_count(stop)
    time.sleep(0.2)                      # let the spinner reach a steady rate
    before = counter[0]
    started = time.monotonic()
    rc = lib.pc_next(thread, handle, buf, len(buf), ctypes.byref(written), BLOCK_MS)
    elapsed = time.monotonic() - started
    ticks = counter[0] - before
    stop.set()

    if rc != ERR_TIMEOUT:
        print(f"  NOTE {label}: pc_next returned {rc}, expected ERR_TIMEOUT ({ERR_TIMEOUT})")
    print(f"  {label:6s} blocked {elapsed:.2f}s, the Python thread advanced {ticks:,} times")
    return ticks


def main() -> int:
    path = library_path()
    if not path.exists():
        print(f"no shared library at {path}\n"
              f"build it with parallel-consumer-proxy-client-go/ffi/build-shared-library.sh session,"
              f" or set PC_EMBEDDED_LIBRARY")
        return 2
    print(f"library: {path}\nblocking for {BLOCK_MS}ms in pc_next, twice\n")

    # CDLL releases the GIL around the call. PyDLL does not. One flag, everything else identical.
    released = measure(ctypes.CDLL(str(path)), "CDLL")
    held = measure(ctypes.PyDLL(str(path)), "PyDLL")

    print()
    if held == 0:
        print("  PyDLL stalled the interpreter completely")
    else:
        print(f"  ratio: CDLL advanced {released / held:.0f}x further than PyDLL")

    # A tenfold difference is far beyond scheduling noise, and the absolute numbers differ by
    # machine, so the assertion is on the ratio rather than on any particular rate.
    if released > held * 10:
        print("\nPASS  ctypes releases the GIL: a blocking pull does NOT stall other Python threads.")
        print("      The pull model removes the GIL objection to embedding in Python.")
        return 0
    print("\nFAIL  the blocking pull stalled the interpreter - the GIL objection stands.")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
