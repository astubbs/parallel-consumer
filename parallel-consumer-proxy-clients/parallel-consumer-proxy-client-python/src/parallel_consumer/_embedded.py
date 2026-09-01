# Copyright (C) 2026 Antony Stubbs and contributors

"""Parallel Consumer linked into this Python process, reached over a C ABI.

The engine is a GraalVM ``--shared`` library built from the proxy's own session core. There is no
sidecar process, no gRPC and no JVM: the frames that would have gone onto a stream are handed
across a function call instead, byte for byte identical.

**Why this is not the "hard" case the FFI notes predicted for Python.** Those notes assume the
engine calls *out* into Python on many threads, which needs the GIL per call and is genuinely
awkward. Nothing here calls out. Python calls in and blocks, and ``ctypes`` releases the GIL for
the duration of a foreign call - measured at 1142x against a ``PyDLL`` control in
``ffi/probe_gil.py``, so a blocking pull does not stall the interpreter.

``ctypes`` rather than ``cffi`` because it is in the standard library, and the library this binds
is already an optional extra.
"""

from __future__ import annotations

import ctypes
import os
import pathlib
import sys
import threading
from collections.abc import Iterator
from typing import Any

from ._generated import proxy_pb2 as pb

OK = 0
ERR_NO_SESSION = -1
ERR_BUFFER_TOO_SMALL = -2
ERR_TIMEOUT = -3
ERR_SESSION_ENDED = -4
ERR_BAD_FRAME = -5
ERR_INTERNAL = -6

_CODE_NAMES = {
    OK: "OK", ERR_NO_SESSION: "ERR_NO_SESSION", ERR_BUFFER_TOO_SMALL: "ERR_BUFFER_TOO_SMALL",
    ERR_TIMEOUT: "ERR_TIMEOUT", ERR_SESSION_ENDED: "ERR_SESSION_ENDED",
    ERR_BAD_FRAME: "ERR_BAD_FRAME", ERR_INTERNAL: "ERR_INTERNAL",
}

#: One pull blocks for at most this long. A timeout is not end-of-stream - it means idle, and the
#: reader asks again. Short enough that a half-close is noticed promptly, long enough not to spin.
_POLL_MILLIS = 200

_READ_BUFFER = 64 * 1024


def code_name(code: int) -> str:
    return _CODE_NAMES.get(code, f"unknown({code})")


class EmbeddedError(RuntimeError):
    """The embedded engine failed. The transport's equivalent of ``grpc.RpcError``."""


def library_path() -> pathlib.Path:
    """Where the shared library is. ``PC_EMBEDDED_LIBRARY`` overrides."""
    override = os.environ.get("PC_EMBEDDED_LIBRARY")
    if override:
        return pathlib.Path(override)
    suffix = "dylib" if sys.platform == "darwin" else "so"
    return pathlib.Path(__file__).resolve().parents[2] / "ffi" / "build" / f"libpc.{suffix}"


class _Library:
    """The loaded library and its one isolate, created once per process.

    An isolate is a heap-sized VM instance. One per session would multiply the footprint for no
    benefit, since sessions are already separated by handle.
    """

    _instance: _Library | None = None
    _lock = threading.Lock()

    def __init__(self, path: pathlib.Path) -> None:
        if not path.exists():
            raise EmbeddedError(
                f"no embedded engine at {path}. Build it with "
                f"parallel-consumer-proxy-client-go/ffi/build-shared-library.sh session, "
                f"or set PC_EMBEDDED_LIBRARY")
        # CDLL, never PyDLL: CDLL releases the GIL around the call and PyDLL does not, which is
        # the entire reason a blocking pull is viable in Python. See ffi/probe_gil.py.
        self.lib = ctypes.CDLL(str(path))
        self._declare()
        isolate = ctypes.c_void_p()
        thread = ctypes.c_void_p()
        if self.lib.graal_create_isolate(None, ctypes.byref(isolate), ctypes.byref(thread)) != 0:
            raise EmbeddedError("graal_create_isolate failed")
        self.isolate = isolate

    def _declare(self) -> None:
        """ctypes defaults every return to int, which truncates a 64-bit handle."""
        lib = self.lib
        lib.graal_create_isolate.restype = ctypes.c_int
        lib.graal_create_isolate.argtypes = [ctypes.c_void_p, ctypes.POINTER(ctypes.c_void_p),
                                             ctypes.POINTER(ctypes.c_void_p)]
        lib.graal_get_current_thread.restype = ctypes.c_void_p
        lib.graal_get_current_thread.argtypes = [ctypes.c_void_p]
        lib.graal_attach_thread.restype = ctypes.c_int
        lib.graal_attach_thread.argtypes = [ctypes.c_void_p, ctypes.POINTER(ctypes.c_void_p)]
        lib.pc_session_open.restype = ctypes.c_longlong
        lib.pc_session_open.argtypes = [ctypes.c_void_p]
        lib.pc_send.restype = ctypes.c_int
        lib.pc_send.argtypes = [ctypes.c_void_p, ctypes.c_longlong, ctypes.c_char_p, ctypes.c_int]
        lib.pc_next.restype = ctypes.c_int
        lib.pc_next.argtypes = [ctypes.c_void_p, ctypes.c_longlong, ctypes.c_char_p, ctypes.c_int,
                                ctypes.POINTER(ctypes.c_int), ctypes.c_int]
        lib.pc_session_close.restype = ctypes.c_int
        lib.pc_session_close.argtypes = [ctypes.c_void_p, ctypes.c_longlong]
        lib.pc_last_error.restype = ctypes.c_int
        lib.pc_last_error.argtypes = [ctypes.c_void_p, ctypes.c_longlong, ctypes.c_char_p,
                                      ctypes.c_int]

    @classmethod
    def get(cls) -> _Library:
        with cls._lock:
            if cls._instance is None:
                cls._instance = cls(library_path())
            return cls._instance

    def thread(self) -> ctypes.c_void_p:
        """An isolate thread valid for the CALLING OS thread.

        Never cached. A GraalVM isolate thread belongs to the OS thread it was attached on, and
        reusing one from another thread corrupts memory rather than raising. Python threads are OS
        threads and do not migrate, but the session runs three of them, so the lookup is still per
        call - and the rule is the same one every binding must follow.
        """
        existing = self.lib.graal_get_current_thread(self.isolate)
        if existing:
            return ctypes.c_void_p(existing)
        thread = ctypes.c_void_p()
        if self.lib.graal_attach_thread(self.isolate, ctypes.byref(thread)) != 0:
            raise EmbeddedError("graal_attach_thread failed for this thread")
        return thread


class EmbeddedTransport:
    """A session carried by a function call. Satisfies ``_transport.Transport``."""

    errors: tuple[type[BaseException], ...] = (EmbeddedError,)

    def __init__(self, first_message: pb.ClientMessage) -> None:
        self._library = _Library.get()
        handle = self._library.lib.pc_session_open(self._library.thread())
        if handle <= 0:
            raise EmbeddedError(f"pc_session_open returned {code_name(int(handle))}")
        self._handle = ctypes.c_longlong(handle)
        self._half_closed = threading.Event()
        self.responses: Iterator[Any] = self._read()
        self.send(first_message)

    def send(self, message: pb.ClientMessage) -> None:
        frame = message.SerializeToString()
        thread = self._library.thread()
        rc = self._library.lib.pc_send(thread, self._handle, frame, len(frame))
        if rc == OK:
            return
        if rc in (ERR_SESSION_ENDED, ERR_NO_SESSION):
            raise EmbeddedError("the session has ended")
        raise EmbeddedError(f"pc_send returned {code_name(rc)}: {self._last_error(thread)}")

    def _read(self) -> Iterator[Any]:
        buf = ctypes.create_string_buffer(_READ_BUFFER)
        written = ctypes.c_int()
        while True:
            if self._half_closed.is_set():
                return
            thread = self._library.thread()
            rc = self._library.lib.pc_next(thread, self._handle, buf, len(buf),
                                           ctypes.byref(written), _POLL_MILLIS)
            if rc == OK:
                message = pb.ProxyMessage()
                message.ParseFromString(buf.raw[:written.value])
                yield message
            elif rc == ERR_TIMEOUT:
                continue                      # idle, not ended
            elif rc == ERR_BUFFER_TOO_SMALL:
                # The frame is still queued and written carries the size it needs, which is why
                # pc_next puts it back rather than dropping it.
                buf = ctypes.create_string_buffer(written.value)
            elif rc in (ERR_SESSION_ENDED, ERR_NO_SESSION):
                return
            else:
                raise EmbeddedError(f"pc_next returned {code_name(rc)}: {self._last_error(thread)}")

    def half_close(self) -> None:
        self._half_closed.set()

    def cancel(self) -> None:
        self.half_close()

    def close(self) -> None:
        self._half_closed.set()
        rc = self._library.lib.pc_session_close(self._library.thread(), self._handle)
        if rc not in (OK, ERR_NO_SESSION):
            raise EmbeddedError(f"pc_session_close returned {code_name(rc)}")

    def describe(self, error: BaseException) -> str:
        return str(error)

    def _last_error(self, thread: ctypes.c_void_p) -> str:
        buf = ctypes.create_string_buffer(8192)
        n = self._library.lib.pc_last_error(thread, self._handle, buf, len(buf))
        return buf.raw[:n].decode("utf-8", "replace") if n > 0 else "(no detail recorded)"
