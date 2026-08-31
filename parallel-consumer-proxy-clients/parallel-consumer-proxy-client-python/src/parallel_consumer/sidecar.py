# Copyright (C) 2026 Antony Stubbs and contributors

"""Spawning the sidecar proxy, and finding the port it bound.

The application supplies the binary's location **explicitly and absolutely**. This library never
searches ``PATH``, never resolves a relative path, and never accepts a directory an attacker
could influence - the client hands this process the Kafka credentials, so which binary runs is a
security decision and it belongs to the application, not to a lookup.

The binary is launched **directly, never through a shell**. A shell wrapper holds the write end
of the child's stdin, which defeats the proxy's parent-death watch and leaks a JVM that still
holds Kafka group membership.
"""

from __future__ import annotations

import collections
import contextlib
import dataclasses
import logging
import os
import pathlib
import subprocess
import threading

from .errors import SidecarError

__all__ = ["Sidecar", "SidecarCommand"]

log = logging.getLogger(__name__)

PORT_LINE_PREFIX = "port: "

_STARTUP_TAIL = 40
"""How many pre-port stdout lines to keep for a startup error message."""


@dataclasses.dataclass(frozen=True)
class SidecarCommand:
    """How to start the sidecar: an absolute executable, plus arguments that are not configuration.

    Nothing the proxy is configured with may travel here. Configuration is code and it travels in
    the ``Configure`` message; ``args`` exists for the few things that are properties of the
    *binary* rather than of the session - a JVM classpath, for instance.
    """

    executable: pathlib.Path
    args: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        executable = pathlib.Path(self.executable)
        if not executable.is_absolute():
            raise ValueError(
                f"the sidecar executable must be an absolute path, got {str(executable)!r} - "
                "this library never resolves the binary through PATH or a relative lookup"
            )
        if not executable.is_file():
            raise ValueError(f"no sidecar executable at {str(executable)!r}")
        object.__setattr__(self, "executable", executable)
        object.__setattr__(self, "args", tuple(self.args))

    @classmethod
    def coerce(cls, value: SidecarCommand | str | os.PathLike[str]) -> SidecarCommand:
        """Accepts a bare path for the common case where the sidecar takes no arguments."""
        if isinstance(value, SidecarCommand):
            return value
        return cls(pathlib.Path(value))


class Sidecar:
    """A running sidecar process, and its stdin - the lifeline that keeps it alive.

    Start it, read :attr:`port`, connect to loopback. Close it by closing its stdin: EOF there is
    the parent-death signal, and it is the only shutdown this class ever sends. Killing the
    process while a session is open turns a clean drain into a reconnect-window recovery for the
    next member of the consumer group.
    """

    def __init__(self, command: SidecarCommand) -> None:
        self._command = command
        self._process: subprocess.Popen[bytes] | None = None
        self._startup_lines: collections.deque[str] = collections.deque(maxlen=_STARTUP_TAIL)
        self.port: int | None = None

    def start(self, timeout: float = 60.0) -> int:
        """Spawns the process and returns the loopback port it reports on stdout.

        The specification has the port on stdout's first line. The test-mode harness writes log
        lines before it, so this scans for the line rather than asserting its position - a
        tolerance that costs nothing and keeps one code path for both.
        """
        if self._process is not None:
            raise RuntimeError("this sidecar has already been started")

        # No shell, no PATH lookup, no configuration by argv or environment: the argument vector
        # is exactly what the application named, and stdin stays open as the lifeline.
        self._process = subprocess.Popen(
            [str(self._command.executable), *self._command.args],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            # Kept and drained rather than discarded: a sidecar that dies before its port line
            # says why on stderr, and a startup error with no reason attached is the difference
            # between a five-second fix and an afternoon.
            stderr=subprocess.PIPE,
            shell=False,
        )
        threading.Thread(target=self._drain_stderr, name="pc-sidecar-stderr", daemon=True).start()

        port = self._await_port_line(timeout)
        self.port = port
        # Keep reading stdout for the process's life: a child whose stdout pipe fills up blocks
        # on its next log line, which would look exactly like a hung proxy.
        threading.Thread(target=self._drain_stdout, name="pc-sidecar-stdout", daemon=True).start()
        log.debug("sidecar listening on 127.0.0.1:%d", port)
        return port

    def close(self, timeout: float = 30.0) -> int | None:
        """Closes stdin - the parent-death signal - and reaps the process.

        Idempotent. A sidecar that has not exited when the timeout elapses is killed, because a
        leaked JVM still holds group membership; that is a fallback, not the normal path.
        """
        process = self._process
        if process is None:
            return None
        if process.stdin is not None and not process.stdin.closed:
            # An already-broken pipe is the same outcome: the child has had its EOF either way.
            with contextlib.suppress(OSError):
                process.stdin.close()
        try:
            return process.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            log.warning("sidecar did not exit within %.1fs of losing its parent; killing it",
                        timeout)
            process.kill()
            return process.wait(timeout=timeout)

    @property
    def returncode(self) -> int | None:
        return None if self._process is None else self._process.returncode

    def _await_port_line(self, timeout: float) -> int:
        assert self._process is not None and self._process.stdout is not None
        found: list[int] = []

        def scan() -> None:
            for raw in self._process.stdout:  # type: ignore[union-attr]
                line = raw.decode("utf-8", "replace").rstrip("\n")
                if line.startswith(PORT_LINE_PREFIX):
                    found.append(int(line[len(PORT_LINE_PREFIX):].strip()))
                    return
                self._startup_lines.append(line)

        scanner = threading.Thread(target=scan, name="pc-sidecar-startup", daemon=True)
        scanner.start()
        scanner.join(timeout)

        if not found:
            self.close(timeout=5.0)
            tail = " | ".join(self._startup_lines) or "(nothing)"
            raise SidecarError(
                f"the sidecar printed no '{PORT_LINE_PREFIX}<n>' line within {timeout:.0f}s "
                f"(exit code {self.returncode}); its last output was: {tail}"
            )
        return found[0]

    def _drain_stderr(self) -> None:
        assert self._process is not None and self._process.stderr is not None
        try:
            for raw in self._process.stderr:
                self._startup_lines.append(raw.decode("utf-8", "replace").rstrip("\n"))
        except (ValueError, OSError):
            pass  # the pipe closed under us - the process is gone

    def _drain_stdout(self) -> None:
        assert self._process is not None and self._process.stdout is not None
        try:
            for _ in self._process.stdout:
                # The sidecar's own logging. Deliberately discarded rather than re-logged: it is
                # already the proxy's to record, and re-emitting it here would put text this
                # library did not compose into the application's logs.
                pass
        except (ValueError, OSError):
            pass  # the pipe closed under us - the process is gone, which close() reports


def spawn(command: SidecarCommand | str | os.PathLike[str], timeout: float = 60.0) -> Sidecar:
    """Convenience: coerce, start, return the running sidecar."""
    sidecar = Sidecar(SidecarCommand.coerce(command))
    sidecar.start(timeout)
    return sidecar
