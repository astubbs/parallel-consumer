// Copyright (C) 2026 Antony Stubbs and contributors

import { type ChildProcessByStdio, spawn } from "node:child_process";
import type { Readable, Writable } from "node:stream";
import * as readline from "node:readline";

import { SidecarError } from "./errors";
import type { ResolvedOptions, SidecarCommand } from "./options";

/** The lifecycle channel's entire vocabulary: the proxy prints `port: <n>` and connects follow. */
const PORT_LINE = /^port:\s*(\d+)\s*$/;

/**
 * The sidecar child process and the stdin pipe that keeps it honest.
 *
 * THE PIPE IS THE PARENT-DEATH SIGNAL. This process holds the write end and never writes to it, so
 * EOF on the child's stdin is proof the parent is gone and the proxy exits on its own. That is why
 * the binary is launched DIRECTLY and never through a shell: a shell wrapper would hold the write
 * end open and leak a JVM that still holds Kafka group membership.
 */
export class Sidecar {
  private constructor(
    private readonly child: ChildProcessByStdio<Writable, Readable, null>,
    readonly port: number,
  ) {}

  /** Spawns the sidecar and resolves once it has announced its port. */
  static async start(command: SidecarCommand, options: ResolvedOptions): Promise<Sidecar> {
    // spawn without `shell` - see the class comment. stdin stays piped and unwritten.
    const child: ChildProcessByStdio<Writable, Readable, null> = spawn(
      command.executable,
      [...(command.args ?? [])],
      { stdio: ["pipe", "pipe", command.stderr === "ignore" ? "ignore" : "inherit"] },
    );

    try {
      const port = await readPort(child, options.startupTimeoutMs);
      return new Sidecar(child, port);
    } catch (error) {
      child.kill("SIGKILL");
      throw error;
    }
  }

  get exited(): boolean {
    return this.child.exitCode !== null || this.child.signalCode !== null;
  }

  /**
   * Closes the lifecycle pipe and reaps the child.
   *
   * Closing stdin IS the reap - it is the parent-death signal the proxy watches, and it is the only
   * thing that ends the test-mode harness, which serves until stdin EOF and does not exit after a
   * clean drain. The kill is the backstop for a child that honours neither.
   */
  async stop(graceMs: number): Promise<void> {
    if (this.exited) {
      return;
    }
    this.child.stdin.end();
    const exited = new Promise<void>((resolve) => this.child.once("exit", () => { resolve(); }));
    let timer: NodeJS.Timeout | undefined;
    const grace = new Promise<"timeout">((resolve) => {
      timer = setTimeout(() => { resolve("timeout"); }, graceMs);
      timer.unref();
    });
    const winner = await Promise.race([exited.then(() => "exited" as const), grace]);
    if (timer !== undefined) {
      clearTimeout(timer);
    }
    if (winner === "timeout") {
      this.child.kill("SIGKILL");
      await exited;
    }
  }
}

/**
 * Scans the lifecycle channel for the port line.
 *
 * The specification's contract is that the port is stdout's FIRST line. The conformance harness
 * diverges - it logs before it - and the guide says a test absorbs that rather than asserting the
 * position, so this scans for the line instead of reading exactly one. Scanning satisfies both, and
 * reading continues afterwards so a chatty child never blocks on a full stdout pipe.
 */
function readPort(
  child: ChildProcessByStdio<Writable, Readable, null>,
  timeoutMs: number,
): Promise<number> {
  return new Promise<number>((resolve, reject) => {
    const lines = readline.createInterface({ input: child.stdout });
    let settled = false;
    const settle = (act: () => void) => {
      if (settled) {
        return;
      }
      settled = true;
      clearTimeout(timer);
      // The interface keeps consuming stdout after the port line; only its events stop mattering.
      lines.removeAllListeners("line");
      lines.resume();
      act();
    };

    const timer = setTimeout(() => {
      settle(() => {
        reject(new SidecarError(`the sidecar did not print a "port: <n>" line within ${timeoutMs}ms`));
      });
    }, timeoutMs);

    lines.on("line", (line: string) => {
      const match = PORT_LINE.exec(line);
      if (match?.[1] !== undefined) {
        const port = Number.parseInt(match[1], 10);
        settle(() => { resolve(port); });
      }
    });
    lines.on("close", () => {
      settle(() => {
        reject(new SidecarError('the sidecar\'s stdout ended before a "port: <n>" line'));
      });
    });
    child.on("error", (error: Error) => {
      settle(() => {
        reject(new SidecarError(`the sidecar could not be started: ${error.message}`, { cause: error }));
      });
    });
  });
}
