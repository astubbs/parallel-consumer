// Copyright (C) 2026 Antony Stubbs and contributors

/**
 * Where the demo's sidecar binary comes from.
 *
 * **The client library never goes looking for a sidecar** - the application supplies an absolute
 * path, because that process is about to be handed the application's Kafka credentials. So this is
 * the *application's* answer to that question, and it is deliberately the same answer
 * `test/harness.ts` gives: a JVM from `JAVA_HOME`, and a classpath handed over in
 * `PC_PROXY_SIDECAR_CLASSPATH`. `demo/run.sh` and `demo/Dockerfile` are the two things that set it,
 * and both build it from the proxy module rather than downloading anything.
 *
 * It is NOT `PC_DEMO_SIDECAR_CLASSPATH`: `PC_DEMO_` is the flag namespace this demo publishes -
 * one variable per flag and no others - and the classpath is not a flag. The name is already this
 * module's, from the end-to-end test harness.
 */

import fs from "node:fs";
import path from "node:path";

import type { SidecarCommand } from "@parallel-consumer/proxy-client";

/** The real sidecar - not `TestModeMain`. This demo runs against a real broker. */
const MAIN_CLASS = "bz.stub.parallelconsumer.proxy.Main";

const CLASSPATH_VARIABLE = "PC_PROXY_SIDECAR_CLASSPATH";

const HOW_TO_GET_ONE =
  "start the demo through parallel-consumer-proxy-clients/parallel-consumer-proxy-client-typescript" +
  "/demo/run.sh, which builds the proxy module and sets it";

/**
 * The command the client library will spawn, supervise, and reap.
 *
 * It throws rather than falling back to anything, and the message names the entry point. A demo
 * that silently found *some* sidecar would be demonstrating a path no application should take.
 */
export function sidecarCommand(): SidecarCommand {
  const classpath = process.env[CLASSPATH_VARIABLE];
  if (classpath === undefined || classpath.trim().length === 0) {
    throw new Error(`${CLASSPATH_VARIABLE} is not set - ${HOW_TO_GET_ONE}`);
  }
  return {
    executable: javaBinary(),
    args: ["-cp", classpath.trim(), MAIN_CLASS],
    // The sidecar's own startup failures must be visible. A demo that hid them would fail with
    // "the sidecar did not print a port line" and no reason.
    stderr: "inherit",
  };
}

/** The JVM to run the sidecar with. */
function javaBinary(): string {
  const javaHome = process.env["JAVA_HOME"];
  if (javaHome !== undefined && javaHome.length > 0) {
    const candidate = path.join(javaHome, "bin", "java");
    if (fs.existsSync(candidate)) {
      return candidate;
    }
  }
  // A PATH lookup is acceptable HERE and nowhere else: this is an application choosing its own
  // sidecar's JVM, which is exactly the decision the library refuses to make on its behalf.
  const onPath = (process.env["PATH"] ?? "")
    .split(path.delimiter)
    .map((directory) => path.join(directory, "java"))
    .find((candidate) => fs.existsSync(candidate));
  if (onPath === undefined) {
    throw new Error("no JVM found: set JAVA_HOME, or put a JDK 17 java on PATH");
  }
  return onPath;
}
