// Copyright (C) 2026 Antony Stubbs and contributors

/**
 * Locates the JVM-side sidecar, so a Node test can spawn it as an ordinary child process.
 *
 * The sidecar is `parallel-consumer-proxy`'s production `Main`, which is a classpath invocation
 * rather than a binary path. Maven writes that classpath to `target/sidecar-classpath.txt` (the
 * `typescript-sidecar-harness` profile in this module's pom.xml). Everything awkward about that
 * lives here rather than in each test.
 *
 * WHAT THIS BUILD'S SIDECAR DOES. It hosts no Parallel Consumer engine: it binds, announces its
 * port, admits one connection under the transport's rules, and answers every session
 * `UNIMPLEMENTED` (astubbs/parallel-consumer#384). So a test that spawns it exercises the whole
 * client-side path up to and including the handshake, and stops exactly where the engine would
 * begin. The dispatch scenarios below are the shared conformance suite's identities, and they are
 * deferred until an engine exists to run them against - nothing here stands in for one.
 *
 * This is test scaffolding. Nothing here is part of the library's surface, and the library itself
 * knows only what any application knows: a path to a binary and arguments that are not
 * configuration.
 */

import fs from "node:fs";
import path from "node:path";

import type { SidecarCommand } from "../src/index";

const MODULE_ROOT = path.resolve(__dirname, "..", "..");
const CLASSPATH_FILE = path.join(MODULE_ROOT, "target", "sidecar-classpath.txt");
const MAIN_CLASS = "bz.stub.parallelconsumer.proxy.Main";

/** What the sidecar's refusal must name, so a client author does not debug their own code. */
export const NO_ENGINE_DESCRIPTION = "hosts no Parallel Consumer engine";

/**
 * The conformance scenario names. A scenario name is its identity everywhere - the harness CLI,
 * this list, and the test names that run it - and it is ALSO the topic name, because the harness
 * seeds its records on the topic it is named after.
 */
export const Scenario = {
  processedRecordAdvancesTheCommittedOffset: "a-processed-record-advances-the-committed-offset",
  unreportedRecordHoldsBackTheCommit: "an-unreported-record-holds-back-the-commit",
  failedRecordIsRedeliveredWithItsFailureHistory:
    "a-failed-record-is-redelivered-with-its-failure-history",
  recordsSharingAKeyShareAShard:
    "records-sharing-a-key-share-a-shard-distinct-keys-run-concurrently",
} as const;

const HOW_TO_BUILD_IT =
  "run `./mvnw -pl :parallel-consumer-proxy-client-typescript -am -Dpc.foreignClients " +
  "-DskipTests generate-test-resources` from the repository root, which is the same wiring the " +
  "CI matrix row uses";

/**
 * Builds the launch command for the real sidecar shell.
 *
 * NO ARGUMENTS, and that is the sidecar's own rule rather than this function being terse: it takes
 * none and refuses to start when given one, because everything is configured connect-time over the
 * protocol.
 *
 * It THROWS rather than skipping when the sidecar is not built. A test that quietly does not run is
 * not a passing test, and nothing goes red to say so; the message names the command instead.
 */
export function engineLessSidecar(): SidecarCommand {
  return {
    executable: javaBinary(),
    args: ["-cp", sidecarClasspath(), MAIN_CLASS],
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
  // A PATH lookup is acceptable HERE and nowhere else: this is test scaffolding choosing a JVM,
  // not a client library choosing which sidecar receives an application's Kafka credentials.
  const onPath = (process.env["PATH"] ?? "")
    .split(path.delimiter)
    .map((directory) => path.join(directory, "java"))
    .find((candidate) => fs.existsSync(candidate));
  if (onPath === undefined) {
    throw new Error("no JVM found: set JAVA_HOME, or put a JDK 17 java on PATH");
  }
  return onPath;
}

function sidecarClasspath(): string {
  const fromEnvironment = process.env["PC_PROXY_SIDECAR_CLASSPATH"];
  if (fromEnvironment !== undefined && fromEnvironment.length > 0) {
    return fromEnvironment;
  }
  if (!fs.existsSync(CLASSPATH_FILE)) {
    throw new Error(`${CLASSPATH_FILE} is missing - ${HOW_TO_BUILD_IT}`);
  }
  const classpath = fs.readFileSync(CLASSPATH_FILE, "utf8").trim();
  if (classpath.length === 0) {
    throw new Error(`${CLASSPATH_FILE} is empty - ${HOW_TO_BUILD_IT}`);
  }
  return classpath;
}
