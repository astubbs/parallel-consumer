// Copyright (C) 2026 Antony Stubbs and contributors

/**
 * Regenerates the committed TypeScript stubs under `src/generated/` from the FROZEN schema at
 * `parallel-consumer-proxy-protocol/src/main/proto/parallelconsumer/proxy/v1/proxy.proto`.
 *
 * THE OUTPUT IS COMMITTED, like Go's and Python's. An npm consumer must not need `protoc`, and a
 * `npm install` from a tarball has no codegen step to hang one off; committing is also what makes
 * "regenerating from the .proto produces no diff" a checkable claim rather than an aspiration.
 * `--check` regenerates into a scratch directory and fails on any difference - that is the check.
 *
 * TOOLCHAIN. Unlike the Go wave (which had to borrow the protoc that the protocol module's
 * protobuf-maven-plugin downloads, and unzip the well-known types out of protobuf-java because the
 * Maven artifact ships the bare executable), this module uses an ordinary standalone `protoc`. That
 * ships its own `include/google/protobuf/*.proto`, so the duration/timestamp imports resolve with
 * no include path of ours at all. Set PC_PROTOC to point at a specific one.
 *
 * ts-proto is the generator, pinned in devDependencies (never `@latest`), and its options are the
 * decisions worth reading:
 *   env=node          bytes become Buffer, which is what a Kafka key/value is in Node.
 *   forceLong=bigint  int64 becomes bigint. Token.epoch is 64-bit and the golden corpus carries a
 *                     deliberately beyond-int32 value; `number` would truncate at 2^53 and a
 *                     truncating parser is exactly what the golden bytes exist to catch.
 *   useOptionals=all  every field is `?: T | undefined`, so absence is expressible and never
 *                     fabricated as a zero. The whole schema marks scalars `optional` for that
 *                     reason, and the presence-vs-zero distinction is load-bearing throughout.
 *   useDate=true      google.protobuf.Timestamp becomes Date. Durations stay as {seconds, nanos}
 *                     messages - there is no idiomatic TS duration type to map them onto.
 *   outputServices=grpc-js  a typed ClientDuplexStream<ClientMessage, ProxyMessage>, which is the
 *                     one call this protocol uses.
 */

import { execFileSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";

const MODULE_DIR = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const REPO_ROOT = path.resolve(MODULE_DIR, "..", "..");
const PROTO_ROOT = path.join(REPO_ROOT, "parallel-consumer-proxy-protocol", "src", "main", "proto");
const PROTO_FILE = "parallelconsumer/proxy/v1/proxy.proto";
const OUT_DIR = path.join(MODULE_DIR, "src", "generated");

const TS_PROTO_OPTS = [
  "env=node",
  "forceLong=bigint",
  "useOptionals=all",
  "useDate=true",
  "esModuleInterop=true",
  "outputServices=grpc-js",
].join(",");

const check = process.argv.includes("--check");

/** Runs protoc into `destination`, which is created empty first. */
function generate(destination) {
  fs.rmSync(destination, { recursive: true, force: true });
  fs.mkdirSync(destination, { recursive: true });
  const plugin = path.join(MODULE_DIR, "node_modules", ".bin", "protoc-gen-ts_proto");
  if (!fs.existsSync(plugin)) {
    throw new Error(`ts-proto is not installed - run \`npm ci\` in ${MODULE_DIR} first`);
  }
  execFileSync(process.env.PC_PROTOC ?? "protoc", [
    "-I",
    PROTO_ROOT,
    `--plugin=protoc-gen-ts_proto=${plugin}`,
    `--ts_proto_out=${destination}`,
    `--ts_proto_opt=${TS_PROTO_OPTS}`,
    PROTO_FILE,
  ], { stdio: "inherit", cwd: MODULE_DIR });
}

/** Every generated file, relative to the generation root, sorted so two trees compare directly. */
function listing(root) {
  const found = [];
  const walk = (dir) => {
    for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
      const full = path.join(dir, entry.name);
      if (entry.isDirectory()) {
        walk(full);
      } else {
        found.push(path.relative(root, full));
      }
    }
  };
  walk(root);
  return found.sort();
}

if (check) {
  const scratch = fs.mkdtempSync(path.join(os.tmpdir(), "pc-proto-check-"));
  try {
    generate(scratch);
    const fresh = listing(scratch);
    const committed = fs.existsSync(OUT_DIR) ? listing(OUT_DIR) : [];
    const differences = [];
    for (const file of new Set([...fresh, ...committed])) {
      const a = fresh.includes(file) ? fs.readFileSync(path.join(scratch, file), "utf8") : null;
      const b = committed.includes(file) ? fs.readFileSync(path.join(OUT_DIR, file), "utf8") : null;
      if (a !== b) {
        differences.push(file);
      }
    }
    if (differences.length > 0) {
      console.error(
        `generate-proto: the committed stubs have drifted from ${PROTO_FILE}:\n  ` +
          differences.join("\n  ") +
          "\nRun `npm run proto` and commit the result.",
      );
      process.exit(1);
    }
    console.log(`generate-proto: ${committed.length} committed file(s) match ${PROTO_FILE}`);
  } finally {
    fs.rmSync(scratch, { recursive: true, force: true });
  }
} else {
  generate(OUT_DIR);
  console.log(`generate-proto: wrote ${listing(OUT_DIR).length} file(s) under src/generated/`);
}
