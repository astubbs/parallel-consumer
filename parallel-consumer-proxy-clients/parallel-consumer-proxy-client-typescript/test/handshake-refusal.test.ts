// Copyright (C) 2026 Antony Stubbs and contributors

/**
 * The handshake, against a real sidecar process, over the real wire.
 *
 * This module's one against-a-real-process test, and the only claim it can honestly make on this
 * stack. The sidecar spawned is `parallel-consumer-proxy`'s production entry point - a real bind,
 * the real authority allowlist, the real single-connection guard, and the real session service.
 * That service hosts no engine and refuses every session, so there is no dispatch to observe here
 * and none is invented.
 *
 * What IS observed is everything this library does before an engine would matter: launch the child
 * directly, read `port:` off its stdout, hold its stdin as the parent-death lifeline, open the
 * channel, put `Configure` on the wire, and turn what came back into a rejection. The dispatch
 * scenarios - one record end to end, the in-flight ceiling, the redelivery history - belong to the
 * shared conformance suite and are deferred until an engine exists to run them against.
 *
 * THE STATUS CODE IS THE ASSERTION, NOT MERELY "IT FAILED". A refusal from the authority allowlist
 * is `PERMISSION_DENIED` and one from the admission slot is `RESOURCE_EXHAUSTED`, both raised by
 * interceptors BEFORE the service method runs. Only `UNIMPLEMENTED` can have come from the service
 * itself, so the code is what separates "the connection was turned away" from "the handshake was
 * delivered and answered".
 */

import assert from "node:assert/strict";
import { chmodSync, mkdtempSync, writeFileSync } from "node:fs";
import net from "node:net";
import { tmpdir } from "node:os";
import path from "node:path";
import { test } from "node:test";

import { ParallelConsumerClient } from "../src/index";
import { NO_ENGINE_DESCRIPTION, engineLessSidecar } from "./harness";

const TEST_TIMEOUT_MS = 90_000;

/** `@grpc/grpc-js`'s numeric code for UNIMPLEMENTED, which is the gRPC status value itself. */
const UNIMPLEMENTED = 12;

void test(
  "the handshake reaches the session service and its refusal reaches the caller",
  { timeout: TEST_TIMEOUT_MS },
  async () => {
    const refused = await rejectionOf(
      ParallelConsumerClient.open({
        sidecar: engineLessSidecar(),
        topics: ["handshake-topic"],
        // The sidecar reads no properties at all on this build. Real credentials never belong in a
        // test, and there is nothing here to give them to.
        kafkaProperties: {},
        instanceTag: "typescript-handshake",
      }),
    );

    const code = (refused as { code?: number }).code;
    assert.equal(
      code,
      UNIMPLEMENTED,
      `handshake failed with code ${String(code)} - UNIMPLEMENTED is the only code the session ` +
        `SERVICE raises, so it is what proves the Configure was delivered rather than turned away ` +
        `by an interceptor: ${String(refused)}`,
    );
    assert.ok(
      String((refused as { details?: string }).details ?? refused).includes(NO_ENGINE_DESCRIPTION),
      `the refusal must name what is missing, or a client author debugs their own code: ${String(refused)}`,
    );
  },
);

/**
 * The control arm, permanent rather than a one-off demonstration: pointed at a port nothing is
 * listening on, the same client fails in a way that is not the refusal above. Without it, the test
 * that matters could be passing on any failure at all - which is the shape of an assertion that
 * cannot fail for the reason it names.
 *
 * The stand-in announces a port and then holds its stdin, which is the spawning contract's whole
 * client-visible surface, so the library takes its REAL connect path at a dead port rather than the
 * different path a child that printed nothing would take.
 */
void test(
  "a sidecar that is not listening fails differently from one that refuses",
  { timeout: TEST_TIMEOUT_MS },
  async () => {
    const deadPort = await reserveThenReleaseAPort();
    const announcer = writeAnnouncer(deadPort);

    const failed = await rejectionOf(
      ParallelConsumerClient.open({
        sidecar: { executable: announcer, args: [] },
        topics: ["handshake-topic"],
        kafkaProperties: {},
        instanceTag: "typescript-handshake-control",
      }),
    );

    assert.notEqual(
      (failed as { code?: number }).code,
      UNIMPLEMENTED,
      `nothing answered, so nothing can have refused: ${String(failed)}`,
    );
  },
);

/** The rejection, or an assertion failure if the promise resolved - which would be the finding. */
async function rejectionOf(promise: Promise<{ close: () => Promise<void> }>): Promise<unknown> {
  try {
    const client = await promise;
    await client.close();
  } catch (error: unknown) {
    return error;
  }
  throw new assert.AssertionError({
    message: "the sidecar hosts no engine, so open() must reject rather than report a session",
  });
}

/** A loopback port the OS has just handed out and nothing is listening on. */
function reserveThenReleaseAPort(): Promise<number> {
  return new Promise<number>((resolve, reject) => {
    const server = net.createServer();
    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => {
      const address = server.address();
      if (address === null || typeof address === "string") {
        reject(new Error("the reserved listener has no numeric port"));
        return;
      }
      server.close(() => { resolve(address.port); });
    });
  });
}

/**
 * A sidecar that announces a port and then holds its stdin. `printf` and `read` are shell builtins,
 * so it is one process holding its own lifeline and no grandchild survives the library's reap.
 */
function writeAnnouncer(port: number): string {
  const directory = mkdtempSync(path.join(tmpdir(), "pc-announcer-"));
  const script = path.join(directory, "announcer.sh");
  writeFileSync(
    script,
    `#!/bin/sh\nprintf 'port: ${String(port)}\\n'\nwhile read -r _ignored; do :; done\nexit 0\n`,
    "utf8",
  );
  chmodSync(script, 0o700);
  return script;
}
