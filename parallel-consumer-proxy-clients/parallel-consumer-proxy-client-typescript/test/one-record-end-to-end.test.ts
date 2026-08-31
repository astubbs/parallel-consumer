// Copyright (C) 2026 Antony Stubbs and contributors

/**
 * The one-record scenario, end to end, over the real wire.
 *
 * `a-processed-record-advances-the-committed-offset` is the baseline every client in every language
 * runs first: one record in, processed once, the offset advances past it. The offset half of that
 * is engine state no client can see, and the harness carries no verdict channel - it exits 0
 * whatever happened - so what this asserts is the wire-observable consequence: THE RECORD ARRIVES
 * EXACTLY ONCE, AND THE SUCCESS IS FOLLOWED BY SILENCE RATHER THAN A REDELIVERY. A report the proxy
 * rejected - a mangled token, an epoch this client had taken apart and rebuilt - would leave the
 * record in flight and it would come back.
 *
 * Real everything: a real child process, a real gRPC stream over a real port, a real user closure.
 * The only unreality is the sidecar's Kafka, which is mock clients seeded with the scenario's
 * records.
 *
 * The subscription is NOT a control here. The mock harness seeds its records regardless of the
 * topic a client subscribes to, so subscribing to the wrong topic would still pass - which is why
 * the assertions below are about the record's own content and its delivery count, never about the
 * fact that a record arrived at all.
 */

import assert from "node:assert/strict";
import { after, test } from "node:test";

import { Capability, type InboundRecord, ParallelConsumerClient } from "../src/index";
import { Scenario, sidecarFor } from "./harness";

const SCENARIO = Scenario.processedRecordAdvancesTheCommittedOffset;

/** Seconds of silence that stand for "not redelivered". The scenario's retry delay is shorter. */
const QUIET_PERIOD_MS = 3_000;

const TEST_TIMEOUT_MS = 90_000;

// `void`: node:test returns a promise the runner owns, and the type-aware lint (correctly)
// insists that every promise is disposed of deliberately.
void test(
  "a processed record advances the committed offset",
  { timeout: TEST_TIMEOUT_MS },
  async (t) => {
    const warnings: string[] = [];
    const client = await ParallelConsumerClient.open({
      sidecar: sidecarFor(SCENARIO),
      // THE SCENARIO NAME IS ALSO THE TOPIC NAME.
      topics: [SCENARIO],
      // --mock builds mock Kafka clients and reads no properties. Real credentials never belong in
      // a conformance test.
      kafkaProperties: {},
      instanceTag: "typescript-client-wave-one",
      onWarning: (message) => warnings.push(message),
    });
    after(() => client.close());

    const session = client.session;
    assert.ok(session.maxConcurrency >= 1, `effective max_concurrency was ${session.maxConcurrency}`);
    assert.ok(session.executorCount >= 1, `effective executor_count was ${session.executorCount}`);
    assert.ok(
      session.capabilities.has(Capability.dispatch),
      `dispatch was not negotiated; the session negotiated [${[...session.capabilities].join(", ")}]`,
    );

    const delivered: InboundRecord[] = [];
    let firstArrival: (() => void) | undefined;
    const arrived = new Promise<void>((resolve) => { firstArrival = resolve; });

    client.poll((record) => {
      delivered.push(record);
      firstArrival?.();
      // Returning nothing is a bare success - the common spelling, exercised deliberately here.
    });

    await Promise.race([
      arrived,
      client.done().then(() => { throw new Error("the session ended before any record arrived"); }),
    ]);

    // A success is followed by silence. Had the report not landed, or not been honoured, the record
    // would be redelivered inside this window.
    await new Promise((resolve) => setTimeout(resolve, QUIET_PERIOD_MS));

    assert.equal(
      delivered.length,
      1,
      `the record was delivered ${delivered.length} times, want exactly 1`,
    );
    const record = delivered[0];
    assert.ok(record !== undefined);
    assert.equal(record.topic, SCENARIO, "the record's own topic is the scenario topic");
    assert.equal(record.attempt, 1, "a first delivery is attempt 1");
    assert.equal(record.lastFailureAt, null, "a first delivery has no previous failure time");
    assert.equal(record.lastFailureReason, null, "a first delivery has no previous failure reason");
    assert.ok(
      record.value !== null && record.value.length > 0,
      "the seeded record carried no value",
    );
    assert.equal(typeof record.offset, "bigint", "a Kafka offset is 64-bit and must not be a number");

    await client.close();
    // Nothing arrived that this client refused to act on: no un-negotiated message, no second
    // Configured, and no queued record discarded at close.
    assert.deepEqual(warnings, [], `the session produced warnings: ${warnings.join(" | ")}`);
    await client.done();

    t.diagnostic(
      `negotiated [${[...session.capabilities].join(", ")}], max_concurrency ` +
        `${session.maxConcurrency}, executor_count ${session.executorCount}`,
    );
  },
);
