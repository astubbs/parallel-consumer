// Copyright (C) 2026 Antony Stubbs and contributors

/**
 * The dispatch queue's normative rules (client-authoring guide §3, KTD39), which the end-to-end
 * test cannot reach: the harness dispatches one record, so FIFO hand-out past the executor count
 * and the overflow negative control never fire there.
 *
 * The named conformance scenario for this section is
 * `the-client-queue-hands-out-fifo-and-releases-on-shutdown`, whose harness support lands with a
 * later engine unit. What is here is the client half of it, asserted directly.
 */

import assert from "node:assert/strict";
import { test } from "node:test";

import { ProtocolViolationError } from "../src/index";
import { DispatchQueue, type QueuedRecord } from "../src/queue";

function record(id: string): QueuedRecord {
  return {
    token: { recordId: id, epoch: 1n },
    record: {
      topic: "t",
      partition: 0,
      offset: 0n,
      key: null,
      value: null,
      attempt: 1,
      lastFailureAt: null,
      lastFailureReason: null,
    },
  };
}

void test("hand-out is FIFO, by arrival and within a wave by record order", async () => {
  const queue = new DispatchQueue(3);
  queue.offer(record("a"));
  queue.offer(record("b"));
  queue.offer(record("c"));

  const order = [await queue.take(), await queue.take(), await queue.take()];
  assert.deepEqual(
    order.map((item) => item?.token.recordId),
    ["a", "b", "c"],
  );
});

void test("a record queued before any executor asks for it is handed to the next asker", async () => {
  const queue = new DispatchQueue(2);
  const waiting = queue.take();
  queue.offer(record("a"));
  assert.equal((await waiting)?.token.recordId, "a");
});

void test("overflow past max_concurrency is a protocol violation, not a load condition", () => {
  const queue = new DispatchQueue(3);
  queue.offer(record("a"));
  queue.offer(record("b"));
  queue.offer(record("c"));

  // The ceiling counts UNRESOLVED records, so records being executed still occupy it: taking two
  // out for executors does not make room, only reporting does.
  assert.equal(queue.inFlight, 3);

  assert.throws(
    () => queue.offer(record("d")),
    (error: unknown) => {
      assert.ok(error instanceof ProtocolViolationError);
      // The specification asks the client to fail the stream "naming the count". A gRPC client
      // cannot set a status, so the count travels in this error instead.
      assert.match(error.message, /max_concurrency of 3/);
      return true;
    },
  );
});

void test("reporting a record makes room for the next one", () => {
  const queue = new DispatchQueue(1);
  queue.offer(record("a"));
  assert.throws(() => queue.offer(record("b")), ProtocolViolationError);
  queue.settle();
  assert.doesNotThrow(() => queue.offer(record("b")));
});

void test("closing discards what was never handed out, and leaves executing records alone", async () => {
  const queue = new DispatchQueue(4);
  queue.offer(record("a"));
  queue.offer(record("b"));
  queue.offer(record("c"));
  const executing = await queue.take();
  assert.equal(executing?.token.recordId, "a", "a is out with an executor and keeps running");

  const discarded = queue.close();

  assert.deepEqual(
    discarded.map((item) => item.token.recordId),
    ["b", "c"],
    "queued records are discarded - they are held by no live worker",
  );
  assert.equal(queue.inFlight, 1, "only the executing record still counts against the ceiling");
  assert.equal(await queue.take(), null, "a closed, drained queue hands out nothing");
});

void test("closing releases an executor already waiting on an empty queue", async () => {
  const queue = new DispatchQueue(2);
  const idle = queue.take();
  queue.close();
  assert.equal(await idle, null, "the executor loop is released rather than left hanging");
});
