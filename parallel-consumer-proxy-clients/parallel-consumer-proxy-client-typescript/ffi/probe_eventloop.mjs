// Copyright (C) 2026 Antony Stubbs and contributors

// Does the pull model survive a runtime with no threads to block?
//
// Go and Python both had real threads: something else keeps running while one blocks. Node does
// not - it is one event loop, and a blocking call on it stops everything. So the question the
// other two languages could not ask is whether "frames as ABI" still works here.
//
// THE CONTROL ARM IS THE POINT. Both arms make the SAME blocking call, for the same duration, on
// the same session. The only difference is which thread makes it. If the loop stalls on the main
// thread and survives on a worker, the cause is thread placement and nothing incidental.
//
// Prediction, recorded before the first run: main-thread blocking drives loop ticks to
// approximately zero; the worker leaves them near baseline.

import { createRequire } from 'node:module';
import path from 'node:path';
import process from 'node:process';
import { fileURLToPath } from 'node:url';
import { Worker } from 'node:worker_threads';

const BLOCK_MS = 2000;
const ERR_TIMEOUT = -3;

const here = path.dirname(fileURLToPath(import.meta.url));
const pc = createRequire(import.meta.url)(path.join(here, 'pc_addon.node'));

/** Counts event-loop turns until stopped. setImmediate re-queues per turn, so it measures the loop
 *  itself rather than timer resolution. */
function countLoopTurns() {
  const state = { turns: 0, running: true };
  const tick = () => {
    if (!state.running) return;
    state.turns += 1;
    setImmediate(tick);
  };
  setImmediate(tick);
  return state;
}

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

async function baseline() {
  const state = countLoopTurns();
  await sleep(BLOCK_MS);
  state.running = false;
  return state.turns;
}

async function onMainThread(thread, handle) {
  const state = countLoopTurns();
  await sleep(50);                       // let the loop reach a steady rate
  const before = state.turns;
  const buffer = Buffer.alloc(64 * 1024);
  const started = Date.now();
  const result = pc.next(thread, handle, buffer, BLOCK_MS);
  const elapsed = Date.now() - started;
  state.running = false;
  return { turns: state.turns - before, rc: result.rc, elapsed };
}

async function onWorkerThread(isolate, handle) {
  const state = countLoopTurns();
  await sleep(50);
  const before = state.turns;
  const started = Date.now();
  const outcome = await new Promise((resolve, reject) => {
    const worker = new Worker(path.join(here, 'pull_worker.mjs'), {
      workerData: { isolate, handle, timeoutMillis: BLOCK_MS },
    });
    worker.on('message', resolve);
    worker.on('error', reject);
  });
  const elapsed = Date.now() - started;
  state.running = false;
  return { turns: state.turns - before, rc: outcome.rc, elapsed };
}

const created = pc.createIsolate();
if (created.rc !== 0) {
  console.error(`graal_create_isolate failed with ${created.rc}`);
  process.exit(1);
}
const handle = pc.sessionOpen(created.thread);
if (handle <= 0n) {
  console.error(`pc_session_open returned ${handle}`);
  process.exit(1);
}

// Nothing is queued on this session, so pc_next is guaranteed to run its full timeout and return
// ERR_TIMEOUT. That makes the blocking duration a constant rather than a variable.
console.log(`blocking for ${BLOCK_MS}ms in pc_next, three ways\n`);

const free = await baseline();
console.log(`  baseline (no FFI call)   loop turned ${free.toLocaleString()} times`);

const main = await onMainThread(created.thread, handle);
console.log(`  blocking on MAIN thread  loop turned ${main.turns.toLocaleString()} times ` +
  `(rc ${main.rc}, ${main.elapsed}ms)`);

const worker = await onWorkerThread(created.isolate, handle);
console.log(`  blocking on WORKER       loop turned ${worker.turns.toLocaleString()} times ` +
  `(rc ${worker.rc}, ${worker.elapsed}ms)`);

for (const [label, arm] of [['main', main], ['worker', worker]]) {
  if (arm.rc !== ERR_TIMEOUT) {
    console.log(`  NOTE ${label} arm returned ${arm.rc}, expected ERR_TIMEOUT (${ERR_TIMEOUT})`);
  }
}

pc.sessionClose(created.thread, handle);

console.log();
// Against the BASELINE, not against each other: the question is whether the loop kept running at
// its normal rate, and the absolute rate is a property of the machine.
const mainShare = main.turns / free;
const workerShare = worker.turns / free;
console.log(`  main thread kept ${(mainShare * 100).toFixed(1)}% of baseline throughput`);
console.log(`  worker thread kept ${(workerShare * 100).toFixed(1)}% of baseline throughput`);

if (mainShare < 0.05 && workerShare > 0.5) {
  console.log('\nPASS  a blocking pull stalls the event loop, and a worker thread fixes it.');
  console.log('      The pull model survives a single-threaded runtime - off the main thread.');
  process.exit(0);
}
console.log('\nFAIL  the arms did not separate as predicted - read the numbers above.');
process.exit(1);
