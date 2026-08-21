// Copyright (C) 2026 Antony Stubbs and contributors

/**
 * The two arms. **This is the whole contract for a language that is not Java.**
 *
 * - **AK core (kafkajs)** - this language's own Kafka client, one record at a time. The role is
 *   always spelled "AK core" - bare "core" reads as `parallel-consumer-core` (`CONCEPTS.md`) - and
 *   the CLIENT is always named beside it, because "AK core" is a category and a reader cannot judge
 *   a comparison without knowing what produced it. TypeScript has a second serious client;
 *   `demo/README.md` says which, and why this arm is kafkajs.
 * - **typescript-grpc (this client)** - the application as a *foreign client*: this module's library
 *   spawns the sidecar, receives records over a socket, runs the user's function, and reports
 *   outcomes back. **On this path the application does no Kafka I/O** - the sidecar owns the
 *   consumer, the producer, the group membership and the offsets. That is a statement about the
 *   *path*, not about this process: the same process seeds the topic and runs the AK core arm with
 *   an ordinary Kafka client, because a comparison needs both sides.
 *
 * Java carries four more arms - `pc-core`, `java-direct`, `java-grpc-uds`, `java-raw-grpc` -
 * because one JVM can hold all of them at once and each *pair* changes exactly one term. TypeScript
 * has nothing to compare a wrapper or a raw wire against, so two arms is the whole demo here.
 *
 * ## The one sanctioned divergence: the simulated work is an AWAITED TIMER
 *
 * Every other language in the fan-out sleeps its worker thread. **Node is a single event loop**: a
 * blocking sleep there stops the transport, the executors and the timers all at once, so the
 * "parallel" arm would run exactly as serially as the AK core one and the demo would be measuring
 * nothing. The work is therefore `await new Promise(resolve => setTimeout(resolve, delayMs))`.
 *
 * That is not a weaker workload - it is the *same* workload, expressed in the wait this runtime
 * actually has, and it is what a real Node processor's work looks like, because a Node processor
 * is overwhelmingly waiting on I/O. It does mean the parallelism in the sidecar arm is **promise
 * concurrency on one event loop**: `Configured.executor_count` concurrent `await`s, not threads.
 * A processor doing CPU work *synchronously* would block the loop and this arm would collapse to
 * serial - the client library's README says so plainly, and this demo does not pretend otherwise.
 */

import { ParallelConsumerClient } from "@parallel-consumer/proxy-client";

import type { DemoBroker } from "./broker";
import type { DemoOptions } from "./options";
import { AK_CORE, SIDECAR_ARM, SIDECAR_ARM_NAME, type ArmResult } from "./report";
import { sidecarCommand } from "./sidecar";

/** No arm may take longer than this before the demo calls it stalled rather than slow. */
const ARM_BUDGET_MS = 10 * 60 * 1_000;

/**
 * The simulated work: the non-occupying wait this runtime has.
 *
 * `setTimeout` and not a spin, and not `Atomics.wait` on the main thread - see the file comment.
 * `delayMs` of 0 still yields to the event loop, which is what makes `--delay-ms 0` mean "no work"
 * rather than "no scheduling".
 */
function work(delayMs: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, delayMs));
}

/** A fresh group per arm per replay, so every arm reads the same records from the beginning. */
function groupId(arm: string): string {
  return `pc-demo-${arm}-${process.hrtime.bigint().toString()}`;
}

/**
 * What an arm did, and how it knows it is finished.
 *
 * It counts towards a target and resolves once, so an arm can wait for "the backlog is done"
 * without polling. It never rejects: the budget below is what turns a stall into a failure.
 *
 * It also collects the DISTINCT KEYS the arm saw. That is the second half of the contract's "every
 * arm reports what it did, not just how fast": the count proves the arm finished the backlog, and
 * the key set proves the backlog was really spread rather than one key repeated. Both are
 * deterministic, which is what makes them the only figures here that compare across languages.
 */
class Tally {
  private count = 0;

  /** Keys as text. A null key is a distinct observation too, so it gets a sentinel rather than
   * being dropped - this demo seeds every record with a key, so seeing one would be a finding. */
  private readonly seen = new Set<string>();

  private release: () => void = () => undefined;

  readonly reached: Promise<void>;

  constructor(private readonly target: number) {
    this.reached = new Promise<void>((resolve) => {
      this.release = resolve;
    });
  }

  hit(key: Buffer | null | undefined): void {
    this.count += 1;
    this.seen.add(key === null || key === undefined ? "<null>" : key.toString("utf8"));
    if (this.count >= this.target) {
      this.release();
    }
  }

  get processed(): number {
    return this.count;
  }

  get keys(): number {
    return this.seen.size;
  }
}

/** The serial arm: one record at a time, the same awaited timer, through kafkajs. */
export async function akCore(
  options: DemoOptions,
  broker: DemoBroker,
  topic: string,
  target: number,
): Promise<ArmResult> {
  announce(AK_CORE, target);
  const tally = new Tally(target);
  const consumer = broker.consumer(groupId("ak-core"));
  await consumer.connect();
  await consumer.subscribe({ topic, fromBeginning: true });

  // The clock starts AFTER the consumer is built and subscribed and stops before it disconnects,
  // because this arm is the denominator of every ratio in both tables and the other arm charges
  // itself for neither construction nor teardown.
  const startedAt = performance.now();
  // `eachMessage` with the default partitionsConsumedConcurrently of 1 IS the serial arm: kafkajs
  // hands over one record, waits for the returned promise, and only then hands over the next -
  // across every assigned partition, not per partition. autoCommit is off for the same reason the
  // reference demo turns it off.
  await consumer.run({
    autoCommit: false,
    // The record IS read here, for its key alone - the table's `keys` column is what shows the
    // backlog was spread rather than one key repeated, and the sidecar arm reads exactly as much.
    // Neither arm deserializes a value: that would be work the other one does not do.
    eachMessage: async ({ message }) => {
      await work(options.delayMs);
      tally.hit(message.key);
    },
  });

  await withinBudget(AK_CORE, tally, target, tally.reached);
  const elapsedMs = performance.now() - startedAt;
  await consumer.disconnect();
  return finished(AK_CORE, elapsedMs, tally);
}

/**
 * The client library over a real sidecar - the arm the whole design exists for.
 *
 * The application spawns a binary, receives records over a socket, runs its own function on them,
 * and reports outcomes back. It opens no consumer, no producer and no admin client on this path.
 */
export async function typescriptGrpc(
  options: DemoOptions,
  broker: DemoBroker,
  topic: string,
  target: number,
): Promise<ArmResult> {
  announce(SIDECAR_ARM, target);
  const tally = new Tally(target);
  const client = await ParallelConsumerClient.open({
    sidecar: sidecarCommand(),
    topics: [topic],
    maxConcurrency: options.maxConcurrency,
    // Set EXPLICITLY. Unspecified means "take parallel-consumer-core's default", which is KEY -
    // so leaving it out would run this arm key-ordered against a serial arm with no ordering at
    // all, and the two tables would be comparing two different questions.
    ordering: "unordered",
    // The arm NAME, not its label: a group id and an instance tag are identifiers, and
    // "typescript-grpc (this client)" is a caption. The label belongs in the table and nowhere else.
    kafkaProperties: broker.consumerProperties(groupId(SIDECAR_ARM_NAME)),
    instanceTag: `${SIDECAR_ARM_NAME}-demo`,
    onWarning: (message) => process.stderr.write(`sidecar arm: ${message}\n`),
  });

  try {
    const startedAt = performance.now();
    // The session ending is a second way this arm can finish, and it is not a good one: a session
    // that ends before the target means the run is over with the backlog unfinished. Raced here so
    // it is reported rather than showing up as a stall ten minutes later.
    const ended = client.done().then(() => "ended" as const);
    // The race may be decided by the tally, leaving this promise's rejection with nobody to
    // hear it. This handler is what stops that becoming an unhandled rejection.
    void ended.catch(() => undefined);

    // The user's function. It reads the record's KEY and nothing else - the same amount the AK
    // core arm reads, so neither arm is charged for work the other avoids - and that is what fills
    // the table's `keys` column. Returning nothing is a success; throwing would be a failure.
    client.poll(async (record) => {
      await work(options.delayMs);
      tally.hit(record.key);
    });

    const outcome = await withinBudget(
      SIDECAR_ARM,
      tally,
      target,
      Promise.race([tally.reached.then(() => "reached" as const), ended]),
    );
    const elapsedMs = performance.now() - startedAt;
    if (outcome === "ended" && tally.processed < target) {
      throw new Error(
        `${SIDECAR_ARM} ended early at ${tally.processed} of ${target} - the session closed ` +
          `before the backlog did`,
      );
    }
    return finished(SIDECAR_ARM, elapsedMs, tally);
  } finally {
    // Closes the stream, the channel and the child process. It runs on the failure path too: a
    // leaked sidecar still holds Kafka group membership.
    await client.close();
  }
}

/**
 * Waits for an arm to finish, or calls it stalled.
 *
 * Reaching the target is not the only thing that can settle the promise, so the caller still checks
 * what it got: a broken run that printed a plausible row at a plausible rate and exited 0 is the
 * worst thing a demo can do.
 */
async function withinBudget<T>(
  arm: string,
  tally: Tally,
  target: number,
  finish: Promise<T>,
): Promise<T> {
  let timer: NodeJS.Timeout | undefined;
  const budget = new Promise<never>((_resolve, reject) => {
    timer = setTimeout(() => {
      reject(new Error(`${arm} stalled at ${tally.processed} of ${target}`));
    }, ARM_BUDGET_MS);
  });
  try {
    return await Promise.race([finish, budget]);
  } finally {
    if (timer !== undefined) {
      clearTimeout(timer);
    }
  }
}

function announce(arm: string, target: number): void {
  process.stdout.write(`\n=== ${arm} starting over ${target} records ===\n`);
}

function finished(arm: string, elapsedMs: number, tally: Tally): ArmResult {
  process.stdout.write(
    `=== ${arm} finished: ${tally.processed} records over ${tally.keys} keys in ` +
      `${Math.round(elapsedMs)}ms ===\n`,
  );
  return { arm, elapsedMs, processed: tally.processed, keys: tally.keys };
}
