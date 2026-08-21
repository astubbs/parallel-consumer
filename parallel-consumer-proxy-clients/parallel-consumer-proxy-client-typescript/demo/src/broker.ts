// Copyright (C) 2026 Antony Stubbs and contributors

/**
 * The broker the demo reads from, and the backlog every arm replays.
 *
 * **This demo never starts a broker itself**, and that is the one structural difference from the
 * Java reference demo, which starts one with Testcontainers when no `--bootstrap` is given.
 * `demo/run.sh` owns that here: it starts the container, waits for it to serve an API request, and
 * hands this program an address. Two reasons, both recorded in `docs/inflight/clients/typescript.md`:
 *
 * - the demo container is **never** granted the host Docker socket, so the containerised path is
 *   always "an address was supplied" anyway - which makes broker-starting a property of the
 *   *launcher*, not of the demo;
 * - the alternative was a 47 MB `@testcontainers/kafka` dependency in a module whose ordinary
 *   `npm ci` is on the CI matrix's critical path, bought for one code path that the container
 *   never takes.
 *
 * The supplied address is never logged. Own-cluster mode puts a user's real broker there.
 */

import { type Admin, Kafka, logLevel, Partitioners, type Producer } from "kafkajs";

/**
 * The key space the seeded records spread over. Ordering is `unordered` in both arms, so this
 * changes nothing today; it exists so a key-ordered lane added later has more than one key to
 * shard across, rather than needing the seeding rewritten first. The reference demo uses the same
 * number, so the two seed identically shaped backlogs.
 */
const KEY_SPACE = 1_000;

/**
 * How many records go into one `producer.send`.
 *
 * kafkajs batches per call rather than by a linger timer, so a per-record `send` would round-trip
 * per record and the big replay's seeding would dominate the demo's wall clock. Measured on the
 * default 40,000-record big replay: it is the difference between seconds and minutes.
 */
const SEED_BATCH = 5_000;

export class DemoBroker {
  private readonly kafka: Kafka;

  constructor(private readonly bootstrap: string) {
    this.kafka = new Kafka({
      clientId: "pc-typescript-demo",
      brokers: [bootstrap],
      // kafkajs logs a WARN per partition on every rebalance. The demo's output is two tables;
      // errors still reach stderr, which is where a broken run must say so.
      logLevel: logLevel.ERROR,
    });
  }

  /** Creates the demo's topic, tolerating one a previous run already left behind. */
  async ensureTopic(topic: string, partitions: number): Promise<void> {
    const admin: Admin = this.kafka.admin();
    await admin.connect();
    try {
      const created = await admin.createTopics({
        topics: [{ topic, numPartitions: partitions, replicationFactor: 1 }],
        waitForLeaders: true,
      });
      if (created) {
        process.stdout.write(`Created topic ${topic} with ${partitions} partitions\n`);
        return;
      }
      // Reusing a topic silently is fine; reusing one with a DIFFERENT partition count is not,
      // because the effective-configuration block would print a --partitions value that never
      // applied - and that block is the demo's whole reproducibility promise.
      const [described] = await admin.fetchTopicMetadata({ topics: [topic] }).then((m) => m.topics);
      const existing = described?.partitions.length ?? 0;
      if (existing !== partitions) {
        throw new Error(
          `topic ${topic} already exists with ${existing} partitions, but this run asked for ` +
            `${partitions} - pass --topic to name a fresh one, or --partitions ${existing}`,
        );
      }
      process.stdout.write(`Topic ${topic} already exists with ${partitions} partitions, reusing it\n`);
    } finally {
      await admin.disconnect();
    }
  }

  /**
   * Produces the backlog both arms then replay.
   *
   * Pre-produced rather than produced alongside the arms, and that is what makes the workload
   * closed-loop - which is in turn why **no arm reports latency**. A per-record timing here would
   * be flattered by however far an arm had fallen behind, so throughput is the only honest number
   * this shape can produce.
   */
  async seed(topic: string, from: number, to: number): Promise<void> {
    if (to <= from) {
      return;
    }
    process.stdout.write(`Producing records ${from} to ${to}...\n`);
    const producer: Producer = this.kafka.producer({
      createPartitioner: Partitioners.DefaultPartitioner,
    });
    await producer.connect();
    try {
      for (let start = from; start < to; start += SEED_BATCH) {
        const end = Math.min(start + SEED_BATCH, to);
        const messages = [];
        for (let index = start; index < end; index += 1) {
          messages.push({
            key: Buffer.from(`key-${index % KEY_SPACE}`),
            value: Buffer.from(`record-${index}`),
          });
        }
        // Awaited, so a send that failed is a rejection here rather than a silently short backlog.
        // Without it the demo would report a full backlog, run both arms against a partial one,
        // and print confident numbers for a workload that never existed.
        await producer.send({ topic, messages });
      }
    } finally {
      await producer.disconnect();
    }
    process.stdout.write(`Produced ${to - from} records\n`);
  }

  /** A kafkajs consumer for the AK core arm - this demo's own Kafka client. */
  consumer(groupId: string) {
    return this.kafka.consumer({ groupId });
  }

  /**
   * The Kafka properties the sidecar arm hands the proxy in `Configure`.
   *
   * `enable.auto.commit` is set for the same reason the reference demo sets it: Parallel Consumer
   * owns offset commits and refuses a consumer with auto-commit on. The sidecar forces the setting
   * itself, so this line is belt-and-braces on this path - it is here so the two demos send an
   * identical `Configure` and a difference in the numbers cannot be a difference in the request.
   */
  consumerProperties(groupId: string): Record<string, string> {
    return {
      "bootstrap.servers": this.bootstrap,
      "group.id": groupId,
      "auto.offset.reset": "earliest",
      "enable.auto.commit": "false",
    };
  }
}
