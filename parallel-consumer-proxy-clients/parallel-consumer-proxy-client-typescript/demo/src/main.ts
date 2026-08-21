// Copyright (C) 2026 Antony Stubbs and contributors

/**
 * **The TypeScript demo.** The same records through this language's own Kafka client and through
 * this language over the sidecar, and the two tables that come out of it.
 *
 * The contract it keeps - the flags, the environment variables, the defaults, the two tables, the
 * effective-configuration fingerprint, and the rule that no arm reports latency - is
 * `parallel-consumer-proxy/demo/README.md`. Read that first; `demo/README.md` beside this file
 * records only what is specific to TypeScript.
 *
 * ```bash
 * parallel-consumer-proxy-clients/parallel-consumer-proxy-client-typescript/demo/run.sh
 * ```
 *
 * With no broker supplied `run.sh` starts one in a container. Inside its own container the demo is
 * handed a compose sibling instead, because **a demo container is never granted the host Docker
 * socket**.
 */

import { DemoBroker } from "./broker";
import { akCore, typescriptGrpc } from "./arms";
import {
  bigReplayRecords,
  bigReplayWanted,
  type DemoOptions,
  fingerprint,
  isHelpRequested,
  parseOptions,
  USAGE,
  UsageError,
} from "./options";
import { AK_CORE, type ArmResult, BANNER, table } from "./report";

/** A misspelled flag must not be reported as a result for settings nobody asked for. */
const EXIT_USAGE = 2;

const EXIT_FAILED = 1;

async function main(argv: readonly string[]): Promise<number> {
  if (isHelpRequested(argv)) {
    process.stdout.write(`${USAGE}\n`);
    return 0;
  }

  let options: DemoOptions;
  try {
    options = parseOptions(argv, process.env);
  } catch (error) {
    if (error instanceof UsageError) {
      process.stderr.write(`${error.message}\n\n${USAGE}\n`);
      return EXIT_USAGE;
    }
    throw error;
  }

  // THE FIRST THING PRINTED NAMES THE PRODUCT, and it is contract rather than decoration: a
  // reader who starts a demo and is met with a configuration line has been told nothing about what
  // they are looking at. Every language prints this same block. It goes here rather than in
  // `run.sh` so that both entry points - `run.sh` and a bare `docker compose up` - open with it,
  // and so it is printed exactly once on either.
  process.stdout.write(`\n${BANNER}\n`);

  if (options.bootstrap === undefined) {
    // run.sh starts the broker and supplies its address; see broker.ts for why that is the
    // launcher's job in TypeScript and the demo's own in Java.
    process.stderr.write(
      "No broker address: this demo does not start one. Run it through demo/run.sh, which starts " +
        "a broker in a container and passes --bootstrap, or supply one yourself.\n",
    );
    return EXIT_USAGE;
  }

  const topic = options.topic ?? `pc-demo-${process.hrtime.bigint().toString()}`;
  // The fingerprint, before anything runs: a number without its settings is not reproducible. The
  // bootstrap address is deliberately not in it.
  process.stdout.write(`\nEffective configuration:\n${fingerprint(options, topic)}\n`);

  const broker = new DemoBroker(options.bootstrap);
  await broker.ensureTopic(topic, options.partitions);
  await broker.seed(topic, 0, options.records);

  const small: ArmResult[] = [];
  small.push(await akCore(options, broker, topic, options.records));
  small.push(await typescriptGrpc(options, broker, topic, options.records));
  const baseline = small.find((result) => result.arm === AK_CORE);
  process.stdout.write(
    table(
      `Small replay - every arm over the same ${options.records} records (the comparison)`,
      small,
      baseline,
      false,
    ),
  );

  if (!bigReplayWanted(options)) {
    process.stdout.write(`\nBig replay skipped (--replay-factor ${options.replayFactor}).\n`);
    return 0;
  }

  const total = bigReplayRecords(options);
  await broker.seed(topic, options.records, total);

  // AK core is excluded here because it does not go parallel: it would need
  // total * delayMs milliseconds to finish a backlog the sidecar arm clears in seconds, and a demo
  // that makes a reader wait that long to learn nothing new is not worth the wall clock.
  const big = [await typescriptGrpc(options, broker, topic, total)];
  const serialSeconds = Math.trunc((total * options.delayMs) / 1_000);
  process.stdout.write(
    table(
      `Big replay - ${total} records, parallel arms only (AK core is serial and would take ` +
        `${serialSeconds}s+)`,
      big,
      baseline,
      true,
    ),
  );
  return 0;
}

main(process.argv.slice(2))
  .then((code) => {
    // The arms leave nothing running, but kafkajs and grpc-js both keep pooled handles that can
    // outlive a finished session. An explicit exit is what makes a scripted caller - CI, or
    // `docker compose --abort-on-container-exit` - see the demo's own code promptly.
    process.exit(code);
  })
  .catch((error: unknown) => {
    process.stderr.write(
      `The demo failed: ${
        error instanceof Error ? (error.stack ?? error.message) : String(error)
      }\n`,
    );
    process.exit(EXIT_FAILED);
  });
