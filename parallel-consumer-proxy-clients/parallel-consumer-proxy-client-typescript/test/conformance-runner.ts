// Copyright (C) 2026 Antony Stubbs and contributors

/**
 * TypeScript's half of the shared cross-language conformance suite (astubbs#242, confluentinc#154).
 *
 * IT ASSERTS NOTHING, DELIBERATELY. The suite that knows what correct looks like - offset frontiers,
 * ordering, redelivery, attempt counts - is the Java module
 * parallel-consumer-proxy-clients/parallel-consumer-proxy-conformance, and it keeps owning that
 * knowledge for every language. This program's whole job is to DO WHAT THE SCENARIO SAYS and then
 * exit; if it were free to decide what "correct" means, ten languages would each decide it slightly
 * differently and the agreement between them would prove nothing.
 *
 * Its contract - flags, exit codes, the stdout line, the behaviour tokens - is documented once, in
 * parallel-consumer-proxy-clients/parallel-consumer-proxy-conformance/README.md, and is identical
 * in every language.
 *
 * IT LIVES UNDER test/ RATHER THAN src/ because it is not part of the published surface: `files` in
 * package.json ships dist/src only, and this is a driver for somebody else's test suite. It is not a
 * `*.test.ts`, so `npm test` does not run it - the Java suite spawns it.
 *
 * THIS DOES NOT REPLACE THIS PACKAGE'S OWN TESTS. The shared suite proves every client behaves
 * identically on the protocol; test/ catches what is invisible from outside the process - a floating
 * promise, a queue that hands out wrongly, a child that outlives its parent. Both layers are
 * load-bearing.
 */

import { isAbsolute } from "node:path";

import { type InboundRecord, ParallelConsumerClient, type Outcome } from "../src/index";

// Exit statuses ARE the verdict channel. There is no results file and no report message: a scenario
// passed if this process exited 0 and the Java suite's own assertions about engine state held.
const EXIT_OK = 0;
const EXIT_BEHAVIOUR_FAILED = 1;
const EXIT_USAGE = 2;

const BEHAVIOURS = [
  "succeed",
  "report-nothing",
  "fail-then-succeed",
  "hold-first-until-second",
] as const;
type Behaviour = (typeof BEHAVIOURS)[number];

/**
 * The exact text a fail-then-succeed run reports. The Java suite asserts the redelivery carries it
 * back VERBATIM, so it is a fixed literal of the contract in every language, never composed here.
 */
const PRESCRIBED_FAILURE_REASON = "conformance-prescribed-failure";

// Fixed session tunables, contract rather than this runner's judgement: they exist only so scenarios
// converge at unit-test speed against the engine's production defaults (a 5s commit interval, a 1s
// retry delay). Every language sets the same two values.
const COMMIT_INTERVAL_MS = 100;
const RETRY_DELAY_MS = 50;

/**
 * How long a report-nothing run keeps its session OPEN after its last observation.
 *
 * IT IS WHAT MAKES THE NEGATIVE CONTROL A CONTROL. Without it the runner ends the instant the record
 * arrives, and a sabotaged runner that DID report success has its report killed in flight by the
 * exit - so the suite sees an unadvanced offset either way and the scenario passes for a broken
 * client. Measured in the Go wave, not reasoned about: reporting success from this behaviour left
 * the suite green until the hold existed.
 */
const REPORT_NOTHING_HOLD_MS = 3_000;

interface Arguments {
  scenario: string;
  behaviour: Behaviour;
  sidecar: string;
  expectDispatches: number;
  timeoutSeconds: number;
}

async function main(argv: readonly string[]): Promise<number> {
  const parsed = parse(argv);
  if (typeof parsed === "number") {
    return parsed;
  }
  return run(parsed);
}

/** The five flags, spelled identically in every language - including the British `--behaviour`. */
function parse(argv: readonly string[]): Arguments | number {
  const values = new Map<string, string>();
  for (let index = 0; index < argv.length; index += 2) {
    const flag = argv[index];
    const value = argv[index + 1];
    if (flag === undefined || value === undefined || !flag.startsWith("--")) {
      return usage(`expected --flag value pairs, got ${argv.slice(index).join(" ")}`);
    }
    values.set(flag, value);
  }

  const scenario = values.get("--scenario");
  const behaviour = values.get("--behaviour");
  const sidecar = values.get("--sidecar");
  const expect = Number(values.get("--expect-dispatches"));
  const budget = Number(values.get("--timeout-seconds"));

  if (scenario === undefined || scenario.length === 0) return usage("--scenario is required");
  if (behaviour === undefined) return usage("--behaviour is required");
  if (!(BEHAVIOURS as readonly string[]).includes(behaviour)) {
    return usage(`unknown behaviour ${JSON.stringify(behaviour)}`);
  }
  if (sidecar === undefined || sidecar.length === 0) return usage("--sidecar is required");
  if (!isAbsolute(sidecar)) return usage(`--sidecar must be absolute, got ${JSON.stringify(sidecar)}`);
  if (!Number.isInteger(expect) || expect < 1) return usage("--expect-dispatches must be at least 1");
  if (!Number.isInteger(budget) || budget < 1) return usage("--timeout-seconds must be at least 1");

  return {
    scenario,
    behaviour: behaviour as Behaviour,
    sidecar,
    expectDispatches: expect,
    timeoutSeconds: budget,
  };
}

function usage(problem: string): number {
  process.stderr.write(`conformance-runner: ${problem}\n`);
  return EXIT_USAGE;
}

async function run(args: Arguments): Promise<number> {
  const tracker = new Tracker(args.expectDispatches);

  let client: ParallelConsumerClient;
  try {
    client = await ParallelConsumerClient.open({
      sidecar: { executable: args.sidecar, stderr: "inherit" },
      // THE SCENARIO NAME IS ALSO THE TOPIC NAME.
      topics: [args.scenario],
      // Enough executors for every dispatch the scenario prescribes, so a scenario that holds a
      // record cannot deadlock on a ceiling smaller than its own shape.
      maxConcurrency: args.expectDispatches,
      commitIntervalMs: COMMIT_INTERVAL_MS,
      defaultMessageRetryDelayMs: RETRY_DELAY_MS,
      // The mock lane builds mock Kafka clients and reads no properties. Real credentials never
      // belong in a conformance test.
      kafkaProperties: {},
      instanceTag: "conformance-runner-typescript",
    });
  } catch (error) {
    process.stderr.write(`conformance-runner: opening the session: ${describe(error)}\n`);
    return EXIT_BEHAVIOUR_FAILED;
  }

  // A processor that never rejects: the session must end because the scenario finished, not because
  // this runner threw somewhere the client would report as a failed record.
  client.poll((record) => processorFor(args.behaviour, tracker)(record));

  // report-nothing completes at OBSERVATION, because by prescription its records are never reported
  // and so can never complete. Every other behaviour completes when the last record it was handed
  // has had its outcome decided.
  const finished = args.behaviour === "report-nothing" ? tracker.allObserved : tracker.allCompleted;
  const withinBudget = await raceBudget(finished, args.timeoutSeconds * 1_000);
  if (!withinBudget) {
    process.stderr.write(
      `conformance-runner: scenario ${args.scenario} behaviour ${args.behaviour} did not complete ` +
        `within ${args.timeoutSeconds}s - observed ${tracker.observed} of ${args.expectDispatches}, ` +
        `completed ${tracker.completed}\n`,
    );
    await closeQuietly(client);
    return EXIT_BEHAVIOUR_FAILED;
  }

  if (args.behaviour === "report-nothing") {
    // Hold the session open, reporting nothing, so the suite is watching a LIVE client rather than
    // the wreckage of one - see REPORT_NOTHING_HOLD_MS.
    await sleep(REPORT_NOTHING_HOLD_MS);
    // PRESCRIBED: the record is never reported and the session is abandoned rather than drained - a
    // worker that vanished mid-record is exactly what this scenario models. Exiting closes the
    // sidecar's lifecycle pipe, which reaps it, so nothing is leaked by not closing.
    process.exit(EXIT_OK);
  }

  try {
    await client.close();
  } catch (error) {
    process.stderr.write(`conformance-runner: closing the session: ${describe(error)}\n`);
    return EXIT_BEHAVIOUR_FAILED;
  }
  return EXIT_OK;
}

function processorFor(
  behaviour: Behaviour,
  tracker: Tracker,
): (record: InboundRecord) => Promise<void | Outcome> {
  return async (record: InboundRecord): Promise<void | Outcome> => {
    const ordinal = tracker.observe(record);

    switch (behaviour) {
      case "succeed":
        tracker.complete();
        return;

      case "report-nothing":
        // Never report. An await that never settles is how a JavaScript worker says "this record's
        // function has not returned"; the process exits with the record still in flight.
        await new Promise<never>(() => {});
        return;

      case "fail-then-succeed":
        tracker.complete();
        if (record.attempt === 1) {
          // A throw IS the language's failure idiom, and the message is the reason verbatim.
          throw new Error(PRESCRIBED_FAILURE_REASON);
        }
        return;

      case "hold-first-until-second":
        if (ordinal === 1) {
          // Hold the first record until a SECOND is dispatched. Whether one arrives at all, and
          // which key it carries, is the whole of what the scenario is asking - and it is the Java
          // suite that decides what the answer means.
          await tracker.secondArrived;
        }
        tracker.complete();
        return;
    }
  };
}

/**
 * Counts deliveries and outcomes, and prints the observation line. It holds no per-record state -
 * only counts - because the client library holds none either, and this runner must not become the
 * place where a client's missing bookkeeping is quietly supplied.
 */
class Tracker {
  observed = 0;

  completed = 0;

  private readonly second = deferred();

  private readonly observedEnough = deferred();

  private readonly completedEnough = deferred();

  constructor(private readonly expected: number) {}

  get secondArrived(): Promise<void> {
    return this.second.promise;
  }

  get allObserved(): Promise<void> {
    return this.observedEnough.promise;
  }

  get allCompleted(): Promise<void> {
    return this.completedEnough.promise;
  }

  /** Prints the delivery and returns its 1-based ordinal in arrival order. */
  observe(record: InboundRecord): number {
    const ordinal = ++this.observed;
    // Printed at the moment of delivery, before the behaviour acts on it. reason comes last because
    // it is worker-supplied text that may contain spaces.
    process.stdout.write(
      `dispatch key=${record.key === null ? "" : record.key.toString("utf8")} ` +
        `offset=${record.offset} attempt=${record.attempt} ` +
        `reason=${record.lastFailureReason ?? ""}\n`,
    );
    if (ordinal >= 2) {
      this.second.resolve();
    }
    if (ordinal >= this.expected) {
      this.observedEnough.resolve();
    }
    return ordinal;
  }

  complete(): void {
    this.completed += 1;
    if (this.completed >= this.expected) {
      this.completedEnough.resolve();
    }
  }
}

interface Deferred {
  promise: Promise<void>;
  resolve: () => void;
}

function deferred(): Deferred {
  let resolve: () => void = () => undefined;
  const promise = new Promise<void>((settle) => {
    resolve = settle;
  });
  return { promise, resolve };
}

/** Whether the promise settled inside the budget. The timer is unref'd so it holds nothing open. */
async function raceBudget(promise: Promise<void>, budgetMs: number): Promise<boolean> {
  let timer: NodeJS.Timeout | undefined;
  const expired = new Promise<false>((resolve) => {
    timer = setTimeout(() => {
      resolve(false);
    }, budgetMs);
    timer.unref();
  });
  try {
    return await Promise.race([promise.then(() => true), expired]);
  } finally {
    if (timer !== undefined) {
      clearTimeout(timer);
    }
  }
}

async function sleep(milliseconds: number): Promise<void> {
  await new Promise<void>((resolve) => setTimeout(resolve, milliseconds));
}

/** Shuts down when the outcome is already decided: a close error must not rewrite the verdict. */
async function closeQuietly(client: ParallelConsumerClient): Promise<void> {
  try {
    await client.close();
  } catch (error) {
    process.stderr.write(`conformance-runner: while shutting down: ${describe(error)}\n`);
  }
}

function describe(thrown: unknown): string {
  return thrown instanceof Error ? thrown.message : String(thrown);
}

void main(process.argv.slice(2)).then(
  (status) => {
    process.exitCode = status;
  },
  (error: unknown) => {
    process.stderr.write(`conformance-runner: ${describe(error)}\n`);
    process.exitCode = EXIT_BEHAVIOUR_FAILED;
  },
);
