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
 * Its contract - flags, exit codes, the two stdout lines, the behaviour tokens - is documented once, in
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
  "hold-until-ceiling-full",
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

/**
 * How long `hold-until-ceiling-full` keeps a FULL group held before releasing it.
 *
 * IT IS WHAT TURNS "the ceiling was never exceeded" FROM A RACE INTO A MEASUREMENT. Release the
 * group the instant it fills and a client that declared a larger ceiling still passes - its extra
 * records arrive a few milliseconds later, by which time the outstanding count has already fallen
 * back. Holding the full ceiling still means the extra dispatch arrives INSIDE the window and
 * prints its line while every other record is unresolved. A correct engine cannot dispatch anything
 * during the window at all, so the wait costs a conforming client nothing but time.
 */
const CEILING_SETTLE_MS = 250;

interface Arguments {
  scenario: string;
  behaviour: Behaviour;
  sidecar: string;
  expectDispatches: number;
  maxConcurrency: number;
  timeoutSeconds: number;
}

async function main(argv: readonly string[]): Promise<number> {
  const parsed = parse(argv);
  if (typeof parsed === "number") {
    return parsed;
  }
  return run(parsed);
}

/** The six flags, spelled identically in every language - including the British `--behaviour`. */
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
  const ceiling = Number(values.get("--max-concurrency"));
  const budget = Number(values.get("--timeout-seconds"));

  if (scenario === undefined || scenario.length === 0) return usage("--scenario is required");
  if (behaviour === undefined) return usage("--behaviour is required");
  if (!(BEHAVIOURS as readonly string[]).includes(behaviour)) {
    return usage(`unknown behaviour ${JSON.stringify(behaviour)}`);
  }
  if (sidecar === undefined || sidecar.length === 0) return usage("--sidecar is required");
  if (!isAbsolute(sidecar)) return usage(`--sidecar must be absolute, got ${JSON.stringify(sidecar)}`);
  if (!Number.isInteger(expect) || expect < 1) return usage("--expect-dispatches must be at least 1");
  if (!Number.isInteger(ceiling) || ceiling < 1) return usage("--max-concurrency must be at least 1");
  if (!Number.isInteger(budget) || budget < 1) return usage("--timeout-seconds must be at least 1");

  return {
    scenario,
    behaviour: behaviour as Behaviour,
    sidecar,
    expectDispatches: expect,
    maxConcurrency: ceiling,
    timeoutSeconds: budget,
  };
}

function usage(problem: string): number {
  process.stderr.write(`conformance-runner: ${problem}\n`);
  return EXIT_USAGE;
}

async function run(args: Arguments): Promise<number> {
  const tracker = new Tracker(args.expectDispatches, args.maxConcurrency, args.timeoutSeconds * 1_000);

  let client: ParallelConsumerClient;
  try {
    client = await ParallelConsumerClient.open({
      sidecar: { executable: args.sidecar, stderr: "inherit" },
      // THE SCENARIO NAME IS ALSO THE TOPIC NAME.
      topics: [args.scenario],
      // The ceiling is the SCENARIO's to choose and this runner never derives one: it is whatever
      // --max-concurrency said, and nothing else may set it. Deriving it from --expect-dispatches,
      // which is what this line used to do, is by construction a ceiling no scenario can reach.
      maxConcurrency: args.maxConcurrency,
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

  // A prescribed behaviour that gave up releases every wait rather than letting each time out in
  // turn, so the run finishes inside the budget with the verdict already decided against it.
  if (tracker.failure !== undefined) {
    process.stderr.write(
      `conformance-runner: scenario ${args.scenario} behaviour ${args.behaviour}: ${tracker.failure}\n`,
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
        tracker.complete(record);
        return;

      case "report-nothing":
        // Never report, and print no `settled` line: by prescription this record is never resolved
        // and the ABSENCE is the observation. An await that never settles is how a JavaScript worker
        // says "this record's function has not returned"; the process exits with it still in flight.
        await new Promise<never>(() => {});
        return;

      case "fail-then-succeed": {
        // The reason is the contract's fixed literal on the first attempt and empty afterwards, and
        // it is the reason this runner REPORTS - not the one the record arrived carrying.
        const reason = record.attempt === 1 ? PRESCRIBED_FAILURE_REASON : "";
        tracker.complete(record, reason);
        if (reason !== "") {
          // A throw IS the language's failure idiom, and the message is the reason verbatim.
          throw new Error(reason);
        }
        return;
      }

      case "hold-first-until-second":
        if (ordinal === 1) {
          // Hold the first record until a SECOND is dispatched. Whether one arrives at all, and
          // which key it carries, is the whole of what the scenario is asking - and it is the Java
          // suite that decides what the answer means.
          await tracker.secondArrived;
        }
        tracker.complete(record);
        return;

      case "hold-until-ceiling-full": {
        // Hold until a ceiling's worth of records are held AT ONCE, keep the full group still for
        // CEILING_SETTLE_MS, then release the whole group as successes. An await that has not
        // resolved is how this runtime says the record's function has not returned, so a held
        // record is genuinely unresolved for as long as it looks - the property the scenario
        // measures.
        const filled = await tracker.enterCeilingGroup();
        if (!filled) {
          // The group never filled inside the budget: this runner could not do what was prescribed,
          // so it reports a failure rather than a plausible-looking success, and the run exits 1.
          const reason = tracker.abandon(`the ceiling group of ${tracker.ceiling} never filled`);
          tracker.complete(record, reason);
          throw new Error(reason);
        }
        tracker.complete(record);
        return;
      }
    }
  };
}

/**
 * Counts deliveries and outcomes, and prints the two observation lines. It holds no per-record
 * state - only counts - because the client library holds none either, and this runner must not
 * become the place where a client's missing bookkeeping is quietly supplied.
 *
 * THERE IS NO LOCK HERE, AND NOTHING SERIALIZES THE STDOUT WRITES. That is the one place this
 * differs visibly from the Go, Java and Rust runners, so a reader arriving from one of those will
 * come looking for the mutex the contract asks for - and its absence is deliberate, not an
 * omission. The suite reads overlap purely from the ORDER of these lines, and one event loop
 * already guarantees it: `observe` and `complete` each run to completion without yielding, so no
 * second delivery can interleave between counting a record and printing the line that reports it.
 * A mutex would have nothing to exclude. The contract says as much in its own words - "in a
 * single-threaded async runtime (TypeScript) the event loop already serializes writes".
 */
class Tracker {
  observed = 0;

  completed = 0;

  /** Set the first time a prescribed behaviour gives up, and it is the run's verdict from then on. */
  failure: string | undefined;

  private readonly second = deferred();

  private readonly observedEnough = deferred();

  private readonly completedEnough = deferred();

  /** The `hold-until-ceiling-full` group: how many are held right now, and of which generation. */
  private held = 0;

  private generation = 0;

  /** Resolved when the CURRENT generation is released - a fresh one replaces it each time. */
  private groupReleased = deferred();

  /** When the whole run's budget runs out, so a group that never fills fails instead of hanging. */
  private readonly deadline: number;

  constructor(
    private readonly expected: number,
    readonly ceiling: number,
    budgetMs: number,
  ) {
    this.deadline = Date.now() + budgetMs;
  }

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
    // Printed at the moment of delivery, before the behaviour acts on it, and it OPENS this
    // record's unresolved window - the settled line closes it.
    this.print("dispatch", record, record.lastFailureReason ?? "");
    if (ordinal >= 2) {
      this.second.resolve();
    }
    if (ordinal >= this.expected) {
      this.observedEnough.resolve();
    }
    return ordinal;
  }

  /**
   * Prints the `settled` line and counts the outcome. Called the moment the prescribed behaviour has
   * DECIDED this record's outcome, immediately before the runner reports it, because that is when
   * the record stops being unresolved.
   *
   * @param reason the failure this runner is reporting, empty for a success - never the reason the
   *     record arrived carrying
   */
  complete(record: InboundRecord, reason = ""): void {
    this.print("settled", record, reason);
    this.completed += 1;
    if (this.completed >= this.expected) {
      this.completedEnough.resolve();
    }
  }

  /**
   * The cyclic barrier at the heart of `hold-until-ceiling-full`: hold this record until it is one
   * of `ceiling` held at once, keep the full group still for CEILING_SETTLE_MS, and release it.
   *
   * A group also releases once every prescribed delivery has been observed, so a scenario whose
   * record count is not a multiple of its ceiling cannot strand its last, short group.
   *
   * @returns false if the group never filled inside the budget - this runner failing, rather than
   *     the client being wrong about anything
   */
  async enterCeilingGroup(): Promise<boolean> {
    const myGeneration = this.generation;
    let released = this.groupReleased.promise;
    this.held += 1;
    const releasing = this.held >= this.ceiling || this.observed >= this.expected;

    if (!releasing) {
      // The waiter's half. In a runtime with threads this is a condition-variable wait re-checking
      // the generation under the lock; here the generation's own promise IS the condition, and the
      // loop re-reads the counter for exactly the same reason - to wake for THIS group's release
      // rather than for whatever happens to resolve first.
      while (this.generation === myGeneration) {
        if (!(await raceBudget(released, this.deadline - Date.now()))) {
          return false;
        }
        released = this.groupReleased.promise;
      }
      return true;
    }

    // THE SETTLE WINDOW. Nothing is held across it in the sense the other languages mean - this
    // await yields the event loop, which is the whole point: a record the engine should not be
    // dispatching can still arrive and print its line during the window, and that arrival is what
    // the scenario looks for. A correct engine cannot dispatch anything here, so the wait costs a
    // conforming client only time.
    await sleep(CEILING_SETTLE_MS);
    this.held = 0;
    this.generation += 1;
    const ending = this.groupReleased;
    this.groupReleased = deferred();
    ending.resolve();
    return true;
  }

  /**
   * "Exit 1" from inside a behaviour: record the verdict and release every wait rather than leaving
   * each to time out in turn.
   *
   * @returns the failure reason to report for the record that gave up
   */
  abandon(problem: string): string {
    this.failure ??= problem;
    this.observedEnough.resolve();
    this.completedEnough.resolve();
    return `conformance: ${problem}`;
  }

  /** reason comes LAST because it is worker-supplied text that may contain spaces. */
  private print(kind: "dispatch" | "settled", record: InboundRecord, reason: string): void {
    process.stdout.write(
      `${kind} key=${record.key === null ? "" : record.key.toString("utf8")} ` +
        `offset=${record.offset} attempt=${record.attempt} reason=${reason}\n`,
    );
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
