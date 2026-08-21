// Copyright (C) 2026 Antony Stubbs and contributors

/**
 * The demo's dials - **the same surface the Java reference demo publishes**, in TypeScript.
 *
 * The contract is `parallel-consumer-proxy/demo/README.md`: the same seven flags with the same
 * defaults, one `PC_DEMO_` environment variable per flag, and the ordinary precedence - flags beat
 * the environment beats the defaults. A container passes configuration by environment while a
 * person at a terminal passes flags, and each must be able to override the other's layer.
 *
 * R39 constrains how configuration reaches the *proxy*. A demo is an application, so its flags are
 * not a violation of it; without this note somebody reads `--records` as breaking the plan's own
 * rule and deletes it.
 */

/** Prefix for every environment variable this demo reads, so a reader can grep one string. */
export const ENV_PREFIX = "PC_DEMO_";

/**
 * The ceiling on `records * replayFactor`.
 *
 * It is `2^31 - 1` because the reference demo counts records in a Java `int` and refuses the same
 * product. TypeScript would carry it happily in a double, and diverging here would mean the two
 * demos accept different inputs - so the limit is copied deliberately rather than inherited.
 */
const MOST_RECORDS_THE_DEMO_COUNTS = 2_147_483_647;

export interface DemoOptions {
  readonly records: number;
  readonly delayMs: number;
  readonly maxConcurrency: number;
  readonly partitions: number;
  readonly replayFactor: number;
  /** An existing broker, when the caller supplied one. Absent means "start one". */
  readonly bootstrap?: string;
  /** An existing topic, when the caller supplied one. Absent means the demo names its own. */
  readonly topic?: string;
}

const DEFAULTS = {
  records: 2_000,
  delayMs: 2,
  maxConcurrency: 100,
  partitions: 10,
  replayFactor: 20,
} as const;

/** Thrown for a flag this demo does not know, a missing value, or a value out of range. */
export class UsageError extends Error {}

/** Whether the caller asked for the usage text rather than a run. */
export function isHelpRequested(argv: readonly string[]): boolean {
  return argv.includes("-h") || argv.includes("--help");
}

export const USAGE = `usage: demo/run.sh [options]

  --records N        records in the comparison replay   (default ${DEFAULTS.records})
  --delay-ms N       simulated work per record, ms      (default ${DEFAULTS.delayMs})
  --concurrency N    max in-flight records              (default ${DEFAULTS.maxConcurrency})
  --partitions N     partitions on the demo topic       (default ${DEFAULTS.partitions})
  --replay-factor N  big replay = records x N; 1 skips  (default ${DEFAULTS.replayFactor})
  --bootstrap ADDR   an existing broker; omit to start one
  --topic NAME       an existing topic; omit to create one

Every flag has an environment variable: --delay-ms is ${ENV_PREFIX}DELAY_MS.
Flags beat the environment beats the defaults.`;

/**
 * Parses the demo's command line, falling back to the environment and then to the defaults.
 *
 * An unknown flag, a missing value or a value out of range is a {@link UsageError} rather than
 * something quietly ignored: a demo that swallows a misspelled flag reports numbers for settings
 * the user did not ask for.
 *
 * @param argv the process arguments, which may legitimately be empty - that is the double-click
 *             case, and it must work
 * @param env  the environment to read, passed in rather than reached for, so this is testable
 */
export function parseOptions(
  argv: readonly string[],
  env: Readonly<Record<string, string | undefined>>,
): DemoOptions {
  let records = fromEnvironment(env, "RECORDS", positive) ?? DEFAULTS.records;
  let delayMs = fromEnvironment(env, "DELAY_MS", nonNegative) ?? DEFAULTS.delayMs;
  let maxConcurrency = fromEnvironment(env, "CONCURRENCY", positive) ?? DEFAULTS.maxConcurrency;
  let partitions = fromEnvironment(env, "PARTITIONS", positive) ?? DEFAULTS.partitions;
  let replayFactor = fromEnvironment(env, "REPLAY_FACTOR", nonNegative) ?? DEFAULTS.replayFactor;
  let bootstrap = text(env[`${ENV_PREFIX}BOOTSTRAP`]);
  let topic = text(env[`${ENV_PREFIX}TOPIC`]);

  for (let index = 0; index < argv.length; index += 1) {
    const flag = argv[index] ?? "";
    switch (flag) {
      case "--records":
        records = positive(flag, value(argv, (index += 1), flag));
        break;
      case "--delay-ms":
        delayMs = nonNegative(flag, value(argv, (index += 1), flag));
        break;
      case "--concurrency":
        maxConcurrency = positive(flag, value(argv, (index += 1), flag));
        break;
      case "--partitions":
        partitions = positive(flag, value(argv, (index += 1), flag));
        break;
      // 1 or less skips the big replay, so this one is allowed to be zero
      case "--replay-factor":
        replayFactor = nonNegative(flag, value(argv, (index += 1), flag));
        break;
      case "--bootstrap":
        bootstrap = value(argv, (index += 1), flag);
        break;
      case "--topic":
        topic = value(argv, (index += 1), flag);
        break;
      default:
        throw new UsageError(`unknown option: ${flag}`);
    }
  }

  // Checked here rather than trusted later: the reference demo overflows a Java int on this
  // product, and a demo that accepted more here would be a different demo.
  const bigReplay = records * Math.max(1, replayFactor);
  if (bigReplay > MOST_RECORDS_THE_DEMO_COUNTS) {
    throw new UsageError(
      `--records times --replay-factor is ${bigReplay}, which is more records than the demo can ` +
        `count; lower one of them`,
    );
  }

  return { records, delayMs, maxConcurrency, partitions, replayFactor, bootstrap, topic };
}

/** The records the big replay consumes in total, including the small replay's own. */
export function bigReplayRecords(options: DemoOptions): number {
  return options.records * Math.max(1, options.replayFactor);
}

/** True when the big replay is worth running at all; a factor of 1 or less skips it. */
export function bigReplayWanted(options: DemoOptions): boolean {
  return options.replayFactor > 1;
}

/**
 * The effective configuration, for printing before the run.
 *
 * A number without its settings is not reproducible, so this is part of the contract rather than a
 * debugging aid. **The bootstrap address is deliberately absent**: own-cluster mode puts a user's
 * real broker there, and the credential-hygiene rule that binds the proxy binds a demo too -
 * nothing logged, nothing echoed.
 */
export function fingerprint(options: DemoOptions, topic: string): string {
  return [
    `records = ${options.records}`,
    `delayMs = ${options.delayMs}`,
    `maxConcurrency = ${options.maxConcurrency}`,
    `partitions = ${options.partitions}`,
    `replayFactor = ${options.replayFactor}`,
    `topic = ${topic}`,
  ]
    .map((line) => `  ${line}`)
    .join("\n");
}

function value(argv: readonly string[], index: number, flag: string): string {
  const raw = argv[index];
  if (raw === undefined) {
    throw new UsageError(`${flag} needs a value`);
  }
  return raw;
}

function fromEnvironment(
  env: Readonly<Record<string, string | undefined>>,
  suffix: string,
  check: (name: string, raw: string) => number,
): number | undefined {
  const raw = text(env[ENV_PREFIX + suffix]);
  return raw === undefined ? undefined : check(ENV_PREFIX + suffix, raw);
}

function text(raw: string | undefined): string | undefined {
  const trimmed = raw?.trim();
  return trimmed === undefined || trimmed.length === 0 ? undefined : trimmed;
}

function positive(name: string, raw: string): number {
  const parsed = whole(name, raw);
  if (parsed < 1) {
    throw new UsageError(`${name} must be at least 1, got ${parsed}`);
  }
  return parsed;
}

function nonNegative(name: string, raw: string): number {
  const parsed = whole(name, raw);
  if (parsed < 0) {
    throw new UsageError(`${name} must not be negative, got ${parsed}`);
  }
  return parsed;
}

function whole(name: string, raw: string): number {
  const parsed = Number(raw.trim());
  // Number("") is 0 and Number("2x") is NaN - both are refused, and so is 2.5: this is a count.
  if (raw.trim().length === 0 || !Number.isInteger(parsed)) {
    throw new UsageError(`${name} needs a whole number, got '${raw}'`);
  }
  return parsed;
}
