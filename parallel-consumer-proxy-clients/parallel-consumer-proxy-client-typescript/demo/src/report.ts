// Copyright (C) 2026 Antony Stubbs and contributors

/**
 * What one arm achieved, the banner the demo opens with, and the two tables the whole demo exists
 * to print.
 *
 * **Throughput only, and no latency anywhere.** The backlog is pre-produced, so the workload is
 * closed-loop: a per-record timing is flattered by however far an arm had fallen behind, and
 * reporting one would make the slower arm look better the further behind it got. Records per
 * second over a fixed backlog is the honest number this shape can produce, so it is the only one.
 *
 * **Throughput alone cannot show the work happened**, though, which is why every row also carries
 * `records` and `keys`. Those two are *deterministic* - every language replaying the same backlog
 * reports the same pair - so they are the only figures in the table that compare across languages
 * at all, and `bin/ci-demo-conformance.sh` leans on exactly that.
 */

/** The serial arm's role, spelled the same everywhere. Never bare "core" - that reads as
 * `parallel-consumer-core` (`CONCEPTS.md`). */
export const AK_CORE_ROLE = "AK core";

/**
 * The serial arm's label: the role AND the client that produced the number.
 *
 * **"AK core" on its own is a category, not a client**, and a reader cannot judge a comparison
 * without knowing what ran. TypeScript's answer is kafkajs; Go's is franz-go; Ruby's is rdkafka.
 * `demo/README.md` records why kafkajs and not `@confluentinc/kafka-javascript`, which is the other
 * serious choice here.
 */
export const AK_CORE = `${AK_CORE_ROLE} (kafkajs)`;

/**
 * The sidecar arm's label: this language, over the socket, through this module's client library -
 * and the library is named, because that is the thing being demonstrated. The application itself
 * runs no Kafka client on this path at all.
 */
export const SIDECAR_ARM = "typescript-grpc (this client)";

/** The arm name `bin/ci-demo-conformance.sh` normalises across languages, without its label. */
export const SIDECAR_ARM_NAME = "typescript-grpc";

/**
 * The first thing the demo prints, and it names the PRODUCT.
 *
 * Not the module, not the arm, not a configuration line: a reader who starts this and is met with
 * `typescript-grpc: the proxy granted 100 executor threads` has been told nothing about what they
 * are looking at. Every language prints this same block, differing only in its own name - so it is
 * contract, not decoration.
 */
export const BANNER = [
  "================================================================",
  "  PARALLEL CONSUMER  -  TypeScript demo",
  "  The same records, twice: one at a time, then all at once.",
  "================================================================",
].join("\n");

export interface ArmResult {
  readonly arm: string;
  readonly elapsedMs: number;
  /** Records this arm actually processed. Short of the target is a FAILED arm, not a fast one. */
  readonly processed: number;
  /** Distinct keys this arm observed - what shows the backlog was spread rather than one key. */
  readonly keys: number;
}

export function ratePerSecond(result: ArmResult): number {
  return result.elapsedMs > 0 ? (result.processed * 1_000) / result.elapsedMs : 0;
}

/**
 * The column layout.
 *
 * **Width is deliberately not contract** - column IDENTITY and ORDER are (`demo/README.md` in the
 * proxy module, and the note at the top of `bin/ci-demo-conformance.sh`). This one is wider than
 * Java's because `typescript-grpc (this client)` is a longer label than `java-grpc (this client)`,
 * and that is allowed to be true.
 */
const ARM_WIDTH = 31;
const RECORDS_WIDTH = 9;
const KEYS_WIDTH = 7;
const ELAPSED_WIDTH = 10;
const RATE_WIDTH = 12;
const RATIO_WIDTH = 13;

/**
 * One replay's table.
 *
 * @param baseline       the AK core row every ratio is against, or undefined when the replay had no
 *                       serial arm to compare with
 * @param acrossReplays  true for the big replay, whose baseline comes from the SMALL replay and is
 *                       therefore not like-for-like. Marked in the column heading and footnoted,
 *                       rather than quietly printed as if it were.
 */
export function table(
  title: string,
  results: readonly ArmResult[],
  baseline: ArmResult | undefined,
  acrossReplays: boolean,
): string {
  const baselineRate = baseline === undefined ? 0 : ratePerSecond(baseline);
  const heading = acrossReplays ? `vs ${AK_CORE_ROLE}*` : `vs ${AK_CORE_ROLE}`;
  const lines = [
    "",
    title,
    `  ${"arm".padEnd(ARM_WIDTH)}${"records".padStart(RECORDS_WIDTH)}` +
      `${"keys".padStart(KEYS_WIDTH)}${"elapsed".padStart(ELAPSED_WIDTH)}` +
      `${"msg/s".padStart(RATE_WIDTH)}${heading.padStart(RATIO_WIDTH)}`,
  ];
  for (const result of results) {
    const ratio = baselineRate === 0 ? "-" : `${(ratePerSecond(result) / baselineRate).toFixed(1)}x`;
    lines.push(
      `  ${result.arm.padEnd(ARM_WIDTH)}` +
        `${count(result.processed).padStart(RECORDS_WIDTH)}` +
        `${count(result.keys).padStart(KEYS_WIDTH)}` +
        `${`${(result.elapsedMs / 1_000).toFixed(1)}s`.padStart(ELAPSED_WIDTH)}` +
        `${count(Math.trunc(ratePerSecond(result))).padStart(RATE_WIDTH)}` +
        `${ratio.padStart(RATIO_WIDTH)}`,
    );
  }
  if (acrossReplays) {
    lines.push("");
    lines.push(
      `  * against the SMALL replay's ${AK_CORE_ROLE} arm. Across replays, so not like-for-like.`,
    );
  }
  return `${lines.join("\n")}\n`;
}

function count(value: number): string {
  return value.toLocaleString("en-US");
}
