// Copyright (C) 2026 Antony Stubbs and contributors

/**
 * What one arm achieved, and the two tables the whole demo exists to print.
 *
 * **Throughput only, and no latency anywhere.** The backlog is pre-produced, so the workload is
 * closed-loop: a per-record timing is flattered by however far an arm had fallen behind, and
 * reporting one would make the slower arm look better the further behind it got. Records per
 * second over a fixed backlog is the honest number this shape can produce, so it is the only one.
 */

/** The serial arm's name, spelled the same everywhere. Never bare "core" - that reads as
 * `parallel-consumer-core` (`CONCEPTS.md`). */
export const AK_CORE = "AK core";

/** The sidecar arm's name: this language, over the socket, through this module's client library. */
export const SIDECAR_ARM = "typescript-grpc";

export interface ArmResult {
  readonly arm: string;
  readonly elapsedMs: number;
  readonly processed: number;
}

export function ratePerSecond(result: ArmResult): number {
  return result.elapsedMs > 0 ? (result.processed * 1_000) / result.elapsedMs : 0;
}

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
  const heading = acrossReplays ? "vs AK core*" : "vs AK core";
  const lines = [
    "",
    title,
    `  ${"arm".padEnd(16)}${"elapsed".padStart(10)}${"msg/s".padStart(14)}${heading.padStart(14)}`,
  ];
  for (const result of results) {
    const ratio = baselineRate === 0 ? "-" : `${(ratePerSecond(result) / baselineRate).toFixed(1)}x`;
    lines.push(
      `  ${result.arm.padEnd(16)}` +
        `${`${(result.elapsedMs / 1_000).toFixed(1)}s`.padStart(10)}` +
        `${Math.trunc(ratePerSecond(result)).toLocaleString("en-US").padStart(14)}` +
        `${ratio.padStart(14)}`,
    );
  }
  if (acrossReplays) {
    lines.push("");
    lines.push("  * against the SMALL replay's AK core arm. Across replays, so not like-for-like.");
  }
  return `${lines.join("\n")}\n`;
}
