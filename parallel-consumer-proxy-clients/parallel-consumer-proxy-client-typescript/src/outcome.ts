// Copyright (C) 2026 Antony Stubbs and contributors

import type { InboundRecord, OutboundRecord } from "./records";

/**
 * The per-record verdict: success (optionally carrying records to produce) or failure (carrying a
 * reason). A discriminated union rather than a class hierarchy - the TypeScript spelling of the
 * reference surface's closed two-armed value, narrowable by `outcome.kind` with no `instanceof`.
 *
 * There is deliberately no third arm. A processor that cannot decide has not finished processing,
 * and there is no per-record deadline that would let it stop: a record stays in flight until the
 * function returns, which is the product's differentiator rather than an oversight.
 */
export type Outcome =
  | { readonly kind: "success"; readonly produce: readonly OutboundRecord[] }
  | { readonly kind: "failure"; readonly reason: string | null };

/**
 * The user's function: one record in, one verdict out.
 *
 * THREE SPELLINGS, ONE MEANING, because TypeScript has three natural ones and forcing a single
 * spelling would be un-idiomatic in every direction:
 *
 * - return nothing - a bare success, the common case;
 * - throw, or reject - a failure, with the error's message as the reason. Exceptions ARE the
 *   language's error idiom, so a processor that lets one escape has said "failure" clearly;
 * - return an {@link Outcome} - needed when the success carries records to produce, or when a
 *   failure wants a reason without an exception.
 *
 * The translation happens in exactly one place ({@link applyProcessor}), so every spelling reaches
 * the wire through the same path.
 *
 * `async` is fully supported and expected: a processor returning a promise is awaited, and the
 * record stays in flight until it settles.
 */
export type RecordProcessor = (
  record: InboundRecord,
) => void | Outcome | Promise<void | Outcome>;

const BARE_SUCCESS: Outcome = { kind: "success", produce: [] };

/** Success, optionally with records for Parallel Consumer to produce. */
export function success(produce?: readonly OutboundRecord[]): Outcome {
  if (produce === undefined || produce.length === 0) {
    return BARE_SUCCESS;
  }
  return { kind: "success", produce: [...produce] };
}

/**
 * Failure: the record returns to Parallel Consumer's retry scheduling with its attempt count
 * consumed, and the reason travels with the redelivery as `lastFailureReason`.
 */
export function failure(reason?: string): Outcome {
  return { kind: "failure", reason: reason ?? null };
}

/**
 * Runs the processor and reduces every way it can end to one {@link Outcome}. The single place a
 * throw, a rejection, or a `void` return becomes a verdict.
 *
 * A processor that returns something that is not an outcome is a bug in the application, reported
 * as a failure rather than allowed to tear down the client: one bad record must not take the
 * session with it.
 */
export async function applyProcessor(
  processor: RecordProcessor,
  record: InboundRecord,
): Promise<Outcome> {
  let returned: void | Outcome;
  try {
    returned = await processor(record);
  } catch (thrown) {
    return failure(describe(thrown));
  }
  if (returned === undefined || returned === null) {
    return BARE_SUCCESS;
  }
  if (returned.kind === "success" || returned.kind === "failure") {
    return returned;
  }
  return failure("the processor returned a value that is not an Outcome");
}

/** An error's message, or its best textual form. Never a whole object dump. */
function describe(thrown: unknown): string {
  if (thrown instanceof Error) {
    return thrown.message.length > 0 ? thrown.message : thrown.name;
  }
  return typeof thrown === "string" ? thrown : "the processor threw a non-Error value";
}
