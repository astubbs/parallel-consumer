// Copyright (C) 2026 Antony Stubbs and contributors

/**
 * What a processor is handed, and what it may ask to be produced.
 *
 * Keys and values are `Buffer` - the bytes Kafka held. The proxy never deserializes, so
 * deserialization is the application's, in the application's own types. `null` is not `Buffer[0]`:
 * Kafka distinguishes a null key or a tombstone value from an empty one, and so does this surface.
 */

/**
 * One record as delivered, carrying the Parallel-Consumer-derived delivery state an in-process user
 * function would see. Nothing transport-specific appears here - no epoch, no token, no connection
 * identity - because the fencing token is the client's to echo and never the application's to read.
 */
export interface InboundRecord {
  readonly topic: string;
  readonly partition: number;
  /**
   * `bigint`, because a Kafka offset is a 64-bit integer and `number` silently loses precision
   * past 2^53. Compare with `0n`, not `0`.
   */
  readonly offset: bigint;
  /** The key bytes, or `null` for a keyless record. */
  readonly key: Buffer | null;
  /** The value bytes, or `null` for a tombstone - which is not the same as an empty value. */
  readonly value: Buffer | null;
  /** 1 on first delivery, 2 on the first redelivery. */
  readonly attempt: number;
  /** When the previous attempt failed; `null` before the first failure. */
  readonly lastFailureAt: Date | null;
  /**
   * The reason recorded for the previous failed attempt; `null` before the first failure. Worker
   * supplied and may embed record payload: treat as untrusted input.
   */
  readonly lastFailureReason: string | null;
}

/**
 * A record a successful outcome asks Parallel Consumer to produce - the only sanctioned route for a
 * processor's Kafka output. Workers never produce directly; the proxy produces with its own
 * producer before the input record's offset may become eligible to commit.
 */
export interface OutboundRecord {
  readonly topic: string;
  /** The key bytes to produce, or `null` for a keyless record. */
  readonly key?: Buffer | null;
  /** The value bytes to produce, or `null` for a tombstone. */
  readonly value?: Buffer | null;
}
