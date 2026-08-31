// Copyright (C) 2026 Antony Stubbs and contributors

/**
 * Parallel Consumer for TypeScript - the public surface, and nothing else.
 *
 * Everything reachable from here is stable for wave one's purposes; anything not exported is this
 * client's own business, including every generated protobuf type. The wire is the proxy's contract,
 * not the application's: no epoch, no token, no `Configure` message appears on this surface.
 */

export { ParallelConsumerClient } from "./client";
export type { ClientOptions, CommitMode, Ordering, SidecarCommand } from "./options";
export { failure, success } from "./outcome";
export type { Outcome, RecordProcessor } from "./outcome";
export type { InboundRecord, OutboundRecord } from "./records";
export { Capability, DECLARED_CAPABILITIES } from "./session";
export type { SessionInfo } from "./session";
export {
  ConfigurationError,
  ParallelConsumerError,
  ProtocolViolationError,
  SessionClosedError,
  SidecarError,
} from "./errors";
