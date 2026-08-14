// Copyright (C) 2026 Antony Stubbs and contributors

/**
 * The errors this client raises. Errors are thrown rather than returned: TypeScript has one error
 * idiom and it is exceptions (rejections, for the async surface), and a `Result` type here would be
 * the un-idiomatic mirror of a language that has no exceptions.
 *
 * NOTHING HERE EVER EMBEDS `kafkaProperties`, or any value from it. The property map carries
 * credentials, and an error message is a log line waiting to happen (client-authoring guide §6). A
 * message may name a property KEY; it may never name a value.
 */

/** Base class, so an application can catch everything this library raises with one clause. */
export class ParallelConsumerError extends Error {
  constructor(message: string, options?: { cause?: unknown }) {
    super(message, options);
    this.name = new.target.name;
  }
}

/**
 * The peer broke the protocol, and the session cannot continue.
 *
 * The specification's own example is a `Dispatch` that overflows the client's queue past
 * `max_concurrency` - the proxy exceeding its own declared in-flight ceiling. It says the client
 * "fails the stream with FAILED_PRECONDITION naming the count", and that is not reachable from any
 * gRPC client: only a server sets a status. What a client can do is CANCEL the call, which the
 * proxy observes as a cancellation, and raise this - carrying the count the specification wanted
 * named. Recorded as a specification defect in `docs/inflight/clients/typescript.md`.
 */
export class ProtocolViolationError extends ParallelConsumerError {}

/** The session ended - cleanly or otherwise - and the operation asked for needs a live one. */
export class SessionClosedError extends ParallelConsumerError {}

/** The sidecar process could not be started, or died before it announced its port. */
export class SidecarError extends ParallelConsumerError {}

/** `ClientOptions` could not be used as given. Thrown before anything is spawned or connected. */
export class ConfigurationError extends ParallelConsumerError {}
