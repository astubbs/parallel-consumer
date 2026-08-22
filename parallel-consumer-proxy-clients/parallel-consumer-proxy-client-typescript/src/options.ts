// Copyright (C) 2026 Antony Stubbs and contributors

import { ConfigurationError } from "./errors";

/**
 * How to launch the sidecar proxy: an absolute path to a binary, and its arguments.
 *
 * THE APPLICATION SUPPLIES THIS EXPLICITLY, and the library never goes looking. No `PATH` lookup,
 * no relative resolution, no directory an attacker could influence - this process is about to be
 * handed the application's Kafka credentials, so which binary runs is a security decision and it
 * belongs to the application (client-authoring guide §2).
 *
 * `args` are the sidecar's own arguments and are NOT configuration: configuration is code and
 * travels in the connect-time `Configure` message, never by argv, environment or file.
 */
export interface SidecarCommand {
  readonly executable: string;
  readonly args?: readonly string[];
  /**
   * Where the sidecar's stderr goes. `"inherit"` (the default) puts it on this process's stderr,
   * which is what makes a sidecar that fails at startup say so.
   */
  readonly stderr?: "inherit" | "ignore";
}

/** Processing order, spelled as a string union - the TypeScript idiom for a closed set of names. */
export type Ordering = "unordered" | "partition" | "key";

/** Commit mode. The transactional mode is expressible on the wire and always refused by the proxy. */
export type CommitMode = "periodic-consumer-sync" | "periodic-consumer-async";

/**
 * Connect-time configuration: what the application tells Parallel Consumer, in the application's
 * own language, handed over at construction.
 *
 * A PLAIN OBJECT, NOT A BUILDER. An omitted property means "take the proxy's default" - which is
 * exactly what the wire means by an absent field, so the two conventions agree with no translation
 * table in between, and `Configured` reports what each default resolved to.
 *
 * `kafkaProperties` is credential-bearing. It is written to the stream and nowhere else: never to
 * argv, never to an environment variable, never to a temp file, and never to a log line at any
 * level (client-authoring guide §6).
 */
export interface ClientOptions {
  /** How to launch the sidecar. Required - the library will not go looking for one. */
  readonly sidecar: SidecarCommand;

  /** Topic-list subscription. Exactly one of `topics` / `topicPattern` must be given. */
  readonly topics?: readonly string[];
  /** Regex subscription, in Java regex syntax - the proxy compiles it. */
  readonly topicPattern?: string;

  /**
   * The in-flight ceiling: the most records the proxy will have out to this client at once, and
   * therefore this client's dispatch-queue depth. There is no "unlimited".
   */
  readonly maxConcurrency?: number;

  /** Kafka client configuration - bootstrap servers, group id, credentials. Never logged. */
  readonly kafkaProperties?: Readonly<Record<string, string>>;

  readonly ordering?: Ordering;
  readonly commitMode?: CommitMode;

  /** Milliseconds between offset commits. */
  readonly commitIntervalMs?: number;
  /** Milliseconds a failed record waits before redelivery. */
  readonly defaultMessageRetryDelayMs?: number;

  /** Instance tag for the engine's own metrics and logging. */
  readonly instanceTag?: string;

  /**
   * Called when the client notices something it will not act on - an un-negotiated message, say.
   * Default: silence. Whatever is passed here must never be given the options object itself, which
   * would put `kafkaProperties` in a log line.
   */
  readonly onWarning?: (message: string) => void;

  /** How long to wait for the sidecar to announce its port. Default 30s. */
  readonly startupTimeoutMs?: number;
  /** How long `close()` waits for the sidecar to exit before killing it. Default 10s. */
  readonly shutdownTimeoutMs?: number;
}

/** Validated, defaulted options - the shape the rest of the client uses. */
export interface ResolvedOptions extends ClientOptions {
  readonly onWarning: (message: string) => void;
  readonly startupTimeoutMs: number;
  readonly shutdownTimeoutMs: number;
}

/**
 * Checks what can be checked before anything is spawned or connected.
 *
 * Only the subscription rule is enforced here, and only because getting it wrong costs a process
 * launch and a round trip to learn `INVALID_ARGUMENT`. Everything else is the proxy's to judge:
 * duplicating its validation here would be two rules that drift apart, and the effective values
 * come back in `Configured` anyway.
 */
export function resolveOptions(options: ClientOptions): ResolvedOptions {
  const hasTopics = options.topics !== undefined && options.topics.length > 0;
  const hasPattern = options.topicPattern !== undefined && options.topicPattern.length > 0;
  if (hasTopics === hasPattern) {
    throw new ConfigurationError(
      "exactly one of topics / topicPattern must be given - the subscription is fixed for the " +
        "proxy's lifetime and the proxy refuses both or neither",
    );
  }
  if (options.sidecar.executable.length === 0) {
    throw new ConfigurationError("sidecar.executable must be the path to the sidecar binary");
  }
  if (options.maxConcurrency !== undefined && options.maxConcurrency < 1) {
    throw new ConfigurationError(
      `maxConcurrency must be at least 1, got ${options.maxConcurrency} - there is no "unlimited"`,
    );
  }
  return {
    ...options,
    onWarning: options.onWarning ?? (() => undefined),
    startupTimeoutMs: options.startupTimeoutMs ?? 30_000,
    shutdownTimeoutMs: options.shutdownTimeoutMs ?? 10_000,
  };
}
