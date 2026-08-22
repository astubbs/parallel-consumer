// Copyright (C) 2026 Antony Stubbs and contributors
//
// The user's function, and what it returns.
//
// SWIFT HAS BOTH HALVES the authoring guide's section 1 describes, so this surface offers both: an
// explicit `.failure(reason:)` for code that DECIDES a record failed, and a single translation of a
// THROWN error into a failure outcome. `throws` is a typed, checked part of a Swift function's
// signature rather than an ambient possibility, so the two are genuinely different statements - and
// the translation happens in exactly one place, `ParallelConsumerClient.run(_:on:)`, so there is no
// second spelling of it to keep in step.

/// What one invocation decided about one record.
public enum Outcome: Sendable {
    /// The record was processed. The attached records, if any, are what the proxy should produce
    /// with its own producer before the input record's offset may become eligible to commit - the
    /// only sanctioned route for worker output to Kafka.
    case success(produce: [OutboundRecord])

    /// The record failed and should be redelivered. The reason travels to the proxy and comes back
    /// on the next delivery verbatim: DO NOT put record payload or credentials in it.
    case failure(reason: String)

    /// The record was processed, with no output.
    public static var success: Outcome { .success(produce: []) }

    /// Whether this outcome reports success.
    public var isSuccess: Bool {
        if case .success = self { return true }
        return false
    }
}

/// The user's function: takes one record, returns its outcome, or throws.
///
/// It is invoked concurrently on as many child tasks as the proxy asked for, so an implementation
/// that holds state must be safe to call concurrently - which the Swift 6 language mode checks
/// rather than trusts, since the closure is `@Sendable`.
///
/// It is `async` because that is the shape a Swift caller expects of work that may wait on anything,
/// and because a synchronous function is trivially expressible as one that never suspends.
public typealias RecordProcessor = @Sendable (InboundRecord) async throws -> Outcome
