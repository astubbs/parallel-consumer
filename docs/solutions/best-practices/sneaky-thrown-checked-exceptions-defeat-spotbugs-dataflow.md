---
title: Sneaky-thrown checked exceptions defeat SpotBugs dataflow - key invariant checks on data, not exception types
date: 2026-08-18
category: best-practices
module: parallel-consumer-core
problem_type: best_practice
component: tooling
severity: medium
related_components:
  - testing_framework
applies_when:
  - "a checked exception is sneaky-thrown (Lombok @SneakyThrows) and later caught via catch(Exception)"
  - "SpotBugs flags BC_IMPOSSIBLE_INSTANCEOF (or a similar dataflow finding) on a check that is provably live at runtime"
  - "the condition the exception represents can be recomputed from data already in scope (payload type, enum, mode flag)"
  - "suppressing the finding would have to be re-justified on every PR that touches the file"
tags: [spotbugs, lombok, sneaky-throws, static-analysis, exception-handling, checked-exceptions, offset-encoding]
---

# Sneaky-thrown checked exceptions defeat SpotBugs dataflow - key invariant checks on data, not exception types

## Context

Lombok `@SneakyThrows` lets a checked exception propagate out of a method whose signature does not
declare it, so a downstream `catch (Exception)` block can genuinely receive it at runtime - while
SpotBugs' dataflow analysis, working from declared signatures, proves the checked type cannot reach
that block and flags any `instanceof <checked-type>` test there as `BC_IMPOSSIBLE_INSTANCEOF`. The
code is correct at runtime and "impossible" to the analyser at the same time.

This surfaced on the offset-encoding density work (PR astubbs#306, issue astubbs#192). The decode
choke point `OffsetMapCodecManager.decodeCompressedOffsets`
(`parallel-consumer-core/src/main/java/bz/stub/parallelconsumer/offsets/OffsetMapCodecManager.java`)
tested `decodeFailure instanceof EncodingNotSupportedException` inside `catch (Exception)`. The test
was live: `EncodedOffsetPair.getDecodedIncompletes` is annotated `@SneakyThrows` and throws the
checked `KafkaStreamsEncodingNotSupported` (a subclass of `EncodingNotSupportedException`) without
declaring it. SpotBugs flagged the `instanceof` as impossible anyway.

The finding would not go away on its own. This repo's `static: spotbugs` CI job diffs each PR
against a baseline of the base branch's latent findings (`.spotbugs-baseline.xml`, used as an
exclude filter so only NEW bugs are reported) - so a finding introduced by a branch is annotated
onto the PR and re-surfaces as a new-findings warning on every push until fixed or suppressed. (The
job itself is report-only - `spotbugs:spotbugs`, not `spotbugs:check` - so this is recurring review
noise rather than a hard merge block.)

The catch-broadly-and-dispatch-on-type structure follows the shared-handler design of PR
astubbs#207 (open, unmerged as of this writing), which proposed one handler for the whole class of
unreadable-metadata decode failures; the structure itself entered this tree in PR #306's own
choke-point commit (session history). Notably, #207's first cut of that handler also mishandled a failure by treating it
generically instead of reasoning about the data that produced it - it returned the committed offset
as "seen and succeeded" and silently dropped a record, caught during self-review (session history).
Same lesson, earlier instance.

## Guidance

When a sneaky-thrown checked exception must be recognised downstream, key the check on the data
that determines the exception, not on the exception's type.

Exception-*type* dispatch on a sneaky-thrown checked exception is control flow that exists only at
runtime; it is invisible to analysis by construction. Branching on the datum that determines the
exception is dispatch both the runtime and the analyser can see, because it is ordinary value
comparison with no dependence on undeclared exception flow. The two dispatches are equivalent
exactly when the data condition and the exception are in one-to-one correspondence - verify that
correspondence in the throwing code before relying on it.

In `decodeCompressedOffsets`, only the `KafkaStreams` / `KafkaStreamsV2` arms of
`getDecodedIncompletes` can throw the policy verdict, so the payload's `OffsetEncoding` is an exact,
analysable proxy for the exception type. The fix (PR astubbs#306, commit "key the Kafka Streams
pass-through on the encoding, not an instanceof SpotBugs can disprove") also hoisted
`EncodedOffsetPair.unwrap` out of the `try` so its result is in scope for the data-keyed check -
and that hoist made the separate `catch (OffsetDecodingError)` rethrow arm redundant: the one
`OffsetDecodingError` still raisable inside the `try` (the registered-but-undecodable default arm
in `getDecodedIncompletes`) would only be re-wrapped into another `OffsetDecodingError`, so the
outcome class is unchanged.

Do not reach for `@SuppressFBWarnings` or a baseline/exclude entry instead: the code is genuinely
reachable, so a suppression asserts the analyser is wrong rather than making the code analysable.
In this repo the SpotBugs baseline exists to freeze *pre-existing* latent findings
(`docs/inflight/static-spotbugs-latent-findings.md`), not to absorb new ones, and the repo's
recorded norm is to restructure or fix rather than suppress (session history).

## Why This Matters

`@SneakyThrows` exploits the fact that checked exceptions are a javac fiction, not a JVM one:
Lombok removes the `throws` clause so the exception crosses the method boundary undeclared. Any
analysis that models exception flow from declared signatures then concludes the checked type cannot
arrive in a downstream `catch (Exception)`. An `instanceof` test there is either dead (the analyser
is right) or invisible-to-analysis control flow (the analyser will fight you forever). Neither is
worth keeping - and because the SpotBugs check is baseline-diffed, "the analyser fights you
forever" concretely means a new-findings warning re-litigated on every push of every PR touching
the file.

Keying on data instead also reads as what is actually meant - "Kafka Streams metadata gets the
policy verdict" - rather than as exception-plumbing.

## When to Apply

- A checked exception is laundered through `@SneakyThrows` and something downstream needs to
  recognise it.
- SpotBugs (or another dataflow analyser) flags a runtime-live check as impossible - treat the flag
  as a design smell report, not a false positive to suppress.
- More broadly: treat any `instanceof <checked exception type>` inside `catch (Exception)` /
  `catch (Throwable)` downstream of a `@SneakyThrows` method as a smell, whether or not an analyser
  has complained yet.
- If the exception's type genuinely must carry the dispatch, make the exception unchecked (or
  declare it honestly in `throws` and catch it by name) so the type flow is visible to both javac
  and the analyser. Do not launder a checked type and then try to recover it by type downstream.

## Examples

Before (behaviourally correct, but the `instanceof` is provably impossible to SpotBugs because
`KafkaStreamsEncodingNotSupported` is sneaky-thrown):

```java
try {
    var result = EncodedOffsetPair.unwrap(decodedBytes);
    return result.getDecodedIncompletes(nextExpectedOffset, errorPolicy);
} catch (OffsetDecodingError alreadyClassified) {
    throw alreadyClassified; // already the recoverable signal - never double-wrap it
} catch (Exception decodeFailure) {
    if (decodeFailure instanceof EncodingNotSupportedException) { // BC_IMPOSSIBLE_INSTANCEOF
        throw decodeFailure;
    }
    throw new OffsetDecodingError(msg("Error decoding offset metadata payload ..."), decodeFailure);
}
```

After (branch on the datum that determines the exception - the payload's encoding; `unwrap` hoisted
so `result` is in scope, which also made the `OffsetDecodingError` rethrow arm redundant):

```java
// unknown magic byte raises OffsetDecodingError here - already the recoverable signal
var result = EncodedOffsetPair.unwrap(decodedBytes);
try {
    return result.getDecodedIncompletes(nextExpectedOffset, errorPolicy);
} catch (Exception decodeFailure) {
    // Only the Kafka Streams arms can raise the FAIL policy's verdict. Keying on the encoding
    // rather than the exception type keeps this visible to static analysis.
    if (result.getEncoding() == OffsetEncoding.KafkaStreams
            || result.getEncoding() == OffsetEncoding.KafkaStreamsV2) {
        throw decodeFailure;
    }
    // everything else is corrupt or foreign metadata, converted to the recoverable signal
    throw new OffsetDecodingError(msg("Error decoding offset metadata payload, input (base64) was: {}",
            Base64.getEncoder().encodeToString(decodedBytes)), decodeFailure);
}
```

## Related

- PR astubbs#306 - the offset-encoding density PR carrying the fix
- Issue astubbs#192 - the driving offset-encoding issue (mirror of confluentinc#903)
- PR astubbs#207 (open, unmerged as of this writing) - design source of the shared decode-failure handler pattern
- `docs/plans/2026-08-17-001-perf-offset-encoding-density-plan.md` - the plan this fix was implemented under
- `docs/inflight/static-spotbugs-latent-findings.md` - the repo's SpotBugs baseline policy (freeze latent findings, never absorb new ones)
