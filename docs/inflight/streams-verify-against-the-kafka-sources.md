# Kafka Streams work: read Kafka's sources, do not reason about its behaviour

<!-- inflight-type: register -->
<!-- inflight-impact: misdirection -->

**Binds any work on the Streams wrapper (astubbs#242) or on Streams-over-PC (astubbs#255).** Consulted,
never completed: it has no done state, and it is here rather than in a topic doc because the Streams
workstreams are live and this is the practice that most changes their output right now.

**The rule: when a plan, a test or a design turns on how Kafka Streams behaves, open the sources for
the version in the pom before writing the sentence.** The jar and its sources sit in `~/.m2`;
`javap` and unzipping the sources jar are faster than the web and are the only authority that is
version-exact.

## Why this framework and not others

Streams' observable behaviour is largely emergent from internals the public javadoc does not state,
and the plausible answer is usually wrong in a specific, checkable way:

- **The load-bearing facts live in `internal` packages.** The windowed-key byte layout comes from
  `WindowKeySchema`; no KIP declares it a contract. You cannot look it up, only read it.
- **Defaults are behavioural, not cosmetic.** A window store's retention defaults to size plus
  grace, so a tumbling window with no grace retains roughly the open window only. Nothing warns you.
- **Deprecated paths differ in behaviour, not only in style.** The deprecated window constructors
  silently give `max(24h - size, 0)` grace where the replacements give zero.
- **Timing is emergent.** Stream time advances only on record arrival; wall clock never moves it;
  the suppression processor schedules no punctuator, so a quiet partition never emits its final
  window. None of that is deducible from the API shape.

## The evidence: three times in this workstream, and it was decisive each time

- **The invocation-bundling plan was PARKED because its central mechanism was falsified by reading
  the sources** - `StateStore.flush()` runs *after* commit, and `forward()` is illegal from a flush
  hook. Found by reading, not by running. See its banner in
  [`../plans/2026-08-24-002-feat-streams-invocation-bundling-plan.md`](../plans/2026-08-24-002-feat-streams-invocation-bundling-plan.md).
- **A type gate in astubbs#255 let `VersionedKeyValueStore` through an `instanceof WindowStore`
  chain**, because it extends `StateStore` directly. The lesson recorded there - enumerate the
  framework's implementations against the version you build on, and default a gate to refuse rather
  than allow - is the same lesson as this one:
  [`../solutions/architecture-patterns/a-type-gate-is-a-claim-about-a-hierarchy-you-did-not-write.md`](../solutions/architecture-patterns/a-type-gate-is-a-claim-about-a-hierarchy-you-did-not-write.md).
- **The windowing plan's review, 2026-08-25.** Five reviewers ran in independent contexts. Four
  reasoned about the document; one verified against the 3.9.2 sources. **The source-reader found the
  errors that would have produced a wrong measurement rather than a wrong sentence, and corrected
  two of the reasoning reviewers' conclusions.** Specifically it found that the driver does not
  disable caching (it commits per record, which flushes the cache) *and that the resulting bias runs
  the opposite way to what everyone had written down*; that the backward window-store reads the plan
  refused as unavailable are in fact implemented all the way down; that a "control arm" was the
  experimental arm under another name, because advance-equal-to-size *is* tumbling; and that the
  headline call-count assertion was wrong at the epoch, because window assignment clamps the
  earliest window start at zero.

## What it costs when skipped

Not a broken build - a **confident wrong number, or a durable write-up asserting something false**.
Every instance above would have passed review, run green, and been recorded as a result. That is why
this sits under `misdirection`: the failure is silent and arrives with evidence attached.

## How to apply it

- **Cite the source you read**, with the version, next to the claim. A version-pinned claim can be
  rechecked; "Kafka does X" cannot.
- **Say what you could not establish**, as prominently as what you could. A gap named is cheap; a
  gap filled with a plausible guess is what this note exists to prevent.
- **A reviewer with repo access beats one without, for this framework.** When dispatching review of
  Streams work, give at least one reviewer the tools to read `~/.m2` and the explicit instruction to
  verify rather than assess.

Retire this into a topic doc if the Streams workstreams land and the practice needs to outlive them.
