# Parked, post-v6: run the chaos suite against every client library

The chaos harness should be runnable against all the proxy client libraries, not only the JVM engine.
Owner's idea, 2026-08-14, post-v6. The shape proposed: **the existing Java orchestrator drives the
chaos**, and the assertions stay in the same Java suite by having each client **publish its test
results to a Kafka topic the chaos suite consumes** — so one thorough suite covers ten languages
instead of ten suites drifting apart.

## Why it fits better than it first appears

- **It fills a gap the specification probe already found.** The test-mode harness has *no verdict
  channel*: it exits 0 whether or not a scenario's assertions held. Every foreign client has
  therefore had to invent its own assertions. A results topic is that missing channel, and it is
  worth building for the ordinary conformance suite even before chaos.
- **The orchestrator can assert far more than the frontier, by decoding the commit metadata.** The
  committed offset is only the summary; the metadata beside it encodes which individual offsets above
  it are already complete. Decoding that gives the suite the *shape* of completion - how much work
  sits above the frontier, and exactly which offsets are still holding it down - without any client
  cooperation. That single assertion covers both sides at once: it proves the client resolved the
  records it claims to have resolved, and it proves the engine's own encoding round-trips, which is
  otherwise only tested from inside the JVM.
  **The trap:** the encoded set is a snapshot of whenever the periodic commit last fired, so
  asserting it mid-run reintroduces exactly the timing dependence this project already learned to
  avoid (assert the frontier, not the tick path). Assert the decoded map **at quiescence**, where it
  is determinate; use it mid-run only for diagnostics, never as a pass condition.
- **The split falls out cleanly, and only half of it needs the topic.** Broker-side truth is already
  observable to the orchestrator through the **Kafka Admin API** — committed offsets per group
  (`listConsumerGroupOffsets`), topic contents, group membership — with no client cooperation at all,
  so the suite keeps asserting those directly and no language needs a Kafka client of its own to be
  tested. What the Admin API cannot see is *client-side* truth: which records a worker actually
  received, in what order, at what concurrency, with which retry attempts. **That** is what the
  results topic carries. Getting this boundary right is what keeps the clients thin — a client that
  had to run an admin client to be tested would have re-acquired the dependency this whole design
  exists to remove.
- **A client cannot reach Kafka directly — by design — so its results must travel the produce path.**
  Workers never produce to Kafka themselves; output goes back through the engine, which produces.
  So a client publishing its verdict *uses the R6 produce payload*, which means the verdict channel
  exercises the produce path as a side effect. That is a real bonus: today the produce-failure branch
  has no test at all (see the parked review findings).
- **Assertions stay in one language.** The suite that already knows what correct looks like — offset
  frontiers, ordering, redelivery, encoding — keeps owning that knowledge, and each client's job
  shrinks to reporting what it saw. This is the leverage: the hard-won assertions do not get
  re-implemented ten times, badly.

## The three things that will bite, recorded now

- **Absence must fail, not pass.** A client that crashes and publishes nothing looks exactly like a
  client with nothing to report. The suite must assert an *expected* count or a liveness marker per
  client, or it joins this repo's most recurrent failure class — a check that reports success
  without having run (`docs/solutions/workflow-issues/a-check-that-reports-success-without-having-run.md`).
- **Chaos breaks the very channel the results travel on.** Broker outages are the point of the
  suite, and the results topic is on that broker. Either results buffer client-side and flush after
  recovery (then the suite must tolerate late arrival without tolerating never), or the verdict path
  is deliberately separated from the cluster under test. Decide explicitly rather than discovering it
  in a red run.
- **One result format across ten languages** is the same problem the wire already solved. Reuse the
  approach, not the schema: a small, separately-frozen results message, generated per language from
  one `.proto`, rather than JSON that each client spells differently.

## Where this sits

`docs/testing.md` owns the chaos suite; the language-proxy plan (astubbs#242) owns the clients and
the conformance suite each one runs. This note is the join between them, and belongs to whichever
wave picks it up after v6. The conformance suite's growth is already a named unit in the plan — the
results channel is the piece that would let it grow *once* rather than per language.
