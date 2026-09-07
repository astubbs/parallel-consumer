# Product logging sits at INFO, and the test harness now hides it

<!-- inflight-type: feature -->
<!-- inflight-state: parked - the test half is done; demoting product log levels is a user-visible default nobody has argued for yet -->

**Parked deliberately.** The *test* half of this was fixed (see below); the *product* half is a
user-visible change nobody has argued for yet.

## What was fixed

The test harness defaulted to `info`, so every suite carried a full `ParallelConsumerOptions`
`toString` per constructed instance - **196 of them across the 387-test core unit suite**, part of
5,520 lines of output now down to 3,687. Both levels in each library module's `logback-test.xml`
now read the `pc.log.level` system property,
defaulting to `warn`, enforced by `bin/check-test-log-config.sh`. `docs/testing.md`, "Seeing test
output", owns the how-to.

## What is parked

**Whether the product's own log statements are at the right levels.** The banner in
`AbstractParallelEoSStreamProcessor` - grep `Confluent Parallel Consumer initialise` - is the clearest
case: one multi-line options dump per instance, at INFO, which every embedder gets by default. There
is a wider version of the question (demote most product INFO to DEBUG, and most DEBUG to TRACE).

**Why it is parked rather than done:**

- The test-config change makes the *symptom* go away for us without touching a single line users see.
  Doing both at once would have made a library behaviour change ride along inside a test-noise fix.
- It is a genuine judgement call about the library's default verbosity, not a defect. An operator who
  wants the effective options recorded at startup is not wrong.
- It cannot be validated by the suites: they now run at `warn`, so a demotion changes nothing they
  observe. The check is reading each call site and asking who the line is for.

**Restarting this:** decide the banner first and independently - it is one statement, it is the whole
measured cost, and it needs no sweep. A blanket INFO→DEBUG pass is the larger, more arguable change
and should not be bundled with it.

**Watch for the inversion.** With the harness at `warn`, demoting a product statement to DEBUG makes
it invisible in tests by default. Anything load-bearing for diagnosis wants to stay at WARN or above,
or be pinned in the config the way
`org.apache.kafka.clients.consumer.internals.SubscriptionState` is.
