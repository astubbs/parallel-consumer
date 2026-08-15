# Rules that bind while you are writing a test

Short by design. This file is imported by a `CLAUDE.md` bridge in every module's test tree, so it
arrives **at the moment a test file is touched** rather than waiting to be looked up.
[`docs/testing.md`](testing.md) owns the testing topic and holds the detail; what is here is only what
must fire while the test is being written.

## Break the code before you trust the test

**Before committing a new or changed test, sabotage the behaviour it guards and watch it fail.**
Then restore. Manual mutation testing, one test at a time.

A test that passes whether or not the behaviour is present is worse than no test, because it stops
anyone looking. This is not a formality - it repeatedly finds tests that assert nothing, and the
failure is always silent, since a test that cannot fail simply never goes red.

**Three ways a test passed while the thing it tested was broken, all found this way:**

- **The sabotage never reached the system.** A conformance runner was made to report the wrong
  outcome, and the scenario stayed green - the runner exited so fast it killed its own report in
  flight. The test was asserting on a message that never arrived.
- **The system made the sabotage invisible.** A scenario asserting that same-key records do not run
  concurrently was sabotaged by declining to hold the first record - still green, because the engine
  dispatches both shards in one wave regardless. The sabotage that worked was a mutex around the
  processor.
- **The test was named for a property it could not detect.** A queue-overflow test overflowed by
  sending one wave larger than the ceiling - which the *wrong* bound also rejects - so it passed
  against the very defect it was named for, in two languages, until someone broke the counting
  deliberately and watched it not notice.

**What "sabotage" means here**: change the production code so the property is genuinely violated -
return the wrong outcome, remove the guard, restore the old broken counting - not change the test.
If the test still passes, the test is the problem. Note which mutation you used; a reviewer cannot
tell a proven test from an assumed one by reading it.

## The rules this repo already had, restated because they bite at the same moment

- **Never weaken a test to make it pass.** A test failing under parallelism or load may be exposing a
  real bug that only appears under stress. Establish which it is - test-infra contention, or a
  genuine defect - and say which in the commit. Loosening a deadline hides exactly the bugs this
  library exists to prevent.
- **A flake fails the build; there is no retry, deliberately.** The lever is `@Quarantined` with a
  diagnosis, which relocates the signal rather than destroying it.
- **Reuse the shared test utilities** - search before adding one. A drifted copy of topic-creation
  logic once became a flaky-CI source.
- **Assert the converged state, not the path to it.** Await a quiescent condition and then read;
  never compare two independently-moving values.
