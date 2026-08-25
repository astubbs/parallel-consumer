# After the conformance PoC: mine the existing integration tests instead of reinventing them

Owner's instruction, 2026-08-15, to run once the shared conformance harness has proved its mechanism
with one language. The suite should **not** grow scenario-by-scenario from scratch: this project
already has a large, mature integration suite whose value is not the code but the *edge cases it
encodes* - flake diagnoses, ordering and commit invariants, rebalance behaviour, the chaos lane,
and the timing rules that were learned the hard way and are documented in `docs/testing.md` and
`docs/solutions/`.

"We shouldn't let the structure prevent us from utilizing it. **We can refactor it as we need.**"

## The seam that makes reuse possible

Most integration tests are built around one implicit assumption: **the user function is a Java lambda
running in this JVM.** Everything else they assert - offsets advance, ordering holds per shard,
retries carry attempt counts, a rebalance loses nothing - is about behaviour that is equally true
when the user function lives in another process, in another language.

So the refactor is to separate:

- **what is asserted, and how Kafka is driven** (keep, share, unchanged in meaning), from
- **how the user function is invoked** (parameterise: in-process Java, proxy + Java client, proxy +
  a foreign runner).

That is the same move the clients already made for transports - one definition, several bindings -
and the spike's own parameterised test is a miniature of it. Where a test body cannot be
parameterised without contortion, moving the *scenario definition* while re-implementing the harness
is still a win: the expensive knowledge is which cases matter and what correct looks like.

## The classification the audit should produce

For each existing integration test, decide which bucket it falls in, and say why:

- **Shareable as-is** once the invocation seam exists - it asserts protocol-observable behaviour
  (commit point, ordering, redelivery, attempt counts, rebalance safety).
- **Shareable in substance, needs adaptation** - the assertion is right but the mechanism is
  JVM-specific (reaches into internals, or drives the user function in a way a foreign runner
  cannot).
- **JVM-only, deliberately** - it tests engine internals (offset encoding, thread pools, the Vert.x
  or Reactor bindings). These stay where they are; naming them explicitly is what stops a later
  reader wondering whether they were missed.

## Rules that bind the refactor

- **Never weaken a test while moving it.** A relocated test must still fail for the same reason it
  failed before, and the repo's standing rule applies with full force: a test failing under
  concurrency may be exposing a real bug, so it is never loosened to make a move go green.
  Prove each moved test still fails when the behaviour it guards is broken.
- **The integration tests need Docker; the conformance PoC deliberately does not.** Decide per test
  whether it belongs to a broker lane or can run on the mock lane, rather than dragging Docker into
  the fast suite by accident.
- **Do not fork the assertions.** If a test is shared, one definition serves every binding; two
  copies that drift is worse than not sharing at all - that is the divergence this whole fan-out is
  organised to prevent.

## Why it is worth doing before more languages arrive

Each of the four remaining runners is cheap only if the scenarios already exist. Every scenario
mined from the integration suite is one that four languages get for free, and one nobody has to
invent under time pressure while writing their fifth client.
