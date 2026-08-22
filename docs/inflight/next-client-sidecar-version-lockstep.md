# Client and sidecar run in lockstep — and the check is owed

**Settled by the owner, 2026-08-15:** a client and the sidecar it talks to are the *same version*.
Mixing versions is not a supported configuration and not a compatibility matrix to be tested - it is
a **runtime error that should be detected and refused**. The check itself is deferred to a later
phase; the rule binds now.

This closes what was previously recorded as an open unknown ("client v1 against sidecar v2 is
untested"). There is nothing to test, because there is nothing to support: the answer is to fail
loudly at the handshake.

## Why this is the right call rather than a limitation

The sidecar and the client are two halves of one artifact that happen to be written in different
languages and shipped to different registries. They are versioned and released together, and a user
who mixes them has an installation error, not a configuration choice. Supporting skew would mean a
compatibility matrix growing with every language and every release - the largest possible cost for
the least valuable property.

## How to enforce it, when that phase arrives

The wire is frozen, so the mechanism must be **additive**, and it is: a new optional field in
`Configure` carrying the client's version, compared at the handshake, with a mismatch refused as a
protocol violation naming both versions. Adding an optional field is exactly what the freeze permits,
so `buf breaking` stays green.

**This does not contradict the freeze's "no version field" decision**, and the next reader should not
read it as a reversal. That decision settled how *features* are negotiated - capabilities, not a
version number - and it remains right: a version number is a poor way to ask "do you support
heartbeats". What is being added here is *identity*, for refusing a mismatched pair, which is a
different question and does not become the feature-negotiation mechanism.

Two things for whoever implements it:

- **Refuse rather than warn.** A warning at startup is read by nobody and the failure it predicts
  arrives much later, disguised as something else.
- **The error must name both versions and say they must match** - the user's next action is to fix
  an install, and the message is the only place they will learn that.

## Consequences to carry

- **Release automation must publish every client at one version**, in step with the sidecar. That is
  a hard constraint on the (not yet started) publishing work across npm, PyPI, crates.io, RubyGems,
  NuGet, Go module tags and Maven Central - it is not "each library releases when ready".
- The demo and container images must pin matching pairs for the same reason.
