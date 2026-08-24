# Experimental code should say so at runtime, consistently, once

<!-- inflight-type: feature -->
<!-- inflight-impact: reliability -->

Owner, 2026-08-24. This fork now ships several things at different levels of doneness - incomplete
proofs of concept, drafts, work explicitly seeking feedback, and stable engine code - and the only
place that distinction currently lives is documentation nobody re-reads after the first time. A user
who enables one of them gets no signal at the moment it matters, which is when their process starts.

**There should be one consistent way for a mode or module labelled at a given maturity to warn at
runtime.** Not per-feature ad-hoc log lines written to different levels with different wording, as
happens today.

Design notes rather than decisions:

- A central registry of what is labelled what is the obvious shape, and may be more trouble than it
  is worth. Weigh it against simply putting the label where the feature is already declared.
- **The options configuration is the natural place to start**, since that is where a user turns a
  thing on, and it is already the surface every language binding funnels through.
- **Warn once, not per occurrence** - and that is a configuration concern, so the option surface
  should control it rather than each feature inventing its own rate limiting.
- For the non-JVM clients (the C library and the rest of the proxy fan-out), the once-only behaviour
  is probably best managed **Java-side for every foreign language at once** rather than reimplemented
  per binding, which is also the only way the wording stays consistent across them.

Existing instances to fold in when this is built, as evidence of the inconsistency it fixes:
adaptive concurrency logs a warning when refused by an engine, the direct-pull engine carries a
measurement-only banner in javadoc and warns nowhere, and virtual threads throw on an unsupported
JVM. Three maturity signals, three different mechanisms, three different levels.
