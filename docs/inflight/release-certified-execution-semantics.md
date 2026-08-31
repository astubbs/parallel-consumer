# Certified execution semantics: publish the conformance matrix as a product claim

<!-- inflight-type: feature -->
<!-- inflight-state: deferred - productises evidence astubbs#293 already generates; lands with the polyglot release story -->

From the follow-up Codex strategy conversation, weekend of 2026-08-29/30 (breakdown root:
[`core-engine-thesis.md`](core-engine-thesis.md)).

astubbs#293 already runs one shared conformance suite - assertions written once in Java - across
thirteen bindings with core itself as the control arm. Turn that inward-facing test infrastructure
outward: each release publishes the matrix (binding x {ordering, retry, drain, rebalance, commit})
as a certification table. The claim upgrade is the point: not "we have Python support" but
*"Python and Java are proven against the same executable behavioural contract."* Almost nothing to
build - the honest work is presentation plus the discipline that a cell only prints ✓ when the
scenario genuinely ran (the skip-reads-as-pass trap the repo's own gates exist to prevent; a
certification table that cannot show ✗ certifies nothing).
