# Branch: HTTP-strategy ideation (astubbs#242)

<!-- inflight-priority: low -->

> Extracted from `origin/docs/proxy-http-ideation` @1867d15db, `docs/inflight/branch-proxy-http-ideation.md`.
> That branch has since been merged into `feats/proxy-requirements` (astubbs/parallel-consumer#293)
> and deleted, so the body's "no PR yet" framing is out of date: the ideation content now rides on
> that PR, but this note did not, which is why it is here.

Branch `docs/proxy-http-ideation` (off `feats/proxy-requirements`, no PR yet) carries Part 2 of
`docs/ideation/2026-08-14-language-proxy-interaction-model-ideation.html`: the ideation pass on the
HTTP dialect, generated clients, and Confluent REST Proxy compatibility that
[`next-http-strategy-ideas.md`](next-http-strategy-ideas.md) records as owed. Seven ranked survivors;
read the doc, not this note, for the ideas.

What it settled that other notes still state as open:

- **The REST Proxy feasibility unknown is discharged** (primary sources, 2026-08-17): v2's only
  client-visible progress unit is a cumulative per-partition offset watermark - the number the engine
  already computes - so beyond-partition concurrency is invisible to their wire contract.
  Compatible-and-beneficial; v3 has no consumer data plane, so compatibility means emulating v2.
- **Pull stays quarantined**: the per-executor hanging-GET native dialect was refuted (a credit
  ledger of granularity 1); pull exists only behind the compat boundary.

Open, and why this note exists:

- **Priority is undecided**: compat surface first (cheapest reach, zero new clients) vs the native
  dialect's prerequisite (session-scoped lease re-spec). Demand decides, and nobody has asked yet.
- **Escalate regardless of the HTTP decision** (doc, survivor 7): `KafkaClientFactory` applies no
  key allowlist to client-supplied `kafka_properties` - verified live 2026-08-17.
- When this branch merges, `next-http-strategy-ideas.md`'s "a proper ideation pass is owed" framing
  is stale - the same merge updates it to point at the ideation doc.
