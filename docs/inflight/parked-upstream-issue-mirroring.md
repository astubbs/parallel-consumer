# Parked: user-facing upstream issue mirroring

Internal upstream tracking is strong (`upstream-map.yaml`, this directory, `docs/solutions/`), but
nothing is user-facing: someone on the fork's Issues tab cannot tell whether `upstream #857` is fixed
here, in flight, or won't-fix - and upstream is judged **likely to be archived**, timing unknown.

**Decision changed 2026-08-04** - this supersedes the earlier "mirror on touch, no bulk import, one
issue per invocation" plan. Full plan:
`docs/plans/2026-08-04-001-chore-mirror-upstream-issues-plan.md`.

- **Mirror all 78 open upstream issues in bulk**, not on touch. An unconditional import needs no
  per-issue judgement call, which is what made the incremental version a maintenance mess.
- The body is a **summary that captures the original**, not a verbatim copy - a placeholder landing
  page that preserves the substance and links out. **No `@mentions` in mirrored content.**
- Title `upstream #NNN: ...`, `upstream-mirror` label, one area label, one type label.
- **Each upstream issue gets one backlink comment** naming its fork mirror number.
- **Does not block the 0.6.0 release.** Until it runs, comment on affected upstream issues by hand.
- **Archive asymmetry drives the ordering.** *Reading* upstream survives archival - the mirror can be
  built at any time, even afterwards - but *writing* does not: backlink comments are only possible
  while upstream stays open, and a per-issue backlink needs its mirror to exist first. So the mirror
  gates the time-critical half.
- **Hedge if archival looks imminent** before the mirror runs: post a short fork-awareness comment on
  the high-traffic issues now. It needs no mirror and still survives.
- **Manual-comment candidates now:** `upstream #857` (the stall saga - its 2026-04-13 "I think I fixed
  it" comment points at unmerged #29 and is four months stale, while #100, #80 and #108 have since
  landed), `upstream #907` (maintenance question), `upstream #859` + `#893`/`#905` (PCMetrics leak,
  #57), `upstream #912` (vertx leak), `upstream #909`.
