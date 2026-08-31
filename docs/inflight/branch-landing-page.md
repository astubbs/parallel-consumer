# The landing-page prototype lives on `web/landing-page`, saved but not proposed

<!-- inflight-type: task -->
<!-- inflight-impact: stranded-work -->

Branch `web/landing-page` (head commit `284e685fb`) carries the landing-page prototype -
`docs/landing-page/` plus `docs/data/landing-page.yaml` - extracted verbatim from
`perf/engine-concurrency` (astubbs#363), where it was buried in the integration branch. The owner's
decision: a branch to save the work, **no PR yet**. The design reasoning is in the originating
commit bodies on the source branch (`79aee3780`, `d0b31ece2`) - read them before reworking the page;
they record a deliberate departure from the brief and a known visual-identity gap.

**The branch is deliberately red on four gates**, and its PR-time work is exactly those fixes:

- the copyright scanner classifies no `.css` on `master` - add it to a table in
  `bin/check-copyright-headers.sh`;
- `landing-page-content` is not declared in `docs/data/schema.yaml`;
- `index.html` cites bench scripts and inflight notes that live on the perf and proxy branches -
  they resolve once astubbs#362 and friends land, or take the gate's paragraph opt-outs;
- one issue-ref hit is a false positive on the HTML entity `&#183;` - takes an in-file
  `issue-refs: exempt`.

## Delete when

`web/landing-page` gets its PR (which does the fixes above), or the page is abandoned.
