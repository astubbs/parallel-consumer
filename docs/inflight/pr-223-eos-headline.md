# Cite the KIP in the README's Exactly-once comparison row

<!-- inflight-type: task -->
<!-- inflight-impact: stranded-work -->
<!-- inflight-vetted: 2026-09-07 - astubbs#223 merged and `STRATEGY.md` now carries the KIP-932 quote, `share.isolation.level` and the imprecise-delivery-counts caveat, and `AGENTS.md`'s "Where things live" table now has its `STRATEGY.md` row - both of those asks are DONE and were removed. The README's comparison table is unchanged: its `Exactly-once` row still asserts without citing -->

The positioning argument and its evidence live in `STRATEGY.md` - read that first.
This entry records only the one change that has not been made anywhere.

`README.adoc`'s "When to use this library (vs KIP-932 Share Groups)" table has an `Exactly-once` row
reading "Not supported. There is no transactional read-process-write for share consumers". That is
correct but **uncited**, and
`docs/solutions/documentation-gaps/competitor-comparison-docs-must-cite-the-primary-spec.md` records
that competitor comparisons must quote the primary spec rather than assert from summary. The row
should carry KIP-932's own sentence:

> "Although it is possible to read transactionally written records, the current protocol does not
> include the ability to acknowledge message delivery within an atomic transaction."

Also worth adding there, since the table already has rows for both: isolation level is a group-level
setting (`share.isolation.level`), not per-consumer, and the delivery counts behind the poison-message
row "cannot be relied upon to be precise" because those updates are themselves not exactly-once.
`STRATEGY.md` states both already, so this is a matter of the README catching up, not of settling
anything.
