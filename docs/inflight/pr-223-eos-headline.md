# Cite the KIP in astubbs#223's Exactly-once comparison row

The positioning argument and its evidence live in `STRATEGY.md` on this branch - read that first.
This entry records only the one change that belongs to a *different* branch.

astubbs/parallel-consumer#223 (`docs/strategy-and-share-groups-comparison`) adds a Share Groups
comparison table whose `Exactly-once` row reads "Not supported. There is no transactional
read-process-write for share consumers". That is correct but **uncited**, and
`docs/solutions/documentation-gaps/competitor-comparison-docs-must-cite-the-primary-spec.md` records
that competitor comparisons must quote the primary spec rather than assert from summary. The row
should carry KIP-932's own sentence:

> "Although it is possible to read transactionally written records, the current protocol does not
> include the ability to acknowledge message delivery within an atomic transaction."

Also worth adding there, since the table already has rows for both: isolation level is a group-level
setting (`share.isolation.level`), not per-consumer, and the delivery counts behind the poison-message
row "cannot be relied upon to be precise" because those updates are themselves not exactly-once.

- `STRATEGY.md` is missing from `AGENTS.md`'s "Where things live" table. astubbs#223 is the PR that
  introduces it properly, so it should add that entry too - otherwise the file exists but nothing
  points an agent at it.

Delete this file once astubbs#223 carries the citation.
