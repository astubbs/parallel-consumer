# Two repo-wide gates are red on debt, not on anyone's change

<!-- inflight-type: task -->
<!-- inflight-impact: ci -->

`PR Checklist` and `inflight: tags` fail on accumulated debt rather than on the change under review.
Recorded because the failure mode is **misattribution**: a contributor sees their PR red, assumes
they broke it, and either hunts a phantom or learns to ignore the gate. Both outcomes are worse than
the debt.

## `PR Checklist` - 76 unresolved citations

`bin/check-file-refs.sh` fails a cited path that does not exist. The current failures split three
ways, and only the third is ordinary rot:

- **~26 are a gate bug, not a broken citation.** The `src/test/CLAUDE.md` bridge files cite
  `@../../../docs/testing-at-write-time.md`. That file **exists**; the gate does not strip the
  leading `@` of a CLAUDE.md import before resolving, so it reports a live path as missing. One-line
  fix in the gate. **Not covered** by `fix/node-gates-preflight`, which addresses a different defect
  in the same script (a node that cannot start being reported as a policy violation).
  <!-- file-refs: N/A - the import path above is this note's specimen of the gate bug, not a citation to follow -->
- **~50 are plan documents citing files that were planned and never built.** The gate's own message
  prescribes the repair: close the paragraph with `<!-- file-refs: N/A - <reason> -->`. Note that
  dated plans may not be rewritten to match today's code - [`../citations.md`](../citations.md)
  **owns that procedure**.
- **~13 are genuinely stale paths** - a moved `demo/run.sh`, generated `target/*.txt` files, a
  deleted note.

## `inflight: tags` - 68 notes with no `inflight-type`

`bin/check-inflight-tags.sh` requires one on every note. 68 predate the requirement. An untagged
note is **invisible to the tracker it belongs in** - it sorts nowhere and appears in no ranked view -
so this is not only a red tick.

Tagging one is cheap and the sets are small ([`AGENTS.md`](AGENTS.md) owns them). The rule that
keeps it from being a single sweeping commit nobody reviews: **tag the notes you touch, in the
change that touches them.** Two were tagged that way today.

## Why this is one note and not two

Both are the same shape - a gate whose red says nothing about the change in front of it - and
whoever fixes either will be looking at the same question: is this gate telling me about my work, or
about the repo's backlog? Splitting them would hide that.
