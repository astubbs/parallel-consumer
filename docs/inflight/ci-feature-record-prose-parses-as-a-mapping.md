# A feature record's prose sentence stops being prose when it contains a colon and a space

<!-- inflight-type: bug -->
<!-- inflight-impact: misdirection -->

A prose list item in `docs/features/*.yaml` - a `boundaries` entry, a `use_this_when` entry - is an
unquoted YAML scalar. YAML reads the first `": "` in it as the key/value separator, so the sentence
its author wrote becomes a **single-key mapping**: everything before the colon is the key, everything
after is the value. The words are all still in the file. Every consumer that reads the *parsed* value
gets a `dict` where the contract says a string.

`bin/check-docs-data.sh` passes these files.

## Reproduce it

```bash
python3 - <<'PY'
import yaml, glob, os
def walk(n, p, out):
    if isinstance(n, dict):
        for k, v in n.items(): walk(v, f"{p}.{k}" if p else str(k), out)
    elif isinstance(n, list):
        for i, it in enumerate(n):
            if isinstance(it, dict) and len(it) == 1 and " " in next(iter(it)):
                out.append((p, i, next(iter(it))))
            walk(it, f"{p}[{i}]", out)
for f in sorted(glob.glob("docs/features/*.yaml")):
    out = []
    walk(yaml.safe_load(open(f, encoding="utf-8")), "", out)
    for p, i, k in out:
        print(f"{os.path.basename(f)}: {p}[{i}] parsed as a mapping - {k[:70]}")
PY
```

The single-key-with-a-space test is what separates the defect from the legitimate mappings in the
same files: a `references` item really is `label:`/`path:`, and its keys are single words.

## Why the gate is green on exactly the inputs it exists to protect

`bin/check-docs-data.sh` says in its own header that it is "deliberately structural only: it
verifies that a file parses". That is a different question from whether prose parsed as prose, and
the first one is the one that is easy to write. A mis-parsed sentence *is* structurally valid YAML -
it is a mapping, and mappings are legal in a list.

Nothing downstream of the parse closes the gap either:

- `boundaries` and `use_this_when` are not named in `feature.item_contracts` in
  `docs/data/schema.yaml`, so no per-item contract is applied to their elements. The gate's
  `require_fields` only asks whether the list is present and non-empty, which it is.
- `check_refs` walks dicts happily, so there is no crash to notice.

**The shape the gate DOES catch is the noisier one.** A sentence with two occurrences of `": "`
raises `mapping values are not allowed here` and fails the gate loudly. One occurrence is silent.
So the failure mode is inverted: the sentence that is obviously punctuation-heavy gets caught, and
the ordinary one - a clause, a colon, an explanation - sails through.

## Which records carry it today

Written from the astubbs#504 stack, where the affected records are
`docs/features/client-side-work-queue.yaml`,
`docs/features/hand-back-without-an-attempt.yaml`,
`docs/features/invalid-offset-metadata-policy.yaml`, `docs/features/pause-and-resume.yaml`,
`docs/features/per-throw-retry-delay.yaml`, `docs/features/result-models.yaml`.
`invalid-offset-metadata-policy.yaml` appears more than once in that output because separate items in
it are affected, not because it was double-counted.

The set moves with every record anyone writes, so re-answer it with the block above rather than
trusting this list. `docs/data/*.yaml` and `docs/features/staging/` are clean as of writing.

## It is NOT only inherited - the open stack adds instances

The first report of this said every instance was inherited. That is wrong, and it matters, because
"inherited" is what makes it safe to leave alone.

- Inherited, present on `origin/master`: `client-side-work-queue.yaml`, `result-models.yaml`,
  `invalid-offset-metadata-policy.yaml`.
- **Added by the astubbs#504 stack**: `hand-back-without-an-attempt.yaml` and
  `per-throw-retry-delay.yaml` are new files on it, and `pause-and-resume.yaml` gained its affected
  item there. Run the block above against `origin/master` and against the astubbs#504 tip and
  diff the two outputs to re-derive this.

A worker who wrote new records reported checking that its own items parsed back as strings, and the
technique does work - it just was not applied to these. Which is the argument for a gate rather than
a habit.

## Is anything broken today

**Latent, with one live blind spot.** No renderer consumes these files yet -
`docs/features/README.md` says the data is owned here and the rendering is not. Nothing calls a
string method on a `boundaries` item, so nothing crashes and no page is wrong.

The live part is inside the gate itself. `check_refs` in `bin/check-docs-data.sh` recurses into a
mapping's **values** and folds its **keys** into the label only - a key is never scanned for
path-shaped tokens. So in a mis-parsed sentence, any file path cited *before* the colon is not
resolved, and the gate reports the record clean. That function's own docstring says it resolves
"anything path-shaped, wherever it appears - including inside prose", and the corpus's dominant
citation style is exactly the prose form it names, so a boundary reading `Not a durable queue: see
docs/features/ordering-modes.yaml for why.` is the shape that falls in. **There is no second gate
behind it**: `bin/check-file-refs.sh` treats `.yaml` as a citable target and never as a citing
source, so `check_refs` is the sole coverage for a path cited inside a feature record. No affected
key carries a path-shaped token as of writing, so nothing is being missed right now; the trigger is
one edit away, and it is silent when it fires.

`bin/test-check-docs-data.sh` has no case for this shape, so a fix would be landing the first one.

The real cost is deferred: the first consumer to be written will be written against a contract the
data does not keep, and the records that break it are the ones with the most carefully qualified
prose.

## Options, and what each costs - the owner's call

- **Teach the gate to assert that a prose list item is a string.** Closes the class rather than
  today's instances, and the check is the block above. Costs: it needs a list of which collections
  are prose and which are legitimately mappings, which is a second place that has to track
  `docs/data/schema.yaml`; and it turns every existing instance into a red build, so it lands with
  the repairs or not at all.
- **Quote the affected items.** Smallest diff, no tooling. Costs: it fixes instances, not the class -
  the next sentence with a colon in it reintroduces the defect, and nothing will say so.
- **Forbid `": "` in prose items.** Enforceable on the raw text, so it needs no parse and no schema
  knowledge. Costs: it bans a punctuation mark from prose that is doing real work in these records -
  several affected sentences read worse rewritten - and it is a lint rule authors will resent.
- **Accept it and document the shape.** Costs nothing now, and pushes the whole cost onto whoever
  writes the first consumer, who must handle both a string and a single-key mapping everywhere -
  the outcome where the data's contract is decided by an accident of punctuation.

The first and second are not exclusive: the gate is the class, the quoting is the backlog it creates.

## Delete when

The gate asserts prose items are strings, or the mapping shape is written into the page contract in
`docs/features/README.md` as accepted.
