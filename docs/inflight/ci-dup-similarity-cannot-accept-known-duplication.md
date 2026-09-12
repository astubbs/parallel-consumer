# The similarity engine has no way to accept duplication that is correct

<!-- inflight-type: bug -->
<!-- inflight-impact: misdirection -->
<!-- inflight-vetted: 2026-09-12 - re-read `maven.yml`'s `dups: similarity` step and the pinned action's own `action.yml`: the step still passes only `directories`, `file_extensions`, `ignore_below`, `fail_above: 80`, `warn_above`, `one_comment`, `compare_with_base`, `max_increase`, and the action declares no `ignore_files` or accepted-pairs input to pass, so the delete-when condition is not met. Also ran the engine locally at the pinned sha (recipe below) -->

`dups: similarity` (`astubbs/duplicate-code-detection-tool`, pinned in `maven.yml`) compares whole
files pairwise and fails above 80%. It has **no allowlist**: no `ignore_files`, no accepted-pairs
baseline, no glob. `ignore_directories` is the only exclusion and it takes directories.

## Why that is a defect rather than a missing nicety

Its only baseline is **the base branch, computed live**. So identical files are accepted or rejected
purely by which side of a merge they sit on.

<!-- post-merge: checked-begin -->
The worked example: when astubbs/parallel-consumer#326 added `TestConventionsArchTest` wrappers to
three example modules, they failed at 84.3% while the four already on master (core, vertx, reactor,
mutiny) passed at ~84% - because those four were not an *increase*. Same files, same similarity,
opposite verdicts. Once astubbs#326 landed, its three joined the baseline and stopped being flagged,
which is the whole problem: nothing about the code changed, only which side of the merge it sat on.
<!-- post-merge: checked-end -->

ArchUnit opts a module in **only** through its own two-line wrapper pointing `@AnalyzeClasses` at
that module's packages, so every wrapper in the repo is near-identical by construction. There is no
way to write one that is not. This recurs on **every new module**.

## Why the obvious workarounds are all wrong

<!-- post-merge: checked -->
- **Exclude the example modules** - tried on astubbs#326 and reverted. Each wrapper shares its
  directory with that module's real app test (`CoreAppTest`, `VertxAppTest`, `ReactorAppTest`), and
  those five example apps solve the same problem five ways, so they are the *most* likely place for
  genuine duplication. The exclusion blinds the metric to the one thing in examples worth measuring.
- **`only_code: true`** (strip comments before analysis) does nothing at all on this corpus: the
  engine strips comments only from `.py` files, through `ast.parse`, so on a Java tree the flag is
  inert. This bullet used to claim it would make matters WORSE for the wrappers, which was never
  tested because it cannot be - and the stripping experiment below found the opposite, that the
  wrappers stay at the top of a comment-stripped table while prose-driven pairs fall off it.
- **Lower `fail_above`** - blinds the metric everywhere to fix one file class.
- **Make it advisory** - it has a demonstrated catch. On astubbs#325 this same engine flagged 83%
  between the two drain control arms; the fix stopped two controls drifting apart. `dups: clones` is
  density-based and would not have caught it: two near-identical 40-line files barely move a 0.13%
  figure. Downgrading a check that works, because it lacks an allowlist, treats a missing feature as
  a reason to stop enforcing.

## What the score is actually measuring (2026-09-12)

<!-- post-merge: checked-begin -->
`dups: similarity` failed the fluent-API PR, astubbs/parallel-consumer#502, on two new pairs, both
involving `ParallelConsumerDefinition`: one against `RouteDispatcher`, the other against
`WorkClaimStateMachineTest` - a test of an unrelated subsystem, in another package, which that PR
did not touch. Neither was duplication. A mechanical line intersection of the fluent pair finds no
shared statement sequence at all; the identical lines are the package statement, the copyright
header, the imports, `@Slf4j`, `@Override`, `return;` and `} else {`. Delegation runs one way, and
the one thing that could have been duplicated - the unrouted-topic refusal wording - had already
been extracted to a shared static before the check ever fired.

**The engine scores prose, because it tokenises the whole file.** `duplicate_code_detection.py`
word-tokenises each file exactly as read, javadoc and comments included, and builds its tf-idf model
from that. In a repository whose main-code files run close to half comment lines, the dominant signal
is the English, so two heavily documented classes about one subject score as duplicates whatever
their code does. Here the shared vocabulary's *weights* were even inverted between the two files -
the definition class is about definitions and instances, the dispatcher about records, parks and
attempts - and a whole-file bag of words cannot see that.

**The control arm settles it.** Strip comments from every Java file in the tree and re-measure: the
fluent pair falls below `warn_above`, the `WorkClaimStateMachineTest` pair drops off the report
entirely, and the pairs that are genuinely duplicated *code* - the `TestConventionsArchTest`
wrappers above, and `MutinyBatchTest` against `ReactorBatchTest` - stay at the top of the table, one
of them scoring higher than it did with its comments. The check was not too strict there; it was
reading the wrong text.
<!-- post-merge: checked-end -->

### Running it locally, which is the part nobody had

The finding lived only in the job log, because the report could not reach the PR at all - see
[`ci-duplication-report-can-fail-to-post.md`](ci-duplication-report-can-fail-to-post.md). The action
is a thin wrapper over a Python engine that needs no network once installed, so the measurement is
reproducible on a workstation and matched CI to two decimal places:

```bash
git clone https://github.com/astubbs/duplicate-code-detection-tool   # then check out the sha maven.yml pins
python3 -m venv venv && venv/bin/pip install -r <tool>/requirements.txt requests
venv/bin/python -c "import nltk; nltk.download('punkt_tab')"
# Scan an EXPORTED tree: `scan: repo` builds nothing, so CI has no target/ dirs and a worktree does.
git archive <ref> | tar -x -C /tmp/scan-tree
cd /tmp/scan-tree && ../venv/bin/python <tool>/duplicate_code_detection.py \
  --directories . --file-extensions java --project-root-dir . \
  --ignore-threshold 30 --fail-threshold 80 --csv-output /tmp/scan.csv
```

Two traps. **The absolute table is not the gate** - `compare_with_base` means the verdict is the
delta, so scan the base ref the same way and feed both results through `run_action.compute_delta`
and `delta_to_markdown` to see which rows actually fail. And **use `--csv-output`, not `--json`**: on
a current numpy the engine's own JSON dump raises `TypeError: Object of type float32 is not JSON
serializable`, because `round()` no longer returns a plain float the way it did on the Python 3.7
image the action pins. That is a local-environment trap only; CI is unaffected.

## The fix

Add an accepted-duplication input to the tool - `ignore_files` taking paths or globs, or a checked-in
accepted-pairs baseline. **The action is `astubbs/duplicate-code-detection-tool`, this project's own
fork**, so this is available rather than blocked on a third party.

An accepted-pairs baseline is the better shape: it records *which* duplication was reviewed and
accepted, so a NEW pair between the same files still fails. A path ignore silences the file forever,
including duplication nobody has looked at.

The path-ignore half is smaller than it looks: the engine **already** accepts `--ignore-files`, and
`run_action.py` pins it to `None` with no matching `action.yml` input. That is a wired input rather
than new code. An accepted-pairs baseline is still the better shape, for the reason above.

**A second mechanism would settle a different half of this, and the two are not alternatives.** An
allowlist accepts duplication that is correct; **making comment stripping work for Java** stops the
engine producing the finding in the first place. The flag for it already exists and is inert, and the
stripping experiment above said implementing it *sharpens* the metric rather than blinding it - the
genuine code pairs survived comment stripping and the prose-driven ones did not. Prefer it where it
applies, because an allowlist accepts a false finding whereas this one removes it.

One caveat that experiment turned up: with comments gone, a file that is almost entirely javadoc - a
marker annotation, say - shrinks to a handful of lines, and very short documents score erratically
against each other. A Java code-only mode needs a minimum-length floor, or those become the next
false-positive class.

## Related

- `.github/workflows/maven.yml` - the job, whose own comment already makes this argument about
  `bin/`: check/test-check twins are similar by design, and flagging them "yields findings that are
  all correct and all unactionable, which is how a check gets ignored."

## Delete when

The tool can express accepted duplication and `maven.yml` uses it.
