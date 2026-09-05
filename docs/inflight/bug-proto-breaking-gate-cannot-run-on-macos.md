# The proto breaking-change gate cannot run locally on macOS

<!-- inflight-type: bug -->
<!-- inflight-impact: ci -->

`bin/check-proto-breaking.sh` uses `mapfile`, a bash 4 builtin. macOS ships bash 3.2 as
`/bin/bash`, so the script dies with `mapfile: command not found` before it checks anything:

```
bin/check-proto-breaking.sh: line 69: mapfile: command not found
```

Found 2026-08-23 while verifying that an experimental schema in a new module left the frozen wire
alone. It is pre-existing - the line arrives with the commit that made the two freeze guards able
to fail - and unrelated to that work.

**CI is unaffected**, which is why nobody has hit it: GitHub runners have bash 5. The cost is
local, and it is the shape this repo already has a name for - a developer running the gate before
pushing gets exit 127, not a verdict, and 127 is easy to read as "tooling missing, never mind"
rather than "the gate did not run".

**The fix is small**: replace `mapfile -t x < <(cmd)` with a `while IFS= read -r` loop, which
`bin/AGENTS.md`'s cross-platform rule already asks for. Its sibling `bin/check-proto-lint.sh` runs
fine on 3.2, so only this one is affected.

Worth checking the rest of `bin/` for the same builtin at the same time - the defect class is
"bash 4 builtin in a script whose shebang resolves to 3.2 on macOS", not this one call.
