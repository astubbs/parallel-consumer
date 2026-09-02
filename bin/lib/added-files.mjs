// Copyright (C) 2026 Antony Stubbs and contributors
//
// WHICH FILES THIS BRANCH ADDED - the set an `added-files` rule in bin/lib/source-patterns.mjs is
// scoped to. It lives here, apart from the runner, because the runner walks git and this does not:
// given three lists it is a pure set computation, so it can be tested by control pair like every
// rule in the table, rather than by standing up a fixture repository.
//
// SUBTRACTING WHAT IS ALREADY ON THE BASE IS THE WHOLE POINT, and the reason is a merge commit.
// While `git merge master` is staged but not yet committed, HEAD is still the pre-merge tip, so the
// merge base is where the branch was cut - and every file master has added since then is sitting in
// the index as an addition. The union of committed and staged additions therefore contains master's
// files, and a "no new shell scripts" rule fires on scripts that landed on master and are
// grandfathered there.
//
// Observed on the merge of ten master commits into fix/offset-encoding-policy-bypass: seven shell
// scripts from astubbs/parallel-consumer#381's experiment runners, none of them touched by that
// branch, and the gate's advice was to commit with --no-verify. A gate that fires on work somebody
// else already merged teaches the bypass, which costs more than the rule earns.
//
// Membership of the base ref settles it on its own: a path already on origin/master is not new to
// this repository, whoever staged it and for whatever reason. That also covers the case where a
// branch and master added the same path independently - it is grandfathered on master either way.

/**
 * @param {{committed: string[], staged: string[], alreadyOnBase: Iterable<string>}} lists
 * @returns {string[]} paths this branch added, deduplicated, in first-seen order
 */
export function addedByBranch({ committed, staged, alreadyOnBase }) {
  const onBase = alreadyOnBase instanceof Set ? alreadyOnBase : new Set(alreadyOnBase)
  return [...new Set([...committed, ...staged])].filter(f => f && !onBase.has(f))
}
