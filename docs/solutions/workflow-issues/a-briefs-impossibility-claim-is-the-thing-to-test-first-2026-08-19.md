---
title: "A brief's 'this is structurally impossible' is the claim to test first, not the one to build on"
date: 2026-08-19
category: workflow-issues
module: development_workflow
problem_type: workflow_issue
component: development_workflow
severity: medium
applies_when:
  - Picking up a handover brief that explains why a problem cannot be solved the simple way
  - A requirements section argues a constraint is structural rather than incidental
  - About to build tooling whose only purpose is detecting a state that should not be created
---

# A brief's "this is structurally impossible" is the claim to test first, not the one to build on

## What happened

A handover brief asked for a gate. Its middle section - explicitly marked *"the requirement, not
background"* - argued that `upstream-map.yaml` going stale at merge was **structurally
unavoidable**:

> The rule is *update at every lifecycle transition, in the same commit that causes it*. The merging
> transition is the one commit that rule cannot produce: you cannot honestly write `status: merged`
> before the merge, and after it your branch is gone.

Taking that as given, the work followed inevitably: if staleness cannot be prevented it must be
detected, so detection needs to run *after* merge, so it needs a script plus a scheduled workflow
plus live `gh` queries, on master and on a cron. All of that was built, tested and committed.

The premise was wrong, and one sentence dissolved it: **write `merged` in the branch and push it
before merging.** Branch content is visible to nobody until it lands, and the moment it lands the
statement is true. There is no dishonest interval, so there is no staleness, so there is nothing to
detect. The two scripts and the workflow were deleted and replaced by a ~80-line pre-merge hook.

## Why it is worth writing down

The brief was good. It was specific, it cited evidence, it had already anticipated the lazy answer
(*"do not fix this with a checklist line"*) and explained why. Its quality is exactly what made the
false premise easy to inherit: everything downstream of it was sound, so the reasoning felt
verified when only the conclusions had been.

The tell was available and unread: **the artefact being built existed only to observe a bad state,
never to prevent one.** A gate that reports "this is already wrong on master" is a gate whose
existence concedes the state is unpreventable. That concession is a claim, and claims get checked.

## What to do

- When a brief says a constraint is *structural*, test that sentence before building on it. It is
  load-bearing for everything after it, and it is usually the only sentence that is.
- Ask what the tooling would be *for*. If the answer is "noticing afterwards", ask once more whether
  the state can be prevented instead. Detection is what you build when prevention is genuinely
  impossible, not the default.
- Weigh the lifespan. This machinery would have run on a schedule indefinitely to police a field
  that exists only until the fork's upstream links are closed out. A fix that expires by deleting
  one file beats one that has to be unpicked.
- Inheriting a premise is not the same as verifying it. The correction here came from a human
  reading one paragraph, which is roughly the cost of having checked it in the first place.

## Related

- [`docs/upstream.md`](../../upstream.md) - the rule as it now stands, and the hook that enforces it
- `.claude/hooks/check-upstream-map-merged.sh` - what replaced the scripts
