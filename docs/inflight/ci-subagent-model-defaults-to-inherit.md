# A dispatched subagent inherits the foreground model when nobody chose one

<!-- inflight-type: task -->
<!-- inflight-impact: ci -->

Omitting `model` on an Agent dispatch is not a neutral default - the subagent runs on whatever the
foreground session is on, which is the expensive tier precisely when the foreground is doing the
judgement work. The intent was repeatedly to tier the dispatch down, and the field was simply left
off; nothing distinguishes a deliberate inherit from a forgotten one, so the miss is invisible at the
moment it happens and shows up only in the bill.

**A nudge cannot fix this one.** Injected context arrives alongside the tool call, by which point the
subagent has already launched on the wrong model. Only a refusal changes the outcome, and the cost of
complying with it is one field.

## The shape

A `PreToolUse` hook on the Agent tool that denies when `model` is absent, with a
`permissionDecisionReason` naming the policy rather than restating the schema. Three things to settle
before writing it, and the first is not optional:

- **Verify the matcher name against the installed Claude Code version.**
  [`docs/agent-harness.md`](../agent-harness.md)'s standing rule is that harness claims are tested,
  not read off the documentation - its own first version asserted four things that turned out false,
  each with a design already built on top.
- **Exempt `subagent_type: fork`.** A model override is ignored there by definition, so requiring one
  would be demanding a field that does nothing.
- **Deny or ask.** Deny is the only one that reliably changes behaviour; ask degrades to a habit of
  approving, which is the failure mode of every gate people learn to wave through.

Whether `effort` is required on the same terms is the open half - it is the same failure with the same
cause, and requiring two fields on every dispatch is a real friction cost that has not been weighed.

## It needs a negative control

`bin/test-check-agent-hooks.sh` is where it goes. That suite exists because the harness shipped its
first hook without one and a review then found six defects in a 25-line parser, four of them letting
through the exact mistake it was named after.

## Delete when

The hook is registered and its self-test is red against a deliberately broken copy.
