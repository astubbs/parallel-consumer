# When do we ship v6? The "enough is enough" decision, not the blocker list

<!-- inflight-type: task -->
<!-- inflight-impact: release-gate -->

**Deliberately its own file, not a section inside
[`release-0.6.0.0.md`](release-0.6.0.0.md).** A section is invisible until someone opens that file;
a note gets its own line in the session-start index. This question needs to be *met*, not looked up
- so it is filed where an agent trips over it.

The other two release notes answer different questions and neither answers this one:
[`release-0.6.0.0.md`](release-0.6.0.0.md) is the content and the breaking-change record;
[`release-0600-blockers.md`](release-0600-blockers.md) is correctness of the artefacts we are about
to publish. **Both answer "is it ready?". This one asks "is it enough?"** - which is a judgement, not
a checklist, and no gate will ever go red for it.

## The tension, stated so it can be argued with

**People are more likely to look at a first release than a second.** For a revived fork that is
sharper than usual: the attention comes once, when the project visibly comes back to life. A second
release does not get a second launch.

Which creates real pressure to cram v6 - to make the one release anyone definitely looks at carry
everything worth seeing. That pressure is why this note exists rather than a bare "ship when the
blockers close": the failure mode is not shipping something broken, it is **never shipping**, with
each addition individually justified.

## What resolves it

**Announce the roadmap alongside the release.** If people hear what is coming, the first release
does not have to be the only thing they see. That decouples "what ships in v6" from "what people
learn about the project", which is what the cramming instinct was really protecting.

The roadmap data already exists (`docs/data/roadmap.yaml`, `docs/features/`), so this is a
publishing decision rather than new work - which is exactly why it can be decided rather than built.

## The date, set 2026-08-24

**Target: Wednesday 26 August 2026**, and it is the fork announcement rather than a routine tag - the
roadmap publishes alongside it, which is the resolution above being taken rather than deferred. Every
piece of work in flight now is to be judged against that launch: not "is this good?" but "does this
have to be in the thing people look at once?"

**It is gated on the bugs that are already open, not on scope.** Anything not fixing one of those is
arguing to be in a release it does not block - which is the cramming instinct this note exists to
name. The bar the date sets is therefore narrower than the one under "Still open": those questions
remain live, but a proposal that cannot point at a currently-open bug does not need them answered to
be declined.

A date makes the failure mode concrete. Until now the risk was never shipping, with each addition
individually justified; with a date the risk becomes shipping the date and quietly dropping the
announcement or the roadmap, which are the parts that make the release cost less than it looks.

## Still open

- **What is the actual bar for v6?** "Stability release" is stated in `release-0.6.0.0.md` under
  *"This release is a stability release, and that is the point"*. Is that the whole bar, or does
  something else have to be true?
- **Which currently-open items are genuinely v6, and which are v6 only because they are finished?**
  A thing being ready is not a reason to ship it in this release.
- **Does the roadmap announcement have to be simultaneous with the release**, or can it precede it
  and take some of the pressure off sooner?

Nothing here is blocked on engineering. It is blocked on the call.

**High priority, and it needs a conversation rather than a PR.** Flagged as needing discussion by
the owner on 2026-08-19, explicitly NOT as work to fold into whatever PR is open at the time - a
scope judgement decided in passing, inside a branch about something else, is exactly how the
cramming happens. Take it on its own.
