# `controlLoopHooks` is an unsynchronised `ArrayList` behind a public API - HIGH PRIORITY, v6

**This branch owns extracting it. Do it first, in its own PR off `master`, labelled `0.6.0.0`.**

The fix currently exists only as one line on this branch (`feats/web-gui`, #215), where it is
invisible to everyone else and gated on a large feature landing. Every other in-flight branch is
exposed to the same defect in the meantime, and the fix is worth roughly nothing until it is on
`master`. Extract it, then rebase this branch onto it and drop the line here.

## The defect

`AbstractParallelEoSStreamProcessor:170` declares `controlLoopHooks` as a plain `ArrayList`. The
control thread iterates it on **every** pass at `:897` (`this.controlLoopHooks.forEach(Runnable::run)`),
while `addLoopEndCallBack(Runnable)` at `:1473` is **public, unsynchronised, and callable from any
thread at any time** - including against an already-running processor.

A registration that interleaves with an iteration throws `ConcurrentModificationException` out of
`controlLoop`. Nothing near the hook catches it; it lands in the control thread's generic handler at
`:849-856`, which records it as `failureReason`, calls `doClose(shutdownTimeout)` and rethrows. So the
outcome is not a logged warning - **the instance shuts down**.

This is pre-existing on `master`, not something the dashboard introduced. What the dashboard did was
supply the first *production* caller: `SnapshotPublisher.createAndRegister` registers against a live
processor. Until now every caller was a test that registered during setup, single-threaded, before the
control loop was running - which is why nothing has ever hit it.

## What the fix PR owes

- **An exposure test that fails on today's `master`.** A registration racing the control loop, driven
  hard enough to be reliable rather than incidental - and asserting the *observable* outcome (the
  instance closed / `failureReason` set), not merely that the exception type was thrown. A test that
  only passes because it happened not to race is the failure mode to avoid here.
- **The fix:** `CopyOnWriteArrayList`. Registration is rare, iteration is once per control-loop pass,
  so copy-on-write is the right shape and the read path stays allocation-free.
- **Javadoc on the field saying why**, since the next reader will otherwise "simplify" it back. The
  wording on `feats/web-gui` is already good - lift it rather than rewrite it.

## Second-instance check before merging

The defect class is *unsynchronised mutable state reachable from a public API while the control thread
reads it*. `controlLoopHooks` is the instance we know about; look for siblings on the same class
before closing this out, and report what was checked and cleared, not only what was found.

## Knock-ons

- **`feats/web-gui` (#215) drops the line** when this lands, and rebases onto it.
- It earns its own `CHANGELOG` line at release time. It is a user-visible stability fix, not an
  internal tidy-up, and it should not arrive buried inside a dashboard feature entry.
