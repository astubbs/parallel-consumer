# The two key-ordered chaos cells implement the same four hooks, and the same guard test

<!-- inflight-type: task -->
<!-- inflight-impact: refactor -->
<!-- inflight-state: deferred - the fix changes a base class every chaos scenario extends, and it is not worth destabilising the suite for boilerplate the gate already passes -->

`ChaosKeyOrderIT` and `ChaosRevokeUnderWorkKeyOrderIT` each override `orderRecorder()`, `keyFor()`,
`identityFor()` and `identityOf()` with identical bodies, and each carries its own verbatim copy of
the `heavyRecordsMustNotAllShareOneKey` guard. The duplication detector reports 41 lines - most of it
javadoc, since the tool runs with `only_code: false`.

## The fix, and why it is not done yet

`ChaosScenarioBase` already provides defaults for all four hooks. The clean shape is a `keySpace()`
hook there - zero meaning "not key-ordered", preserving today's behaviour for every other cell - with
the base supplying the key-ordered variants when it is set. Both cells then override one method
instead of four and drop their copies of the guard.

That is a change to the class **every** chaos scenario extends, and the chaos suite is the thing this
work exists to make trustworthy. Doing it immediately after the suite got its first ordering
assertions would put a base-class refactor underneath findings that have not yet been reproduced
twice. The duplication is boilerplate, it is below the 80% gate, and it costs a reader nothing that a
cross-reference does not fix.

## What would change the answer

A third key-ordered cell. Two copies is duplication; three is a pattern nobody will keep in sync, and
at that point the hook is cheaper than the drift.

## Related

- `docs/inflight/test-chaos-phase2.md` - owns the chaos suite and its lanes
