# Vert.x non-200 HTTP handling has no test, and the stub that says so was nearly deleted

**Priority: high.** The vertx module ships response-code handling that nothing exercises, and we have
live vertx defects (astubbs/parallel-consumer#116 / astubbs/parallel-consumer#122), so this is a gap
that can be hiding real bugs rather than a tidy-up.

## What exists

`VertxTest.handleHttpResponseCodes` - born disabled 2020-05-27 (`61f4c0e41`), body:

```java
assertThat(true).isFalse();
```

The nearest real test in that file, `testHttp`, asserts `statusCode()` is 200 on the happy path only.
**Non-200 handling is untested.** A 4xx/5xx from the user's HTTP call has no test saying what PC does
with it - retried? failed? committed as success?

## Why it is still here, and must not be deleted before it is implemented

`docs/test-hardening/inactive-tests-audit-2026-08-08.md` §1.3 judged it a *"deletion candidate, not a
re-enablement candidate - there is nothing here to restore"*. That reads the body and misses the
purpose: **an empty or trivially-false test behind `@Disabled` is being used as a TODO that carries a
name and a location** - it names the missing coverage and puts the reminder where the test would go.
Deleting it removes the reminder and leaves the gap.

Stated by the repo owner, 2026-08-18: *"The whole point of those empty tests was for me to remind
myself that something needs testing that I haven't had time to do yet."*

So the audit's verdict stands as a description of the body and is wrong as an instruction. The
2026-08-08 sweep re-enabled what it could and left this one; that decision is the actual miss.

## What to write

Cover the response codes a user's HTTP call can return, and assert what PC does with each: whether
the record is retried, failed, or treated as processed, and whether its offset is committed. A test
that only proves "no exception" would repeat the class of test this audit found 15 of.

## Related

- `docs/test-hardening/inactive-tests-audit-2026-08-08.md` §1.3 and §3.1
- astubbs/parallel-consumer#116, astubbs/parallel-consumer#122 - live vertx defects
