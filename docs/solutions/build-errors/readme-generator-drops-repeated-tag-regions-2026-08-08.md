---
title: The README generator emits only the first region of a repeated tag, silently truncating examples
date: 2026-08-08
category: build-errors
module: build-system
problem_type: build_error
component: development_workflow
symptoms:
  - "A tagged example region renders in README.adoc with its setup but without its processing call"
  - "Two `// tag::name[]` regions in one file, but only the first appears in the generated output"
  - "No error, no warning - the build succeeds and the generated file looks plausible"
  - "grep for a distinctive identifier from the second region returns nothing in README.adoc"
root_cause: incorrect_assumption_about_tool_behaviour
resolution_type: code_change
---

## What happened

Four new example apps each split their documented snippet into two `// tag::` regions sharing one tag
name - the options-builder setup, and the processing call (`pollAndProduce` / `vertxFuture` / `react`).
The intent was that AsciiDoc concatenates same-named regions in file order, so the reader would see setup
then processing as one snippet. Three separate implementers wrote that assumption into javadoc, and the
plan repeated it.

**It is false for this repository's generator.** Regenerating produced README sections containing the PC
options builder and *nothing else* - every example lost the actual call to Parallel Consumer, which is
the entire point of the snippet. The build succeeded and the output looked plausible.

## Why

`README.adoc` is not built by Asciidoctor's own include-with-tag handling. It is produced by
`io.whelk.asciidoc:asciidoc-template-maven-plugin` (root `pom.xml`, bound to `process-sources`).
Disassembling its `TemplateMojo.readTaggedLines` shows a started/ended latch that is set once and never
reset: after the first `end::name[]` the reader stops collecting, so any later region with the same tag
name is discarded.

Asciidoctor's documented multi-region behaviour does not apply, because Asciidoctor is not what is
running.

## Fix

Give every region its own tag name and add a second `include::` per section:

```
// tag::parcelTracking[]          -> setup block
// tag::parcelTrackingProcess[]   -> processing block
```

Each README section then includes the two blocks in order with its own callout list. The javadoc in the
four example apps that asserted concatenation now states the real rule.

## How to not be caught by it

**Grep the generated file for a distinctive identifier from every tagged region.** A missing region
produces no error - the only signal is absence. Checking that `README.adoc` merely changed, or that no
literal `include::` survives, does not catch it: both are true of a silently truncated file.

There is also **no CI check that `README.adoc` matches its template**, so a stale or truncated generated
file can be committed and stay wrong. That gap is recorded in the industry-grounded-examples plan's scope
boundaries as deferred work.

## Related

- `README.adoc` is generated - never hand-edit it. Editing the generated file instead of the source is a
  recorded past mistake here (astubbs#196 / astubbs#197).
- The learnings corpus previously had nothing at all about the README pipeline's failure modes.
