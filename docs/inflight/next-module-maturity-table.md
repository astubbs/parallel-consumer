# A per-module maturity table, and what "not 1.0" actually means

Wanted for 0.6.0.0. Two problems it solves at once: the release ships a stable core *and* alpha
experiments, and the version number implies something about readiness that is no longer accurate.

Ideation has run. This note is now the spec, and it carries the alternatives that were rejected so
they are not re-proposed. Full working: `docs/ideation/2026-08-10-module-maturity-table-ideation.md`.

## The tension to resolve honestly

- The project is `<1.0`, which conventionally signals that it is not ready for production use.
- A substantial number of people already run it in production, successfully, and have done for years.
- The position taken at 0.6.0.0 is that **every known critical defect has been addressed**, under
  considerably more - and more rigorous - testing than before, and that the project is **ready for
  production use as far as reliability is concerned**.
- What actually remains before 1.0 is refactoring and a set of contained follow-up items. None of it
  is reliability work.

The resolution is that **a single version number is being asked to carry two independent facts**:

| Axis | 0.6.0.0 position |
|---|---|
| **Reliability** - will it lose or double-process your data, stall, or leak | The claim being made. Known critical issues fixed, and demonstrably tested |
| **API stability** - will your code still compile next release | Not yet settled. This is what `<1.0` is really reserving |

"Pre-1.0 refers to the API surface, not to reliability" is a defensible, checkable sentence.

Do not oversell what testing proves. It shows the known failure modes are covered and no longer
reproduce - not that no unknown ones exist. The claim to make is **"all known critical issues"**.

## Settled: what the artefact actually is

**One sentence, and a table that draws one line.** Not a per-module gradient.

Reliability does not vary across the shipped modules. Core, vertx, reactor and mutiny are one engine
with thin adapters, every open fix lands before the tag, and the release ships on the condition that
known critical defects are fixed - so a column grading them against each other would have every cell
reading the same thing. A column whose cells are all identical is padding, not a claim.

Reliability varies at exactly one place: the line between the shipped library and the experiments
(Connect, Kafka Streams, the GUI). Drawing that line unmissably is the table's whole job.

Both axes move together across that line - an experiment is neither reliable nor API-stable, a
shipped module is reliable with a reserved API - so two columns would carry one distinction twice.
The two-axis point therefore lands as **prose**: one prominent sentence saying pre-1.0 reserves the
API surface, not reliability. This follows the house pattern of the neighbouring Java Version per
Module table, which states its default in prose and lets the table carry the exception.

**Columns:** module, what it is for and who should reach for it. No alpha/beta/stable vocabulary - a
status word invites an argument about which word, a use statement does not. Experiment rows say
plainly that they are not for production use yet.

**Experiment rows carry the blast radius**, because the reader's real question about an experimental
module is not "how mature is it" but "can it hurt me if I do not use it". Depending on the artifact
is the entire opt-in. The three reasons that make that provable rather than reassuring are in
`release-0.6.0.0.md`.

**Venue.** Near the existing "Java Version per Module" table in `src/docs/README_TEMPLATE.adoc`, which
also needs `parallel-consumer-examples` added - it omits it today and nobody caught it. `README.adoc`
is generated: edit the template and regenerate with `./mvnw -N asciidoc-template:build`.

## Rejected, with reasons

- **Citing the quarantine release gate as the reliability claim.** Every ideation frame reached this
  independently and every one of them was wrong. The gate proves that no known-failing test is held
  out of the gating suites; that is not a reliability guarantee. The registry is empty, so it
  currently gates nothing. And it is engineering-system machinery, which belongs to the
  testing-as-product section, not here.
- **Keying rows by processing guarantee rather than module.** Its force came from the EoS path having
  far thinner production exposure than the unordered path, but the deadlock fix lands before the tag,
  and the line that survives is shipped-versus-experiment, not configuration.
- **Splitting reliability into correctness and liveness**, and **an open-advisories cell beside the
  rating.** Both existed to state the open deadlock honestly without hedging. The fix lands before the
  tag, so the problem goes away and the content becomes the release-time cross-check below.
- **A separate readiness tracker kept in step by every PR.** A per-PR maintenance burden that drifts.
- **The "1.0 is nearer than the backlog implies" aside.** It is an assertion about capacity rather
  than a verifiable fact, it explains schedule rather than quality, and the target reader discounts
  exactly this kind of claim. Leave it out.

## Correction to an earlier premise

This note previously implied the README carries a misleading `<1.0` signal that the table would
correct. It does not. The README makes no stability, maturity or production-readiness claim at all,
and there is no status or badge column anywhere in the template. The artefact is additive, and the
thing being corrected is silence.

## Open, and blocking

- **The experiment rows are not writable yet.** Connect, Kafka Streams and the GUI are spikes with no
  open PR and are not in the reactor. Whichever PR lands each module adds its own row; writing the row
  first would assert something the tree contradicts.
- **A release-time cross-check, not a documentation task.** Before the tag, confirm the gate actually
  held: astubbs#29 landed and confluentinc#857 closed. If it did not, name the open defect in the
  release material rather than softening the sentence. Background in `bug-857-family.md`.
- **Re-verify the blast-radius reasoning against what ships.** It was established on a spike branch
  against `origin/master`; if the experimental modules change shape before landing, the three reasons
  need re-checking.
- **A row-drift check is wanted but not yet written.** The module set is about to change three times,
  and the neighbouring table already demonstrates how easily a row is forgotten. Shape it on
  `bin/check-quarantine-registry.sh`, with an explicit exclusion for the never-published
  `parallel-consumer-examples`.
- **The two axes are not equally backed, and nothing says so.** Reliability has machinery behind it;
  API stability has none. The `internal` package is a naming convention, not a boundary - its classes
  are public, with no `package-info.java` and no ArchUnit rule. Either state the asymmetry or build
  the ratchet (an `@ApiStability` annotation, an ArchUnit rule, a drift check). The ratchet is
  separately schedulable, and tightening `internal` is a migration rather than a rule, since reducing
  visibility risks breaking anyone already importing those classes.

## Delete when

The table and its sentence are published in the README template, the pre-1.0 wording is in place, and
the open items above have either landed or moved to their own notes.
