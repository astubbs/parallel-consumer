# `maxFailureHistory` is settable and does nothing

`ParallelConsumerOptions.maxFailureHistory` is declared with a default of 10 and is **read nowhere in
the source tree**. `WorkContainer` keeps only `lastFailureReason`. Setting the option changes nothing.

Found while writing feature records for the documentation data, and confirmed by a tree-wide search:
the only other occurrences of the name were the sentences describing it.

## Why it is not a documentation problem

A record for it was written and then removed rather than shipped. Documenting an option that does
nothing generates a page telling a user to configure something inert, which is worse than the option
being undocumented. There is also no honest `maturity` value for it: `stable` means supported and
covered by the reliability claim, `deprecated` means superseded, and neither describes a control that
was never wired.

## The decision to make

Either implement the retention it declares, or delete the option. Deleting is a public API change and
therefore belongs with the API settlement work before 1.0. Implementing it means deciding what a
failure history is for, given only the most recent failure is exposed today through
`RecordContext.getLastFailureReason()`.

Whichever way it goes, the feature record can be restored from git history on this branch.

## Delete when

The option is either implemented or removed.
