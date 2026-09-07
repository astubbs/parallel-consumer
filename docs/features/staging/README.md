<!-- Copyright (C) 2026 Antony Stubbs and contributors -->

# Staged feature records

Records for capabilities that are not settled in the tree yet: a module that has not landed, or a
claim whose evidence is a release that has not been cut. They are held here rather than published,
because a record that asserts something the tree contradicts is worse than a missing one.

Staging is deliberately outside `bin/check-docs-data.sh`. Staged content references things that do
not exist, so gating it would either fail permanently or push authors to fake the references.

**Moving one out is part of the change that makes it true.** Whichever PR lands the module, or
whichever release cut makes the evidence real, moves the record up into `docs/features/` in the same
change and re-checks its wording against what actually shipped.
