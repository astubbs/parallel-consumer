# CLAUDE.md

Bridge only. Claude Code lazy-loads a nested `CLAUDE.md` when it reads or writes a file in that
directory, and does not load `AGENTS.md` at all - so this arrives with the concurrency rules while
you are editing the engine, rather than after a detector catches you.

@AGENTS.md
