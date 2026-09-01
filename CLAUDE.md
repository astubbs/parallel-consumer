# CLAUDE.md

Claude Code reads **`CLAUDE.md`** and never reads `AGENTS.md` (verified against 2.1.223). This file
exists only to bridge that gap, so the repo's conventions are loaded rather than merely available.

@AGENTS.md

It is deliberately a pure import with no rules of its own. Anything written here instead of in
`AGENTS.md` would be a second copy that Codex and other agents cannot see, and that drifts from the
first. `docs/agent-harness.md` explains the layers, what each can enforce, and how to add to them.
