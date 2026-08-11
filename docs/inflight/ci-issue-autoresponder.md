# Issue first-response bot - what is still open

`.github/workflows/claude-issue-response.yml` auto-answers issues opened by non-maintainers. Two
things about it are not finished.

## It is not live yet

Posting is gated on the repo variable `CLAUDE_ISSUE_AUTOREPLY`; until that is `true` the drafted
reply only reaches the Actions job summary. That is deliberate - it is the equivalent of Inkeep's
preview mode, and the point is to read several real drafts before a stranger does. Go live with:

```bash
gh variable set CLAUDE_ISSUE_AUTOREPLY --repo astubbs/parallel-consumer --body true
```

**Nothing has exercised it end to end.** The `if:` excludes OWNER, so the maintainer cannot trigger
it by filing an issue - the first real run will be a real outside reporter. If you want it proven
before then, the honest options are a throwaway account or temporarily dropping the
`author_association` clause on a branch; reasoning about the expression is not the same as running
it. Until one run has produced a draft, treat the whole lane as untested.

## Scope is newly opened issues, and only that - decided, not pending

Frigate routes support questions to Discussions and answers them there. **This lane deliberately
does not.** The trigger is `issues: [opened]` and nothing else: no `issue_comment`, no `discussion`,
no backfill of issues that already exist. Follow-up questions are already served by `claude.yml`,
which answers an `@claude` mention anywhere.

Recorded because it will be re-proposed, and because two of the reasons are load-bearing:

- **`claude-code-action` has no `discussion` trigger.** Its events are `issues`, `issue_comment`,
  `pull_request_review` and `pull_request_review_comment`. Its own `docs/security.md` lists
  Discussions read/write as *"requested but not yet actively used ... will enable planned features in
  future releases"* - a stated roadmap item, not an oversight to route around. A hand-rolled lane
  would need `discussion: [created]`, an explicit `prompt:`, and the `addDiscussionComment` GraphQL
  mutation, since discussion comments have no REST endpoint.
- **Discussions are disabled here anyway** (`has_discussions: false`), and enabling them only pays
  off alongside a stated routing rule - bugs to issues, help to discussions - or it is just a second
  inbox nobody reads.

Revisit only if the action ships discussion triggers *and* there is discussion volume to answer.
