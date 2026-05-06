# Agent Instructions

See [GLOSSARY.md](GLOSSARY.md) for explanations of key concepts.

Use `gh`, the github CLI, for any interactions with github. Meaning opening PRs, listing issues, finding comments etc..


## Issue Tracking

Issues are tracked via GitHub Issues. Use `gh issue list` and `gh issue view <number>` to find and review work.

Priority labels follow a P0–P4 hierarchy:
- **P0-critical** — Must be done immediately, blocks everything else
- **P1-high** — Important, should be tackled next
- **P2-medium** — Standard priority, the bulk of planned work
- **P3-low** — Nice to have, do when higher-priority work is clear
- **P4-backlog** — Long-term ideas, research, and speculative improvements

Issues are also labeled by type: `bug`, `enhancement`, `task`, `epic`.

When creating or updating issues, apply the appropriate priority and type labels and assign them to the **Triplox** project board.

## Style

Keep comments to one line where possible. Multi-line is fine for complex logic.

Prefer one-line TODOs.

## Git

Only commit and push when explicitly asked to by the user.
End Codex-created commits with `Co-authored-by: Codex <noreply@openai.com>`.

## Formatting

Run `cargo fmt` before committing. CI checks formatting via `cargo fmt --check`.

## Testing

Once the new tests pass, also run all tests with:

```bash
cargo test
```

For JVM client integration tests, see [triplox-jvm/README.md](triplox-jvm/README.md).
