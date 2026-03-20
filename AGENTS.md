# Agent Instructions

This project uses **bd** (beads) for issue tracking. Run `bd onboard` to get started.

**New to the project?** See [GLOSSARY.md](GLOSSARY.md) for explanations of key concepts.

## Quick Reference

```bash
bd ready              # Find available work
bd show <id>          # View issue details
bd update <id> --status in_progress  # Claim work
bd close <id>         # Complete work
bd sync               # Sync with git
```

## Style

Keep comments to one line where possible. Multi-line is fine for complex logic.

## Git

Only commit and push when explicitly asked to by the user.

## Testing

Once the new tests pass, also run all tests with:

```bash
cargo test
```

For JVM client integration tests, see [triplox-jvm/README.md](triplox-jvm/README.md).

## Landing the Plane (Session Completion)

**When ending a work session**, you MUST complete ALL steps below. Work is NOT complete until `git push` succeeds.

**MANDATORY WORKFLOW:**

1. **File issues for remaining work** - Create issues for anything that needs follow-up
2. **Run quality gates** (if code changed) - Tests, linters, builds
3. **Update issue status** - Close finished work, update in-progress items
4. **PUSH TO REMOTE** - This is MANDATORY:
   ```bash
   git pull --rebase
   bd sync
   git push
   git status  # MUST show "up to date with origin"
   ```
5. **Clean up** - Clear stashes, prune remote branches
6. **Verify** - All changes committed AND pushed
7. **Hand off** - Provide context for next session
