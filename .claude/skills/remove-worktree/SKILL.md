---
name: remove-worktree
description: Remove a git worktree at ../<repo-name>-<feature-name> and optionally delete the branch
disable-model-invocation: true
argument-hint: <feature-name>
allowed-tools: Bash, AskUserQuestion
---

Remove the git worktree for the feature named `$ARGUMENTS`.

Steps:
1. Get the repo name from the basename of the current git toplevel directory: `basename $(git rev-parse --show-toplevel)`
2. Verify the worktree exists at `../<repo-name>-$ARGUMENTS` using `git worktree list`
   - If it does not exist: print an error and stop
3. Remove the worktree: `git worktree remove ../<repo-name>-$ARGUMENTS`
   - If that fails due to uncommitted changes, ask the user if they want to force removal with `git worktree remove --force ../<repo-name>-$ARGUMENTS`
4. Prune stale worktree metadata: `git worktree prune`
5. Ask the user if they also want to delete the local branch `$ARGUMENTS`
   - If yes: `git branch -d $ARGUMENTS` (use `-D` if it fails due to unmerged changes, after confirming with the user)
6. Run `git worktree list` to confirm removal
