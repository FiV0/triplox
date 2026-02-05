---
name: add-worktree
description: Add a git worktree at ../<repo-name>-<feature-name>
disable-model-invocation: true
argument-hint: <feature-name>
allowed-tools: Bash
---

Add a git worktree for the feature named `$ARGUMENTS`.

Steps:
1. Get the repo name from the basename of the current git toplevel directory: `basename $(git rev-parse --show-toplevel)`
2. Run `git fetch origin` to ensure remote refs are up to date
3. Check if a local branch named `$ARGUMENTS` already exists (`git rev-parse --verify $ARGUMENTS`)
   - If yes: add the worktree using the existing local branch: `git worktree add ../<repo-name>-$ARGUMENTS $ARGUMENTS`
   - If no: check if a remote branch `origin/$ARGUMENTS` exists (`git rev-parse --verify origin/$ARGUMENTS`)
     - If yes: add the worktree tracking the remote branch: `git worktree add ../<repo-name>-$ARGUMENTS -b $ARGUMENTS origin/$ARGUMENTS`
     - If no: create a new branch from current HEAD: `git worktree add ../<repo-name>-$ARGUMENTS -b $ARGUMENTS HEAD`
4. Print the worktree path and run `git worktree list` to confirm
