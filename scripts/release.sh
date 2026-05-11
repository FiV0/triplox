#!/usr/bin/env bash
set -euo pipefail

usage() {
    echo "Usage: $0 <0.1.0-alpha.N|v0.1.0-alpha.N>"
}

if [[ $# -ne 1 ]]; then
    usage
    exit 2
fi

input_version="$1"
version="${input_version#v}"
tag="v$version"

if [[ ! "$version" =~ ^0\.1\.0-alpha\.[0-9]+$ ]]; then
    echo "Expected version format 0.1.0-alpha.N, got: $input_version" >&2
    exit 2
fi

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "$script_dir/.." && pwd)"

cd "$repo_root"

if [[ -n "$(git status --porcelain)" ]]; then
    echo "Working tree must be clean before preparing a release." >&2
    exit 1
fi

if git rev-parse -q --verify "refs/tags/$tag" >/dev/null; then
    echo "Tag already exists locally: $tag" >&2
    exit 1
fi

if git ls-remote --exit-code --tags origin "refs/tags/$tag" >/dev/null 2>&1; then
    echo "Tag already exists on origin: $tag" >&2
    exit 1
fi

current_branch="$(git branch --show-current)"
if [[ -z "$current_branch" ]]; then
    echo "Release preparation must run on a branch, not a detached HEAD." >&2
    exit 1
fi

perl -0pi -e 's/(?m)^version = "0\.1\.0-alpha\.\d+"$/version = "'"$version"'"/' Cargo.toml
perl -0pi -e 's/(edn = \{ package = "triplox-edn", path = "edn", version = ")[^"]+(")/${1}'"$version"'${2}/' Cargo.toml
perl -0pi -e 's/(triplox-client = \{ path = "triplox-client", version = ")[^"]+(")/${1}'"$version"'${2}/' Cargo.toml

cargo metadata --format-version 1 >/dev/null
cargo fmt --check
cargo test --workspace
cargo clippy --workspace --all-targets --all-features -- -D warnings

git add Cargo.toml Cargo.lock
git commit -m "Release $tag" \
    -m "Co-authored-by: Codex <noreply@openai.com>"
git tag -a "$tag" -m "Release $tag"

cat <<EOF
Prepared release $tag on branch $current_branch.

Review the commit, then publish with:
  git push origin $current_branch
  git push origin $tag
EOF
