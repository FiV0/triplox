#!/usr/bin/env bash
set -euo pipefail

usage() {
    echo "Usage: $0 <VERSION|vVERSION>"
    echo "Examples: $0 0.1.0, $0 0.1.0-alpha.1, $0 0.1.0-beta.1"
}

if [[ $# -ne 1 ]]; then
    usage
    exit 2
fi

input_version="$1"
version="${input_version#v}"
tag="v$version"

if [[ ! "$version" =~ ^[0-9]+\.[0-9]+\.[0-9]+(-(alpha|beta)\.[0-9]+)?$ ]]; then
    echo "Expected version format X.Y.Z, X.Y.Z-alpha.N, or X.Y.Z-beta.N; got: $input_version" >&2
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

perl -0pi -e 's/(?m)^version = "[0-9]+\.[0-9]+\.[0-9]+(?:-(?:alpha|beta)\.[0-9]+)?"$/version = "'"$version"'"/' Cargo.toml
perl -0pi -e 's/(edn = \{ package = "triplox-edn", path = "edn", version = ")[^"]+(")/${1}'"$version"'${2}/' Cargo.toml
perl -0pi -e 's/(triplox-client = \{ path = "triplox-client", version = ")[^"]+(")/${1}'"$version"'${2}/' Cargo.toml
perl -0pi -e 's{(ghcr\.io/fiv0/triplox:)[0-9]+\.[0-9]+\.[0-9]+(?:-(?:alpha|beta)\.[0-9]+)?}{${1}'"$version"'}g' README.md

cargo metadata --format-version 1 >/dev/null
cargo fmt --check
cargo test --workspace
cargo clippy --workspace --all-targets --all-features -- -D warnings

git add Cargo.toml Cargo.lock README.md
git commit -m "Release $tag"
git tag -a "$tag" -m "Release $tag"

cat <<EOF
Prepared release $tag on branch $current_branch.

Review the commit, then publish with:
  git push origin $current_branch
  git push origin $tag
EOF
