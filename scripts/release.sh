#!/usr/bin/env bash
# T115: cut a release — one place that knows how a version becomes a tag.
#
#   scripts/release.sh 0.1.4            bump, sync Cargo.lock, commit, tag, push
#   scripts/release.sh 0.1.4 --dry-run  do everything, then revert (no commit)
#
# `release.yml` refuses to publish when the tag disagrees with
# `[workspace.package] version` (T114), so bumping the version is not optional.
# This script is the supported way to do it.
set -euo pipefail
cd "$(cd "$(dirname "$0")/.." && pwd)"

version="${1:-}"
dry_run="${2:-}"

if [ -z "$version" ]; then
    echo "usage: scripts/release.sh <version> [--dry-run]   e.g. scripts/release.sh 0.1.4" >&2
    exit 2
fi
case "$version" in
    [0-9]*.[0-9]*.[0-9]*) ;;
    *) echo "version must look like 0.1.4, without a leading v" >&2; exit 2 ;;
esac
tag="v$version"

read_version() {
    awk -F'"' '/^\[workspace\.package\]/ { f = 1 } f && /^version/ { print $2; exit }' Cargo.toml
}

current="$(read_version)"
if [ -z "$current" ]; then
    echo "could not read [workspace.package] version from Cargo.toml" >&2
    exit 1
fi
if [ "$current" = "$version" ]; then
    echo "Cargo.toml is already at $version" >&2
    exit 1
fi
if git rev-parse -q --verify "refs/tags/$tag" >/dev/null; then
    echo "tag $tag already exists locally" >&2
    exit 1
fi
if [ -n "$(git status --porcelain)" ]; then
    echo "working tree is dirty; commit or stash first" >&2
    exit 1
fi

echo "bumping $current -> $version"
awk -v v="$version" '
  /^\[workspace\.package\]/ { inpkg = 1 }
  /^\[/ && !/^\[workspace\.package\]/ { inpkg = 0 }
  inpkg && /^version = / && !done { print "version = \"" v "\""; done = 1; next }
  { print }
' Cargo.toml > Cargo.toml.release.tmp
mv Cargo.toml.release.tmp Cargo.toml

# `release.yml` builds with `--locked`, and every workspace member carries its
# version inside Cargo.lock — a stale lock file fails that build outright.
cargo update --workspace --offline >/dev/null 2>&1 || cargo update --workspace >/dev/null

if [ "$dry_run" = "--dry-run" ]; then
    git --no-pager diff --stat
    git checkout -- Cargo.toml Cargo.lock
    echo "--dry-run: reverted, nothing committed"
    exit 0
fi

git add Cargo.toml Cargo.lock
git commit -q -m "chore(release): $tag"
git tag -a "$tag" -m "$tag"
echo "commit $(git rev-parse --short HEAD), tag $tag"
git push origin HEAD
git push origin "$tag"
echo "pushed — watch the Release Build workflow at https://github.com/cuukenn/Kafka-Manage-Rust/actions"
