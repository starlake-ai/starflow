#!/usr/bin/env bash
set -euo pipefail

# Publishes an unmodified copy of a Spark tarball to a GitHub release, where the installer
# (src/main/java/Setup.java) looks first: archive.apache.org serves it at under 1 MB/s.
#
# The tag is spark-X.Y.Z, so it starts no release workflow (v*.*.*) and stays out of the version
# menus of the launchers (vX.Y.Z only). A prerelease is never the latest release, which the
# installer reads to find the Starflow version.
#
# Usage, from the root of the repository, with the versions of Setup.java by default:
#   scripts/publish-spark-tarball.sh [spark version] [hadoop version]
#
# SPARK_MIRROR gets the tarball from a faster mirror: it is checked against the SHA-512 of Apache.
#   SPARK_MIRROR=https://mirrors.huaweicloud.com/apache scripts/publish-spark-tarball.sh

GH_REPO=starlake-ai/starflow
APACHE=https://archive.apache.org/dist

# Empty outside the root of the repository, so that the check below explains it, despite set -e.
pinned() { sed -n "s/.*getEnv(\"$1\").orElse(\"\([^\"]*\)\").*/\1/p" src/main/java/Setup.java 2>/dev/null || true; }

spark_version=${1:-$(pinned SPARK_VERSION)}
hadoop_version=${2:-$(pinned HADOOP_VERSION)}
[ -n "$spark_version" ] && [ -n "$hadoop_version" ] || { echo "Could not read the versions from Setup.java: run this from the root of the repository." >&2; exit 1; }
# Spark 3 ships a build per Scala version: Starflow 1.7 runs on Scala 2.13.
tarball="spark-$spark_version-bin-hadoop$hadoop_version-scala2.13.tgz"
tag="spark-$spark_version"
path="spark/spark-$spark_version/$tarball"

command -v gh >/dev/null || { echo "Install the GitHub CLI (gh) first: the tarball is published with it." >&2; exit 1; }
# Only github.com: without --hostname, gh fails when an account of another host has a problem.
gh auth status --hostname github.com >/dev/null 2>&1 || { echo "Log in with gh auth login first: the tarball is published with gh." >&2; exit 1; }

# Several Hadoop builds can share a release. grep reads all of the list, so that gh never gets a SIGPIPE.
if gh release view "$tag" --repo "$GH_REPO" --json assets --jq '.assets[].name' 2>/dev/null | grep -xF "$tarball" >/dev/null; then
  echo "$tarball is already on the release $tag."
  exit 0
fi

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

expected=$(curl -fsSL "$APACHE/$path.sha512" | awk '{print $1}') || true
[[ "$expected" =~ ^[0-9a-f]{128}$ ]] || { echo "No SHA-512 from Apache for $tarball." >&2; exit 1; }

echo "Downloading $tarball from ${SPARK_MIRROR:-$APACHE}..."
curl -fL --progress-bar -o "$work/$tarball" "${SPARK_MIRROR:-$APACHE}/$path"

actual=$(shasum -a 512 "$work/$tarball" | awk '{print $1}')
if [ "$actual" != "$expected" ]; then
  echo "SHA-512 mismatch for $tarball: got $actual, Apache publishes $expected." >&2
  exit 1
fi

if gh release view "$tag" --repo "$GH_REPO" >/dev/null 2>&1; then
  gh release upload "$tag" --repo "$GH_REPO" "$work/$tarball"
else
  gh release create "$tag" --repo "$GH_REPO" --title "Apache Spark $spark_version tarball" --prerelease --latest=false \
    --notes "Unmodified copy of \`$tarball\` from Apache Spark, for the Starflow installer: $APACHE/$path

SHA-512, as published by Apache: \`$expected\`" \
    "$work/$tarball"
fi

echo
echo "Published. If it is new, add this to SPARK_SHA512 in src/main/java/Setup.java:"
echo "  \"$tarball\", \"$expected\""
