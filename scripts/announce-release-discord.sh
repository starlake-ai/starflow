#!/usr/bin/env bash
#
# Announce a release on the Starlake Discord #news channel: repo + release-notes
# links followed by the version's CHANGELOG.md section, split into messages
# under Discord's 2000-character limit.
#
# Called by scripts/local-release.sh's step 10, right after the GitHub release
# is published. Also safe to run standalone to (re)announce a version:
#   ./scripts/announce-release-discord.sh 1.8.0
#
# Modeled on quack-on-demand's scripts/announce-release-discord.sh.
#
# Webhook: SL_DISCORD_WEBHOOK_URL env var, falling back to a
# SL_DISCORD_WEBHOOK_URL=... line in the untracked .env at the repo root.
# The URL is a credential (anyone holding it can post to the channel) -
# never commit it.

set -euo pipefail
SCRIPT_DIR="$( cd "$( dirname -- "${BASH_SOURCE[0]}" )" && pwd )"
source "$SCRIPT_DIR/release-lib.sh"

version="${1:-}"
[[ -n "$version" ]] || { echo "usage: $0 <version>   (e.g. 1.8.0)" >&2; exit 1; }
version="${version#v}"

webhook="$(discord_webhook_url)"
[[ -n "$webhook" ]] \
  || { echo "ERROR: SL_DISCORD_WEBHOOK_URL not set (env var or .env line)." >&2; exit 1; }
command -v python3 >/dev/null 2>&1 || { echo "ERROR: python3 not on PATH." >&2; exit 1; }

# The section body between "# <version>[:]" and the next "# " heading.
# CHANGELOG.md headings are a single "# " (not "## "), with or without a
# trailing colon (older entries, e.g. "# 1.4.0", omit it) - match both, and
# never match the "# Release notes" title line above every section.
section="$(awk -v v="$version" '
  $0 == "# " v || $0 == "# " v ":" { found = 1; next }
  found && /^# / { exit }
  found { print }
' "$REPO_DIR/CHANGELOG.md")"
# Non-blank check via grep, NOT ${section//[[:space:]]/}: macOS /bin/bash 3.2
# evaluates that substitution quadratically (minutes of CPU on a long section).
# A missing section must not break the release flow: warn and announce the
# repo + release-notes links without a changelog body.
grep -q '[^[:space:]]' <<<"$section" \
  || echo "WARNING: no '# $version' section in CHANGELOG.md; announcing links only." >&2

header="**Starflow v released**
https://github.com/starlake-ai/starlake
Release notes: https://github.com/starlake-ai/starlake/releases/tag/v${version}"

post() {
  python3 -c 'import json,sys; print(json.dumps({"content": sys.stdin.read().rstrip()}))' \
    <<<"$1" \
    | curl -fsS -H 'Content-Type: application/json' -d @- "${webhook}?wait=true" >/dev/null
}

# Discord caps a message at 2000 characters; chunk on line boundaries with
# headroom. Sequential posts keep the chunks in channel order.
limit=1900
chunk="$header"$'\n'
while IFS= read -r line; do
  if (( ${#chunk} + ${#line} + 1 > limit )); then
    post "$chunk"
    chunk=""
  fi
  chunk+="$line"$'\n'
done <<<"$section"
if grep -q '[^[:space:]]' <<<"$chunk"; then
  post "$chunk"
fi

echo "announced v${version} on Discord #news."
