#!/usr/bin/env bash
# Builds the documentation UI and copies it into src/main/resources/site-ui.
# Run after any change under site-ui/; CI fails when the committed bundle is stale.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
UI="$ROOT/site-ui"
OUT="$ROOT/src/main/resources/site-ui"

WANT="$(tr -d '[:space:]' < "$UI/.nvmrc")"
HAVE="$(node -v | sed 's/^v//; s/\..*//')"
if [ "$HAVE" != "${WANT#v}" ]; then
  echo "ERROR: Use Node $WANT (see site-ui/.nvmrc), found $(node -v)" >&2
  exit 1
fi

cd "$UI"
npm ci
npm test
npm run build

rm -rf "$OUT"
mkdir -p "$OUT"
cp -R dist/. "$OUT/"
# Dev data copied from site-ui/public into dist must never ship in the bundle
rm -f "$OUT"/sl-*.json

if [ -n "$(find "$OUT" -type f -name '*.json')" ]; then
  echo "ERROR: $OUT contains json files, the bundle must not ship data:" >&2
  find "$OUT" -type f -name '*.json' >&2
  exit 1
fi
if ! grep -q '<!--sl-data-->' "$OUT/index.html"; then
  echo "ERROR: $OUT/index.html lost the <!--sl-data--> marker" >&2
  exit 1
fi
if grep -q 'type="module"' "$OUT/index.html" || grep -q 'crossorigin' "$OUT/index.html"; then
  echo "ERROR: $OUT/index.html loads a module or crossorigin script, which breaks file:// viewing" >&2
  exit 1
fi

(cd "$OUT" && find . -type f ! -name files.txt | sed 's|^\./||' | LC_ALL=C sort > files.txt)
echo "Site UI bundle written to $OUT:"
cat "$OUT/files.txt"
