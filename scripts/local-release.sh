#!/usr/bin/env bash
set -euo pipefail

# ============================================================================
# Starlake Release Script
#
# Releases starlake-core and starlake-api in lockstep to GitHub Releases
# (distribution moved off Maven Central), then bumps all version references
# to the next SNAPSHOT.
#
# Each release is a tag v{X.Y.Z} on starlake-ai/starflow carrying 4 assets:
#   starlake-core_{scala}-{v}-assembly.jar (+ .sha256)
#   starlake-api_{scala}-{v}.zip           (+ .sha256)
#
# Every step is idempotent: a failed release is resumed by simply re-running
# the script (optionally with --steps). Steps skip work already done:
#   - version.sbt already at the release version -> skip the set + commit
#   - ui/ already synced to the current starlake-ui2 commit -> skip UI build
#   - tag v{v} already exists                    -> skip tag
#   - artifact already built                     -> skip build
#   - release / asset already on GitHub          -> skip create / upload
#   - version.sbt already bumped to -SNAPSHOT    -> skip the bump
#
# Usage (from repo root):
#   ./scripts/local-release.sh                  # all steps
#   ./scripts/local-release.sh --steps 7        # only the GitHub release
#   ./scripts/local-release.sh --dry-run        # preview
#   RELEASE_VERSION=1.6.0 NEXT_VERSION=1.6.1-SNAPSHOT ./scripts/local-release.sh
#   RELEASE_VERSION=1.6.0 ./scripts/local-release.sh --steps 7   # resume a specific release after version.sbt moved on
#
# Steps:
#   1 - Preflight (gh auth, clean trees, version alignment)  [always runs]
#   2 - Set release version + commit (core + api)
#   3 - Build starlake-ui2, inject the static export into starlake-api/ui
#   4 - Tag v{v} (core, api, ui)
#   5 - Build core assembly + publishLocal, build api zip
#   6 - Push commits (core + api) and the api/ui tags; the starlake tag is
#       created remotely when step 7 publishes the release
#   7 - Create the GitHub release (draft), upload sha256 assets, publish
#   8 - Bump to next SNAPSHOT + commit + push (both repos)
#   9 - Housekeeping: propagate versions, setup.jar, full assembly
#  10 - Announce on Discord (best effort; skips silently if unconfigured)
# ============================================================================

SCRIPT_DIR="$( cd "$( dirname -- "${BASH_SOURCE[0]}" )" && pwd )"
source "$SCRIPT_DIR/release-lib.sh"
cd "$REPO_DIR"

API_DIR="${SL_API_DIR:-$HOME/git/starlake-api}"
UI_DIR="${SL_UI_DIR:-$HOME/git/starlake-ui2}"
PROFILE="$HOME/.bash_profile"

DRY_RUN=false
STEPS="1,2,3,4,5,6,7,8,9,10"
while [[ "${1:-}" == --* ]]; do
  case "$1" in
    --dry-run)  DRY_RUN=true; shift ;;
    --steps)    STEPS="1,$2"; shift 2 ;;
    *) die "Unknown option: $1" ;;
  esac
done

should_run() { [[ ",$STEPS," == *",$1,"* ]]; }

run() { # execute, or echo under --dry-run
  if [[ "$DRY_RUN" == true ]]; then
    echo "  [DRY-RUN] $*"
  else
    "$@"
  fi
}

# ============================================================================
# Step 1: Preflight (always runs)
# ============================================================================
echo "============================================"
echo "Step 1: Preflight"
echo "============================================"

require_gh_auth
require_cmd shasum
require_cmd sbt
require_cmd node
require_cmd yarn

[[ -d "$API_DIR" ]] || die "starlake-api repo not found at $API_DIR (set SL_API_DIR)"
[[ -d "$UI_DIR" ]] || die "starlake-ui2 repo not found at $UI_DIR (set SL_UI_DIR)"

CURRENT_VERSION="$(read_version "$REPO_DIR/version.sbt")"
API_CURRENT_VERSION="$(read_version "$API_DIR/version.sbt")"
echo "  starlake-core: $CURRENT_VERSION"
echo "  starlake-api:  $API_CURRENT_VERSION"
if [[ "$CURRENT_VERSION" != "$API_CURRENT_VERSION" ]]; then
  if [[ "$API_CURRENT_VERSION" != *-SNAPSHOT && "$CURRENT_VERSION" == "$(next_snapshot "$API_CURRENT_VERSION")" ]]; then
    # Interrupted step 8: core already bumped to the next snapshot, api still
    # at the release version. Resume that release rather than dying.
    echo "  resuming interrupted release: core already bumped, api still at $API_CURRENT_VERSION"
    RELEASE_VERSION="${RELEASE_VERSION:-$API_CURRENT_VERSION}"
  elif [[ "$(strip_snapshot "$CURRENT_VERSION")" == "$(strip_snapshot "$API_CURRENT_VERSION")" ]]; then
    # Interrupted step 2: one repo already committed the release version, the
    # other still holds its -SNAPSHOT. Same release either way.
    echo "  resuming interrupted release: versions agree modulo -SNAPSHOT"
  else
    die "starlake-api version ($API_CURRENT_VERSION) != starlake-core version ($CURRENT_VERSION)"
  fi
fi

RELEASE_VERSION="${RELEASE_VERSION:-$(strip_snapshot "$CURRENT_VERSION")}"
NEXT_VERSION="${NEXT_VERSION:-$(next_snapshot "$RELEASE_VERSION")}"
TAG="v$RELEASE_VERSION"

CORE_JAR_NAME="starlake-core_${SCALA_VERSION}-${RELEASE_VERSION}-assembly.jar"
API_ZIP_NAME="starlake-api_${SCALA_VERSION}-${RELEASE_VERSION}.zip"
CORE_JAR="$REPO_DIR/target/scala-${SCALA_VERSION}/$CORE_JAR_NAME"
API_ZIP_BUILT="$API_DIR/target/universal/starlake-api-${RELEASE_VERSION}.zip"

echo ""
echo "  release as:    $RELEASE_VERSION"
echo "  next snapshot: $NEXT_VERSION"
echo "  steps to run:  $STEPS"
echo ""

if [[ "$DRY_RUN" == false ]]; then
  require_clean_tree "$REPO_DIR" starlake-core
  require_clean_tree "$API_DIR" starlake-api
  require_clean_tree "$UI_DIR" starlake-ui2
  warn_if_not_master "$REPO_DIR" starlake-core
  confirm "Proceed?" || exit 1
  # Confirmed once here; later prompts in this run are pre-approved.
  export RELEASE_YES=1
fi

# ============================================================================
# Step 2: Set release version + commit (idempotent)
# ============================================================================
if should_run 2; then
  echo "============================================"
  echo "Step 2: Set release version $RELEASE_VERSION"
  echo "============================================"
  for repo in "$REPO_DIR" "$API_DIR"; do
    v="$(read_version "$repo/version.sbt")"
    if [[ "$v" == "$RELEASE_VERSION" ]]; then
      echo "  $(basename "$repo"): already at $RELEASE_VERSION, skipping."
    elif [[ "$v" == "$NEXT_VERSION" ]]; then
      # Resumed run after the step 8 bump: don't regress to the release version.
      echo "  $(basename "$repo"): already bumped to $NEXT_VERSION, skipping."
    elif [[ "$v" == *-SNAPSHOT ]]; then
      echo "  $(basename "$repo"): $v -> $RELEASE_VERSION"
      run set_version "$repo/version.sbt" "$RELEASE_VERSION"
      run git -C "$repo" commit -am "Setting version to $RELEASE_VERSION"
    else
      die "$(basename "$repo") version.sbt is at unexpected version $v (expected $RELEASE_VERSION or a -SNAPSHOT)"
    fi
  done
fi

# ============================================================================
# Step 3: Build UI and inject into starlake-api (idempotent)
# Runs before tagging so the api tag carries the freshly built UI. The Next.js
# static export ($UI_DIR/build, per distDir/output in next.config.js) replaces
# $API_DIR/ui and is committed with the starlake-ui2 commit SHA it was built
# from; that SHA in the commit subject is what makes a rerun on the same UI
# commit skip the rebuild.
# ============================================================================
if should_run 3; then
  echo "============================================"
  echo "Step 3: Build UI"
  echo "============================================"
  UI_SHA="$(git -C "$UI_DIR" rev-parse HEAD)"
  LAST_UI_SYNC="$(git -C "$API_DIR" log -1 --format=%s -- ui)"
  if [[ -f "$API_ZIP_BUILT" ]]; then
    echo "  starlake-api-${RELEASE_VERSION}.zip already built, skipping UI rebuild."
  elif [[ "$LAST_UI_SYNC" == *"starlake-ui2@$UI_SHA"* ]]; then
    echo "  ui/ already built from starlake-ui2@${UI_SHA:0:12}, skipping."
  else
    echo "  building starlake-ui2@${UI_SHA:0:12}..."
    ( cd "$UI_DIR" && run yarn install --frozen-lockfile )
    run rm -rf "$UI_DIR/build"
    ( cd "$UI_DIR" && run yarn build )
    [[ "$DRY_RUN" == true || -f "$UI_DIR/build/index.html" ]] \
      || die "yarn build did not produce $UI_DIR/build/index.html"
    run rm -rf "$API_DIR/ui"
    run mkdir -p "$API_DIR/ui"
    run cp -R "$UI_DIR/build/." "$API_DIR/ui/"
    if [[ "$DRY_RUN" == true ]]; then
      echo "  [DRY-RUN] Would commit ui/ as: Update UI to starlake-ui2@$UI_SHA for $TAG"
    else
      git -C "$API_DIR" add -A ui
      if git -C "$API_DIR" diff --cached --quiet; then
        echo "  UI output unchanged, nothing to commit."
      else
        git -C "$API_DIR" commit -m "Update UI to starlake-ui2@$UI_SHA for $TAG"
      fi
    fi
  fi
fi

# ============================================================================
# Step 4: Tag (idempotent)
# starlake-ui2 has no version.sbt; its tag is what ties a UI state to the
# release.
# ============================================================================
if should_run 4; then
  echo "============================================"
  echo "Step 4: Tag $TAG"
  echo "============================================"
  for repo in "$REPO_DIR" "$API_DIR" "$UI_DIR"; do
    if tag_exists "$repo" "$TAG"; then
      echo "  $(basename "$repo"): tag $TAG already exists, skipping."
    else
      run git -C "$repo" tag "$TAG"
      echo "  $(basename "$repo"): tagged $TAG"
    fi
  done
fi

# ============================================================================
# Step 5: Build artifacts (idempotent)
# Builds must happen while version.sbt still holds the release version. A
# resumed run that lost an artifact after the step 8 bump rebuilds from the
# tag:  git show v{v}:version.sbt > version.sbt && sbt assembly
#       && git checkout version.sbt
# ============================================================================
if should_run 5; then
  echo "============================================"
  echo "Step 5: Build artifacts"
  echo "============================================"
  if [[ -f "$CORE_JAR" ]]; then
    echo "  $CORE_JAR_NAME already built, skipping."
  else
    [[ "$DRY_RUN" == true || "$(read_version "$REPO_DIR/version.sbt")" == "$RELEASE_VERSION" ]] \
      || die "starlake-core version.sbt moved past $RELEASE_VERSION; rebuild from the tag (see comment above step 5)."
    echo "  building $CORE_JAR_NAME + publishLocal..."
    run sbt assembly publishLocal
    [[ "$DRY_RUN" == true || -f "$CORE_JAR" ]] || die "sbt assembly did not produce $CORE_JAR"
  fi
  if [[ -f "$API_ZIP_BUILT" ]]; then
    echo "  starlake-api-${RELEASE_VERSION}.zip already built, skipping."
  else
    [[ "$DRY_RUN" == true || "$(read_version "$API_DIR/version.sbt")" == "$RELEASE_VERSION" ]] \
      || die "starlake-api version.sbt moved past $RELEASE_VERSION; rebuild from the tag (see comment above step 5)."
    echo "  building starlake-api zip..."
    ( cd "$API_DIR" && run sbt Universal/packageBin )
    [[ "$DRY_RUN" == true || -f "$API_ZIP_BUILT" ]] || die "sbt Universal/packageBin did not produce $API_ZIP_BUILT"
  fi
fi

# ============================================================================
# Step 6: Push commits and tags (unconditional no-ops when up to date)
# The starlake tag is NOT pushed here: pushing it now would trigger the
# tag-triggered Docker workflow (.github/workflows/release.yml) before the
# release assets exist, and its build would 404 downloading them. The
# starlake tag instead reaches GitHub when the release is published at the
# end of step 7, after the assets are uploaded, so the Docker workflow finds
# them. The api and ui repos' tags have no such workflow, so they are pushed
# as usual.
# ============================================================================
if should_run 6; then
  echo "============================================"
  echo "Step 6: Push commits and tags"
  echo "============================================"
  run git -C "$REPO_DIR" push origin HEAD
  run git -C "$API_DIR" push origin HEAD
  run git -C "$API_DIR" push origin "$TAG"
  run git -C "$UI_DIR" push origin "$TAG"
fi

# ============================================================================
# Step 7: GitHub release (idempotent)
# The .sha256 companion assets are the integrity source for starlake.sh,
# starlake.cmd and Setup.java. Content is the standard "hash  basename" line
# so `shasum -c` works next to the download.
#
# The release is created as a draft targeting the tagged commit: a draft
# release has no remote tag and so does not trigger the tag-triggered Docker
# workflow. Only after both assets are uploaded is the release published,
# which creates the starlake tag on GitHub and triggers that workflow, by
# which time the assets it downloads are already in place.
# ============================================================================
if should_run 7; then
  echo "============================================"
  echo "Step 7: GitHub release $TAG"
  echo "============================================"
  if [[ "$DRY_RUN" == true ]]; then
    echo "  [DRY-RUN] Would create release $TAG on $GH_REPO with assets:"
    echo "  [DRY-RUN]   $CORE_JAR_NAME (+ .sha256)"
    echo "  [DRY-RUN]   $API_ZIP_NAME (+ .sha256)"
  else
    STAGE_DIR="$REPO_DIR/target/gh-release-$RELEASE_VERSION"
    mkdir -p "$STAGE_DIR"

    if ! release_exists "$TAG"; then
      echo "  creating draft GitHub release $TAG"
      gh release create "$TAG" --repo "$GH_REPO" --title "$TAG" --generate-notes --draft \
        --target "$(git -C "$REPO_DIR" rev-parse "$TAG^{commit}")"
    else
      echo "  release $TAG already exists."
    fi

    # ensure_asset <asset name> <local build output>
    # Uploads the artifact and its .sha256 when missing. When the artifact is
    # already published but its .sha256 is not, hash the PUBLISHED file, not
    # a local rebuild: sbt builds are not byte-reproducible, so a rebuilt
    # file's hash would not match what users actually download.
    ensure_asset() {
      local asset="$1" local_file="$2"
      if ! release_has_asset "$TAG" "$asset"; then
        [[ -f "$local_file" ]] || die "$local_file missing; run step 5 first (rebuild from the tag if version.sbt moved on)."
        cp "$local_file" "$STAGE_DIR/$asset"
        ( cd "$STAGE_DIR" && shasum -a 256 "$asset" > "$asset.sha256" )
        echo "  uploading $asset (+ .sha256)"
        gh release upload "$TAG" --repo "$GH_REPO" --clobber "$STAGE_DIR/$asset" "$STAGE_DIR/$asset.sha256"
      elif ! release_has_asset "$TAG" "$asset.sha256"; then
        echo "  backfilling $asset.sha256 from the published asset"
        local dl_dir; dl_dir="$(mktemp -d)"
        gh release download "$TAG" --repo "$GH_REPO" --pattern "$asset" --dir "$dl_dir"
        ( cd "$dl_dir" && shasum -a 256 "$asset" > "$asset.sha256" )
        gh release upload "$TAG" --repo "$GH_REPO" "$dl_dir/$asset.sha256"
        rm -rf "$dl_dir"
      else
        echo "  asset $asset already uploaded, skipping."
      fi
    }

    ensure_asset "$CORE_JAR_NAME" "$CORE_JAR"
    ensure_asset "$API_ZIP_NAME" "$API_ZIP_BUILT"
    rm -rf "$STAGE_DIR"

    if [[ "$(gh release view "$TAG" --repo "$GH_REPO" --json isDraft --jq .isDraft)" == "true" ]]; then
      echo "  publishing release $TAG (creates the remote tag and triggers the Docker workflow)"
      gh release edit "$TAG" --repo "$GH_REPO" --draft=false
    fi

    # The just-released version's rolling snapshot pre-release is now stale:
    # delete it and its tag (assets for X.Y.Z-SNAPSHOT stop making sense once
    # vX.Y.Z exists). The next snapshot pre-release appears lazily on the first
    # snapshot publish after the version bump.
    SNAP_TAG="v${RELEASE_VERSION}-SNAPSHOT"
    if release_exists "$SNAP_TAG"; then
      echo "  deleting stale snapshot pre-release $SNAP_TAG"
      run gh release delete "$SNAP_TAG" --repo "$GH_REPO" --yes --cleanup-tag
    fi
  fi
fi

# ============================================================================
# Step 8: Bump to next SNAPSHOT + push (idempotent)
# ============================================================================
if should_run 8; then
  echo "============================================"
  echo "Step 8: Bump to $NEXT_VERSION"
  echo "============================================"
  for repo in "$REPO_DIR" "$API_DIR"; do
    v="$(read_version "$repo/version.sbt")"
    if [[ "$v" == *-SNAPSHOT ]]; then
      echo "  $(basename "$repo"): already at $v, skipping bump."
    else
      run set_version "$repo/version.sbt" "$NEXT_VERSION"
      run git -C "$repo" commit -am "Setting version to $NEXT_VERSION"
    fi
    run git -C "$repo" push origin HEAD
  done
fi

# ============================================================================
# Step 9: Housekeeping (kept from the pre-GitHub release script)
# ============================================================================
if should_run 9; then
  echo "============================================"
  echo "Step 9: Housekeeping"
  echo "============================================"

  # The version to propagate is whatever version.sbt now holds when it is
  # already a -SNAPSHOT (correct even on a resumed run after the step 8
  # bump); fall back to NEXT_VERSION otherwise.
  PROPAGATE_VERSION="$(read_version "$REPO_DIR/version.sbt")"
  [[ "$PROPAGATE_VERSION" == *-SNAPSHOT ]] || PROPAGATE_VERSION="$NEXT_VERSION"

  # --- 9a. Propagate PROPAGATE_VERSION to non-SBT config files ---
  SL_VERSION_FILES=(
    "$API_DIR/.versions"
    "$API_DIR/versions.sh"
    "$UI_DIR/.versions"
  )
  BROAD_VERSION_FILES=(
    "$UI_DIR/Dockerfile"
  )
  # Version pattern: matches X.Y.Z or X.Y.Z-SNAPSHOT
  VER_RE='[0-9][0-9]*\.[0-9][0-9]*\.[0-9][0-9]*\(-SNAPSHOT\)*'

  for file in "${SL_VERSION_FILES[@]}"; do
    if [[ -f "$file" ]] && grep -q "SL_VERSION" "$file"; then
      if [[ "$DRY_RUN" == true ]]; then
        echo "  [DRY-RUN] Would update SL_VERSION lines in: $file"
      else
        sed -i '' "/SL_VERSION/s/$VER_RE/$PROPAGATE_VERSION/g" "$file"
        echo "  Updated: $file"
      fi
    else
      echo "  Skipped (missing or no SL_VERSION): $file"
    fi
  done

  for file in "${BROAD_VERSION_FILES[@]}"; do
    if [[ -f "$file" ]]; then
      if [[ "$DRY_RUN" == true ]]; then
        echo "  [DRY-RUN] Would update version references in: $file"
      else
        sed -i '' "s/$VER_RE/$PROPAGATE_VERSION/g" "$file"
        echo "  Updated: $file"
      fi
    else
      echo "  Skipped (missing): $file"
    fi
  done

  if [[ "$DRY_RUN" == false && -f "$PROFILE" ]] && grep -q "LOCAL_STARLAKE_VERSION" "$PROFILE"; then
    sed -i '' "s/LOCAL_STARLAKE_VERSION=.*/LOCAL_STARLAKE_VERSION=$PROPAGATE_VERSION/" "$PROFILE"
    echo "  Updated LOCAL_STARLAKE_VERSION in $PROFILE"
  fi

  # --- 9b. Rebuild and push setup.jar ---
  # No `clean` here: it would wipe the step 5 assembly needed by a resumed
  # step 7.
  if [[ "$DRY_RUN" == true ]]; then
    echo "  [DRY-RUN] Would run: sbt packageSetup + push distrib/setup.jar"
  else
    sbt packageSetup
    if [[ -f "$REPO_DIR/distrib/setup.jar" ]]; then
      git add distrib/setup.jar
      git commit -m "Update setup.jar for $PROPAGATE_VERSION" || echo "  setup.jar unchanged, nothing to commit."
      git push origin HEAD
    fi
  fi

  # --- 9c. Optional full assembly ---
  # tmpsbt.sh derives the version from version.sbt, already bumped by step 8.
  if [[ -x "$REPO_DIR/tmpsbt.sh" ]]; then
    if [[ "$DRY_RUN" == true ]]; then
      echo "  [DRY-RUN] Would run: tmpsbt.sh"
    else
      "$REPO_DIR/tmpsbt.sh"
    fi
  else
    echo "  tmpsbt.sh not found or not executable, skipping full assembly."
  fi
fi

# ============================================================================
# Step 10: Announce on Discord (best effort - never blocks the release)
# Unlike steps 2-9, this is not idempotency-checked against remote state
# (there's no way to ask Discord "was this already posted"): re-running step
# 10 re-announces. That's fine for a deliberate rerun (e.g. `--steps 10` to
# announce a version released earlier); just don't include it in a routine
# resume of a failed run past step 10.
# ============================================================================
if should_run 10; then
  echo "============================================"
  echo "Step 10: Announce on Discord"
  echo "============================================"
  if [[ "$DRY_RUN" == true ]]; then
    echo "  [DRY-RUN] Would run: ./scripts/announce-release-discord.sh $RELEASE_VERSION"
  elif [[ -z "$(discord_webhook_url)" ]]; then
    echo "  SL_DISCORD_WEBHOOK_URL not set (env var or .env); skipping announce."
  else
    "$REPO_DIR/scripts/announce-release-discord.sh" "$RELEASE_VERSION"
  fi
fi

# ============================================================================
# Summary
# ============================================================================
echo ""
echo "============================================"
echo "Done."
echo "  release: https://github.com/$GH_REPO/releases/tag/$TAG"
echo "  now on:  $(read_version "$REPO_DIR/version.sbt")"
echo ""
echo "Remaining manual steps:"
echo "  - Commit & push non-SBT file changes in starlake-api and starlake-ui2"
echo "  - Docker images build automatically once the release is published (creates the tag)"
echo "============================================"
