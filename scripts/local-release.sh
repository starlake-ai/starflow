#!/usr/bin/env bash
set -euo pipefail

# ============================================================================
# Starlake Release Script
#
# Releases starlake-core and starlake-api in lockstep to GitHub Releases
# (distribution moved off Maven Central), then bumps all version references
# to the next SNAPSHOT.
#
# Each release is a tag v{X.Y.Z} on starlake-ai/starlake carrying 4 assets:
#   starlake-core_{scala}-{v}-assembly.jar (+ .sha256)
#   starlake-api_{scala}-{v}.zip           (+ .sha256)
#
# Every step is idempotent: a failed release is resumed by simply re-running
# the script (optionally with --steps). Steps skip work already done:
#   - version.sbt already at the release version -> skip the set + commit
#   - tag v{v} already exists                    -> skip tag
#   - artifact already built                     -> skip build
#   - release / asset already on GitHub          -> skip create / upload
#   - version.sbt already bumped to -SNAPSHOT    -> skip the bump
#
# Usage (from repo root):
#   ./scripts/local-release.sh                  # all steps
#   ./scripts/local-release.sh --steps 6        # only the GitHub release
#   ./scripts/local-release.sh --dry-run        # preview
#   RELEASE_VERSION=1.6.0 NEXT_VERSION=1.6.1-SNAPSHOT ./scripts/local-release.sh
#   RELEASE_VERSION=1.6.0 ./scripts/local-release.sh --steps 6   # resume a specific release after version.sbt moved on
#
# Steps:
#   1 - Preflight (gh auth, clean trees, version alignment)  [always runs]
#   2 - Set release version + commit (both repos)
#   3 - Tag v{v} (both repos)
#   4 - Build core assembly + publishLocal, build api zip
#   5 - Push commits (both repos) and the api tag; the starlake tag is
#       created remotely when step 6 publishes the release
#   6 - Create the GitHub release (draft), upload sha256 assets, publish
#   7 - Bump to next SNAPSHOT + commit + push (both repos)
#   8 - Housekeeping: propagate versions, setup.jar, full assembly
# ============================================================================

SCRIPT_DIR="$( cd "$( dirname -- "${BASH_SOURCE[0]}" )" && pwd )"
source "$SCRIPT_DIR/release-lib.sh"
cd "$REPO_DIR"

API_DIR="${SL_API_DIR:-$HOME/git/starlake-api}"
UI_DIR="${SL_UI_DIR:-$HOME/git/starlake-ui2}"
PROFILE="$HOME/.bash_profile"

DRY_RUN=false
STEPS="1,2,3,4,5,6,7,8"
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

[[ -d "$API_DIR" ]] || die "starlake-api repo not found at $API_DIR (set SL_API_DIR)"

CURRENT_VERSION="$(read_version "$REPO_DIR/version.sbt")"
API_CURRENT_VERSION="$(read_version "$API_DIR/version.sbt")"
echo "  starlake-core: $CURRENT_VERSION"
echo "  starlake-api:  $API_CURRENT_VERSION"
[[ "$CURRENT_VERSION" == "$API_CURRENT_VERSION" ]] \
  || die "starlake-api version ($API_CURRENT_VERSION) != starlake-core version ($CURRENT_VERSION)"

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
# Step 3: Tag (idempotent)
# ============================================================================
if should_run 3; then
  echo "============================================"
  echo "Step 3: Tag $TAG"
  echo "============================================"
  for repo in "$REPO_DIR" "$API_DIR"; do
    if tag_exists "$repo" "$TAG"; then
      echo "  $(basename "$repo"): tag $TAG already exists, skipping."
    else
      run git -C "$repo" tag "$TAG"
      echo "  $(basename "$repo"): tagged $TAG"
    fi
  done
fi

# ============================================================================
# Step 4: Build artifacts (idempotent)
# Builds must happen while version.sbt still holds the release version. A
# resumed run that lost an artifact after the step 7 bump rebuilds from the
# tag:  git show v{v}:version.sbt > version.sbt && sbt assembly
#       && git checkout version.sbt
# ============================================================================
if should_run 4; then
  echo "============================================"
  echo "Step 4: Build artifacts"
  echo "============================================"
  if [[ -f "$CORE_JAR" ]]; then
    echo "  $CORE_JAR_NAME already built, skipping."
  else
    [[ "$DRY_RUN" == true || "$(read_version "$REPO_DIR/version.sbt")" == "$RELEASE_VERSION" ]] \
      || die "starlake-core version.sbt moved past $RELEASE_VERSION; rebuild from the tag (see comment above step 4)."
    echo "  building $CORE_JAR_NAME + publishLocal..."
    run sbt assembly publishLocal
    [[ "$DRY_RUN" == true || -f "$CORE_JAR" ]] || die "sbt assembly did not produce $CORE_JAR"
  fi
  if [[ -f "$API_ZIP_BUILT" ]]; then
    echo "  starlake-api-${RELEASE_VERSION}.zip already built, skipping."
  else
    [[ "$DRY_RUN" == true || "$(read_version "$API_DIR/version.sbt")" == "$RELEASE_VERSION" ]] \
      || die "starlake-api version.sbt moved past $RELEASE_VERSION; rebuild from the tag (see comment above step 4)."
    echo "  building starlake-api zip..."
    ( cd "$API_DIR" && run sbt Universal/packageBin )
    [[ "$DRY_RUN" == true || -f "$API_ZIP_BUILT" ]] || die "sbt Universal/packageBin did not produce $API_ZIP_BUILT"
  fi
fi

# ============================================================================
# Step 5: Push commits and tags (unconditional no-ops when up to date)
# The starlake tag is NOT pushed here: pushing it now would trigger the
# tag-triggered Docker workflow (.github/workflows/release.yml) before the
# release assets exist, and its build would 404 downloading them. The
# starlake tag instead reaches GitHub when the release is published at the
# end of step 6, after the assets are uploaded, so the Docker workflow finds
# them. The api repo's tag has no such workflow, so it is pushed as usual.
# ============================================================================
if should_run 5; then
  echo "============================================"
  echo "Step 5: Push commits and tags"
  echo "============================================"
  run git -C "$REPO_DIR" push origin HEAD
  run git -C "$API_DIR" push origin HEAD
  run git -C "$API_DIR" push origin "$TAG"
fi

# ============================================================================
# Step 6: GitHub release (idempotent)
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
if should_run 6; then
  echo "============================================"
  echo "Step 6: GitHub release $TAG"
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
        [[ -f "$local_file" ]] || die "$local_file missing; run step 4 first (rebuild from the tag if version.sbt moved on)."
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
# Step 7: Bump to next SNAPSHOT + push (idempotent)
# ============================================================================
if should_run 7; then
  echo "============================================"
  echo "Step 7: Bump to $NEXT_VERSION"
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
# Step 8: Housekeeping (kept from the pre-GitHub release script)
# ============================================================================
if should_run 8; then
  echo "============================================"
  echo "Step 8: Housekeeping"
  echo "============================================"

  # The version to propagate is whatever version.sbt now holds when it is
  # already a -SNAPSHOT (correct even on a resumed run after the step 7
  # bump); fall back to NEXT_VERSION otherwise.
  PROPAGATE_VERSION="$(read_version "$REPO_DIR/version.sbt")"
  [[ "$PROPAGATE_VERSION" == *-SNAPSHOT ]] || PROPAGATE_VERSION="$NEXT_VERSION"

  # --- 8a. Propagate PROPAGATE_VERSION to non-SBT config files ---
  SL_VERSION_FILES=(
    "$API_DIR/.versions"
    "$API_DIR/versions.sh"
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

  if [[ "$DRY_RUN" == false && -f "$PROFILE" ]] && grep -q "LOCAL_STARLAKE_VERSION" "$PROFILE"; then
    sed -i '' "s/LOCAL_STARLAKE_VERSION=.*/LOCAL_STARLAKE_VERSION=$PROPAGATE_VERSION/" "$PROFILE"
    echo "  Updated LOCAL_STARLAKE_VERSION in $PROFILE"
  fi

  # --- 8b. Rebuild and push setup.jar ---
  # No `clean` here: it would wipe the step 4 assembly needed by a resumed
  # step 6.
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

  # --- 8c. Optional full assembly ---
  # tmpsbt.sh derives the version from version.sbt, already bumped by step 7.
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
