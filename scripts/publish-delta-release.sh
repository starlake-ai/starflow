#!/usr/bin/env bash
set -euo pipefail

# Publishes our build of Delta, and of the Unity Catalog snapshot it pins, to the GitHub release
# deps-delta-<version>. The build resolves them from there (Resolvers.deltaGithubRelease in
# project/Common.scala) and so does the installer (DELTA_BASE_URL in src/main/java/Setup.java).
# Only needed while Delta has no release for our Spark line: see Versions.deltaSpark.
#
# The modules come from ~/.ivy2/local, where the Delta build publishes them: the command is in
# the comment above Versions.deltaSpark. Each one is published as flat assets: the jar, the pom and
# an ivy.xml generated from its pom, named after the module so that the seven modules can share
# one release.
#
# The tag is deps-delta-X, so it starts no release workflow (v*.*.*) and stays out of the version
# menus of the launchers (vX.Y.Z only). A prerelease is never the latest release, which the
# installer reads to find the Starflow version.
#
# Usage, from the root of the repository, with the versions of Setup.java:
#   DELTA_SRC=../delta scripts/publish-delta-release.sh

GH_REPO=starlake-ai/starflow
IVY_LOCAL="${IVY_LOCAL:-$HOME/.ivy2/local}"
DELTA_SRC="${DELTA_SRC:-../delta}"

pinned() { sed -n "s/.*getEnv(\"$1\").orElse(\"\([^\"]*\)\").*/\1/p" src/main/java/Setup.java; }

delta_version=$(pinned SPARK_DELTA)
uc_version=$(pinned UNITY_CATALOG_VERSION)
spark_version=$(pinned SPARK_VERSION)
[ -n "$delta_version" ] && [ -n "$uc_version" ] && [ -n "$spark_version" ] || { echo "Could not read the versions from Setup.java: run this from the root of the repository." >&2; exit 1; }
[[ "$delta_version" == *-starlake.* ]] || { echo "SPARK_DELTA=$delta_version is not one of our builds (-starlake.N): official Delta comes from Maven Central." >&2; exit 1; }
spark_minor=${spark_version%.*}
tag="deps-delta-$delta_version"

modules=(
  "io.delta delta-spark_${spark_minor}_2.13 $delta_version"
  "io.delta delta-storage $delta_version"
  "io.delta delta-kernel-api $delta_version"
  "io.delta delta-kernel-defaults $delta_version"
  "io.delta delta-kernel-unitycatalog $delta_version"
  "io.unitycatalog unitycatalog-client $uc_version"
  "io.unitycatalog unitycatalog-hadoop $uc_version"
)

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

# The ivy.xml that Delta's publishLocal writes is not usable: delta-spark depends there on
# delta-spark-v1 and delta-spark-v2, internal modules that are merged into its jar and published
# nowhere. Delta post-processes only its pom, which lists the kernel modules instead, so the
# ivy.xml of each module is generated from its pom (compile and runtime dependencies).
ivy_from_pom() {
  python3 - "$@" <<'EOF'
import sys, xml.etree.ElementTree as ET
from xml.sax.saxutils import quoteattr as q
pom, org, module, revision = sys.argv[1:5]
ns = {"m": "http://maven.apache.org/POM/4.0.0"}
deps = []
for d in ET.parse(pom).getroot().findall("m:dependencies/m:dependency", ns):
    scope = (d.findtext("m:scope", "compile", ns) or "compile").strip()
    if scope not in ("compile", "runtime") or d.findtext("m:optional", "false", ns).strip() == "true":
        continue
    excludes = "".join(
        "\n      <exclude org=%s module=%s/>" % (q(e.findtext("m:groupId", "*", ns)), q(e.findtext("m:artifactId", "*", ns)))
        for e in d.findall("m:exclusions/m:exclusion", ns))
    deps.append('    <dependency org=%s name=%s rev=%s conf="%s->default(%s)">%s\n    </dependency>' % (
        q(d.findtext("m:groupId", None, ns)), q(d.findtext("m:artifactId", None, ns)),
        q(d.findtext("m:version", None, ns)), scope, scope, excludes))
print("""<?xml version="1.0" encoding="UTF-8"?>
<ivy-module version="2.0">
  <info organisation=%s module=%s revision=%s status="release"/>
  <configurations>
    <conf name="compile" visibility="public"/>
    <conf name="runtime" visibility="public" extends="compile"/>
    <conf name="default" visibility="public" extends="runtime"/>
  </configurations>
  <publications>
    <artifact name=%s type="jar" ext="jar" conf="compile"/>
  </publications>
  <dependencies>
%s
  </dependencies>
</ivy-module>""" % (q(org), q(module), q(revision), q(module), "\n".join(deps)))
EOF
}

for m in "${modules[@]}"; do
  read -r org module revision <<<"$m"
  dir="$IVY_LOCAL/$org/$module/$revision"
  [ -f "$dir/jars/$module.jar" ] && [ -f "$dir/poms/$module.pom" ] || { echo "Missing $org:$module:$revision in $IVY_LOCAL: build Delta first (see Versions.deltaSpark)." >&2; exit 1; }
  cp "$dir/jars/$module.jar" "$work/$module-$revision.jar"
  cp "$dir/poms/$module.pom" "$work/$module-$revision.pom"
  ivy_from_pom "$dir/poms/$module.pom" "$org" "$module" "$revision" >"$work/$module-$revision-ivy.xml"
done

# An io.delta or io.unitycatalog dependency that is not in the release would only fail at
# resolution time, in the build of whoever depends on it.
for ivy in "$work"/*-ivy.xml; do
  while read -r org module revision; do
    [ -f "$work/$module-$revision.jar" ] || { echo "$(basename "$ivy") depends on $org:$module:$revision, which this release does not carry." >&2; exit 1; }
  done < <(grep -oE '<dependency org="io\.(delta|unitycatalog)" name="[^"]+" rev="[^"]+"' "$ivy" | sed -E 's/.*org="([^"]+)" name="([^"]+)" rev="([^"]+)"/\1 \2 \3/')
done

delta_commit=$(git -C "$DELTA_SRC" rev-parse HEAD 2>/dev/null || echo unknown)
uc_commit=$(sed -n 's/^UC_PIN_SHA=//p' "$DELTA_SRC/project/scripts/setup_unitycatalog_main.sh" 2>/dev/null || echo unknown)

echo "Assets for $tag:"
ls -l "$work"

# DRY_RUN=<dir> stages the assets there and publishes nothing.
if [ -n "${DRY_RUN:-}" ]; then
  mkdir -p "$DRY_RUN" && cp "$work"/* "$DRY_RUN"/
  echo "Dry run: assets staged in $DRY_RUN, nothing published."
  exit 0
fi

if gh release view "$tag" --repo "$GH_REPO" >/dev/null 2>&1; then
  gh release upload "$tag" --repo "$GH_REPO" --clobber "$work"/*
else
  gh release create "$tag" --repo "$GH_REPO" --title "Delta $delta_version for Spark $spark_minor" --prerelease --latest=false \
    --notes "Our build of Delta Lake master, for Spark $spark_minor, until Delta publishes a release for it.

- Delta: delta-io/delta@$delta_commit, labelled \`$delta_version\`
- Unity Catalog \`$uc_version\` (unitycatalog/unitycatalog@$uc_commit), the snapshot that this Delta commit pins

Each module is a jar, its pom and an ivy.xml generated from the pom. Resolved by the Starflow build (\`Resolvers.deltaGithubRelease\`) and installed by the Starflow installer." \
    "$work"/*
fi

echo
echo "Published $tag: https://github.com/$GH_REPO/releases/tag/$tag"
