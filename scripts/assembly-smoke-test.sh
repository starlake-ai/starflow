#!/usr/bin/env bash
# Installs Starflow from the pins of this checkout (src/main/java/Setup.java) with a locally
# built assembly, then runs the stage, load and transform of samples/any-engine on Spark with
# the real launcher (distrib/starlake.sh).
#
# The test suite runs on the sbt classpath, so it cannot see what only the shipped jar and the
# installed dependencies break: shading (sbt assembly), a pinned dependency that does not
# resolve, a jar that clashes with Spark's own.
#
# Usage: scripts/assembly-smoke-test.sh <assembly jar> <work dir>
# The install goes to <work dir>/install, kept between runs: Setup.java only downloads what is
# missing, so a local run can start from a copy of an existing bin/spark and bin/deps.
set -euo pipefail

if [ $# -ne 2 ]; then
  echo "Usage: $0 <assembly jar> <work dir>" >&2
  exit 2
fi

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
JAR="$(cd "$(dirname "$1")" && pwd)/$(basename "$1")"
WORK_DIR="$(mkdir -p "$2" && cd "$2" && pwd)"
INSTALL_DIR="$WORK_DIR/install"
PROJECT_DIR="$WORK_DIR/project"
LOGS_DIR="$WORK_DIR/logs"
SL_VERSION="$(sed -n 's/.*"\(.*\)".*/\1/p' "$REPO_DIR/version.sbt")"

[ -f "$JAR" ] || { echo "No assembly at $JAR" >&2; exit 1; }
mkdir -p "$LOGS_DIR"

fail() {
  echo "SMOKE FAILED: $1" >&2
  [ -n "${2:-}" ] && [ -f "$2" ] && tail -50 "$2" >&2
  exit 1
}

echo "== Install Starflow $SL_VERSION from the pins of this checkout"
(cd "$REPO_DIR" && SL_VERSION="$SL_VERSION" SL_CORE_JAR="$JAR" ENABLE_API=false \
  java src/main/java/Setup.java "$INSTALL_DIR") > "$LOGS_DIR/setup.log" 2>&1 ||
  fail "Setup.java" "$LOGS_DIR/setup.log"
cp "$REPO_DIR/distrib/starlake.sh" "$INSTALL_DIR/starlake.sh"

echo "== Copy samples/any-engine"
rm -rf "$PROJECT_DIR"
cp -R "$REPO_DIR/samples/any-engine" "$PROJECT_DIR"
mkdir -p "$PROJECT_DIR/incoming"
cp -R "$PROJECT_DIR/sample-data/sales" "$PROJECT_DIR/sample-data/hr" "$PROJECT_DIR/incoming/"

cd "$PROJECT_DIR"
export SL_ROOT="$PROJECT_DIR"
export SL_ENV=LOCAL
export SPARK_DRIVER_MEMORY="${SPARK_DRIVER_MEMORY:-2G}"

run() {
  local step="$1"
  shift
  echo "== starlake $*"
  "$INSTALL_DIR/starlake.sh" "$@" > "$LOGS_DIR/$step.log" 2>&1 || fail "starlake $*" "$LOGS_DIR/$step.log"
}

run stage stage
run load load
run transform transform --name sales_kpi.byseller_kpi0

for table in sales.db/customers sales.db/orders sales.db/products hr.db/sellers \
  sales_kpi.db/byseller_kpi audit.db/audit; do
  # partitioned tables keep their files in one folder per partition
  [ -n "$(find "$PROJECT_DIR/datasets/$table" -name '*.parquet' 2>/dev/null | head -1)" ] ||
    fail "no parquet file in datasets/$table" "$LOGS_DIR/load.log"
done
CLASS_ERRORS="NoSuchMethodError|NoClassDefFoundError|ClassNotFoundException|LinkageError"
if grep -qE "$CLASS_ERRORS" "$LOGS_DIR"/*.log; then
  fail "class loading error" "$(grep -lE "$CLASS_ERRORS" "$LOGS_DIR"/*.log | head -1)"
fi

echo "SMOKE OK"
