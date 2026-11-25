#!/usr/bin/env bash
# Regenerates the dbt artifacts that the `starlake dbt import` specs read, from the fixture
# project src/test/resources/dbt/shop. CI does not run it: its output is committed.
#
#   src/test/resources/dbt/shop/target/manifest.json  `dbt parse`
#   src/test/resources/dbt/shop-expected/compiled/     `dbt compile` of each model, on an empty database
#   src/test/resources/dbt/shop-expected/run1/         every table and view after `dbt build`
#   src/test/resources/dbt/shop-expected/run2/         the same after a second `dbt build`, with the
#                                                      seeds of src/test/resources/dbt/shop-run2
#
# Needs python3 and network access (pip, dbt hub). DBT_VENV reuses a venv that already has the
# pinned versions.
set -euo pipefail

DBT_CORE_VERSION="1.12.7"
DBT_DUCKDB_VERSION="1.11.1"

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
FIXTURE="$ROOT/src/test/resources/dbt/shop"
RUN2_SEEDS="$ROOT/src/test/resources/dbt/shop-run2"
EXPECTED="$ROOT/src/test/resources/dbt/shop-expected"
VENV="${DBT_VENV:-$ROOT/target/dbt-venv}"

if [ ! -x "$VENV/bin/dbt" ]; then
  python3 -m venv "$VENV"
  "$VENV/bin/pip" install --quiet "dbt-core==$DBT_CORE_VERSION" "dbt-duckdb==$DBT_DUCKDB_VERSION"
fi
INSTALLED="$("$VENV/bin/python" -I -c 'import importlib.metadata as m; print(m.version("dbt-core"), m.version("dbt-duckdb"))')"
if [ "$INSTALLED" != "$DBT_CORE_VERSION $DBT_DUCKDB_VERSION" ]; then
  echo "$VENV has dbt-core and dbt-duckdb $INSTALLED, not $DBT_CORE_VERSION $DBT_DUCKDB_VERSION" >&2
  exit 1
fi

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT
cp -R "$FIXTURE" "$WORK/shop"
rm -rf "$WORK/shop/target"
cd "$WORK/shop"
export DBT_SEND_ANONYMOUS_USAGE_STATS=false
export DBT_PROFILES_DIR="$WORK/shop"
unset SHOP_CHANNEL
DBT="$VENV/bin/dbt"

# Writes every table and view of the fixture schemas as <schema>.<name>.csv, sorted on all columns
export_tables() {
  mkdir -p "$1"
  "$VENV/bin/python" -I - "$WORK/shop/shop.duckdb" "$1" <<'PY'
import sys
import duckdb

db, out = sys.argv[1], sys.argv[2]
con = duckdb.connect(db, read_only=True)
rows = con.sql(
    "SELECT table_schema, table_name FROM information_schema.tables "
    "WHERE table_schema IN ('raw', 'staging', 'marts') "
    # sl_import_side_effect is dbt's side effect of writes_at_import, not a model
    "AND table_name <> 'sl_import_side_effect' ORDER BY 1, 2"
).fetchall()
for schema, name in rows:
    con.sql(
        f"COPY (SELECT * FROM \"{schema}\".\"{name}\" ORDER BY ALL) "
        f"TO '{out}/{schema}.{name}.csv' (HEADER, DELIMITER ',')"
    )
PY
}

"$DBT" deps
"$DBT" parse
mkdir -p "$FIXTURE/target"
cp target/manifest.json "$FIXTURE/target/manifest.json"

rm -rf "$EXPECTED"
mkdir -p "$EXPECTED/compiled"
rm -f shop.duckdb
"$DBT" compile --exclude tag:introspection
find target/compiled/shop/models -name '*.sql' -not -path '*.yml/*' -exec cp {} "$EXPECTED/compiled/" \;

rm -f shop.duckdb
"$DBT" build
export_tables "$EXPECTED/run1"
"$DBT" compile --select tag:introspection
find target/compiled/shop/models -name '*.sql' -not -path '*.yml/*' -exec cp {} "$EXPECTED/compiled/" \;

cp "$RUN2_SEEDS"/*.csv seeds/
"$DBT" build
export_tables "$EXPECTED/run2"

echo "Wrote $FIXTURE/target/manifest.json and $EXPECTED"
