#!/usr/bin/env bash
# Acceptance of `starlake dbt import` on the real jaffle-shop, local only (not CI):
#   1. dbt deps, parse and build jaffle-shop on DuckDB (load_source_data: true);
#   2. starlake bootstrap, dbt import, stage, load and transform, on another DuckDB file;
#   3. every table of dbt's database equals Starlake's, except metricflow_time_spine, which
#      the import skips: dbt.date_spine queries the warehouse at compile time.
#
#   JAFFLE_SHOP  a checkout of https://github.com/dbt-labs/jaffle-shop (not modified)
#   DBT_VENV     venv with dbt-core 1.12.7 and dbt-duckdb 1.11.1 (default: target/dbt-venv)
#   SL_HOME      an installed Starflow, for its bin/spark/jars and bin/deps (default: ~/starlake)
#   WORK         working directory (default: a new temporary one, kept for inspection)
#
# Needs network access for `dbt deps`. Builds the assembly when target/scala-2.13 has none.
# `dbt deps` and `dbt build` run the project's macros and hooks with your privileges: only
# run this on a jaffle-shop checkout you trust.
# A WORK directory that already exists is only reused (and its jaffle and sl folders deleted)
# when it holds the marker file .dbt-acceptance, written when the script creates it.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
JAFFLE_SHOP="${JAFFLE_SHOP:?set JAFFLE_SHOP to a jaffle-shop checkout}"
VENV="${DBT_VENV:-$ROOT/target/dbt-venv}"
SL_HOME="${SL_HOME:-$HOME/starlake}"
WORK="${WORK:-$(mktemp -d)}"
if [ -e "$WORK" ] && [ ! -f "$WORK/.dbt-acceptance" ] && [ -n "$(ls -A "$WORK")" ]; then
  echo "WORK=$WORK exists and has no .dbt-acceptance marker: refusing to delete in it." >&2
  exit 1
fi
mkdir -p "$WORK"
WORK="$(cd "$WORK" && pwd)"
touch "$WORK/.dbt-acceptance"
VARS='{load_source_data: true}'

if [ ! -x "$VENV/bin/dbt" ]; then
  python3 -m venv "$VENV"
  "$VENV/bin/pip" install --quiet "dbt-core==1.12.7" "dbt-duckdb==1.11.1"
fi
for dir in "$SL_HOME/bin/deps" "$SL_HOME/bin/spark/jars"; do
  if ! compgen -G "$dir/*.jar" > /dev/null; then
    echo "SL_HOME=$SL_HOME is not an installed Starflow (no jars in $dir). Unset SL_HOME or point it at an installation, not a dev distrib folder." >&2
    exit 1
  fi
done
newest_jar() { ls -t "$ROOT"/target/scala-2.13/starlake-core_2.13-*-assembly.jar 2>/dev/null | sed -n 1p || true; }
JAR="$(newest_jar)"
if [ -z "$JAR" ]; then
  (cd "$ROOT" && sbt assembly)
  JAR="$(newest_jar)"
fi
[ -n "$JAR" ] || { echo "no assembly jar under $ROOT/target/scala-2.13" >&2; exit 1; }

starlake() {
  (cd "$WORK" && SL_ROOT="$WORK/sl" java \
    --add-opens=java.base/java.lang=ALL-UNNAMED \
    --add-opens=java.base/java.lang.invoke=ALL-UNNAMED \
    --add-opens=java.base/java.lang.reflect=ALL-UNNAMED \
    --add-opens=java.base/java.io=ALL-UNNAMED \
    --add-opens=java.base/java.net=ALL-UNNAMED \
    --add-opens=java.base/java.nio=ALL-UNNAMED \
    --add-opens=java.base/java.util=ALL-UNNAMED \
    --add-opens=java.base/java.util.concurrent=ALL-UNNAMED \
    --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED \
    --add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
    --add-opens=java.base/sun.nio.cs=ALL-UNNAMED \
    --add-opens=java.base/sun.util.calendar=ALL-UNNAMED \
    --sun-misc-unsafe-memory-access=allow \
    -cp "$JAR:$SL_HOME/bin/deps/*:$SL_HOME/bin/spark/jars/*" ai.starlake.job.Main "$@")
}

echo "Working in $WORK"
rm -rf "$WORK/jaffle" "$WORK/sl"
cp -R "$JAFFLE_SHOP" "$WORK/jaffle"
rm -rf "$WORK/jaffle/target" "$WORK/jaffle/dbt_packages" "$WORK/jaffle"/*.duckdb
cat > "$WORK/jaffle/profiles.yml" <<'YML'
default:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: jaffle_shop.duckdb
      schema: main
      threads: 4
YML

# jaffle-shop declares require-dbt-version ">=2.0.0": dbt-core 1.12 runs it without the check
export DBT_SEND_ANONYMOUS_USAGE_STATS=false DBT_VERSION_CHECK=false DBT_PROFILES_DIR="$WORK/jaffle"
(
  cd "$WORK/jaffle"
  "$VENV/bin/dbt" deps
  "$VENV/bin/dbt" parse --vars "$VARS"
  cp target/manifest.json "$WORK/manifest.json"
  # jaffle's models read the seeds through sources: dbt build does not order the seeds first
  "$VENV/bin/dbt" build --vars "$VARS" --select "resource_type:seed"
  "$VENV/bin/dbt" build --vars "$VARS"
  # the import reads the manifest of `dbt parse`, as a user would
  cp "$WORK/manifest.json" target/manifest.json
)

starlake bootstrap --template empty-project --no-exit
starlake dbt import --manifest "$WORK/jaffle/target/manifest.json" --connection duckdb --vars "$VARS"
REPORT="$WORK/sl/dbt-import-report.md"
grep -q 'model `model.jaffle_shop.metricflow_time_spine`: needs the warehouse at import' "$REPORT"
starlake stage
starlake load
TASKS="$(sed -n 's/^- next: starlake transform --name //p' "$REPORT")"
if [ -z "$TASKS" ]; then
  echo "no '- next: starlake transform --name' line in $REPORT: nothing to transform" >&2
  exit 1
fi
starlake transform --name "$TASKS"

"$VENV/bin/python" -I - "$WORK/jaffle/jaffle_shop.duckdb" "$WORK/sl/datasets/duckdb.db" <<'PY'
import sys
import duckdb

def q(name):
    return '"' + name.replace('"', '""') + '"'

def lit(value):
    return "'" + value.replace("'", "''") + "'"

con = duckdb.connect()
con.sql(f"ATTACH {lit(sys.argv[1])} AS dbt (READ_ONLY)")
con.sql(f"ATTACH {lit(sys.argv[2])} AS sl (READ_ONLY)")
skipped = {("main", "metricflow_time_spine")}
SCHEMAS = "('main', 'raw')"

def tables_of(catalog):
    return con.sql(
        f"SELECT table_schema, table_name FROM information_schema.tables "
        f"WHERE table_catalog = {lit(catalog)} AND table_schema IN {SCHEMAS} ORDER BY 1, 2"
    ).fetchall()

tables = [t for t in tables_of("dbt") if t not in skipped]
failures = []
if not tables:
    failures.append("dbt's main and raw schemas hold no table to compare")
dbt_schemas = {s for s, _ in tables}
extra = [t for t in tables_of("sl") if t[0] in dbt_schemas and t not in skipped and t not in tables]
for schema, name in extra:
    failures.append(f"{schema}.{name}: in Starlake, not in dbt")

def columns_of(catalog, schema, name):
    return con.sql(
        f"SELECT column_name, data_type FROM information_schema.columns WHERE table_catalog = {lit(catalog)} "
        f"AND table_schema = {lit(schema)} AND table_name = {lit(name)} ORDER BY ordinal_position"
    ).fetchall()

for schema, name in tables:
    dbt_cols = columns_of("dbt", schema, name)
    sl_cols = columns_of("sl", schema, name)
    if not sl_cols:
        failures.append(f"{schema}.{name}: missing in Starlake")
        continue
    sl_types = dict(sl_cols)
    if {c for c, _ in dbt_cols} != set(sl_types):
        msg = f"{schema}.{name}: column names differ: dbt {sorted(c for c, _ in dbt_cols)} vs Starlake {sorted(sl_types)}"
        print(msg)
        failures.append(msg)
        continue
    def select(catalog):
        cols = []
        for c, _ in dbt_cols:
            tz = catalog == "sl" and sl_types.get(c, "").startswith("TIMESTAMP WITH TIME ZONE")
            cols.append(f"CAST(CAST({q(c)} AS TIMESTAMP) AS VARCHAR)" if tz else f"CAST({q(c)} AS VARCHAR)")
        return f"SELECT {', '.join(cols)} FROM {catalog}.{q(schema)}.{q(name)}"
    diff = con.sql(
        f"SELECT count(*) FROM (({select('dbt')} EXCEPT ALL {select('sl')}) "
        f"UNION ALL ({select('sl')} EXCEPT ALL {select('dbt')}))"
    ).fetchall()[0][0]
    print(f"{schema}.{name}: {'same' if diff == 0 else str(diff) + ' different rows'}")
    if diff:
        failures.append(f"{schema}.{name}: {diff} different rows")

total, failed = con.sql(
    "SELECT count(*), count(*) FILTER (WHERE NOT success) FROM sl.audit.expectations"
).fetchall()[0]
print(f"audit.expectations: {total} rows, {failed} failed")
if total == 0:
    failures.append("sl.audit.expectations has no row: no expectation ran")
if failed:
    failures.append(f"{failed} failed expectations")
if failures:
    print("\n".join(failures))
    sys.exit(1)
print(f"jaffle-shop: {len(tables)} tables equal, no failed expectation")
PY
