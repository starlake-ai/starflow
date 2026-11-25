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
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
JAFFLE_SHOP="${JAFFLE_SHOP:?set JAFFLE_SHOP to a jaffle-shop checkout}"
VENV="${DBT_VENV:-$ROOT/target/dbt-venv}"
SL_HOME="${SL_HOME:-$HOME/starlake}"
WORK="${WORK:-$(mktemp -d)}"
VARS='{load_source_data: true}'

if [ ! -x "$VENV/bin/dbt" ]; then
  python3 -m venv "$VENV"
  "$VENV/bin/pip" install --quiet "dbt-core==1.12.7" "dbt-duckdb==1.11.1"
fi
JAR="$(ls "$ROOT"/target/scala-2.13/starlake-core_2.13-*-assembly.jar 2>/dev/null | head -1 || true)"
if [ -z "$JAR" ]; then
  (cd "$ROOT" && sbt assembly)
  JAR="$(ls "$ROOT"/target/scala-2.13/starlake-core_2.13-*-assembly.jar | head -1)"
fi

starlake() {
  SL_ROOT="$WORK/sl" java \
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
    -cp "$JAR:$SL_HOME/bin/deps/*:$SL_HOME/bin/spark/jars/*" ai.starlake.job.Main "$@"
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
starlake transform --name "$TASKS"

"$VENV/bin/python" -I - "$WORK/jaffle/jaffle_shop.duckdb" "$WORK/sl/datasets/duckdb.db" <<'PY'
import sys
import duckdb

con = duckdb.connect()
con.sql(f"ATTACH '{sys.argv[1]}' AS dbt (READ_ONLY)")
con.sql(f"ATTACH '{sys.argv[2]}' AS sl (READ_ONLY)")
skipped = {("main", "metricflow_time_spine")}
tables = con.sql(
    "SELECT table_schema, table_name FROM information_schema.tables "
    "WHERE table_catalog = 'dbt' AND table_schema IN ('main', 'raw') ORDER BY 1, 2"
).fetchall()
failures = []
for schema, name in tables:
    if (schema, name) in skipped:
        continue
    columns = con.sql(
        f"SELECT column_name FROM information_schema.columns WHERE table_catalog = 'dbt' "
        f"AND table_schema = '{schema}' AND table_name = '{name}' ORDER BY ordinal_position"
    ).fetchall()
    sl_types = dict(con.sql(
        f"SELECT column_name, data_type FROM information_schema.columns WHERE table_catalog = 'sl' "
        f"AND table_schema = '{schema}' AND table_name = '{name}'"
    ).fetchall())
    if not sl_types:
        failures.append(f"{schema}.{name}: missing in Starlake")
        continue
    def select(catalog):
        cols = []
        for (c,) in columns:
            tz = catalog == "sl" and sl_types.get(c, "").startswith("TIMESTAMP WITH TIME ZONE")
            cols.append(f'CAST(CAST("{c}" AS TIMESTAMP) AS VARCHAR)' if tz else f'CAST("{c}" AS VARCHAR)')
        return f'SELECT {", ".join(cols)} FROM {catalog}."{schema}"."{name}"'
    diff = con.sql(
        f"SELECT count(*) FROM (({select('dbt')} EXCEPT ALL {select('sl')}) "
        f"UNION ALL ({select('sl')} EXCEPT ALL {select('dbt')}))"
    ).fetchall()[0][0]
    print(f"{schema}.{name}: {'same' if diff == 0 else str(diff) + ' different rows'}")
    if diff:
        failures.append(f"{schema}.{name}: {diff} different rows")
failed = con.sql("SELECT count(*) FROM sl.audit.expectations WHERE NOT success").fetchall()[0][0]
if failed:
    failures.append(f"{failed} failed expectations")
if failures:
    print("\n".join(failures))
    sys.exit(1)
print(f"jaffle-shop: {len(tables) - len(skipped)} tables equal, no failed expectation")
PY
