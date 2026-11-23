package ai.starlake.impact

import better.files.File

/** A small DuckDB-flavored project: raw.orders (load) -> stg.orders -> kpi.revenue -> kpi.dashboard
  */
object ImpactFixtures {

  val baseFiles: Map[String, String] = Map(
    "metadata/application.sl.yml" ->
    """version: 1
        |application:
        |  connectionRef: duckdb
        |  connections:
        |    duckdb:
        |      type: jdbc
        |      options:
        |        url: "jdbc:duckdb:{{SL_ROOT}}/datasets/duckdb.db"
        |        driver: "org.duckdb.DuckDBDriver"
        |""".stripMargin,
    "metadata/load/raw/_config.sl.yml" ->
    """version: 1
        |load:
        |  name: raw
        |""".stripMargin,
    "metadata/load/raw/orders.sl.yml" ->
    """version: 1
        |table:
        |  name: orders
        |  pattern: "orders.*.csv"
        |  attributes:
        |  - name: id
        |    type: long
        |  - name: amount
        |    type: decimal
        |  - name: status
        |    type: string
        |  metadata:
        |    format: DSV
        |    writeStrategy:
        |      type: APPEND
        |""".stripMargin,
    "metadata/transform/stg/_config.sl.yml" -> "version: 1\ntransform:\n  name: stg\n",
    "metadata/transform/stg/orders.sl.yml" ->
    "version: 1\ntask:\n  name: orders\n  domain: stg\n  table: orders\n  writeStrategy:\n    type: OVERWRITE\n",
    "metadata/transform/stg/orders.sql" ->
    "SELECT o.id, o.amount FROM raw.orders o WHERE o.status = 'ok'\n",
    "metadata/transform/kpi/_config.sl.yml" -> "version: 1\ntransform:\n  name: kpi\n",
    "metadata/transform/kpi/revenue.sl.yml" ->
    "version: 1\ntask:\n  name: revenue\n  domain: kpi\n  table: revenue\n  writeStrategy:\n    type: OVERWRITE\n",
    "metadata/transform/kpi/revenue.sql" ->
    "SELECT s.id, s.amount * 2 AS total FROM stg.orders s\n",
    "metadata/transform/kpi/dashboard.sl.yml" ->
    "version: 1\ntask:\n  name: dashboard\n  domain: kpi\n  table: dashboard\n  writeStrategy:\n    type: OVERWRITE\n",
    "metadata/transform/kpi/dashboard.sql" ->
    "SELECT Sum(r.total) AS rev FROM kpi.revenue r\n",
    "metadata/dags/default.sl.yml" ->
    "version: 1\ndag:\n  template: \"airflow_scheduled_table_bash.py.j2\"\n  filename: \"{{domain}}.py\"\n"
  )

  def writeProject(
    dir: File,
    overrides: Map[String, String] = Map.empty,
    removed: Set[String] = Set.empty
  ): File = {
    (baseFiles ++ overrides).filterNot { case (p, _) => removed.contains(p) }.foreach {
      case (path, content) =>
        val f = dir / path
        f.parent.createDirectories()
        f.writeText(content)
    }
    dir
  }
}
