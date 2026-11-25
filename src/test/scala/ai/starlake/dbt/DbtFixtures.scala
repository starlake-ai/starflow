package ai.starlake.dbt

import ai.starlake.dbt.jinja.{DbtJinja, DbtJinjaContext}
import ai.starlake.dbt.manifest._
import ai.starlake.utils.JinjaUtils
import com.fasterxml.jackson.databind.ObjectMapper

import java.nio.file.{Files, Path, Paths}
import java.time.{ZoneOffset, ZonedDateTime}

/** The committed artifacts of the fixture dbt project (scripts/dbt-fixtures.sh) */
object DbtFixtures {
  private def resource(path: String): Path = Paths.get(getClass.getResource(path).toURI)

  lazy val shopManifestPath: Path = resource("/dbt/shop/target/manifest.json")
  lazy val shopManifest: DbtManifest = ManifestReader.read(shopManifestPath).get
  lazy val expectedDir: Path = resource("/dbt/shop-expected")
  lazy val run2SeedsDir: Path = resource("/dbt/shop-run2")

  val importTime: ZonedDateTime = ZonedDateTime.of(2026, 11, 24, 12, 0, 0, 0, ZoneOffset.UTC)

  def target(adapterType: String = "duckdb"): DbtTarget =
    DbtTarget("dev", adapterType, Some("main"), Some("shop"), None)

  def jinja(
    manifest: DbtManifest = shopManifest,
    cliVars: Map[String, Any] = Map.empty,
    env: Map[String, String] = Map.empty
  ): DbtJinja = {
    val project = DbtProjectFile.read(manifest.projectRoot).get
    val vars = DbtVars(
      project.map(_.vars).getOrElse(Map.empty),
      manifest.macros.map(_.packageName).toSet + manifest.metadata.projectName,
      cliVars
    )
    new DbtJinja(
      manifest,
      new DbtRelations(manifest),
      DbtJinjaContext(
        target(manifest.metadata.adapterType),
        vars,
        project.map(_.dispatch).getOrElse(Map.empty),
        env.get,
        importTime
      )
    )
  }

  /** A manifest without macros nor nodes, for the Jinja constructs themselves */
  def emptyManifest(adapterType: String = "duckdb"): DbtManifest =
    DbtManifest(
      DbtManifestMetadata(12, "1.12.7", adapterType, "shop"),
      Nil,
      Nil,
      Nil,
      Nil,
      Nil,
      Nil,
      Paths.get("/nonexistent/target/manifest.json")
    )

  /** A dbt project of its own, project `p` on DuckDB: dbt_project.yml and target/manifest.json made
    * of the given nodes and sources (manifest fields, unique_id first)
    *
    * @return
    *   the manifest
    */
  def miniProject(
    nodes: List[Map[String, Any]],
    sources: List[Map[String, Any]] = Nil,
    projectYml: String = "name: p\nprofile: p\n"
  ): Path = {
    val dir = Files.createTempDirectory("dbt-mini")
    Files.createDirectories(dir.resolve("target"))
    Files.writeString(dir.resolve("dbt_project.yml"), projectYml)
    def byId(objects: List[Map[String, Any]]) =
      objects.map(o => o("unique_id").toString -> o).toMap
    val manifest = Map(
      "metadata" -> Map(
        "dbt_schema_version" -> "https://schemas.getdbt.com/dbt/manifest/v12.json",
        "dbt_version"        -> "1.12.7",
        "adapter_type"       -> "duckdb",
        "project_name"       -> "p"
      ),
      "nodes"    -> byId(nodes),
      "sources"  -> byId(sources),
      "macros"   -> Map.empty,
      "docs"     -> Map.empty,
      "disabled" -> Map.empty
    )
    val file = dir.resolve("target/manifest.json")
    Files.writeString(file, new ObjectMapper().writeValueAsString(JinjaUtils.toJava(manifest)))
    file
  }

  /** A model of [[miniProject]], in database db */
  def miniModel(
    name: String,
    rawCode: String,
    dependsOn: List[String] = Nil,
    schema: String = "s",
    database: String = "db",
    materialized: String = "table",
    columns: Map[String, Any] = Map.empty
  ): Map[String, Any] = Map(
    "unique_id"          -> s"model.p.$name",
    "resource_type"      -> "model",
    "package_name"       -> "p",
    "name"               -> name,
    "database"           -> database,
    "schema"             -> schema,
    "alias"              -> name,
    "language"           -> "sql",
    "raw_code"           -> rawCode,
    "original_file_path" -> s"models/$name.sql",
    "config"             -> Map("materialized" -> materialized, "enabled" -> true),
    "columns"            -> columns,
    "depends_on"         -> Map("nodes" -> dependsOn, "macros" -> Nil)
  )

  private val Relation = """"([^"]+)"\."([^"]+)"\."([^"]+)"""".r

  /** The SQL on one line, its runs of whitespace a single space */
  def whitespace(sql: String): String = sql.split("\\s+").filter(_.nonEmpty).mkString(" ")

  /** dbt's compiled SQL as the import renders it: "db"."schema"."t" becomes schema.t, on one line
    */
  def dbtNormalize(sql: String): String =
    whitespace(Relation.replaceAllIn(sql, m => s"${m.group(2)}.${m.group(3)}"))

  def node(manifest: DbtManifest, name: String): DbtNode =
    manifest.nodes
      .find(n => n.name == name && n.resourceType != "test")
      .getOrElse(throw new NoSuchElementException(name))

  def model(name: String, config: Map[String, Any] = Map.empty, rawCode: String = ""): DbtNode =
    DbtNode(
      uniqueId = s"model.shop.$name",
      resourceType = "model",
      packageName = "shop",
      name = name,
      database = Some("shop"),
      schema = "marts",
      alias = name,
      version = None,
      latestVersion = None,
      language = "sql",
      rawCode = rawCode,
      originalFilePath = s"models/$name.sql",
      rootPath = None,
      description = "",
      docBlocks = Nil,
      columns = Nil,
      config = Map[String, Any]("materialized" -> "table", "enabled" -> true) ++ config,
      tags = Nil,
      dependsOnNodes = Nil,
      dependsOnMacros = Nil,
      testMetadata = None,
      attachedNode = None,
      columnName = None
    )
}
