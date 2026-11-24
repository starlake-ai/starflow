package ai.starlake.dbt

import ai.starlake.dbt.jinja.{DbtJinja, DbtJinjaContext}
import ai.starlake.dbt.manifest._

import java.nio.file.{Path, Paths}
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
