package ai.starlake.job.ingest

import ai.starlake.TestHelper
import ai.starlake.config.DatasetArea
import ai.starlake.extract.JdbcDbUtils
import ai.starlake.schema.model.{AllSinks, DomainInfo, Format, Metadata, SchemaInfo, TableAttribute}
import ai.starlake.workflow.IngestionWorkflow
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.fs.Path

import java.util.regex.Pattern
import scala.util.Try

/** The `archive` key of the metadata of a domain or a table decides whether the files of a
  * successful load are archived or deleted, the table over its domain, and both over the `archive`
  * setting of the application (#1986).
  */
class ArchivePerTableSpec extends TestHelper {

  private def configuration(archive: Boolean): Config =
    ConfigFactory
      .parseString(
        s"""
           |archive = $archive
           |connectionRef: "test-duckdb"
           |audit.sink.connectionRef = "test-duckdb"
           |connections.test-duckdb {
           |    type = "jdbc"
           |    options {
           |      "url": "jdbc:duckdb:$starlakeTestRoot/archive_1986_$archive.db"
           |      "driver": "org.duckdb.DuckDBDriver"
           |    }
           |}
           |""".stripMargin
      )
      .withFallback(super.testConfiguration)

  private class Project(archive: Boolean) extends WithSettings(configuration(archive)) {

    /** @param tables
      *   the tables of the domain, with the `archive` of their metadata
      */
    def writeDomain(
      domain: String,
      domainArchive: Option[Boolean],
      tables: (String, Option[Boolean])*
    ): Unit =
      DomainInfo(
        name = domain,
        metadata = Some(
          Metadata(directory = Some(s"$starlakeTestRoot/incoming"), archive = domainArchive)
        ),
        tables = tables.toList.map { case (table, tableArchive) =>
          SchemaInfo(
            table,
            Pattern.compile(s"$table-.*[.]csv"),
            attributes = List(TableAttribute("id", "long")),
            metadata = Some(
              Metadata(
                format = Some(Format.DSV),
                withHeader = Some(true),
                separator = Some(";"),
                loader = Some("native"),
                sink = Some(AllSinks(connectionRef = Some("test-duckdb"))),
                archive = tableArchive
              )
            )
          )
        }
      ).writeDomainAsYaml(DatasetArea.load)(storageHandler)

    def load(domain: String, table: String): Try[_] = {
      storageHandler.write("id\n1\n2\n", new Path(DatasetArea.stage(domain), s"$table-1.csv"))
      new IngestionWorkflow(storageHandler, settings.schemaHandler(reload = true)).load(
        LoadConfig(
          domains = List(domain),
          tables = List(table),
          accessToken = None,
          test = false,
          scheduledDate = None
        )
      )
    }

    /** Loads the file of the table, which must succeed with its 2 rows */
    def loaded(domain: String, table: String): Unit = {
      load(domain, table).isSuccess shouldBe true
      JdbcDbUtils.withJDBCConnection(
        settings.schemaHandler().dataBranch(),
        settings.appConfig.connections("test-duckdb").options
      ) { connection =>
        JdbcDbUtils
          .executeQueryAsMap(s"SELECT COUNT(*) AS total FROM $domain.$table", connection)
          .toList
          .map(_("total").toLong) shouldBe List(2L)
      }
    }

    /** Where the file of the table is after its load: archived, kept, moved elsewhere or deleted */
    def fileOf(domain: String, table: String): String = {
      def holds(area: Path, recursive: Boolean = false): Boolean =
        storageHandler.exists(area) &&
        storageHandler.list(area, recursive = recursive).exists(_.path.getName == s"$table-1.csv")
      if (holds(DatasetArea.archive(domain))) "archived"
      else if (holds(DatasetArea.stage(domain)) || holds(DatasetArea.ingesting(domain))) "kept"
      else if (holds(new Path(settings.appConfig.datasets), recursive = true)) "elsewhere"
      else "deleted"
    }
  }

  new Project(archive = true) {
    "The archive key of a table" should "override the archive setting of the application" in {
      writeDomain("archive_app", None, "archived" -> None, "deleted" -> Some(false))
      loaded("archive_app", "archived")
      loaded("archive_app", "deleted")
      fileOf("archive_app", "archived") shouldBe "archived"
      fileOf("archive_app", "deleted") shouldBe "deleted"
    }

    "The archive key of a domain" should "apply to its tables, unless a table sets its own" in {
      writeDomain("archive_domain", Some(false), "inherits" -> None, "overrides" -> Some(true))
      loaded("archive_domain", "inherits")
      loaded("archive_domain", "overrides")
      fileOf("archive_domain", "inherits") shouldBe "deleted"
      fileOf("archive_domain", "overrides") shouldBe "archived"
    }

    "The archive key" should "be read back from the YAML of the domain and of its tables" in {
      writeDomain("archive_yaml", Some(false), "kept_yaml" -> Some(true), "plain_yaml" -> None)
      val domain = settings.schemaHandler(reload = true).getDomain("archive_yaml").get
      domain.metadata.flatMap(_.archive) shouldBe Some(false)
      val tables = domain.tables.map(t => t.name -> t.mergedMetadata(domain.metadata).archive).toMap
      tables shouldBe Map("kept_yaml" -> Some(true), "plain_yaml" -> Some(false))
    }
  }

  new Project(archive = false) {
    "The archive key of a table, when the application deletes the files," should
    "archive the files of that table only" in {
      writeDomain("archive_off", None, "archived_off" -> Some(true), "deleted_off" -> None)
      loaded("archive_off", "archived_off")
      loaded("archive_off", "deleted_off")
      fileOf("archive_off", "archived_off") shouldBe "archived"
      fileOf("archive_off", "deleted_off") shouldBe "deleted"
    }
  }
}
