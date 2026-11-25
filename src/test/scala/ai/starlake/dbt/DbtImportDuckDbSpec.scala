package ai.starlake.dbt

import ai.starlake.TestHelper
import ai.starlake.dbt.report.EntryStatus
import ai.starlake.config.DatasetArea
import ai.starlake.extract.JdbcDbUtils
import ai.starlake.job.ingest.{LoadConfig, StageConfig}
import ai.starlake.job.transform.TransformConfig
import ai.starlake.schema.handlers.ValidateConfig
import ai.starlake.schema.model.Severity
import ai.starlake.workflow.IngestionWorkflow
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.fs.Path

import java.nio.file.{Files, Path => JPath, Paths}
import scala.jdk.CollectionConverters._
import scala.util.Try

/** The fixture dbt project imported, loaded and transformed on DuckDB gives the tables of `dbt
  * build`, on a first run and on a second one with new rows, and its tests pass as expectations
  */
class DbtImportDuckDbSpec extends TestHelper {

  private def configuration(db: String): Config =
    ConfigFactory
      .parseString(
        s"""
           |connectionRef: "test-duckdb"
           |audit.sink.connectionRef = "test-duckdb"
           |expectations.failOnError = true
           |connections.test-duckdb {
           |  type = "jdbc"
           |  loader = "native"
           |  options {
           |    "url": "jdbc:duckdb:$starlakeTestRoot/$db.db"
           |    "driver": "org.duckdb.DuckDBDriver"
           |  }
           |}
           |""".stripMargin
      )
      .withFallback(super.testConfiguration)

  /** Not in the Starlake project: the model needs the warehouse at import, and the one that reads
    * it
    */
  private val notImported = Set("marts.needs_warehouse", "marts.warehouse_consumer")

  private abstract class Project(db: String) extends WithSettings(configuration(db)) {
    def importShop(): DbtImportResult =
      DbtImporter
        .run(
          DbtImportConfig(
            action = "import",
            manifest = DbtFixtures.shopManifestPath.toString,
            connection = Some("test-duckdb"),
            force = true
          )
        )
        .get

    private def workflow() =
      new IngestionWorkflow(storageHandler, settings.schemaHandler(reload = true))

    def loadSeeds(): Try[_] =
      workflow().stage(StageConfig(domains = Seq("raw"))).flatMap { _ =>
        workflow().load(
          LoadConfig(domains = List("raw"), accessToken = None, test = false, scheduledDate = None)
        )
      }

    def transform(result: DbtImportResult): Try[String] = {
      workflow().autoJob(TransformConfig(name = result.report.runnableTasks.mkString(",")))
    }

    def incoming(file: JPath): Unit =
      storageHandler.copyFromLocal(
        new Path(file.toUri),
        new Path(DatasetArea.incoming("raw"), file.getFileName.toString)
      )

    def rows(sql: String): List[Map[String, String]] =
      JdbcDbUtils.withJDBCConnection(
        settings.schemaHandler().dataBranch(),
        settings.appConfig.connections("test-duckdb").options
      ) { connection =>
        JdbcDbUtils.executeQueryAsMap(sql, connection).toList.map(_.toMap)
      }

    def execute(sql: String): Unit =
      JdbcDbUtils.withJDBCConnection(
        settings.schemaHandler().dataBranch(),
        settings.appConfig.connections("test-duckdb").options
      ) { connection =>
        val statement = connection.createStatement()
        try statement.execute(sql)
        finally statement.close()
      }

    /** The rows of the table that are not in dbt's CSV, and the other way around; the rows that
      * satisfy `ignore` are left out of the table
      */
    def differences(
      csv: JPath,
      ignore: Option[String] = None,
      ignoreInCsv: Option[String] = None
    ): Int = {
      val table = csv.getFileName.toString.stripSuffix(".csv")
      val Array(schema, name) = table.split('.')
      val header = Files.readAllLines(csv).get(0).split(",").toList
      val types = rows(
        s"SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = '$schema' AND table_name = '$name'"
      ).map(r => r("column_name") -> r("data_type")).toMap
      val columns = header.map { c =>
        if (types.getOrElse(c, "").startsWith("TIMESTAMP WITH TIME ZONE"))
          s"CAST(CAST($c AS TIMESTAMP) AS VARCHAR) AS $c"
        else s"CAST($c AS VARCHAR) AS $c"
      }
      val actual =
        s"SELECT ${columns.mkString(", ")} FROM $table WHERE NOT COALESCE(${ignore.getOrElse("false")}, false)"
      val expected =
        s"SELECT * FROM read_csv('$csv', all_varchar = true, header = true) WHERE NOT COALESCE(${ignoreInCsv
            .getOrElse("false")}, false)"
      rows(
        s"SELECT CAST(count(*) AS VARCHAR) AS c FROM (($actual EXCEPT ALL $expected) UNION ALL ($expected EXCEPT ALL $actual))"
      ).head("c").toInt
    }

    /** @param sentinels
      *   per table, the condition that selects the sentinel row: it is left out of the comparison
      *   and must be there exactly once
      */
    def compareWith(
      run: String,
      sentinels: Map[String, String] = Map.empty,
      tampered: Map[String, (String, String)] = Map.empty
    ): Unit = {
      val csvs = Files.list(DbtFixtures.expectedDir.resolve(run)).iterator().asScala.toList
      val compared =
        csvs.filterNot(f => notImported.contains(f.getFileName.toString.stripSuffix(".csv")))
      compared should not be empty
      compared
        .map { f =>
          val table = f.getFileName.toString.stripSuffix(".csv")
          val ignore =
            (sentinels
              .get(table) ++ tampered.get(table).map(_._1)).map(c => s"($c)").mkString(" OR ")
          f.getFileName.toString -> differences(
            f,
            Some(ignore).filter(_.nonEmpty),
            tampered.get(table).map(_._2)
          )
        }
        .filter(_._2 != 0) shouldBe Nil
      sentinels.foreach { case (table, condition) =>
        withClue(s"sentinel of $table: ") {
          rows(s"SELECT CAST(count(*) AS VARCHAR) AS c FROM $table WHERE $condition")
            .head("c")
            .toInt shouldBe 1
        }
      }
    }

    /** The tables compared with dbt's CSVs */
    def comparedTables(run: String): Set[String] =
      Files
        .list(DbtFixtures.expectedDir.resolve(run))
        .iterator()
        .asScala
        .map(_.getFileName.toString.stripSuffix(".csv"))
        .filterNot(notImported.contains)
        .toSet

    /** An imported file, by its path under the project */
    def importedFile(result: DbtImportResult, suffix: String): String =
      Files.readString(
        Paths.get(
          result.written
            .map(_.toUri.getPath)
            .find(_.endsWith(suffix))
            .getOrElse(fail(s"$suffix was not written"))
        )
      )

    /** The audit log writes the quotes of an expectation as double quotes */
    def failedExpectations(): List[String] =
      rows("SELECT CAST(params AS VARCHAR) AS expect FROM audit.expectations WHERE NOT success")
        .map(_("expect").replace('"', '\''))
  }

  private val shopProject = new Project("dbt_import") {
    private var imported: DbtImportResult = _

    "starlake dbt import" should "give dbt's tables on a first run, its tests passing as expectations" in {
      imported = importShop()
      imported.exitCode shouldBe 0
      val report = imported.report
      // the exclusions: the model that needs the warehouse is skipped, its reader is converted and
      // blocked, the snapshot is reported only
      report.skipped.filter(_.kind == "model").map(e => e.dbtId -> e.status) match {
        case List((id, EntryStatus.Skipped(reason))) =>
          id shouldBe "model.shop.needs_warehouse"
          reason should include("needs the warehouse at import")
        case other => fail(s"unexpected skipped models: $other")
      }
      report.downstreamOfSkipped shouldBe List(
        "marts.warehouse_consumer" -> List("model.shop.needs_warehouse")
      )
      report.reportedOnly.map(_.dbtId) should contain("snapshot.shop.customers_snapshot")
      report.convertedTasks should not contain "marts.needs_warehouse"
      // what was converted is what is compared: the models but the reader of the skipped one, and
      // the seeds; audit_log is written by the hooks of orders, int_order_totals is ephemeral (a
      // view, no CSV of dbt)
      val seeds = report.converted.filter(_.kind == "seed").flatMap(_.target)
      (report.convertedTasks.filterNot(_ == "marts.warehouse_consumer") ++ seeds).toSet shouldBe
      comparedTables("run1") - "marts.audit_log" + "staging.int_order_totals"
      comparedTables("run2") shouldBe comparedTables("run1")
      // Starlake's own validation of the imported project; the connections of the test configuration are not the import's
      val (messages, _, _) =
        settings.schemaHandler(reload = true).checkValidity(ValidateConfig(reload = true)).get
      messages.filter(m => m.severity == Severity.Error && m.target != "Connection") shouldBe Nil
      loadSeeds().isSuccess shouldBe true
      transform(imported).isSuccess shouldBe true
      compareWith("run1")
      failedExpectations() shouldBe Nil
      rows("SELECT CAST(count(*) AS VARCHAR) AS c FROM audit.expectations")
        .head("c")
        .toInt shouldBe 11
    }

    it should "give dbt's tables on a second run with new rows, through the incremental tasks" in {
      // the imported incremental tasks: the incremental branch and the write strategy of each
      def task(name: String) = importedFile(imported, s"/transform/marts/$name.sl.yml")
      def sql(name: String) = importedFile(imported, s"/transform/marts/$name.sql")
      List("customer_last_event", "daily_events", "events_append").foreach { name =>
        sql(name) should include("{% if sl_incremental %}")
      }
      task("customer_last_event") should (include("UPSERT_BY_KEY") and include("customer_id"))
      task("daily_events") should (include("DELETE_THEN_INSERT") and include(
        "event_date"
      ) and include(
        "event_type"
      ))
      task("events_append") should include("APPEND")
      // a sentinel row that no source has and that is older than the data: an incremental run
      // keeps it, a full reload drops it
      execute(
        "INSERT INTO marts.customer_last_event VALUES (999, TIMESTAMP '2000-01-01 00:00:00', 7)"
      )
      execute("INSERT INTO marts.daily_events VALUES (DATE '2000-01-01', 'sentinel', 7)")
      execute(
        "INSERT INTO marts.events_append VALUES (999, 999, 'sentinel', TIMESTAMP '2000-01-01 00:00:00')"
      )
      // rows the incremental filters never recompute (customer 2 has no newer event; 2024-03-01 is
      // before the cutoff max(event_date)), set to impossible values: a run 2 with sl_incremental
      // false would recompute them from the sources and overwrite the 99
      execute("UPDATE marts.customer_last_event SET event_count = 99 WHERE customer_id = 2")
      execute(
        "UPDATE marts.daily_events SET events = 99 WHERE event_date = DATE '2024-03-01' AND event_type = 'login'"
      )
      incoming(DbtFixtures.run2SeedsDir.resolve("raw_events.csv"))
      loadSeeds().isSuccess shouldBe true
      transform(imported).isSuccess shouldBe true
      val tampered = Map(
        "marts.customer_last_event" -> ("customer_id = 2", "customer_id = '2'"),
        "marts.daily_events" -> (
          "event_date = DATE '2024-03-01' AND event_type = 'login'",
          "event_date = '2024-03-01' AND event_type = 'login'"
        )
      )
      rows(
        "SELECT CAST(event_count AS VARCHAR) AS v FROM marts.customer_last_event WHERE customer_id = 2"
      )
        .map(_("v")) shouldBe List("99")
      rows(
        "SELECT CAST(events AS VARCHAR) AS v FROM marts.daily_events WHERE event_date = DATE '2024-03-01' AND event_type = 'login'"
      ).map(_("v")) shouldBe List("99")
      compareWith(
        "run2",
        Map(
          "marts.customer_last_event" -> "customer_id = 999",
          "marts.daily_events"        -> "event_type = 'sentinel'",
          "marts.events_append"       -> "event_id = 999"
        ),
        tampered
      )
      failedExpectations() shouldBe Nil
    }
  }

  private val corrupted = new Project("dbt_import_corrupted") {
    "A dbt unique test" should "fail the load of a seed that has a duplicate key" in {
      importShop().exitCode shouldBe 0
      storageHandler.write(
        "id,customer_id,order_date,status,amount_cents\n1,1,2024-01-05,completed,1250\n1,2,2024-02-11,completed,4300\n",
        new Path(DatasetArea.incoming("raw"), "raw_orders.csv")
      )
      loadSeeds().isFailure shouldBe true
      failedExpectations() shouldBe List("dbt_unique('id')")
      rows(
        "SELECT CAST(count(*) AS VARCHAR) AS c FROM audit.expectations WHERE NOT success AND domain = 'raw' AND \"schema\" = 'raw_orders'"
      ).head("c").toInt shouldBe 1
    }
  }

  private val keptDomain = new Project("dbt_import_kept_domain") {
    "An existing raw/_config.sl.yml" should "be kept, the seeds loading with the metadata of their tables" in {
      // the user's domain only: the files of the imports above are removed
      storageHandler.delete(DatasetArea.metadata)
      storageHandler.delete(new Path(settings.appConfig.datasets))
      storageHandler.delete(ai.starlake.dbt.write.ProjectWriter.reportPath)
      val domainFile = new Path(DatasetArea.load, "raw/_config.sl.yml")
      val user =
        """version: 1
          |load:
          |  name: raw
          |  metadata:
          |    format: JSON
          |    withHeader: false
          |    separator: ";"
          |    directory: "{{SL_DATASETS}}/elsewhere/raw"
          |    writeStrategy:
          |      type: APPEND
          |""".stripMargin
      storageHandler.write(user, domainFile)
      val result = DbtImporter
        .run(
          DbtImportConfig(
            action = "import",
            manifest = DbtFixtures.shopManifestPath.toString,
            connection = Some("test-duckdb")
          )
        )
        .get
      result.exitCode shouldBe 0
      result.written should not contain domainFile
      result.report.general.exists(n =>
        n.contains("metadata/load/raw/_config.sl.yml") && n.contains("kept")
      ) shouldBe true
      loadSeeds().isSuccess shouldBe true
      differences(DbtFixtures.expectedDir.resolve("run1/raw.raw_orders.csv")) shouldBe 0
      storageHandler.read(domainFile) shouldBe user
    }
  }
}
