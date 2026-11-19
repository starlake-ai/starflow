package ai.starlake.integration.load

import ai.starlake.config.DatasetArea
import ai.starlake.integration.IntegrationTestBase
import ai.starlake.job.Main
import ai.starlake.schema.model.{
  AllSinks,
  DomainInfo,
  Format,
  Metadata,
  Position,
  SchemaInfo,
  TableAttribute
}
import better.files.File
import com.google.cloud.bigquery._
import org.apache.hadoop.fs.Path

import java.util.regex.Pattern
import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters._
import scala.util.Try

/** The two-step native loads of #1838 on BigQuery:
  *   - their temporary tables do not inherit the required partition filter of the target, which
  *     would reject the queries of the second step, nor its partitioning, clustering and keys,
  *     whose columns they may lack: a renamed partition, clustering or key column, or a partitioned
  *     POSITION table with a primary key, loads;
  *   - their second step sets the labels of the target, and the keys of a target that it creates;
  *   - a DSV table with `presql` and `postsql` runs them, in its second step;
  *   - several local files are loaded one by one, not concatenated into one upload;
  *   - the second step writes no TRANSFORM audit row next to the LOAD row.
  */
class BigQueryNativeTempTablesIntegrationSpec extends IntegrationTestBase {
  private val dataset = "starlake_it_bq_native_temp_tables"
  private val table = "partitioned"
  private val renamedTable = "renamed"
  private val positionTable = "position_partitioned"
  private val hooksTable = "hooks"
  private val hooksLog = "hooks_log"
  private val filesTable = "local_files"
  private val tables = List(table, renamedTable, positionTable, hooksTable, hooksLog, filesTable)

  private val projectTemplate: File =
    File(getClass.getResource("/integration/bq-roundtrip-types").toURI)

  private lazy val project: File = {
    val dir = File.newTemporaryDirectory("starlake-bq-temp-tables-")
    (projectTemplate / "metadata").copyToDirectory(dir)
    // The load domain is written by the test: git keeps no empty directory
    (dir / "metadata" / "load").createDirectories()
    dir
  }

  override def theSampleFolder: File = project

  private lazy val bigquery: BigQuery = BigQueryOptions.newBuilder().build().getService

  private def run(sql: String): TableResult =
    bigquery.query(QueryJobConfiguration.newBuilder(sql).setUseLegacySql(false).build())

  /** @param primaryKey
    *   the primary key of the table, passed again: `--inPlace` replaces the one of its YAML with
    *   `--primaryKeys`
    */
  private def load(table: String, file: File, others: File*)(primaryKey: String*): Boolean =
    Try(
      new Main().run(
        Array(
          "load",
          "--domains",
          dataset,
          "--tables",
          table,
          "--files",
          (file +: others).map(_.uri.toString).mkString(","),
          "--inPlace"
        ) ++ (if (primaryKey.isEmpty) Nil else List("--primaryKeys", primaryKey.mkString(",")))
      )
    ).getOrElse(false)

  /** @return
    *   the first column of the rows of a query, as strings, none if it fails: the checks that read
    *   them fail, and the others are still reported
    */
  private def values(sql: String): List[String] =
    Try(
      run(sql)
        .iterateAll()
        .asScala
        .map(row => Option(row.get(0)).filterNot(_.isNull).map(_.getStringValue).getOrElse("NULL"))
        .toList
    ).getOrElse(Nil)

  private def path(file: File): String = new Path(file.uri.toString).toString

  /** @return
    *   the step and success of the audit rows of the jobs that loaded a file since this time: their
    *   LOAD row names the file, and their other rows have its job id. The audit log keeps the
    *   second of its timestamp only.
    */
  private def auditSteps(table: String, since: Long, file: File): List[String] = {
    val where =
      s"DOMAIN = '$dataset' AND SCHEMA = '$table' AND TIMESTAMP >= TIMESTAMP_SECONDS(${since / 1000})"
    values(
      s"""SELECT CONCAT(STEP, ':', CAST(SUCCESS AS STRING)) FROM `audit.audit`
         |WHERE $where AND JOBID IN (
         |  SELECT JOBID FROM `audit.audit`
         |  WHERE $where AND STEP = 'LOAD' AND PATHS LIKE '%${path(file)}%'
         |)
         |ORDER BY 1""".stripMargin
    )
  }

  /** @return
    *   the paths of the LOAD audit rows that name a file since this time
    */
  private def loadPaths(table: String, since: Long, file: File): List[String] =
    values(
      s"""SELECT PATHS FROM `audit.audit`
         |WHERE DOMAIN = '$dataset' AND SCHEMA = '$table' AND STEP = 'LOAD'
         |AND TIMESTAMP >= TIMESTAMP_SECONDS(${since / 1000}) AND PATHS LIKE '%${path(
          file
        )}%'""".stripMargin
    )

  private def target(table: String): Option[Table] =
    Try(bigquery.getTable(TableId.of(dataset, table))).toOption.flatMap(Option(_))

  private def definition(table: String): Option[StandardTableDefinition] =
    target(table).map(_.getDefinition[StandardTableDefinition])

  /** @return
    *   the columns of the primary key of a table
    */
  private def primaryKey(table: String): List[String] =
    target(table)
      .flatMap(table => Option(table.getTableConstraints))
      .flatMap(constraints => Option(constraints.getPrimaryKey))
      .map(_.getColumns.asScala.toList)
      .getOrElse(Nil)

  private def labels(table: String): Map[String, String] =
    target(table)
      .flatMap(table => Option(table.getLabels))
      .map(_.asScala.toMap)
      .getOrElse(Map.empty)

  /** The checks that failed, reported together at the end */
  private val failures = ListBuffer.empty[String]

  private def check(description: String)(condition: => Boolean): Unit = {
    val ok = Try(condition).getOrElse(false)
    info(s"${if (ok) "OK" else "FAILED"}: $description")
    if (!ok) failures += description
  }

  private def dsv(
    name: String,
    attributes: List[TableAttribute],
    sink: Option[AllSinks] = None,
    filter: Option[String] = None,
    presql: List[String] = Nil,
    postsql: List[String] = Nil,
    primaryKey: List[String] = Nil,
    tags: Set[String] = Set.empty
  ): SchemaInfo =
    SchemaInfo(
      name,
      Pattern.compile(s"$name.*"),
      attributes = attributes,
      metadata = Some(
        Metadata(
          format = Some(Format.DSV),
          withHeader = Some(true),
          separator = Some(";"),
          sink = sink
        )
      ),
      presql = presql,
      postsql = postsql,
      primaryKey = primaryKey,
      tags = tags,
      filter = filter
    )

  private def writeDomain(): Unit =
    DomainInfo(
      name = dataset,
      metadata = Some(Metadata(directory = Some(s"{{root_path}}/incoming/$dataset"))),
      tables = List(
        dsv(
          table,
          List(
            TableAttribute("id", "long", required = Some(true)),
            TableAttribute("day", "date"),
            TableAttribute("label", "string")
          ),
          Some(AllSinks(partition = Some(Seq("day")), requirePartitionFilter = Some(true))),
          // A filter makes the load take two steps
          filter = Some("label != 'skipped'")
        ),
        // A renamed attribute makes the load take two steps: the temporary table has the source
        // names, the target partitions, clusters and keys on the final ones
        dsv(
          renamedTable,
          List(
            TableAttribute("id", "long"),
            TableAttribute("day_src", "date", rename = Some("day")),
            TableAttribute("label_src", "string", rename = Some("label"))
          ),
          Some(AllSinks(partition = Some(Seq("day")), clustering = Some(Seq("label")))),
          primaryKey = List("id", "day"),
          tags = Set("team=data")
        ),
        // The temporary table of a POSITION file has a single `value` column
        SchemaInfo(
          positionTable,
          Pattern.compile(s"$positionTable.*"),
          attributes = List(
            TableAttribute("id", "long", position = Some(Position(0, 3))),
            TableAttribute("day", "date", position = Some(Position(4, 13)))
          ),
          primaryKey = List("id"),
          metadata = Some(
            Metadata(
              format = Some(Format.POSITION),
              withHeader = Some(false),
              sink = Some(AllSinks(partition = Some(Seq("day"))))
            )
          )
        ),
        // Nothing else makes this load take two steps
        dsv(
          hooksTable,
          List(TableAttribute("id", "long"), TableAttribute("label", "string")),
          presql = List(s"INSERT INTO `$dataset.$hooksLog` (step) VALUES ('pre')"),
          // Run after the rows are written
          postsql = List(
            s"INSERT INTO `$dataset.$hooksLog` (step) SELECT CONCAT('post:', CAST(COUNT(*) AS STRING)) FROM `$dataset.$hooksTable`"
          )
        ),
        // Nothing but its local files makes this load take two steps
        dsv(filesTable, List(TableAttribute("id", "long"), TableAttribute("label", "string")))
      )
    ).writeDomainAsYaml(DatasetArea.load)(settings.storageHandler())

  private def incoming(name: String, content: String): File = {
    val file = (project / "incoming" / dataset).createDirectories() / name
    file.overwrite(content)
  }

  if (sys.env.getOrElse("SL_REMOTE_TEST", "false").toBoolean) {
    "A two-step native load" should "read its temporary tables when its target requires a partition filter" in {
      withEnvs("SL_ROOT" -> project.pathAsString) {
        if (bigquery.getDataset(dataset) == null)
          bigquery.create(DatasetInfo.newBuilder(dataset).setLocation("europe-west1").build())
        bigquery.delete(TableId.of(dataset, table))
        writeDomain()
        val file = incoming(
          s"$table-1.csv",
          "id;day;label\n1;2026-11-01;first\n2;2026-11-02;second\n3;2026-11-02;skipped\n"
        )

        load(table, file)() shouldBe true
        val ids = bigquery
          .query(
            QueryJobConfiguration
              .newBuilder(
                s"SELECT id FROM `$dataset.$table` WHERE day >= '2000-01-01' ORDER BY id"
              )
              .build()
          )
          .iterateAll()
          .asScala
          .map(_.get(0).getLongValue)
          .toList
        ids shouldBe List(1L, 2L)
        // The target keeps it
        val target = bigquery.getTable(TableId.of(dataset, table))
        val partitioning = target.getDefinition[StandardTableDefinition].getTimePartitioning
        (Option(target.getRequirePartitionFilter).exists(_.booleanValue) ||
        Option(partitioning)
          .flatMap(p => Option(p.getRequirePartitionFilter))
          .exists(
            _.booleanValue
          )) shouldBe true
      }
    }

    "Native loads" should "load renamed partition columns, partitioned POSITION files, SQL hooks and several local files" in {
      withEnvs(
        "SL_ROOT" -> project.pathAsString,
        // The audit log is read by the test
        "SL_BQ_AUDIT_SAVE_IN_BATCH_MODE" -> "false"
      ) {
        if (bigquery.getDataset(dataset) == null)
          bigquery.create(DatasetInfo.newBuilder(dataset).setLocation("europe-west1").build())
        tables.foreach(name => bigquery.delete(TableId.of(dataset, name)))
        writeDomain()

        // A renamed partition and clustering column
        val renamedStart = System.currentTimeMillis()
        val renamedFile = incoming(
          s"$renamedTable-1.csv",
          "id;day_src;label_src\n1;2026-11-01;first\n2;2026-11-02;second\n"
        )
        check("the load with a renamed partition column succeeds") {
          load(renamedTable, renamedFile)("id", "day")
        }
        check("its rows are loaded under their final names") {
          values(
            s"""SELECT CONCAT(CAST(id AS STRING), ':', CAST(day AS STRING), ':', label)
               |FROM `$dataset.$renamedTable` ORDER BY id""".stripMargin
          ) == List("1:2026-11-01:first", "2:2026-11-02:second")
        }
        check("the target is partitioned and clustered on the final names") {
          definition(renamedTable).exists { target =>
            Option(target.getTimePartitioning).exists(_.getField == "day") &&
            Option(target.getClustering).exists(_.getFields.asScala.toList == List("label"))
          }
        }
        check("the second step sets the labels of the target") {
          labels(renamedTable).get("team").contains("data")
        }
        check("the second step sets the primary key of the target that it creates") {
          primaryKey(renamedTable) == List("id", "day")
        }
        val renamedAudit = auditSteps(renamedTable, renamedStart, renamedFile)
        check(s"the load writes its LOAD audit row only, no TRANSFORM row: $renamedAudit") {
          renamedAudit == List("LOAD:true")
        }

        // A partitioned POSITION table
        val positionFile = incoming(s"$positionTable-1.txt", "00012026-11-01\n00022026-11-02\n")
        check("the partitioned POSITION load succeeds")(load(positionTable, positionFile)("id"))
        check("its lines are loaded") {
          values(
            s"""SELECT CONCAT(CAST(id AS STRING), ':', CAST(day AS STRING))
               |FROM `$dataset.$positionTable` ORDER BY id""".stripMargin
          ) == List("1:2026-11-01", "2:2026-11-02")
        }
        check("the POSITION target is partitioned") {
          definition(positionTable).exists(target =>
            Option(target.getTimePartitioning).exists(_.getField == "day")
          )
        }
        check("the POSITION target has its primary key")(primaryKey(positionTable) == List("id"))

        // presql and postsql
        run(s"CREATE OR REPLACE TABLE `$dataset.$hooksLog` (step STRING)")
        val hooksFile = incoming(s"$hooksTable-1.csv", "id;label\n1;first\n")
        check("the load with presql and postsql succeeds")(load(hooksTable, hooksFile)())
        check("its row is loaded") {
          values(s"SELECT CAST(id AS STRING) FROM `$dataset.$hooksTable`") == List("1")
        }
        val hooks = values(s"SELECT step FROM `$dataset.$hooksLog` ORDER BY step")
        check(s"presql runs, and postsql runs after the rows are written: $hooks") {
          hooks == List("post:1", "pre")
        }

        // Several local files in one load (`--inPlace` loads all its files together): the first
        // one has no final newline
        val filesStart = System.currentTimeMillis()
        val firstFile = incoming(s"$filesTable-1.csv", "id;label\n1;one\n2;two")
        val secondFile = incoming(s"$filesTable-2.csv", "id;label\n3;three\n")
        check("the load of two local files succeeds")(load(filesTable, firstFile, secondFile)())
        val loaded = values(
          s"""SELECT CONCAT(CAST(id AS STRING), ':', label)
             |FROM `$dataset.$filesTable` ORDER BY id""".stripMargin
        )
        check(s"the rows of both files are loaded, without their headers: $loaded") {
          loaded == List("1:one", "2:two", "3:three")
        }
        val paths = loadPaths(filesTable, filesStart, secondFile)
        check(s"one load audits both files: $paths") {
          paths.size == 1 && paths.head.contains(path(firstFile)) &&
          auditSteps(filesTable, filesStart, secondFile) == List("LOAD:true")
        }

        if (failures.nonEmpty)
          fail(s"${failures.size} checks failed:\n${failures.mkString("\n")}")
      }
    }
  }
}
