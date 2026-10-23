package ai.starlake.integration.extract

import ai.starlake.config.DatasetArea
import ai.starlake.extract.{ExtractBigQuerySchema, TablesExtractConfig}
import ai.starlake.integration.IntegrationTestBase
import ai.starlake.job.Main
import ai.starlake.schema.model.{
  AllSinks,
  DomainInfo,
  Format,
  Metadata,
  SchemaInfo,
  TableAttribute,
  WriteStrategy,
  WriteStrategyType
}
import better.files.File
import com.google.cloud.bigquery._
import com.google.cloud.hadoop.repackaged.gcs.com.google.cloud.storage.{
  BlobInfo,
  Storage,
  StorageOptions
}

import java.util.UUID
import java.util.regex.Pattern
import scala.jdk.CollectionConverters._
import scala.util.Try

/** A BigQuery table extracted with extract-bq-schema, then loaded by the native loader from its
  * data, gives a table of the same schema and values: DATETIME, TIME, BYTES, BIGNUMERIC, GEOGRAPHY,
  * INTERVAL, RANGE and JSON columns included. The data comes from JSON and CSV files, or from an
  * `EXPORT DATA` to Parquet files.
  */
class ExtractLoadBQTypesRoundTripIntegrationSpec extends IntegrationTestBase {
  private val sourceDataset = "starlake_it_bq_roundtrip_source"
  private val targetDataset = "starlake_it_bq_roundtrip"
  private val table = "all_types"
  private val view = "all_types_view"
  // BigQuery loads RANGE values from CSV, not from JSON
  private val ranges = "ranges"

  private val parquetDataset = "starlake_it_bq_roundtrip_parquet"
  // Every type, nullable and required, repeated and nested fields: loaded in two steps
  private val parquetTypes = "parquet_types"
  // Nullable fields, arrays and a record: APPEND loads in one step
  private val parquetLists = "parquet_lists"
  // The top level columns of parquetTypes, from two local files, loaded by one job each. Their
  // repeated column is in the files, not in the table, which has a scripted column instead
  private val parquetLocal = "parquet_local"
  private val notInParquetLocal = Set("tags", "record", "records")

  // EXPORT DATA writes to Cloud Storage: a bucket, or a bucket and a folder
  private lazy val (bucket, exportPrefix): (Option[String], String) = {
    val location = sys.env
      .get("TEMPORARY_GCS_BUCKET")
      .map(_.stripPrefix("gs://").stripSuffix("/"))
      .filter(_.nonEmpty)
    val folder = location.flatMap(_.split("/", 2).lift(1)).map(_ + "/").getOrElse("")
    (
      location.map(_.split("/", 2).head),
      s"${folder}starlake-it/bq-roundtrip-parquet/${UUID.randomUUID()}"
    )
  }
  private lazy val storage: Storage = StorageOptions.newBuilder().build().getService

  private val projectTemplate: File =
    File(getClass.getResource("/integration/bq-roundtrip-types").toURI)

  private lazy val project: File = {
    val dir = File.newTemporaryDirectory("starlake-bq-roundtrip-")
    (projectTemplate / "metadata").copyToDirectory(dir)
    // The load domain is written by the test: git keeps no empty directory
    (dir / "metadata" / "load").createDirectories()
    dir
  }

  override def theSampleFolder: File = project

  private lazy val bigquery: BigQuery = BigQueryOptions.newBuilder().build().getService

  private def run(sql: String): TableResult =
    bigquery.query(QueryJobConfiguration.newBuilder(sql).setUseLegacySql(false).build())

  private def fields(dataset: String, name: String): List[Field] =
    bigquery
      .getTable(TableId.of(dataset, name))
      .getDefinition[TableDefinition]()
      .getSchema
      .getFields
      .asScala
      .toList

  /** @return
    *   the type, mode and parameters of each column, at every level. NUMERIC is the only type of
    *   precision 38 and scale 9: the parameters of the NUMERIC columns that the loader creates are
    *   those of an unparameterized NUMERIC.
    */
  private def describe(fields: List[Field], prefix: String = ""): List[String] =
    fields.flatMap { field =>
      val path = prefix + field.getName
      val standardType = field.getType.getStandardType
      val parameters =
        if (standardType == StandardSQLTypeName.NUMERIC) ""
        else
          List(
            Option(field.getPrecision),
            Option(field.getScale),
            Option(field.getMaxLength),
            Option(field.getRangeElementType).map(_.getType)
          ).flatten.mkString("(", ", ", ")")
      s"$path $standardType$parameters ${Option(field.getMode).getOrElse(Field.Mode.NULLABLE)}" ::
      describe(Option(field.getSubFields).map(_.asScala.toList).getOrElse(Nil), path + ".")
    }

  private def fingerprint(dataset: String, name: String): List[String] =
    fingerprintOf(s"SELECT * FROM `$dataset.$name`")

  /** @return
    *   the row count and fingerprint of the result of the query
    */
  private def fingerprintOf(query: String): List[String] =
    run(
      s"SELECT COUNT(*), BIT_XOR(FARM_FINGERPRINT(TO_JSON_STRING(t))) FROM ($query) t"
    ).iterateAll()
      .asScala
      .map(row => (0 until 2).map(i => row.get(i).getStringValue).toList)
      .toList
      .flatten

  /** @return
    *   the query exporting parquetTypes to Parquet. EXPORT DATA does not export JSON and INTERVAL
    *   columns to Parquet ("Type JSON is not currently supported for parquet exports"): the JSON
    *   values are exported as their JSON text, and the INTERVAL and RANGE values as their canonical
    *   text, which the native loader reads from Parquet files.
    */
  private def parquetExportQuery(
    filter: String = "TRUE",
    except: Set[String] = Set.empty,
    replace: List[String] = Nil
  ): String = {
    val excepted =
      if (except.isEmpty) "" else except.toList.sorted.mkString(" EXCEPT (", ", ", ")")
    val replaced = replace ++ ("TO_JSON_STRING(doc) AS doc" :: List(
      "duration",
      "days",
      "civils",
      "instants"
    ).map(column => s"CAST($column AS STRING) AS $column"))
    s"SELECT *$excepted REPLACE (${replaced.mkString(", ")}) FROM `$sourceDataset.$parquetTypes` WHERE $filter"
  }

  /** @return
    *   the URI of the Parquet files written by EXPORT DATA into the folder of this name
    */
  private def exportParquet(name: String, query: String, files: String = "part"): String = {
    val uri = s"gs://${bucket.get}/$exportPrefix/$name/$files-*.parquet"
    run(s"EXPORT DATA OPTIONS (uri = '$uri', format = 'PARQUET', overwrite = true) AS $query")
    uri
  }

  /** @return
    *   the local copies of the Parquet files exported into the folder of this name
    */
  private def download(name: String, dir: File): List[File] =
    storage
      .list(bucket.get, Storage.BlobListOption.prefix(s"$exportPrefix/$name/"))
      .iterateAll()
      .asScala
      .toList
      .map { blob =>
        val file = dir / s"$name-${blob.getName.split('/').last}"
        blob.downloadTo(file.path)
        file
      }

  private def loadInPlace(table: String, files: String, domain: String = parquetDataset): Boolean =
    Try(
      new Main().run(
        Array("load", "--domains", domain, "--tables", table, "--files", files, "--inPlace")
      )
    ).getOrElse(false)

  /** @return
    *   the error and the path of the rows of the audit rejected table that the loads of the files
    *   of the table wrote since this time: the files of the test have names of their own
    */
  private def rejectedSince(
    domain: String,
    table: String,
    since: Long,
    path: String
  ): List[(String, String)] =
    run(
      s"""SELECT error, path FROM `audit.rejected`
         |WHERE domain = '$domain' AND schema = '$table' AND timestamp >= TIMESTAMP_MILLIS($since)
         |AND path = '$path'
         |ORDER BY error""".stripMargin
    ).iterateAll()
      .asScala
      .map(row => row.get(0).getStringValue -> row.get(1).getStringValue)
      .toList

  /** Writes the Parquet domain from the extracted one, with these write strategies. The types and
    * lists tables are partitioned by day.
    */
  private def writeParquetDomain(
    extracted: DomainInfo,
    typesStrategy: WriteStrategyType,
    listsStrategy: WriteStrategyType
  ): Unit = {
    def withStrategy(
      metadata: Option[Metadata],
      strategy: WriteStrategyType,
      partitioned: Boolean = true
    ) = {
      val withStrategy = metadata
        .getOrElse(Metadata())
        .copy(writeStrategy = Some(WriteStrategy(`type` = Some(strategy))))
      Some(
        if (partitioned)
          withStrategy.copy(sink =
            Some(withStrategy.sink.getOrElse(AllSinks()).copy(partition = Some(Seq("day"))))
          )
        else withStrategy
      )
    }
    val tables = extracted.tables.map(schema => schema.name -> schema).toMap
    extracted
      .copy(
        name = parquetDataset,
        database = None,
        metadata = Some(
          Metadata(
            format = Some(Format.PARQUET),
            directory = Some(s"{{root_path}}/incoming/$parquetDataset")
          )
        ),
        tables = List(
          tables(parquetTypes)
            .copy(metadata = withStrategy(tables(parquetTypes).metadata, typesStrategy)),
          tables(parquetLists)
            .copy(metadata = withStrategy(tables(parquetLists).metadata, listsStrategy)),
          tables(parquetTypes).copy(
            name = parquetLocal,
            pattern = Pattern.compile(s"$parquetLocal-.*\\.parquet"),
            attributes =
              tables(parquetTypes).attributes.filterNot(a => notInParquetLocal(a.name)) :+
              TableAttribute("loaded_from", "string", script = Some("'parquet'")),
            metadata = withStrategy(
              tables(parquetTypes).metadata,
              WriteStrategyType.APPEND,
              partitioned = false
            )
          )
        )
      )
      .writeDomainAsYaml(DatasetArea.load)(settings.storageHandler())
  }

  if (sys.env.getOrElse("SL_REMOTE_TEST", "false").toBoolean) {
    "A BigQuery table extracted then loaded natively" should "keep its schema and values" in {
      withEnvs("SL_ROOT" -> project.pathAsString) {
        List(sourceDataset, targetDataset).foreach { dataset =>
          if (bigquery.getDataset(dataset) == null)
            bigquery.create(DatasetInfo.newBuilder(dataset).setLocation("europe-west1").build())
        }
        List(table, ranges).foreach(name => bigquery.delete(TableId.of(targetDataset, name)))
        run(s"""CREATE OR REPLACE TABLE `$sourceDataset.$table` (
               |  id INT64 NOT NULL,
               |  civil DATETIME,
               |  wall TIME,
               |  payload BYTES,
               |  big BIGNUMERIC,
               |  price NUMERIC,
               |  area GEOGRAPHY,
               |  duration INTERVAL,
               |  doc JSON,
               |  tags ARRAY<STRING>,
               |  record STRUCT<instant TIMESTAMP, civils ARRAY<DATETIME>>
               |)""".stripMargin)
        run(s"""INSERT INTO `$sourceDataset.$table` VALUES
               |(1, DATETIME '2019-12-31 23:59:59.999999', TIME '23:59:59.999999', b'\\x00\\xff\\x10',
               | BIGNUMERIC '1234567890123456789012345678901234567.12345678901234567890123456789012345678',
               | NUMERIC '-12345678901234567890123456789.123456789', ST_GEOGPOINT(1, 2),
               | INTERVAL '1-2 3 4:5:6.789' YEAR TO SECOND, JSON '{"a": [1, "b"]}', ['x', 'y'],
               | STRUCT(TIMESTAMP '2019-12-31 23:59:59.999999 UTC', [DATETIME '2019-01-01 00:00:00'])),
               |(2, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, [], NULL)""".stripMargin)
        run(s"""CREATE OR REPLACE TABLE `$sourceDataset.$ranges` (
               |  id INT64 NOT NULL,
               |  days RANGE<DATE>,
               |  civils RANGE<DATETIME>,
               |  instants RANGE<TIMESTAMP>
               |)""".stripMargin)
        run(s"""INSERT INTO `$sourceDataset.$ranges` VALUES
               |(1, RANGE(DATE '2019-01-01', DATE '2019-12-31'),
               | RANGE(DATETIME '2019-01-01 10:00:00.5', NULL),
               | RANGE(TIMESTAMP '2019-01-01 00:00:00 UTC', TIMESTAMP '2019-12-31 23:59:59.999999 UTC')),
               |(2, NULL, NULL, NULL)""".stripMargin)
        run(
          s"CREATE OR REPLACE VIEW `$sourceDataset.$view` AS SELECT * FROM `$sourceDataset.$table`"
        )

        val extracted = new ExtractBigQuerySchema(
          TablesExtractConfig(
            connectionRef = Some("bigquery"),
            tables = Map(sourceDataset -> List(table, view, ranges)),
            tableTypes = List("TABLE", "VIEW")
          )
        ).extractSchemasAndTables(
          settings.schemaHandler(),
          Map(sourceDataset -> List(table, view, ranges))
        ).head
        val types = extracted.tables
          .map(schema => schema.name -> schema.attributes.map(a => a.name -> a.`type`))
          .toMap
        types(table) shouldBe List(
          "id"       -> "long",
          "civil"    -> "datetime",
          "wall"     -> "time",
          "payload"  -> "binary",
          "big"      -> "bignumeric",
          "price"    -> "decimal",
          "area"     -> "geography",
          "duration" -> "interval",
          "doc"      -> "variant",
          "tags"     -> "string",
          "record"   -> "struct"
        )
        // The effective schema of the view
        types(view) shouldBe types(table)
        types(ranges) shouldBe List(
          "id"       -> "long",
          "days"     -> "range_date",
          "civils"   -> "range_datetime",
          "instants" -> "range_timestamp"
        )

        // The extracted table, loaded into the target dataset from newline delimited JSON
        extracted
          .copy(
            name = targetDataset,
            database = None,
            metadata = Some(
              Metadata(
                format = Some(Format.JSON),
                directory = Some(s"{{root_path}}/incoming/$targetDataset")
              )
            ),
            tables = extracted.tables.collect {
              case schema if schema.name == table =>
                schema.copy(pattern = Pattern.compile(s"$table-.*\\.json"))
              case schema if schema.name == ranges =>
                schema.copy(
                  pattern = Pattern.compile(s"$ranges-.*\\.csv"),
                  metadata = Some(
                    Metadata(
                      format = Some(Format.DSV),
                      withHeader = Some(true),
                      separator = Some(";")
                    )
                  )
                )
            }
          )
          .writeDomainAsYaml(DatasetArea.load)(settings.storageHandler())
        val incoming = (project / "incoming" / targetDataset).createDirectories()
        // GEOGRAPHY as its WKT text, and DATETIME with a space, as JSON loads read them. INTERVAL
        // is written in ISO 8601, as JSON loads read it
        val datetimeFormat = "'%F %H:%M:%E6S'"
        val lines = run(
          s"""SELECT TO_JSON_STRING(t) FROM (
             |  SELECT * REPLACE (
             |    ST_ASTEXT(area) AS area,
             |    FORMAT_DATETIME($datetimeFormat, civil) AS civil,
             |    IF(record IS NULL, NULL, STRUCT(record.instant AS instant, ARRAY(
             |      SELECT FORMAT_DATETIME($datetimeFormat, c) FROM UNNEST(record.civils) c WITH OFFSET o
             |      ORDER BY o) AS civils)) AS record)
             |  FROM `$sourceDataset.$table`) t""".stripMargin
        ).iterateAll().asScala.map(_.get(0).getStringValue).toList
        (incoming / s"$table-1.json").overwrite(lines.mkString("", "\n", "\n"))
        // The canonical text of the RANGE values, as CSV loads read them
        val rangeLines = run(
          s"""SELECT CONCAT(CAST(id AS STRING), ';', IFNULL(CAST(days AS STRING), ''), ';',
             |  IFNULL(CAST(civils AS STRING), ''), ';', IFNULL(CAST(instants AS STRING), ''))
             |FROM `$sourceDataset.$ranges` ORDER BY id""".stripMargin
        ).iterateAll().asScala.map(_.get(0).getStringValue).toList
        (incoming / s"$ranges-1.csv").overwrite(
          ("id;days;civils;instants" :: rangeLines).mkString("", "\n", "\n")
        )
        assert(new Main().run(Array("stage")))
        assert(new Main().run(Array("load")))

        List(table, ranges).foreach { name =>
          describe(fields(targetDataset, name)) shouldBe describe(fields(sourceDataset, name))
          fingerprint(targetDataset, name) shouldBe fingerprint(sourceDataset, name)
        }
      }
    }
  }

  if (sys.env.getOrElse("SL_REMOTE_TEST", "false").toBoolean && bucket.nonEmpty) {
    "A BigQuery table extracted, exported to Parquet then loaded natively" should "keep its schema and values" in {
      withEnvs(
        "SL_ROOT" -> project.pathAsString,
        // The rejected rows are kept with their value, and in replay files
        "SL_REJECT_WITH_VALUE"   -> "true",
        "SL_SINK_REPLAY_TO_FILE" -> "true"
      ) {
        try {
          List(sourceDataset, parquetDataset).foreach { dataset =>
            if (bigquery.getDataset(dataset) == null)
              bigquery.create(DatasetInfo.newBuilder(dataset).setLocation("europe-west1").build())
          }
          List(parquetTypes, parquetLists, parquetLocal).foreach(name =>
            bigquery.delete(TableId.of(parquetDataset, name))
          )
          run(s"""CREATE OR REPLACE TABLE `$sourceDataset.$parquetTypes` (
                 |  id INT64 NOT NULL,
                 |  label STRING NOT NULL,
                 |  day DATE NOT NULL,
                 |  created DATETIME NOT NULL,
                 |  amount BIGNUMERIC NOT NULL,
                 |  raw BYTES NOT NULL,
                 |  total INT64,
                 |  ratio FLOAT64,
                 |  flag BOOL,
                 |  price NUMERIC,
                 |  big BIGNUMERIC,
                 |  civil DATETIME,
                 |  wall TIME,
                 |  instant TIMESTAMP,
                 |  payload BYTES,
                 |  area GEOGRAPHY,
                 |  duration INTERVAL,
                 |  doc JSON,
                 |  days RANGE<DATE>,
                 |  civils RANGE<DATETIME>,
                 |  instants RANGE<TIMESTAMP>,
                 |  tags ARRAY<STRING>,
                 |  record STRUCT<code STRING NOT NULL, civils ARRAY<DATETIME>, amounts ARRAY<NUMERIC>>,
                 |  records ARRAY<STRUCT<name STRING NOT NULL, day DATE>>
                 |) PARTITION BY day""".stripMargin)
          run(s"""INSERT INTO `$sourceDataset.$parquetTypes` VALUES
                 |(1, 'é€😀', DATE '2019-12-31', DATETIME '2019-12-31 23:59:59.999999',
                 | BIGNUMERIC '-1234567890123456789012345678901234567.12345678901234567890123456789012345678',
                 | b'\\x00\\xff', 9223372036854775807, 1.5e300, TRUE,
                 | NUMERIC '-12345678901234567890123456789.123456789',
                 | BIGNUMERIC '1234567890123456789012345678901234567.12345678901234567890123456789012345678',
                 | DATETIME '2019-12-31 23:59:59.999999', TIME '23:59:59.999999',
                 | TIMESTAMP '2019-12-31 23:59:59.999999 UTC', b'\\x00\\xff\\x10', ST_GEOGPOINT(1, 2),
                 | INTERVAL '1-2 3 4:5:6.789' YEAR TO SECOND, JSON '{"a": [1, "b"]}',
                 | RANGE(DATE '2019-01-01', DATE '2019-12-31'),
                 | RANGE(DATETIME '2019-01-01 10:00:00.5', NULL),
                 | RANGE(TIMESTAMP '2019-01-01 00:00:00 UTC', TIMESTAMP '2019-12-31 23:59:59.999999 UTC'),
                 | ['x', 'y'],
                 | STRUCT('c', [DATETIME '2019-01-01 00:00:00'], [NUMERIC '1.5', NUMERIC '-2']),
                 | [STRUCT('n', DATE '2019-01-01'), STRUCT('m', CAST(NULL AS DATE))]),
                 |(2, '', DATE '2020-01-01', DATETIME '0001-01-01 00:00:00', BIGNUMERIC '0', b'',
                 | NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
                 | NULL, [], NULL, [])""".stripMargin)
          run(s"""CREATE OR REPLACE TABLE `$sourceDataset.$parquetLists` (
                 |  id INT64,
                 |  day DATE,
                 |  civil DATETIME,
                 |  big BIGNUMERIC,
                 |  payload BYTES,
                 |  tags ARRAY<STRING>,
                 |  counts ARRAY<INT64>,
                 |  record STRUCT<code STRING, days ARRAY<DATE>>
                 |) PARTITION BY day""".stripMargin)
          // OVERWRITE_BY_PARTITION does not replace the rows of the NULL partition: none here
          run(s"""INSERT INTO `$sourceDataset.$parquetLists` VALUES
                 |(1, DATE '2019-12-31', DATETIME '2019-12-31 23:59:59.999999', BIGNUMERIC '1.5',
                 | b'\\x00', ['x', 'y'], [1, 2], STRUCT('c', [DATE '2019-01-01'])),
                 |(2, DATE '2020-01-01', NULL, NULL, NULL, [], [], NULL),
                 |(3, DATE '2020-01-01', NULL, NULL, NULL, ['z'], [3],
                 | STRUCT(CAST(NULL AS STRING), ARRAY<DATE>[]))""".stripMargin)

          val extracted = new ExtractBigQuerySchema(
            TablesExtractConfig(
              connectionRef = Some("bigquery"),
              tables = Map(sourceDataset -> List(parquetTypes, parquetLists)),
              tableTypes = List("TABLE")
            )
          ).extractSchemasAndTables(
            settings.schemaHandler(),
            Map(sourceDataset -> List(parquetTypes, parquetLists))
          ).head
          extracted.tables
            .find(_.name == parquetTypes)
            .map(_.attributes.map(a => (a.name, a.`type`, a.resolveRequired()))) shouldBe Some(
            List(
              ("id", "long", true),
              ("label", "string", true),
              ("day", "date", true),
              ("created", "datetime", true),
              ("amount", "bignumeric", true),
              ("raw", "binary", true),
              ("total", "long", false),
              ("ratio", "double", false),
              ("flag", "boolean", false),
              ("price", "decimal", false),
              ("big", "bignumeric", false),
              ("civil", "datetime", false),
              ("wall", "time", false),
              ("instant", "timestamp", false),
              ("payload", "binary", false),
              ("area", "geography", false),
              ("duration", "interval", false),
              ("doc", "variant", false),
              ("days", "range_date", false),
              ("civils", "range_datetime", false),
              ("instants", "range_timestamp", false),
              ("tags", "string", false),
              ("record", "struct", false),
              ("records", "struct", false)
            )
          )

          // EXPORT DATA refuses JSON and INTERVAL columns in Parquet; RANGE columns are not
          // documented. They are all exported as strings below. Logged, not checked
          val directExport = Try(
            exportParquet(
              "direct",
              s"SELECT days, civils, instants FROM `$sourceDataset.$parquetTypes`"
            )
          )
          info(
            s"EXPORT DATA of RANGE columns to Parquet: ${directExport
                .fold(e => s"failed: ${e.getMessage}", _ => "succeeded")}"
          )

          // Several Cloud Storage files, by two URIs and by a wildcard URI
          val typesUris = List("a" -> "id = 1", "b" -> "id = 2").map { case (files, filter) =>
            exportParquet(parquetTypes, parquetExportQuery(filter), files)
          }
          val typesUri = s"gs://${bucket.get}/$exportPrefix/$parquetTypes/*.parquet"
          // With a column that the table does not declare, loaded in one step
          val listsUri =
            exportParquet(
              parquetLists,
              s"SELECT *, 'undeclared' AS extra FROM `$sourceDataset.$parquetLists`"
            )
          // Two local files, with a column that the table does not declare
          List(1, 2).foreach { id =>
            exportParquet(
              s"$parquetLocal-$id",
              parquetExportQuery(s"id = $id", except = notInParquetLocal - "tags")
            )
          }
          val localDir = (project / "parquet").createDirectories()
          val localFiles =
            download(s"$parquetLocal-1", localDir) ++ download(s"$parquetLocal-2", localDir)
          localFiles.size should be >= 2

          def checkTypesAndLists(): Unit =
            List(parquetTypes, parquetLists).foreach { name =>
              withClue(name) {
                describe(fields(parquetDataset, name)) shouldBe describe(
                  fields(sourceDataset, name)
                )
                fingerprint(parquetDataset, name) shouldBe fingerprint(sourceDataset, name)
              }
            }

          // APPEND into new tables
          writeParquetDomain(extracted, WriteStrategyType.APPEND, WriteStrategyType.APPEND)
          loadInPlace(parquetTypes, typesUris.mkString(",")) shouldBe true
          loadInPlace(parquetLists, listsUri) shouldBe true
          loadInPlace(parquetLocal, localFiles.map(_.uri.toString).mkString(",")) shouldBe true
          checkTypesAndLists()
          describe(fields(parquetDataset, parquetLocal)) shouldBe describe(
            fields(sourceDataset, parquetTypes).filterNot(f => notInParquetLocal(f.getName)) :+
            Field
              .newBuilder("loaded_from", StandardSQLTypeName.STRING)
              .setMode(Field.Mode.NULLABLE)
              .build()
          )
          fingerprint(parquetDataset, parquetLocal) shouldBe fingerprintOf(
            s"SELECT * EXCEPT (${notInParquetLocal.toList.sorted
                .mkString(", ")}), 'parquet' AS loaded_from FROM `$sourceDataset.$parquetTypes`"
          )

          // The rows without a value for a required attribute, top level, nested or in a repeated
          // record, are rejected, and the others are loaded
          val nullsUri = exportParquet(
            "nulls",
            List(
              parquetExportQuery("id = 1"),
              parquetExportQuery("id = 2", replace = List("CAST(NULL AS STRING) AS label")),
              parquetExportQuery(
                "id = 1",
                replace = List(
                  "STRUCT(CAST(NULL AS STRING) AS code, record.civils AS civils, record.amounts AS amounts) AS record"
                )
              ),
              parquetExportQuery(
                "id = 1",
                replace = List(
                  "ARRAY(SELECT STRUCT(IF(o = 1, CAST(NULL AS STRING), r.name) AS name, r.day AS day) FROM UNNEST(records) r WITH OFFSET o ORDER BY o) AS records"
                )
              )
            ).map(query => s"($query)").mkString(" UNION ALL ")
          )
          val nullsLoad = System.currentTimeMillis()
          loadInPlace(parquetTypes, nullsUri) shouldBe true
          fingerprintOf(
            s"SELECT * FROM `$parquetDataset.$parquetTypes` WHERE id = 2"
          ) shouldBe fingerprintOf(s"SELECT * FROM `$sourceDataset.$parquetTypes` WHERE id = 2")
          fingerprintOf(
            s"SELECT * FROM `$parquetDataset.$parquetTypes` WHERE id = 1"
          ).head shouldBe "2"

          // The rejected rows are kept as the Spark loads keep them (#1856): in the audit rejected
          // table, with one error line per attribute without a value, their file and their value
          val nullsRejected = rejectedSince(parquetDataset, parquetTypes, nullsLoad, nullsUri)
          nullsRejected.map(_._1.split("\n").lift(1)) shouldBe List(
            "`label`  can't be null",
            "`record.code`  can't be null",
            "`records.name`  can't be null"
          ).map(Some(_))
          nullsRejected.foreach { case (error, _) =>
            error should startWith("ERR -> \n")
            error should include(s"\n\nFILE -> $nullsUri")
            error should include("\n\nROW -> {")
            error should not include "sl_input_file_name"
          }
          // and all of them in Parquet replay files, which the same table loads again
          val replayFiles = settings
            .storageHandler()
            .list(DatasetArea.replay(parquetDataset), recursive = false)
            .map(_.path)
            .filter(
              _.getName
                .matches(
                  s"$parquetDataset\\.$parquetTypes\\.\\d{14}\\.replay-[0-9a-f]{8}-\\d{12}\\.parquet"
                )
            )
          replayFiles should not be empty
          replayFiles.foreach { replayFile =>
            storage.create(
              BlobInfo
                .newBuilder(bucket.get, s"$exportPrefix/replay/${replayFile.getName}")
                .build(),
              java.nio.file.Files.readAllBytes(java.nio.file.Paths.get(replayFile.toUri))
            )
          }
          val replayLoad = System.currentTimeMillis()
          val replayUri = s"gs://${bucket.get}/$exportPrefix/replay/*.parquet"
          loadInPlace(parquetTypes, replayUri) shouldBe true
          rejectedSince(parquetDataset, parquetTypes, replayLoad, replayUri).map(
            _._1.split("\n").lift(1)
          ) shouldBe nullsRejected.map(_._1.split("\n").lift(1))

          // The bad records that a CSV load skips are kept with the message of BigQuery
          val csvDomain = s"${parquetDataset}_csv"
          DomainInfo(
            name = csvDomain,
            metadata = Some(
              Metadata(
                format = Some(Format.DSV),
                withHeader = Some(true),
                separator = Some(","),
                directory = Some(s"{{root_path}}/incoming/$csvDomain")
              )
            ),
            tables = List(
              SchemaInfo(
                "bad_lines",
                Pattern.compile("bad_lines.*\\.csv"),
                attributes = List(TableAttribute("id", "long"), TableAttribute("label", "string"))
              )
            )
          ).writeDomainAsYaml(DatasetArea.load)(settings.storageHandler())
          bigquery.delete(TableId.of(csvDomain, "bad_lines"))
          val csvFile = project / "incoming" / csvDomain / "bad_lines.csv"
          csvFile.parent.createDirectories()
          csvFile.overwrite("id,label\n1,one\nx,two\n3,three\ny,four\n")
          val csvLoad = System.currentTimeMillis()
          loadInPlace("bad_lines", csvFile.uri.toString, csvDomain) shouldBe true
          fingerprintOf(s"SELECT * FROM `$csvDomain.bad_lines`").head shouldBe "2"
          val csvPath = new org.apache.hadoop.fs.Path(csvFile.uri.toString).toString
          val csvRejected = rejectedSince(csvDomain, "bad_lines", csvLoad, csvPath)
          info(
            s"Rejected CSV records: ${csvRejected.map(_._1.replace("\n", " | ")).mkString("; ")}"
          )
          // One per bad value, with the message of BigQuery
          csvRejected should have size 2
          List("'x'", "'y'").foreach { value =>
            csvRejected.count(_._1.contains(value)) shouldBe 1
          }
          csvRejected.foreach { case (error, _) =>
            error should startWith("ERR -> \n")
            error should include("\n\nFILE -> ")
          }

          // OVERWRITE replaces the rows, APPEND adds them again
          writeParquetDomain(extracted, WriteStrategyType.OVERWRITE, WriteStrategyType.APPEND)
          loadInPlace(parquetTypes, typesUri) shouldBe true
          loadInPlace(parquetLists, listsUri) shouldBe true
          describe(fields(parquetDataset, parquetTypes)) shouldBe describe(
            fields(sourceDataset, parquetTypes)
          )
          fingerprint(parquetDataset, parquetTypes) shouldBe fingerprint(
            sourceDataset,
            parquetTypes
          )
          val sourceCount = fingerprint(sourceDataset, parquetLists).head.toLong
          fingerprint(parquetDataset, parquetLists).head.toLong shouldBe 2 * sourceCount

          // OVERWRITE_BY_PARTITION replaces the partitions of the loaded rows, and keeps the others
          run(
            s"INSERT INTO `$parquetDataset.$parquetLists` (id, day) VALUES (99, DATE '2021-01-01')"
          )
          writeParquetDomain(
            extracted,
            WriteStrategyType.OVERWRITE_BY_PARTITION,
            WriteStrategyType.OVERWRITE_BY_PARTITION
          )
          loadInPlace(parquetTypes, typesUri) shouldBe true
          loadInPlace(parquetLists, listsUri) shouldBe true
          describe(fields(parquetDataset, parquetTypes)) shouldBe describe(
            fields(sourceDataset, parquetTypes)
          )
          fingerprint(parquetDataset, parquetTypes) shouldBe fingerprint(
            sourceDataset,
            parquetTypes
          )
          describe(fields(parquetDataset, parquetLists)) shouldBe describe(
            fields(sourceDataset, parquetLists)
          )
          fingerprintOf(
            s"SELECT * FROM `$parquetDataset.$parquetLists` WHERE id != 99"
          ) shouldBe fingerprint(sourceDataset, parquetLists)
          fingerprintOf(
            s"SELECT * FROM `$parquetDataset.$parquetLists` WHERE id = 99"
          ).head shouldBe "1"
        } finally {
          Try(
            storage
              .list(bucket.get, Storage.BlobListOption.prefix(exportPrefix))
              .iterateAll()
              .asScala
              .foreach(blob => storage.delete(blob.getBlobId))
          )
        }
      }
    }
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    project.delete(swallowIOExceptions = true)
  }
}
