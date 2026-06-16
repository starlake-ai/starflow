package ai.starlake.extract

import ai.starlake.TestHelper
import ai.starlake.config.Settings
import ai.starlake.extract.JdbcDbUtils.{lastExportTableName, Columns}
import ai.starlake.schema.model.{JDBCSchema, JDBCTable, PrimitiveType, TableAttribute}
import better.files.File
import org.apache.hadoop.fs.Path
import org.scalatest.BeforeAndAfterEach

import java.nio.charset.StandardCharsets
import java.sql.{DriverManager, ResultSet}
import java.time.{LocalDateTime, OffsetDateTime, ZoneId}
import scala.collection.mutable.ListBuffer
import scala.util.Success

class ExtractDataJobSpec extends TestHelper with BeforeAndAfterEach {

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    pgContainer.start()
  }

  override protected def afterEach(): Unit = {
    pgContainer.stop()
    File(starlakeTestRoot).delete(swallowIOExceptions = true)
    super.afterEach()
  }

  implicit class RichResultSet(rs: ResultSet) {
    def asIteratorOf[T](f: ResultSet => T): Iterator[T] = new Iterator[T] {
      override def hasNext: Boolean = rs.next()

      override def next(): T = f(rs)
    }

    def headerNames: String = {
      val row = new StringBuilder()
      (1 to rs.getMetaData.getColumnCount).foreach { i =>
        val columnHeaderName = rs.getMetaData.getColumnName(i)
        row.append(columnHeaderName)
        if (i < rs.getMetaData.getColumnCount)
          row.append(", ")
      }
      row.toString()
    }
  }

  val rowAsString: ResultSet => String = resultSet => {
    val row = new StringBuilder()
    (1 to resultSet.getMetaData.getColumnCount).foreach { i =>
      val columnValue = String.valueOf(resultSet.getObject(i))
      row.append(columnValue)
      if (i < resultSet.getMetaData.getColumnCount)
        row.append(", ")
    }
    row.toString()
  }

  val initSQL: String =
    """
      |CREATE TABLE test_table (
      |	c_str varchar NULL,
      |	c_short int2 NULL,
      |	c_int int4 NULL,
      |	c_long int8 NULL,
      |	c_decimal numeric NULL,
      |	c_ts timestamp NULL,
      | c_date date NULL
      |);
      |insert into test_table values
      |('A"',0, 1, 2, 3.4, '2003-2-1'::timestamp, '2003-2-2'::date),
      |('BÉ',5, 6, 7, 8.9, '2003-12-24'::timestamp, '2003-12-25'::date),
      |(null,null, null, null, null, null, null);""".stripMargin

  "initExportAuditTable" should "create audit schema if it doesn't exist and return its columns" in {
    def checkColumns(columns: Columns) = {
      columns.map(c => c.name -> c.`type`) should contain theSameElementsAs List(
        "domain"       -> PrimitiveType.string.value,
        "schema"       -> PrimitiveType.string.value,
        "last_ts"      -> PrimitiveType.timestamp.value,
        "last_date"    -> PrimitiveType.date.value,
        "last_long"    -> PrimitiveType.long.value,
        "last_decimal" -> PrimitiveType.decimal.value,
        "start_ts"     -> PrimitiveType.timestamp.value,
        "end_ts"       -> PrimitiveType.timestamp.value,
        "duration"     -> PrimitiveType.long.value,
        "mode"         -> PrimitiveType.string.value,
        "count"        -> PrimitiveType.long.value,
        "success"      -> PrimitiveType.boolean.value,
        "message"      -> PrimitiveType.string.value,
        "step"         -> PrimitiveType.string.value
      )
    }

    val tableNames: ResultSet => String =
      (rs: ResultSet) => rs.getString("TABLE_SCHEM") + "." + rs.getString("TABLE_NAME")

    new WithSettings() {
      private val extractDataJob = new ExtractDataJob(settings.schemaHandler())
      val jdbcConnection = settings.appConfig.connections("test-pg")
      val conn = DriverManager.getConnection(
        jdbcConnection.options("url"),
        jdbcConnection.options("user"),
        jdbcConnection.options("password")
      )
      // audit table should not exists
      conn.getMetaData
        .getTables(
          None.orNull,
          "%",
          "%",
          List("TABLE").toArray
        )
        .asIteratorOf(tableNames)
        .exists(_.equalsIgnoreCase(s"audit.$lastExportTableName")) shouldBe false

      // audit table should create it and return columns
      ParUtils.withExecutor() { implicit ec =>
        implicit val extractExecutionContext =
          new ExtractExecutionContext(ec)
        val columns = extractDataJob.initExportAuditTable(settings.appConfig.connections("test-pg"))
        conn.getMetaData
          .getTables(
            None.orNull,
            "%",
            "%",
            List("TABLE").toArray
          )
          .asIteratorOf(tableNames)
          .exists(_.equalsIgnoreCase(s"audit.$lastExportTableName")) shouldBe true
        checkColumns(columns)

        // trying to create table again should not fail and still return audit column names
        checkColumns(extractDataJob.initExportAuditTable(settings.appConfig.connections("test-pg")))
      }
    }
  }

  "getStringPartitionFunc" should "return string partition func for the given database if defined" in {
    new WithSettings() {
      private val extractDataJob = new ExtractDataJob(settings.schemaHandler())
      extractDataJob.getStringPartitionFunc("sqlserver") shouldBe Some(
        "abs( binary_checksum({{col}}) % {{nb_partitions}} )"
      )
      extractDataJob.getStringPartitionFunc("unknown") shouldBe None
    }
  }

  "resolveNumPartitions" should "return partition column with precedence" in {
    new WithSettings() {
      private val extractDataJob = new ExtractDataJob(settings.schemaHandler())
      // Table definition first
      val jdbcConnection = settings.appConfig.connections("test-pg")
      extractDataJob.resolveNumPartitions(
        ExtractJdbcDataConfig(
          jdbcSchema = new JDBCSchema().copy(numPartitions = Some(2)),
          baseOutputDir = new Path("."),
          limit = 1,
          numPartitions = 1,
          parallelism = None,
          cliFullExport = None,
          extractionPredicate = None,
          ignoreExtractionFailure = true,
          cleanOnExtract = true,
          includeTables = Nil,
          excludeTables = Nil,
          outputFormat = FileFormat(),
          data = jdbcConnection,
          audit = jdbcConnection
        ),
        Some(new JDBCTable().copy(numPartitions = Some(3)))
      ) shouldBe 3

      // second precedence: jdbcSchema
      extractDataJob.resolveNumPartitions(
        ExtractJdbcDataConfig(
          jdbcSchema = new JDBCSchema().copy(numPartitions = Some(2)),
          baseOutputDir = new Path("."),
          limit = 1,
          numPartitions = 1,
          parallelism = None,
          cliFullExport = None,
          extractionPredicate = None,
          ignoreExtractionFailure = true,
          cleanOnExtract = true,
          includeTables = Nil,
          excludeTables = Nil,
          outputFormat = FileFormat(),
          data = jdbcConnection,
          audit = jdbcConnection
        ),
        Some(new JDBCTable().copy(numPartitions = None))
      ) shouldBe 2

      // default
      extractDataJob.resolveNumPartitions(
        ExtractJdbcDataConfig(
          jdbcSchema = new JDBCSchema().copy(numPartitions = None),
          baseOutputDir = new Path("."),
          limit = 1,
          numPartitions = 1,
          parallelism = None,
          cliFullExport = None,
          extractionPredicate = None,
          ignoreExtractionFailure = true,
          cleanOnExtract = true,
          includeTables = Nil,
          excludeTables = Nil,
          outputFormat = FileFormat(),
          data = jdbcConnection,
          audit = jdbcConnection
        ),
        Some(new JDBCTable().copy(numPartitions = None))
      ) shouldBe 1
    }
  }

  "resolveTableStringPartitionFunc" should "return string partition function with precedence" in {
    new WithSettings() {
      private val extractDataJob = new ExtractDataJob(settings.schemaHandler())
      // Table definition first
      val jdbcConnection: Settings.ConnectionInfo = settings.appConfig.connections("test-pg")
      extractDataJob.resolveTableStringPartitionFunc(
        ExtractJdbcDataConfig(
          jdbcSchema = new JDBCSchema().copy(stringPartitionFunc = Some("schemaPartitionFunc")),
          baseOutputDir = new Path("."),
          limit = 1,
          numPartitions = 1,
          parallelism = None,
          cliFullExport = None,
          extractionPredicate = None,
          ignoreExtractionFailure = true,
          cleanOnExtract = true,
          includeTables = Nil,
          excludeTables = Nil,
          outputFormat = FileFormat(),
          data = jdbcConnection,
          audit = jdbcConnection
        ),
        Some(new JDBCTable().copy(stringPartitionFunc = Some("tablePartitionFunc")))
      ) shouldBe Some("tablePartitionFunc")

      // second precedence: jdbcSchema
      extractDataJob.resolveTableStringPartitionFunc(
        ExtractJdbcDataConfig(
          jdbcSchema = new JDBCSchema().copy(stringPartitionFunc = Some("schemaPartitionFunc")),
          baseOutputDir = new Path("."),
          limit = 1,
          numPartitions = 1,
          parallelism = None,
          cliFullExport = None,
          extractionPredicate = None,
          ignoreExtractionFailure = true,
          cleanOnExtract = true,
          includeTables = Nil,
          excludeTables = Nil,
          outputFormat = FileFormat(),
          data = jdbcConnection,
          audit = jdbcConnection
        ),
        Some(new JDBCTable().copy(stringPartitionFunc = None))
      ) shouldBe Some("schemaPartitionFunc")

      // default
      extractDataJob.resolveTableStringPartitionFunc(
        ExtractJdbcDataConfig(
          jdbcSchema = new JDBCSchema().copy(stringPartitionFunc = None),
          baseOutputDir = new Path("."),
          limit = 1,
          numPartitions = 1,
          parallelism = None,
          cliFullExport = None,
          extractionPredicate = None,
          ignoreExtractionFailure = true,
          cleanOnExtract = true,
          includeTables = Nil,
          excludeTables = Nil,
          outputFormat = FileFormat(),
          data = jdbcConnection,
          audit = jdbcConnection
        ),
        Some(new JDBCTable().copy(stringPartitionFunc = None))
      ) shouldBe Some("abs( hashtext({{col}}) % {{nb_partitions}} )")

      extractDataJob.resolveTableStringPartitionFunc(
        ExtractJdbcDataConfig(
          jdbcSchema = new JDBCSchema().copy(stringPartitionFunc = None),
          baseOutputDir = new Path("."),
          limit = 1,
          numPartitions = 1,
          parallelism = None,
          cliFullExport = None,
          extractionPredicate = None,
          ignoreExtractionFailure = true,
          cleanOnExtract = true,
          includeTables = Nil,
          excludeTables = Nil,
          outputFormat = FileFormat(),
          data = jdbcConnection.copy(options = Map("url" -> "jdbc:as400//localhost")),
          audit = jdbcConnection
        ),
        Some(new JDBCTable().copy(stringPartitionFunc = None))
      ) shouldBe None
    }
  }

  "resolvePartitionColumn" should "return partition column with precedence" in {
    new WithSettings() {
      private val extractDataJob = new ExtractDataJob(settings.schemaHandler())
      // Table definition first
      val jdbcConnection = settings.appConfig.connections("test-pg")
      extractDataJob.resolvePartitionColumn(
        ExtractJdbcDataConfig(
          jdbcSchema = new JDBCSchema().copy(partitionColumn = Some("schemaPartitionColumn")),
          baseOutputDir = new Path("."),
          limit = 1,
          numPartitions = 1,
          parallelism = None,
          cliFullExport = None,
          extractionPredicate = None,
          ignoreExtractionFailure = true,
          cleanOnExtract = true,
          includeTables = Nil,
          excludeTables = Nil,
          outputFormat = FileFormat(),
          data = jdbcConnection,
          audit = jdbcConnection
        ),
        Some(new JDBCTable().copy(partitionColumn = Some("tablePartitionColumn")))
      ) shouldBe Some("tablePartitionColumn")

      // second precedence: jdbcSchema
      extractDataJob.resolvePartitionColumn(
        ExtractJdbcDataConfig(
          jdbcSchema = new JDBCSchema().copy(partitionColumn = Some("schemaPartitionColumn")),
          baseOutputDir = new Path("."),
          limit = 1,
          numPartitions = 1,
          parallelism = None,
          cliFullExport = None,
          extractionPredicate = None,
          ignoreExtractionFailure = true,
          cleanOnExtract = true,
          includeTables = Nil,
          excludeTables = Nil,
          outputFormat = FileFormat(),
          data = jdbcConnection,
          audit = jdbcConnection
        ),
        Some(new JDBCTable().copy(partitionColumn = None))
      ) shouldBe Some("schemaPartitionColumn")

      // none
      extractDataJob.resolvePartitionColumn(
        ExtractJdbcDataConfig(
          jdbcSchema = new JDBCSchema().copy(partitionColumn = None),
          baseOutputDir = new Path("."),
          limit = 1,
          numPartitions = 1,
          parallelism = None,
          cliFullExport = None,
          extractionPredicate = None,
          ignoreExtractionFailure = true,
          cleanOnExtract = true,
          includeTables = Nil,
          excludeTables = Nil,
          outputFormat = FileFormat(),
          data = jdbcConnection,
          audit = jdbcConnection
        ),
        Some(new JDBCTable().copy(partitionColumn = None))
      ) shouldBe None
    }
  }

  "extractPartitionToFile" should "extract data to a file without index when extract config is not partitionned" ignore {
    new WithSettings() {
      private val extractDataJob = new ExtractDataJob(settings.schemaHandler())
      val jdbcConnection = settings.appConfig.connections("test-pg")
      val conn = DriverManager.getConnection(
        jdbcConnection.options("url"),
        jdbcConnection.options("user"),
        jdbcConnection.options("password")
      )
      val st = conn.createStatement()
      st.execute(initSQL)
      private val outputFolder: File = better.files.File(starlakeTestRoot + "/data")
      outputFolder.createDirectory()
      private val unpartitionnedConfig: TableExtractDataConfig =
        TableExtractDataConfig(
          "d",
          "t",
          None,
          Nil,
          true,
          Some(1000),
          new Path(outputFolder.pathAsString),
          None,
          None
        )
      val sinkResult = extractDataJob.sinkPartitionToFile(
        FileFormat().fillWithDefault(),
        unpartitionnedConfig,
        "unpartitionned",
        st.executeQuery("SELECT * FROM test_table ORDER BY c_str"),
        None
      )(settings.storageHandler())
      sinkResult.isSuccess shouldBe true
      private val outputFileName = s"t-${unpartitionnedConfig.extractionDateTime}.csv"
      outputFolder.list.map(_.name).toList should contain theSameElementsAs List(
        outputFileName
      )

      (outputFolder / outputFileName).contentAsString shouldBe
      """"c_str";"c_short";"c_int";"c_long";"c_decimal";"c_ts";"c_date"
          |"A\"";"0";"1";"2";"3.4";"2003-02-01T00:00:00.000+01:00";"2003-02-02"
          |"BÉ";"5";"6";"7";"8.9";"2003-12-24T00:00:00.000+01:00";"2003-12-25"
          |"";"";"";"";"";"";""
          |""".stripMargin
    }
  }

  it should "extract data to a file with index when extract config is partitionned" in {
    new WithSettings() {
      private val extractDataJob = new ExtractDataJob(settings.schemaHandler())
      val jdbcConnection = settings.appConfig.connections("test-pg")
      val conn = DriverManager.getConnection(
        jdbcConnection.options("url"),
        jdbcConnection.options("user"),
        jdbcConnection.options("password")
      )
      val st = conn.createStatement()
      st.execute(initSQL)
      private val outputFolder: File = better.files.File(starlakeTestRoot + "/data")
      outputFolder.createDirectory()
      private val partitionnedConfig: TableExtractDataConfig =
        TableExtractDataConfig(
          "d",
          "t",
          None,
          Nil,
          true,
          Some(1000),
          new Path(outputFolder.pathAsString),
          None,
          Some(
            PartitionConfig(
              "pColumn",
              PrimitiveType.int,
              None,
              2
            )
          )
        )
      val sinkResult = extractDataJob.sinkPartitionToFile(
        FileFormat().fillWithDefault(),
        partitionnedConfig,
        "partitionned",
        st.executeQuery("SELECT * FROM test_table ORDER BY c_str"),
        Some(1)
      )(settings.storageHandler())
      sinkResult.isSuccess shouldBe true
      outputFolder.list.map(_.name).toList should contain theSameElementsAs List(
        s"t-${partitionnedConfig.extractionDateTime}-1.csv"
      )
    }
  }

  // Using hdfs client create missing folders automatically so testing "not extract data to a folder that doesn't exists should fail" is not necessary

  it should "consider CSV output settings" in {
    new WithSettings() {
      private val extractDataJob = new ExtractDataJob(settings.schemaHandler())
      val jdbcConnection = settings.appConfig.connections("test-pg")
      val conn = DriverManager.getConnection(
        jdbcConnection.options("url"),
        jdbcConnection.options("user"),
        jdbcConnection.options("password")
      )
      val st = conn.createStatement()
      st.execute(initSQL)
      private val outputFolder: File = better.files.File(starlakeTestRoot + "/data")
      outputFolder.createDirectory()
      private val unpartitionnedConfig: TableExtractDataConfig =
        TableExtractDataConfig(
          "d",
          "t",
          None,
          Nil,
          true,
          Some(1000),
          new Path(outputFolder.pathAsString),
          None,
          None
        )
      val sinkResult = extractDataJob.sinkPartitionToFile(
        FileFormat(
          encoding = Some("ISO-8859-1"),
          withHeader = Some(false),
          separator = Some("|"),
          quote = Some("'"),
          escape = Some("?"),
          nullValue = Some("NULL_VALUE"),
          datePattern = Some("dd-MM-yyyy"),
          timestampPattern = Some("dd-MM-yyyy HH:mm:ss")
        ),
        unpartitionnedConfig,
        "unpartitionned",
        st.executeQuery("SELECT * FROM test_table ORDER BY c_str"),
        None
      )(settings.storageHandler())
      sinkResult.isSuccess shouldBe true
      private val outputFileName = s"t-${unpartitionnedConfig.extractionDateTime}.csv"
      outputFolder.list.map(_.name).toList should contain theSameElementsAs List(
        outputFileName
      )
      (outputFolder / outputFileName).contentAsString(StandardCharsets.ISO_8859_1) shouldBe
      """'A"'|'0'|'1'|'2'|'3.4'|'01-02-2003 00:00:00'|'02-02-2003'
          |'BÉ'|'5'|'6'|'7'|'8.9'|'24-12-2003 00:00:00'|'25-12-2003'
          |'NULL_VALUE'|'NULL_VALUE'|'NULL_VALUE'|'NULL_VALUE'|'NULL_VALUE'|'NULL_VALUE'|'NULL_VALUE'
          |""".stripMargin
    }
  }

  "createDomainOutputDir" should "create nested folders" in {
    new WithSettings() {
      private val extractDataJob = new ExtractDataJob(settings.schemaHandler())
      val outputDir = extractDataJob.createDomainOutputDir(
        new Path(starlakeTestRoot, "subfolder/subsubfolder"),
        "domain"
      )(settings.storageHandler())
      File(outputDir.toString).exists shouldBe true
      outputDir.toString should endWith("/subfolder/subsubfolder/domain")
    }
  }

  "computeEligibleTablesForExtraction" should "not change the list of tables when table inclusion is empty" in {
    new WithSettings() {
      private val extractDataJob = new ExtractDataJob(settings.schemaHandler())
      val updatedJdbcSchemas = extractDataJob.computeEligibleTablesForExtraction(
        JDBCSchema(tables =
          List(
            new JDBCTable().copy(name = "T1"),
            new JDBCTable().copy(name = "T2"),
            new JDBCTable().copy(name = "*")
          )
        ),
        Nil,
        Nil
      )
      updatedJdbcSchemas.tables should contain theSameElementsAs List(
        new JDBCTable().copy(name = "T1"),
        new JDBCTable().copy(name = "T2"),
        new JDBCTable().copy(name = "*")
      )
    }
  }

  it should "change the list of tables with an intersection of table inclusion and the configured list" in {
    new WithSettings() {
      private val extractDataJob = new ExtractDataJob(settings.schemaHandler())
      val updatedJdbcSchemas = extractDataJob.computeEligibleTablesForExtraction(
        JDBCSchema(tables =
          List(
            new JDBCTable().copy(name = "T1"),
            new JDBCTable().copy(name = "T2"),
            new JDBCTable().copy(name = "T3")
          )
        ),
        List("T2"),
        Nil
      )
      updatedJdbcSchemas.tables should contain theSameElementsAs List(
        new JDBCTable().copy(name = "T2")
      )
    }
  }

  it should "change the list of tables and consider * as the configuration of table for table's included but not declared" in {
    new WithSettings() {
      private val extractDataJob = new ExtractDataJob(settings.schemaHandler())
      val updatedJdbcSchemas = extractDataJob.computeEligibleTablesForExtraction(
        JDBCSchema(tables =
          List(
            new JDBCTable().copy(name = "T1"),
            new JDBCTable().copy(name = "T2"),
            new JDBCTable().copy(name = "*", fullExport = Some(true))
          )
        ),
        List("T2", "T3", "T4"),
        Nil
      )
      updatedJdbcSchemas.tables should contain theSameElementsAs List(
        new JDBCTable().copy(name = "T2"),
        new JDBCTable().copy(name = "T3", fullExport = Some(true)),
        new JDBCTable().copy(name = "T4", fullExport = Some(true))
      )
    }
  }

  it should "change fill list of tables with included one since empty tables is like *" in {
    new WithSettings() {
      private val extractDataJob = new ExtractDataJob(settings.schemaHandler())
      val updatedJdbcSchemas = extractDataJob.computeEligibleTablesForExtraction(
        JDBCSchema(tables = Nil),
        List("T2", "T3", "T4"),
        Nil
      )
      updatedJdbcSchemas.tables should contain theSameElementsAs List(
        new JDBCTable().copy(name = "T2"),
        new JDBCTable().copy(name = "T3"),
        new JDBCTable().copy(name = "T4")
      )
    }
  }

  "extractTableData" should "extract unpartitionned table" in {
    new WithSettings() {
      private val extractDataJob = new ExtractDataJob(settings.schemaHandler())
      val jdbcConnection = settings.appConfig.connections("test-pg")
      val conn = DriverManager.getConnection(
        jdbcConnection.options("url"),
        jdbcConnection.options("user"),
        jdbcConnection.options("password")
      )
      val st = conn.createStatement()
      st.execute(initSQL)
      private val outputFolder: File = better.files.File(starlakeTestRoot + "/data")
      outputFolder.createDirectory()
      implicit val extractExecutionContext: ExtractExecutionContext =
        new ExtractExecutionContext(None)
      val columns = extractDataJob.initExportAuditTable(settings.appConfig.connections("test-pg"))
      private val unpartitionnedConfig: TableExtractDataConfig =
        TableExtractDataConfig(
          "public",
          "test_table",
          None,
          List(
            new TableAttribute().copy(
              name = "c_str"
            ),
            new TableAttribute().copy(
              name = "c_short"
            ),
            new TableAttribute().copy(
              name = "c_int"
            ),
            new TableAttribute().copy(
              name = "c_long"
            ),
            new TableAttribute().copy(
              name = "c_decimal"
            ),
            new TableAttribute().copy(
              name = "c_ts"
            ),
            new TableAttribute().copy(
              name = "c_date"
            )
          ),
          true,
          Some(1000),
          new Path(outputFolder.pathAsString),
          None,
          None
        )
      val sinkResult = extractDataJob.extractTableData(
        ExtractJdbcDataConfig(
          JDBCSchema().copy(schema = "public"),
          new Path(outputFolder.pathAsString),
          0,
          1,
          None,
          Some(true),
          None,
          false,
          false,
          Nil,
          Nil,
          FileFormat(),
          jdbcConnection,
          jdbcConnection
        ),
        unpartitionnedConfig,
        "test",
        columns,
        (_, resultSet, _) => {
          val rows = resultSet
            .asIteratorOf(rowAsString)
            .toList
          rows should contain theSameElementsAs
          List(
            "A\", 0, 1, 2, 3.4, 2003-02-01 00:00:00.0, 2003-02-02",
            "BÉ, 5, 6, 7, 8.9, 2003-12-24 00:00:00.0, 2003-12-25",
            "null, null, null, null, null, null, null"
          )
          Success(rows.length)
        }
      )
      sinkResult.isSuccess shouldBe true
      st.executeQuery(
        "select domain, schema, last_ts, last_date, last_long, last_decimal, mode, count, success, message, step from audit.sl_last_export"
      ).asIteratorOf(rowAsString)
        .toList should contain theSameElementsAs
      List(
        "public, test_table, null, null, null, null, OVERWRITE, 3, true, FULL, ALL"
      )
    }
  }

  it should "extract unpartitionned table from sql" in {
    new WithSettings() {
      private val extractDataJob = new ExtractDataJob(settings.schemaHandler())
      val jdbcConnection = settings.appConfig.connections("test-pg")
      val conn = DriverManager.getConnection(
        jdbcConnection.options("url"),
        jdbcConnection.options("user"),
        jdbcConnection.options("password")
      )
      val st = conn.createStatement()
      st.execute(initSQL)
      private val outputFolder: File = better.files.File(starlakeTestRoot + "/data")
      outputFolder.createDirectory()
      implicit val extractExecutionContext: ExtractExecutionContext =
        new ExtractExecutionContext(None)
      val columns = extractDataJob.initExportAuditTable(settings.appConfig.connections("test-pg"))
      private val unpartitionnedConfig: TableExtractDataConfig =
        TableExtractDataConfig(
          "public",
          "test_table",
          Some(
            "select c_str as c_str_sql, c_short, c_int, c_long, c_decimal, c_ts, c_date from public.test_table"
          ),
          List(
            new TableAttribute().copy(
              name = "c_str_sql"
            ),
            new TableAttribute().copy(
              name = "c_short"
            ),
            new TableAttribute().copy(
              name = "c_int"
            ),
            new TableAttribute().copy(
              name = "c_long"
            ),
            new TableAttribute().copy(
              name = "c_decimal"
            ),
            new TableAttribute().copy(
              name = "c_ts"
            ),
            new TableAttribute().copy(
              name = "c_date"
            )
          ),
          true,
          Some(1000),
          new Path(outputFolder.pathAsString),
          None,
          None
        )
      val sinkResult = extractDataJob.extractTableData(
        ExtractJdbcDataConfig(
          JDBCSchema().copy(schema = "public"),
          new Path(outputFolder.pathAsString),
          0,
          1,
          None,
          Some(true),
          None,
          false,
          false,
          Nil,
          Nil,
          FileFormat(),
          jdbcConnection,
          jdbcConnection
        ),
        unpartitionnedConfig,
        "test",
        columns,
        (_, resultSet, _) => {
          val rows = resultSet
            .asIteratorOf(rowAsString)
            .toList
          rows should contain theSameElementsAs
          List(
            "A\", 0, 1, 2, 3.4, 2003-02-01 00:00:00.0, 2003-02-02",
            "BÉ, 5, 6, 7, 8.9, 2003-12-24 00:00:00.0, 2003-12-25",
            "null, null, null, null, null, null, null"
          )
          Success(rows.length)
        }
      )
      sinkResult.isSuccess shouldBe true
      st.executeQuery(
        "select domain, schema, last_ts, last_date, last_long, last_decimal, mode, count, success, message, step from audit.sl_last_export"
      ).asIteratorOf(rowAsString)
        .toList should contain theSameElementsAs
      List(
        "public, test_table, null, null, null, null, OVERWRITE, 3, true, FULL, ALL"
      )
    }
  }

  it should "filter out data" in {
    new WithSettings() {
      private val extractDataJob = new ExtractDataJob(settings.schemaHandler())
      val jdbcConnection = settings.appConfig.connections("test-pg")
      val conn = DriverManager.getConnection(
        jdbcConnection.options("url"),
        jdbcConnection.options("user"),
        jdbcConnection.options("password")
      )
      val st = conn.createStatement()
      st.execute(initSQL)
      private val outputFolder: File = better.files.File(starlakeTestRoot + "/data")
      outputFolder.createDirectory()
      implicit val extractExecutionContext: ExtractExecutionContext =
        new ExtractExecutionContext(None)
      val columns = extractDataJob.initExportAuditTable(settings.appConfig.connections("test-pg"))
      private val unpartitionnedConfig: TableExtractDataConfig =
        TableExtractDataConfig(
          "public",
          "test_table",
          None,
          List(
            new TableAttribute().copy(
              name = "c_str"
            ),
            new TableAttribute().copy(
              name = "c_short"
            ),
            new TableAttribute().copy(
              name = "c_int"
            ),
            new TableAttribute().copy(
              name = "c_long"
            ),
            new TableAttribute().copy(
              name = "c_decimal"
            ),
            new TableAttribute().copy(
              name = "c_ts"
            ),
            new TableAttribute().copy(
              name = "c_date"
            )
          ),
          true,
          Some(1000),
          new Path(outputFolder.pathAsString),
          Some("c_date IS NOT NULL"),
          None
        )
      val sinkResult = extractDataJob.extractTableData(
        ExtractJdbcDataConfig(
          JDBCSchema().copy(schema = "public"),
          new Path(outputFolder.pathAsString),
          0,
          1,
          None,
          Some(true),
          None,
          false,
          false,
          Nil,
          Nil,
          FileFormat(),
          jdbcConnection,
          jdbcConnection
        ),
        unpartitionnedConfig,
        "test",
        columns,
        (_, resultSet, _) => {
          val rows = resultSet
            .asIteratorOf(rowAsString)
            .toList
          rows should contain theSameElementsAs
          List(
            "A\", 0, 1, 2, 3.4, 2003-02-01 00:00:00.0, 2003-02-02",
            "BÉ, 5, 6, 7, 8.9, 2003-12-24 00:00:00.0, 2003-12-25"
          )
          Success(rows.length)
        }
      )
      sinkResult.isSuccess shouldBe true
      st.executeQuery(
        "select domain, schema, last_ts, last_date, last_long, last_decimal, mode, count, success, message, step from audit.sl_last_export"
      ).asIteratorOf(rowAsString)
        .toList should contain theSameElementsAs
      List(
        "public, test_table, null, null, null, null, OVERWRITE, 2, true, FULL, ALL"
      )
    }
  }

  it should "project restricted columns" in {
    new WithSettings() {
      private val extractDataJob = new ExtractDataJob(settings.schemaHandler())
      val jdbcConnection = settings.appConfig.connections("test-pg")
      val conn = DriverManager.getConnection(
        jdbcConnection.options("url"),
        jdbcConnection.options("user"),
        jdbcConnection.options("password")
      )
      val st = conn.createStatement()
      st.execute(initSQL)
      private val outputFolder: File = better.files.File(starlakeTestRoot + "/data")
      outputFolder.createDirectory()
      implicit val extractExecutionContext: ExtractExecutionContext =
        new ExtractExecutionContext(None)
      val columns = extractDataJob.initExportAuditTable(settings.appConfig.connections("test-pg"))
      private val unpartitionnedConfig: TableExtractDataConfig =
        TableExtractDataConfig(
          "public",
          "test_table",
          None,
          List(
            new TableAttribute().copy(
              name = "c_str"
            ),
            new TableAttribute().copy(
              name = "c_short"
            ),
            new TableAttribute().copy(
              name = "c_date"
            )
          ),
          true,
          Some(1000),
          new Path(outputFolder.pathAsString),
          None,
          None
        )
      val sinkResult = extractDataJob.extractTableData(
        ExtractJdbcDataConfig(
          JDBCSchema().copy(schema = "public"),
          new Path(outputFolder.pathAsString),
          0,
          1,
          None,
          Some(true),
          None,
          false,
          false,
          Nil,
          Nil,
          FileFormat(),
          jdbcConnection,
          jdbcConnection
        ),
        unpartitionnedConfig,
        "test",
        columns,
        (_, resultSet, _) => {
          val rows = resultSet
            .asIteratorOf(rowAsString)
            .toList
          rows should contain theSameElementsAs
          List(
            "A\", 0, 2003-02-02",
            "BÉ, 5, 2003-12-25",
            "null, null, null"
          )
          Success(rows.length)
        }
      )
      sinkResult.isSuccess shouldBe true
      st.executeQuery(
        "select domain, schema, last_ts, last_date, last_long, last_decimal, mode, count, success, message, step from audit.sl_last_export"
      ).asIteratorOf(rowAsString)
        .toList should contain theSameElementsAs
      List(
        "public, test_table, null, null, null, null, OVERWRITE, 3, true, FULL, ALL"
      )
    }
  }

  it should "limit the number of extracted rows" in {
    new WithSettings() {
      private val extractDataJob = new ExtractDataJob(settings.schemaHandler())
      val jdbcConnection = settings.appConfig.connections("test-pg")
      val conn = DriverManager.getConnection(
        jdbcConnection.options("url"),
        jdbcConnection.options("user"),
        jdbcConnection.options("password")
      )
      val st = conn.createStatement()
      st.execute(initSQL)
      private val outputFolder: File = better.files.File(starlakeTestRoot + "/data")
      outputFolder.createDirectory()
      implicit val extractExecutionContext: ExtractExecutionContext =
        new ExtractExecutionContext(None)
      val columns = extractDataJob.initExportAuditTable(settings.appConfig.connections("test-pg"))
      private val unpartitionnedConfig: TableExtractDataConfig =
        TableExtractDataConfig(
          "public",
          "test_table",
          None,
          List(
            new TableAttribute().copy(
              name = "c_str"
            ),
            new TableAttribute().copy(
              name = "c_short"
            ),
            new TableAttribute().copy(
              name = "c_int"
            ),
            new TableAttribute().copy(
              name = "c_long"
            ),
            new TableAttribute().copy(
              name = "c_decimal"
            ),
            new TableAttribute().copy(
              name = "c_ts"
            ),
            new TableAttribute().copy(
              name = "c_date"
            )
          ),
          true,
          Some(1000),
          new Path(outputFolder.pathAsString),
          None,
          None
        )
      val sinkResult = extractDataJob.extractTableData(
        ExtractJdbcDataConfig(
          JDBCSchema().copy(schema = "public"),
          new Path(outputFolder.pathAsString),
          1,
          1,
          None,
          Some(true),
          None,
          false,
          false,
          Nil,
          Nil,
          FileFormat(),
          jdbcConnection,
          jdbcConnection
        ),
        unpartitionnedConfig,
        "test",
        columns,
        (_, resultSet, _) => {
          val rows = resultSet
            .asIteratorOf(rowAsString)
            .toList
          rows should contain atMostOneElementOf
          List(
            "A\", 0, 1, 2, 3.4, 2003-02-01 00:00:00.0, 2003-02-02",
            "BÉ, 5, 6, 7, 8.9, 2003-12-24 00:00:00.0, 2003-12-25",
            "null, null, null, null, null, null, null"
          )
          Success(rows.length)
        }
      )
      sinkResult.isSuccess shouldBe true
      st.executeQuery(
        "select domain, schema, last_ts, last_date, last_long, last_decimal, mode, count, success, message, step from audit.sl_last_export"
      ).asIteratorOf(rowAsString)
        .toList should contain theSameElementsAs
      List(
        "public, test_table, null, null, null, null, OVERWRITE, 1, true, FULL, ALL"
      )
    }
  }

  "extractTablePartionnedData" should "extract partitionned table" in {
    new WithSettings() {
      private val extractDataJob = new ExtractDataJob(settings.schemaHandler())
      val jdbcConnection = settings.appConfig.connections("test-pg")
      val conn = DriverManager.getConnection(
        jdbcConnection.options("url"),
        jdbcConnection.options("user"),
        jdbcConnection.options("password")
      )
      val st = conn.createStatement()
      st.execute(initSQL)
      private val outputFolder: File = better.files.File(starlakeTestRoot + "/data")
      outputFolder.createDirectory()
      implicit val extractExecutionContext: ExtractExecutionContext =
        new ExtractExecutionContext(None)
      val columns = extractDataJob.initExportAuditTable(settings.appConfig.connections("test-pg"))
      private val partitionnedConfig: TableExtractDataConfig =
        TableExtractDataConfig(
          "public",
          "test_table",
          None,
          List(
            new TableAttribute().copy(
              name = "c_str"
            ),
            new TableAttribute().copy(
              name = "c_short"
            ),
            new TableAttribute().copy(
              name = "c_int"
            ),
            new TableAttribute().copy(
              name = "c_long"
            ),
            new TableAttribute().copy(
              name = "c_decimal"
            ),
            new TableAttribute().copy(
              name = "c_ts"
            ),
            new TableAttribute().copy(
              name = "c_date"
            )
          ),
          true,
          Some(1000),
          new Path(outputFolder.pathAsString),
          None,
          Some(
            PartitionConfig(
              "c_short",
              PrimitiveType.short,
              Some("abs( hashtext({{col}}) % {{nb_partitions}} )"),
              2
            )
          )
        )
      val allRows = ListBuffer[String]()
      val sinkResult = extractDataJob.extractTableData(
        ExtractJdbcDataConfig(
          JDBCSchema().copy(schema = "public"),
          new Path(outputFolder.pathAsString),
          0,
          2,
          None,
          Some(true),
          None,
          false,
          false,
          Nil,
          Nil,
          FileFormat(),
          jdbcConnection,
          jdbcConnection
        ),
        partitionnedConfig,
        "test",
        columns,
        (_, resultSet, _) => {
          val resultSetRows = resultSet
            .asIteratorOf(rowAsString)
            .toList
          allRows.appendAll(resultSetRows)
          Success(resultSetRows.length)
        }
      )
      allRows should contain theSameElementsAs
      List(
        "A\", 0, 1, 2, 3.4, 2003-02-01 00:00:00.0, 2003-02-02",
        "BÉ, 5, 6, 7, 8.9, 2003-12-24 00:00:00.0, 2003-12-25"
      )
      sinkResult.isSuccess shouldBe true
      st.executeQuery(
        "select domain, schema, last_ts, last_date, last_long, last_decimal, mode, count, success, message, step from audit.sl_last_export"
      ).asIteratorOf(rowAsString)
        .toList should contain theSameElementsAs
      List(
        "public, test_table, null, null, 5, null, OVERWRITE, 1, true, c_short, 1",
        "public, test_table, null, null, 5, null, OVERWRITE, 1, true, c_short, 0",
        "public, test_table, null, null, 5, null, OVERWRITE, 2, true, c_short, ALL"
      )
    }
  }

  it should "extract partitionned table from sql" in {
    new WithSettings() {
      private val extractDataJob = new ExtractDataJob(settings.schemaHandler())
      val jdbcConnection = settings.appConfig.connections("test-pg")
      val conn = DriverManager.getConnection(
        jdbcConnection.options("url"),
        jdbcConnection.options("user"),
        jdbcConnection.options("password")
      )
      val st = conn.createStatement()
      st.execute(initSQL)
      private val outputFolder: File = better.files.File(starlakeTestRoot + "/data")
      outputFolder.createDirectory()
      implicit val extractExecutionContext: ExtractExecutionContext =
        new ExtractExecutionContext(None)
      val columns = extractDataJob.initExportAuditTable(settings.appConfig.connections("test-pg"))
      private val partitionnedConfig: TableExtractDataConfig =
        TableExtractDataConfig(
          "public",
          "test_table",
          Some(
            "select c_str as c_str_sql, c_short, c_int, c_long, c_decimal, c_ts, c_date from public.test_table"
          ),
          List(
            new TableAttribute().copy(
              name = "c_str_sql"
            ),
            new TableAttribute().copy(
              name = "c_short"
            ),
            new TableAttribute().copy(
              name = "c_int"
            ),
            new TableAttribute().copy(
              name = "c_long"
            ),
            new TableAttribute().copy(
              name = "c_decimal"
            ),
            new TableAttribute().copy(
              name = "c_ts"
            ),
            new TableAttribute().copy(
              name = "c_date"
            )
          ),
          true,
          Some(1000),
          new Path(outputFolder.pathAsString),
          None,
          Some(
            PartitionConfig(
              "c_short",
              PrimitiveType.short,
              Some("abs( hashtext({{col}}) % {{nb_partitions}} )"),
              2
            )
          )
        )
      val allRows = ListBuffer[String]()
      val sinkResult = extractDataJob.extractTableData(
        ExtractJdbcDataConfig(
          JDBCSchema().copy(schema = "public"),
          new Path(outputFolder.pathAsString),
          0,
          2,
          None,
          Some(true),
          None,
          false,
          false,
          Nil,
          Nil,
          FileFormat(),
          jdbcConnection,
          jdbcConnection
        ),
        partitionnedConfig,
        "test",
        columns,
        (_, resultSet, _) => {
          val resultSetRows = resultSet
            .asIteratorOf(rowAsString)
            .toList
          allRows.appendAll(resultSetRows)
          Success(resultSetRows.length)
        }
      )
      allRows should contain theSameElementsAs
      List(
        "A\", 0, 1, 2, 3.4, 2003-02-01 00:00:00.0, 2003-02-02",
        "BÉ, 5, 6, 7, 8.9, 2003-12-24 00:00:00.0, 2003-12-25"
      )
      sinkResult.isSuccess shouldBe true
      st.executeQuery(
        "select domain, schema, last_ts, last_date, last_long, last_decimal, mode, count, success, message, step from audit.sl_last_export"
      ).asIteratorOf(rowAsString)
        .toList should contain theSameElementsAs
      List(
        "public, test_table, null, null, 5, null, OVERWRITE, 1, true, c_short, 1",
        "public, test_table, null, null, 5, null, OVERWRITE, 1, true, c_short, 0",
        "public, test_table, null, null, 5, null, OVERWRITE, 2, true, c_short, ALL"
      )
    }
  }

  it should "filter out data" in {
    new WithSettings() {
      private val extractDataJob = new ExtractDataJob(settings.schemaHandler())
      val jdbcConnection = settings.appConfig.connections("test-pg")
      val conn = DriverManager.getConnection(
        jdbcConnection.options("url"),
        jdbcConnection.options("user"),
        jdbcConnection.options("password")
      )
      val st = conn.createStatement()
      st.execute(initSQL)
      private val outputFolder: File = better.files.File(starlakeTestRoot + "/data")
      outputFolder.createDirectory()
      implicit val extractExecutionContext: ExtractExecutionContext =
        new ExtractExecutionContext(None)
      val columns = extractDataJob.initExportAuditTable(settings.appConfig.connections("test-pg"))
      private val partitionnedConfig: TableExtractDataConfig =
        TableExtractDataConfig(
          "public",
          "test_table",
          None,
          List(
            new TableAttribute().copy(
              name = "c_str"
            ),
            new TableAttribute().copy(
              name = "c_short"
            ),
            new TableAttribute().copy(
              name = "c_int"
            ),
            new TableAttribute().copy(
              name = "c_long"
            ),
            new TableAttribute().copy(
              name = "c_decimal"
            ),
            new TableAttribute().copy(
              name = "c_ts"
            ),
            new TableAttribute().copy(
              name = "c_date"
            )
          ),
          true,
          Some(1000),
          new Path(outputFolder.pathAsString),
          Some("c_short < 4"),
          Some(
            PartitionConfig(
              "c_short",
              PrimitiveType.short,
              Some("abs( hashtext({{col}}) % {{nb_partitions}} )"),
              2
            )
          )
        )
      val allRows = ListBuffer[String]()
      val sinkResult = extractDataJob.extractTableData(
        ExtractJdbcDataConfig(
          JDBCSchema().copy(schema = "public"),
          new Path(outputFolder.pathAsString),
          0,
          2,
          None,
          Some(true),
          None,
          false,
          false,
          Nil,
          Nil,
          FileFormat(),
          jdbcConnection,
          jdbcConnection
        ),
        partitionnedConfig,
        "test",
        columns,
        (_, resultSet, _) => {
          val resultSetRows = resultSet
            .asIteratorOf(rowAsString)
            .toList
          allRows.appendAll(resultSetRows)
          Success(resultSetRows.length)
        }
      )
      allRows should contain theSameElementsAs
      List(
        "A\", 0, 1, 2, 3.4, 2003-02-01 00:00:00.0, 2003-02-02"
      )
      sinkResult.isSuccess shouldBe true
      st.executeQuery(
        "select domain, schema, last_ts, last_date, last_long, last_decimal, mode, count, success, message, step from audit.sl_last_export"
      ).asIteratorOf(rowAsString)
        .toList should contain theSameElementsAs
      List(
        "public, test_table, null, null, 0, null, OVERWRITE, 1, true, c_short, 0",
        "public, test_table, null, null, 0, null, OVERWRITE, 0, true, c_short, 1",
        "public, test_table, null, null, 0, null, OVERWRITE, 1, true, c_short, ALL"
      )
    }
  }

  it should "project restricted columns and don't affect partition column" in {
    new WithSettings() {
      private val extractDataJob = new ExtractDataJob(settings.schemaHandler())
      val jdbcConnection = settings.appConfig.connections("test-pg")
      val conn = DriverManager.getConnection(
        jdbcConnection.options("url"),
        jdbcConnection.options("user"),
        jdbcConnection.options("password")
      )
      val st = conn.createStatement()
      st.execute(initSQL)
      private val outputFolder: File = better.files.File(starlakeTestRoot + "/data")
      outputFolder.createDirectory()
      implicit val extractExecutionContext: ExtractExecutionContext =
        new ExtractExecutionContext(None)
      val columns = extractDataJob.initExportAuditTable(settings.appConfig.connections("test-pg"))
      private val partitionnedConfig: TableExtractDataConfig =
        TableExtractDataConfig(
          "public",
          "test_table",
          None,
          List(
            new TableAttribute().copy(
              name = "c_str"
            ),
            new TableAttribute().copy(
              name = "c_int"
            ),
            new TableAttribute().copy(
              name = "c_ts"
            )
          ),
          true,
          Some(1000),
          new Path(outputFolder.pathAsString),
          None,
          Some(
            PartitionConfig(
              "c_short",
              PrimitiveType.short,
              Some("abs( hashtext({{col}}) % {{nb_partitions}} )"),
              2
            )
          )
        )
      val allRows = ListBuffer[String]()
      val sinkResult = extractDataJob.extractTableData(
        ExtractJdbcDataConfig(
          JDBCSchema().copy(schema = "public"),
          new Path(outputFolder.pathAsString),
          0,
          2,
          None,
          Some(true),
          None,
          false,
          false,
          Nil,
          Nil,
          FileFormat(),
          jdbcConnection,
          jdbcConnection
        ),
        partitionnedConfig,
        "test",
        columns,
        (_, resultSet, _) => {
          val resultSetRows = resultSet
            .asIteratorOf(rowAsString)
            .toList
          allRows.appendAll(resultSetRows)
          Success(resultSetRows.length)
        }
      )
      allRows should contain theSameElementsAs
      List(
        "A\", 1, 2003-02-01 00:00:00.0",
        "BÉ, 6, 2003-12-24 00:00:00.0"
      )
      sinkResult.isSuccess shouldBe true
      st.executeQuery(
        "select domain, schema, last_ts, last_date, last_long, last_decimal, mode, count, success, message, step from audit.sl_last_export"
      ).asIteratorOf(rowAsString)
        .toList should contain theSameElementsAs
      List(
        "public, test_table, null, null, 5, null, OVERWRITE, 1, true, c_short, 1",
        "public, test_table, null, null, 5, null, OVERWRITE, 1, true, c_short, 0",
        "public, test_table, null, null, 5, null, OVERWRITE, 2, true, c_short, ALL"
      )
    }
  }

  it should "rename output columns" in {
    new WithSettings() {
      private val extractDataJob = new ExtractDataJob(settings.schemaHandler())
      val jdbcConnection = settings.appConfig.connections("test-pg")
      val conn = DriverManager.getConnection(
        jdbcConnection.options("url"),
        jdbcConnection.options("user"),
        jdbcConnection.options("password")
      )
      val st = conn.createStatement()
      st.execute(initSQL)
      private val outputFolder: File = better.files.File(starlakeTestRoot + "/data")
      outputFolder.createDirectory()
      implicit val extractExecutionContext: ExtractExecutionContext =
        new ExtractExecutionContext(None)
      val columns = extractDataJob.initExportAuditTable(settings.appConfig.connections("test-pg"))
      private val partitionnedConfig: TableExtractDataConfig =
        TableExtractDataConfig(
          "public",
          "test_table",
          None,
          List(
            new TableAttribute().copy(
              name = "c_str",
              rename = Some("c_str_renamed")
            ),
            new TableAttribute().copy(
              name = "c_short",
              rename = Some("c_short_renamed")
            ),
            new TableAttribute().copy(
              name = "c_int",
              rename = Some("c_int_renamed")
            ),
            new TableAttribute().copy(
              name = "c_long"
            ),
            new TableAttribute().copy(
              name = "c_decimal"
            ),
            new TableAttribute().copy(
              name = "c_ts",
              rename = Some("c_ts_renamed")
            ),
            new TableAttribute().copy(
              name = "c_date"
            )
          ),
          true,
          Some(1000),
          new Path(outputFolder.pathAsString),
          None,
          Some(
            PartitionConfig(
              "c_short",
              PrimitiveType.short,
              Some("abs( hashtext({{col}}) % {{nb_partitions}} )"),
              2
            )
          )
        )
      val allHeaders = ListBuffer[String]()
      val allRows = ListBuffer[String]()
      val sinkResult = extractDataJob.extractTableData(
        ExtractJdbcDataConfig(
          JDBCSchema().copy(schema = "public"),
          new Path(outputFolder.pathAsString),
          0,
          2,
          None,
          Some(true),
          None,
          false,
          false,
          Nil,
          Nil,
          FileFormat(),
          jdbcConnection,
          jdbcConnection
        ),
        partitionnedConfig,
        "test",
        columns,
        (_, resultSet, _) => {
          if (allHeaders.isEmpty) {
            allHeaders.append(resultSet.headerNames)
          }
          val resultSetRows = resultSet
            .asIteratorOf(rowAsString)
            .toList
          allRows.appendAll(resultSetRows)
          Success(resultSetRows.length)
        }
      )
      allRows should contain theSameElementsAs
      List(
        "A\", 0, 1, 2, 3.4, 2003-02-01 00:00:00.0, 2003-02-02",
        "BÉ, 5, 6, 7, 8.9, 2003-12-24 00:00:00.0, 2003-12-25"
      )
      sinkResult.isSuccess shouldBe true
      st.executeQuery(
        "select domain, schema, last_ts, last_date, last_long, last_decimal, mode, count, success, message, step from audit.sl_last_export"
      ).asIteratorOf(rowAsString)
        .toList should contain theSameElementsAs
      List(
        "public, test_table, null, null, 5, null, OVERWRITE, 1, true, c_short, 1",
        "public, test_table, null, null, 5, null, OVERWRITE, 1, true, c_short, 0",
        "public, test_table, null, null, 5, null, OVERWRITE, 2, true, c_short, ALL"
      )
      allHeaders should contain theSameElementsAs List(
        "c_str_renamed, c_short_renamed, c_int_renamed, c_long, c_decimal, c_ts_renamed, c_date"
      )
    }
  }

  it should "partition data on short" in {
    new WithSettings() {
      private val extractDataJob = new ExtractDataJob(settings.schemaHandler())
      val jdbcConnection = settings.appConfig.connections("test-pg")
      val conn = DriverManager.getConnection(
        jdbcConnection.options("url"),
        jdbcConnection.options("user"),
        jdbcConnection.options("password")
      )
      val st = conn.createStatement()
      st.execute(initSQL)
      private val outputFolder: File = better.files.File(starlakeTestRoot + "/data")
      outputFolder.createDirectory()
      implicit val extractExecutionContext: ExtractExecutionContext =
        new ExtractExecutionContext(None)
      val columns = extractDataJob.initExportAuditTable(settings.appConfig.connections("test-pg"))
      private val partitionnedConfig: TableExtractDataConfig =
        TableExtractDataConfig(
          "public",
          "test_table",
          None,
          List(
            new TableAttribute().copy(
              name = "c_str"
            ),
            new TableAttribute().copy(
              name = "c_short"
            ),
            new TableAttribute().copy(
              name = "c_int"
            ),
            new TableAttribute().copy(
              name = "c_long"
            ),
            new TableAttribute().copy(
              name = "c_decimal"
            ),
            new TableAttribute().copy(
              name = "c_ts"
            ),
            new TableAttribute().copy(
              name = "c_date"
            )
          ),
          true,
          Some(1000),
          new Path(outputFolder.pathAsString),
          None,
          Some(
            PartitionConfig(
              "c_short",
              PrimitiveType.short,
              Some("abs( hashtext({{col}}) % {{nb_partitions}} )"),
              2
            )
          )
        )
      val allRows = ListBuffer[String]()
      val sinkResult = extractDataJob.extractTableData(
        ExtractJdbcDataConfig(
          JDBCSchema().copy(schema = "public"),
          new Path(outputFolder.pathAsString),
          0,
          2,
          None,
          Some(true),
          None,
          false,
          false,
          Nil,
          Nil,
          FileFormat(),
          jdbcConnection,
          jdbcConnection
        ),
        partitionnedConfig,
        "test",
        columns,
        (_, resultSet, _) => {
          val resultSetRows = resultSet
            .asIteratorOf(rowAsString)
            .toList
          allRows.appendAll(resultSetRows)
          Success(resultSetRows.length)
        }
      )
      allRows should contain theSameElementsAs
      List(
        "A\", 0, 1, 2, 3.4, 2003-02-01 00:00:00.0, 2003-02-02",
        "BÉ, 5, 6, 7, 8.9, 2003-12-24 00:00:00.0, 2003-12-25"
      )
      sinkResult.isSuccess shouldBe true
      st.executeQuery(
        "select domain, schema, last_ts, last_date, last_long, last_decimal, mode, count, success, message, step from audit.sl_last_export"
      ).asIteratorOf(rowAsString)
        .toList should contain theSameElementsAs
      List(
        "public, test_table, null, null, 5, null, OVERWRITE, 1, true, c_short, 1",
        "public, test_table, null, null, 5, null, OVERWRITE, 1, true, c_short, 0",
        "public, test_table, null, null, 5, null, OVERWRITE, 2, true, c_short, ALL"
      )
    }
  }

  it should "partition data on int" in {
    new WithSettings() {
      private val extractDataJob = new ExtractDataJob(settings.schemaHandler())
      val jdbcConnection = settings.appConfig.connections("test-pg")
      val conn = DriverManager.getConnection(
        jdbcConnection.options("url"),
        jdbcConnection.options("user"),
        jdbcConnection.options("password")
      )
      val st = conn.createStatement()
      st.execute(initSQL)
      private val outputFolder: File = better.files.File(starlakeTestRoot + "/data")
      outputFolder.createDirectory()
      implicit val extractExecutionContext: ExtractExecutionContext =
        new ExtractExecutionContext(None)
      val columns = extractDataJob.initExportAuditTable(settings.appConfig.connections("test-pg"))
      private val partitionnedConfig: TableExtractDataConfig =
        TableExtractDataConfig(
          "public",
          "test_table",
          None,
          List(
            new TableAttribute().copy(
              name = "c_str"
            ),
            new TableAttribute().copy(
              name = "c_short"
            ),
            new TableAttribute().copy(
              name = "c_int"
            ),
            new TableAttribute().copy(
              name = "c_long"
            ),
            new TableAttribute().copy(
              name = "c_decimal"
            ),
            new TableAttribute().copy(
              name = "c_ts"
            ),
            new TableAttribute().copy(
              name = "c_date"
            )
          ),
          true,
          Some(1000),
          new Path(outputFolder.pathAsString),
          None,
          Some(
            PartitionConfig(
              "c_int",
              PrimitiveType.int,
              Some("abs( hashtext({{col}}) % {{nb_partitions}} )"),
              2
            )
          )
        )
      val allRows = ListBuffer[String]()
      val sinkResult = extractDataJob.extractTableData(
        ExtractJdbcDataConfig(
          JDBCSchema().copy(schema = "public"),
          new Path(outputFolder.pathAsString),
          0,
          2,
          None,
          Some(true),
          None,
          false,
          false,
          Nil,
          Nil,
          FileFormat(),
          jdbcConnection,
          jdbcConnection
        ),
        partitionnedConfig,
        "test",
        columns,
        (_, resultSet, _) => {
          val resultSetRows = resultSet
            .asIteratorOf(rowAsString)
            .toList
          allRows.appendAll(resultSetRows)
          Success(resultSetRows.length)
        }
      )
      allRows should contain theSameElementsAs
      List(
        "A\", 0, 1, 2, 3.4, 2003-02-01 00:00:00.0, 2003-02-02",
        "BÉ, 5, 6, 7, 8.9, 2003-12-24 00:00:00.0, 2003-12-25"
      )
      sinkResult.isSuccess shouldBe true
      st.executeQuery(
        "select domain, schema, last_ts, last_date, last_long, last_decimal, mode, count, success, message, step from audit.sl_last_export"
      ).asIteratorOf(rowAsString)
        .toList should contain theSameElementsAs
      List(
        "public, test_table, null, null, 6, null, OVERWRITE, 1, true, c_int, 1",
        "public, test_table, null, null, 6, null, OVERWRITE, 1, true, c_int, 0",
        "public, test_table, null, null, 6, null, OVERWRITE, 2, true, c_int, ALL"
      )
    }
  }

  it should "partition data on long" in {
    new WithSettings() {
      private val extractDataJob = new ExtractDataJob(settings.schemaHandler())
      val jdbcConnection = settings.appConfig.connections("test-pg")
      val conn = DriverManager.getConnection(
        jdbcConnection.options("url"),
        jdbcConnection.options("user"),
        jdbcConnection.options("password")
      )
      val st = conn.createStatement()
      st.execute(initSQL)
      private val outputFolder: File = better.files.File(starlakeTestRoot + "/data")
      outputFolder.createDirectory()
      implicit val extractExecutionContext: ExtractExecutionContext =
        new ExtractExecutionContext(None)
      val columns = extractDataJob.initExportAuditTable(settings.appConfig.connections("test-pg"))
      private val partitionnedConfig: TableExtractDataConfig =
        TableExtractDataConfig(
          "public",
          "test_table",
          None,
          List(
            new TableAttribute().copy(
              name = "c_str"
            ),
            new TableAttribute().copy(
              name = "c_short"
            ),
            new TableAttribute().copy(
              name = "c_int"
            ),
            new TableAttribute().copy(
              name = "c_long"
            ),
            new TableAttribute().copy(
              name = "c_decimal"
            ),
            new TableAttribute().copy(
              name = "c_ts"
            ),
            new TableAttribute().copy(
              name = "c_date"
            )
          ),
          true,
          Some(1000),
          new Path(outputFolder.pathAsString),
          None,
          Some(
            PartitionConfig(
              "c_long",
              PrimitiveType.long,
              Some("abs( hashtext({{col}}) % {{nb_partitions}} )"),
              2
            )
          )
        )
      val allRows = ListBuffer[String]()
      val sinkResult = extractDataJob.extractTableData(
        ExtractJdbcDataConfig(
          JDBCSchema().copy(schema = "public"),
          new Path(outputFolder.pathAsString),
          0,
          2,
          None,
          Some(true),
          None,
          false,
          false,
          Nil,
          Nil,
          FileFormat(),
          jdbcConnection,
          jdbcConnection
        ),
        partitionnedConfig,
        "test",
        columns,
        (_, resultSet, _) => {
          val resultSetRows = resultSet
            .asIteratorOf(rowAsString)
            .toList
          allRows.appendAll(resultSetRows)
          Success(resultSetRows.length)
        }
      )
      allRows should contain theSameElementsAs
      List(
        "A\", 0, 1, 2, 3.4, 2003-02-01 00:00:00.0, 2003-02-02",
        "BÉ, 5, 6, 7, 8.9, 2003-12-24 00:00:00.0, 2003-12-25"
      )
      sinkResult.isSuccess shouldBe true
      st.executeQuery(
        "select domain, schema, last_ts, last_date, last_long, last_decimal, mode, count, success, message, step from audit.sl_last_export"
      ).asIteratorOf(rowAsString)
        .toList should contain theSameElementsAs
      List(
        "public, test_table, null, null, 7, null, OVERWRITE, 1, true, c_long, 1",
        "public, test_table, null, null, 7, null, OVERWRITE, 1, true, c_long, 0",
        "public, test_table, null, null, 7, null, OVERWRITE, 2, true, c_long, ALL"
      )
    }
  }

  it should "partition data on decimal" in {
    new WithSettings() {
      private val extractDataJob = new ExtractDataJob(settings.schemaHandler())
      val jdbcConnection = settings.appConfig.connections("test-pg")
      val conn = DriverManager.getConnection(
        jdbcConnection.options("url"),
        jdbcConnection.options("user"),
        jdbcConnection.options("password")
      )
      val st = conn.createStatement()
      st.execute(initSQL)
      private val outputFolder: File = better.files.File(starlakeTestRoot + "/data")
      outputFolder.createDirectory()
      implicit val extractExecutionContext: ExtractExecutionContext =
        new ExtractExecutionContext(None)
      val columns = extractDataJob.initExportAuditTable(settings.appConfig.connections("test-pg"))
      private val partitionnedConfig: TableExtractDataConfig =
        TableExtractDataConfig(
          "public",
          "test_table",
          None,
          List(
            new TableAttribute().copy(
              name = "c_str"
            ),
            new TableAttribute().copy(
              name = "c_short"
            ),
            new TableAttribute().copy(
              name = "c_int"
            ),
            new TableAttribute().copy(
              name = "c_long"
            ),
            new TableAttribute().copy(
              name = "c_decimal"
            ),
            new TableAttribute().copy(
              name = "c_ts"
            ),
            new TableAttribute().copy(
              name = "c_date"
            )
          ),
          true,
          Some(1000),
          new Path(outputFolder.pathAsString),
          None,
          Some(
            PartitionConfig(
              "c_decimal",
              PrimitiveType.decimal,
              Some("abs( hashtext({{col}}) % {{nb_partitions}} )"),
              2
            )
          )
        )
      val allRows = ListBuffer[String]()
      val sinkResult = extractDataJob.extractTableData(
        ExtractJdbcDataConfig(
          JDBCSchema().copy(schema = "public"),
          new Path(outputFolder.pathAsString),
          0,
          2,
          None,
          Some(true),
          None,
          false,
          false,
          Nil,
          Nil,
          FileFormat(),
          jdbcConnection,
          jdbcConnection
        ),
        partitionnedConfig,
        "test",
        columns,
        (_, resultSet, _) => {
          val resultSetRows = resultSet
            .asIteratorOf(rowAsString)
            .toList
          allRows.appendAll(resultSetRows)
          Success(resultSetRows.length)
        }
      )
      allRows should contain theSameElementsAs
      List(
        "A\", 0, 1, 2, 3.4, 2003-02-01 00:00:00.0, 2003-02-02",
        "BÉ, 5, 6, 7, 8.9, 2003-12-24 00:00:00.0, 2003-12-25"
      )
      sinkResult.isSuccess shouldBe true
      st.executeQuery(
        "select domain, schema, last_ts, last_date, last_long, last_decimal, mode, count, success, message, step from audit.sl_last_export"
      ).asIteratorOf(rowAsString)
        .toList should contain theSameElementsAs
      List(
        "public, test_table, null, null, null, 8.9, OVERWRITE, 1, true, c_decimal, 1",
        "public, test_table, null, null, null, 8.9, OVERWRITE, 1, true, c_decimal, 0",
        "public, test_table, null, null, null, 8.9, OVERWRITE, 2, true, c_decimal, ALL"
      )
    }
  }

  it should "partition data on timestamp" in {
    new WithSettings() {
      private val extractDataJob = new ExtractDataJob(settings.schemaHandler())
      val jdbcConnection = settings.appConfig.connections("test-pg")
      val conn = DriverManager.getConnection(
        jdbcConnection.options("url"),
        jdbcConnection.options("user"),
        jdbcConnection.options("password")
      )
      val st = conn.createStatement()
      st.execute(initSQL)
      private val outputFolder: File = better.files.File(starlakeTestRoot + "/data")
      outputFolder.createDirectory()
      implicit val extractExecutionContext: ExtractExecutionContext =
        new ExtractExecutionContext(None)
      val columns = extractDataJob.initExportAuditTable(settings.appConfig.connections("test-pg"))
      private val partitionnedConfig: TableExtractDataConfig =
        TableExtractDataConfig(
          "public",
          "test_table",
          None,
          List(
            new TableAttribute().copy(
              name = "c_str"
            ),
            new TableAttribute().copy(
              name = "c_short"
            ),
            new TableAttribute().copy(
              name = "c_int"
            ),
            new TableAttribute().copy(
              name = "c_long"
            ),
            new TableAttribute().copy(
              name = "c_decimal"
            ),
            new TableAttribute().copy(
              name = "c_ts"
            ),
            new TableAttribute().copy(
              name = "c_date"
            )
          ),
          true,
          Some(1000),
          new Path(outputFolder.pathAsString),
          None,
          Some(
            PartitionConfig(
              "c_ts",
              PrimitiveType.timestamp,
              Some("abs( hashtext({{col}}) % {{nb_partitions}} )"),
              2
            )
          )
        )
      val allRows = ListBuffer[String]()
      val sinkResult = extractDataJob.extractTableData(
        ExtractJdbcDataConfig(
          JDBCSchema().copy(schema = "public"),
          new Path(outputFolder.pathAsString),
          0,
          2,
          None,
          Some(true),
          None,
          false,
          false,
          Nil,
          Nil,
          FileFormat(),
          jdbcConnection,
          jdbcConnection
        ),
        partitionnedConfig,
        "test",
        columns,
        (_, resultSet, _) => {
          val resultSetRows = resultSet
            .asIteratorOf(rowAsString)
            .toList
          allRows.appendAll(resultSetRows)
          Success(resultSetRows.length)
        }
      )
      allRows should contain theSameElementsAs
      List(
        "A\", 0, 1, 2, 3.4, 2003-02-01 00:00:00.0, 2003-02-02",
        "BÉ, 5, 6, 7, 8.9, 2003-12-24 00:00:00.0, 2003-12-25"
      )
      sinkResult.isSuccess shouldBe true
      st.executeQuery(
        "select domain, schema, last_ts, last_date, last_long, last_decimal, mode, count, success, message, step from audit.sl_last_export"
      ).asIteratorOf(rowAsString)
        .toList should contain theSameElementsAs
      List(
        "public, test_table, 2003-12-24 00:00:00.0, null, null, null, OVERWRITE, 1, true, c_ts, 1",
        "public, test_table, 2003-12-24 00:00:00.0, null, null, null, OVERWRITE, 1, true, c_ts, 0",
        "public, test_table, 2003-12-24 00:00:00.0, null, null, null, OVERWRITE, 2, true, c_ts, ALL"
      )
    }
  }

  it should "partition data on date" in {
    new WithSettings() {
      private val extractDataJob = new ExtractDataJob(settings.schemaHandler())
      val jdbcConnection = settings.appConfig.connections("test-pg")
      val conn = DriverManager.getConnection(
        jdbcConnection.options("url"),
        jdbcConnection.options("user"),
        jdbcConnection.options("password")
      )
      val st = conn.createStatement()
      st.execute(initSQL)
      private val outputFolder: File = better.files.File(starlakeTestRoot + "/data")
      outputFolder.createDirectory()
      implicit val extractExecutionContext: ExtractExecutionContext =
        new ExtractExecutionContext(None)
      val columns = extractDataJob.initExportAuditTable(settings.appConfig.connections("test-pg"))
      private val partitionnedConfig: TableExtractDataConfig =
        TableExtractDataConfig(
          "public",
          "test_table",
          None,
          List(
            new TableAttribute().copy(
              name = "c_str"
            ),
            new TableAttribute().copy(
              name = "c_short"
            ),
            new TableAttribute().copy(
              name = "c_int"
            ),
            new TableAttribute().copy(
              name = "c_long"
            ),
            new TableAttribute().copy(
              name = "c_decimal"
            ),
            new TableAttribute().copy(
              name = "c_ts"
            ),
            new TableAttribute().copy(
              name = "c_date"
            )
          ),
          true,
          Some(1000),
          new Path(outputFolder.pathAsString),
          None,
          Some(
            PartitionConfig(
              "c_date",
              PrimitiveType.date,
              Some("abs( hashtext({{col}}) % {{nb_partitions}} )"),
              2
            )
          )
        )
      val allRows = ListBuffer[String]()
      val sinkResult = extractDataJob.extractTableData(
        ExtractJdbcDataConfig(
          JDBCSchema().copy(schema = "public"),
          new Path(outputFolder.pathAsString),
          0,
          2,
          None,
          Some(true),
          None,
          false,
          false,
          Nil,
          Nil,
          FileFormat(),
          jdbcConnection,
          jdbcConnection
        ),
        partitionnedConfig,
        "test",
        columns,
        (_, resultSet, _) => {
          val resultSetRows = resultSet
            .asIteratorOf(rowAsString)
            .toList
          allRows.appendAll(resultSetRows)
          Success(resultSetRows.length)
        }
      )
      allRows should contain theSameElementsAs
      List(
        "A\", 0, 1, 2, 3.4, 2003-02-01 00:00:00.0, 2003-02-02",
        "BÉ, 5, 6, 7, 8.9, 2003-12-24 00:00:00.0, 2003-12-25"
      )
      sinkResult.isSuccess shouldBe true
      st.executeQuery(
        "select domain, schema, last_ts, last_date, last_long, last_decimal, mode, count, success, message, step from audit.sl_last_export"
      ).asIteratorOf(rowAsString)
        .toList should contain theSameElementsAs
      List(
        "public, test_table, null, 2003-12-25, null, null, OVERWRITE, 1, true, c_date, 1",
        "public, test_table, null, 2003-12-25, null, null, OVERWRITE, 1, true, c_date, 0",
        "public, test_table, null, 2003-12-25, null, null, OVERWRITE, 2, true, c_date, ALL"
      )
    }
  }

  it should "partition data on string" in {
    new WithSettings() {
      private val extractDataJob = new ExtractDataJob(settings.schemaHandler())
      val jdbcConnection = settings.appConfig.connections("test-pg")
      val conn = DriverManager.getConnection(
        jdbcConnection.options("url"),
        jdbcConnection.options("user"),
        jdbcConnection.options("password")
      )
      val st = conn.createStatement()
      st.execute(initSQL)
      private val outputFolder: File = better.files.File(starlakeTestRoot + "/data")
      outputFolder.createDirectory()
      implicit val extractExecutionContext: ExtractExecutionContext =
        new ExtractExecutionContext(None)
      val columns = extractDataJob.initExportAuditTable(settings.appConfig.connections("test-pg"))
      private val partitionnedConfig: TableExtractDataConfig =
        TableExtractDataConfig(
          "public",
          "test_table",
          None,
          List(
            new TableAttribute().copy(
              name = "c_str"
            ),
            new TableAttribute().copy(
              name = "c_short"
            ),
            new TableAttribute().copy(
              name = "c_int"
            ),
            new TableAttribute().copy(
              name = "c_long"
            ),
            new TableAttribute().copy(
              name = "c_decimal"
            ),
            new TableAttribute().copy(
              name = "c_ts"
            ),
            new TableAttribute().copy(
              name = "c_date"
            )
          ),
          true,
          Some(1000),
          new Path(outputFolder.pathAsString),
          None,
          Some(
            PartitionConfig(
              "c_str",
              PrimitiveType.string,
              Some("abs( hashtext({{col}}) % {{nb_partitions}} )"),
              2
            )
          )
        )
      val allRows = ListBuffer[String]()
      val sinkResult = extractDataJob.extractTableData(
        ExtractJdbcDataConfig(
          JDBCSchema().copy(schema = "public"),
          new Path(outputFolder.pathAsString),
          0,
          2,
          None,
          Some(true),
          None,
          false,
          false,
          Nil,
          Nil,
          FileFormat(),
          jdbcConnection,
          jdbcConnection
        ),
        partitionnedConfig,
        "test",
        columns,
        (_, resultSet, _) => {
          val resultSetRows = resultSet
            .asIteratorOf(rowAsString)
            .toList
          allRows.appendAll(resultSetRows)
          Success(resultSetRows.length)
        }
      )
      allRows should contain theSameElementsAs
      List(
        "A\", 0, 1, 2, 3.4, 2003-02-01 00:00:00.0, 2003-02-02",
        "BÉ, 5, 6, 7, 8.9, 2003-12-24 00:00:00.0, 2003-12-25"
      )
      sinkResult.isSuccess shouldBe true
      st.executeQuery(
        "select domain, schema, last_ts, last_date, last_long, last_decimal, mode, count, success, message, step from audit.sl_last_export"
      ).asIteratorOf(rowAsString)
        .toList should contain theSameElementsAs
      List(
        "public, test_table, null, null, null, null, OVERWRITE, 1, true, c_str, 0",
        "public, test_table, null, null, null, null, OVERWRITE, 1, true, c_str, 1",
        "public, test_table, null, null, null, null, OVERWRITE, 2, true, c_str, ALL"
      )
    }
  }

  it should "extract full data in incremental mode when recovering on first failed extraction for short partition" in {
    new WithSettings() {
      private val extractDataJob = new ExtractDataJob(settings.schemaHandler())
      val jdbcConnection = settings.appConfig.connections("test-pg")
      val conn = DriverManager.getConnection(
        jdbcConnection.options("url"),
        jdbcConnection.options("user"),
        jdbcConnection.options("password")
      )
      val st = conn.createStatement()
      st.execute(initSQL)
      private val outputFolder: File = better.files.File(starlakeTestRoot + "/data")
      outputFolder.createDirectory()
      implicit val extractExecutionContext: ExtractExecutionContext =
        new ExtractExecutionContext(None)
      val columns = extractDataJob.initExportAuditTable(settings.appConfig.connections("test-pg"))
      st.execute(
        """insert into audit.sl_last_export ("domain", "schema", "last_long", "start_ts", "end_ts", "duration", "mode", "count", "success", "message", "step")
          |values
          |('public', 'test_table', 5,  '2024-04-08'::timestamp, '2024-04-08'::timestamp, 10, 'OVERWRITE', 1, true, 'c_short', '0'),
          |('public', 'test_table', 5,  '2024-04-08'::timestamp, '2024-04-08'::timestamp, 10, 'OVERWRITE', 0, false, 'c_short', '1'),
          |('public', 'test_table', 5,  '2024-04-08'::timestamp, '2024-04-08'::timestamp, 10, 'OVERWRITE', 1, false, 'c_short', 'ALL');
          |""".stripMargin
      )
      private val partitionnedConfig: TableExtractDataConfig =
        TableExtractDataConfig(
          "public",
          "test_table",
          None,
          List(
            new TableAttribute().copy(
              name = "c_str"
            ),
            new TableAttribute().copy(
              name = "c_short"
            ),
            new TableAttribute().copy(
              name = "c_int"
            ),
            new TableAttribute().copy(
              name = "c_long"
            ),
            new TableAttribute().copy(
              name = "c_decimal"
            ),
            new TableAttribute().copy(
              name = "c_ts"
            ),
            new TableAttribute().copy(
              name = "c_date"
            )
          ),
          false,
          Some(1000),
          new Path(outputFolder.pathAsString),
          None,
          Some(
            PartitionConfig(
              "c_short",
              PrimitiveType.short,
              Some("abs( hashtext({{col}}) % {{nb_partitions}} )"),
              2
            )
          )
        )
      val allRows = ListBuffer[String]()
      val sinkResult = extractDataJob.extractTableData(
        ExtractJdbcDataConfig(
          JDBCSchema().copy(schema = "public"),
          new Path(outputFolder.pathAsString),
          0,
          2,
          None,
          Some(false),
          None,
          false,
          false,
          Nil,
          Nil,
          FileFormat(),
          jdbcConnection,
          jdbcConnection
        ),
        partitionnedConfig,
        "test",
        columns,
        (_, resultSet, _) => {
          val resultSetRows = resultSet
            .asIteratorOf(rowAsString)
            .toList
          allRows.appendAll(resultSetRows)
          Success(resultSetRows.length)
        }
      )
      allRows should contain theSameElementsAs
      List(
        "A\", 0, 1, 2, 3.4, 2003-02-01 00:00:00.0, 2003-02-02",
        "BÉ, 5, 6, 7, 8.9, 2003-12-24 00:00:00.0, 2003-12-25"
      )
      sinkResult.isSuccess shouldBe true
      st.executeQuery(
        "select domain, schema, last_ts, last_date, last_long, last_decimal, mode, count, success, message, step from audit.sl_last_export"
      ).asIteratorOf(rowAsString)
        .toList should contain theSameElementsAs
      List(
        "public, test_table, null, null, 5, null, OVERWRITE, 1, true, c_short, 1",
        "public, test_table, null, null, 5, null, OVERWRITE, 1, true, c_short, 0",
        "public, test_table, null, null, 5, null, OVERWRITE, 2, true, c_short, ALL",
        "public, test_table, null, null, 5, null, OVERWRITE, 0, false, c_short, 1",
        "public, test_table, null, null, 5, null, OVERWRITE, 1, true, c_short, 0",
        "public, test_table, null, null, 5, null, OVERWRITE, 1, false, c_short, ALL"
      )
    }
  }

  it should "extract full data in incremental mode when recovering on first failed extraction for decimal partition" in {
    new WithSettings() {
      private val extractDataJob = new ExtractDataJob(settings.schemaHandler())
      val jdbcConnection = settings.appConfig.connections("test-pg")
      val conn = DriverManager.getConnection(
        jdbcConnection.options("url"),
        jdbcConnection.options("user"),
        jdbcConnection.options("password")
      )
      val st = conn.createStatement()
      st.execute(initSQL)
      private val outputFolder: File = better.files.File(starlakeTestRoot + "/data")
      outputFolder.createDirectory()
      implicit val extractExecutionContext: ExtractExecutionContext =
        new ExtractExecutionContext(None)
      val columns = extractDataJob.initExportAuditTable(settings.appConfig.connections("test-pg"))
      st.execute(
        """insert into audit.sl_last_export ("domain", "schema", "last_decimal", "start_ts", "end_ts", "duration", "mode", "count", "success", "message", "step")
          |values
          |('public', 'test_table', 8.9,  '2024-04-08'::timestamp, '2024-04-08'::timestamp, 10, 'OVERWRITE', 1, true, 'c_decimal', '0'),
          |('public', 'test_table', 8.9,  '2024-04-08'::timestamp, '2024-04-08'::timestamp, 10, 'OVERWRITE', 0, false, 'c_decimal', '1'),
          |('public', 'test_table', 8.9,  '2024-04-08'::timestamp, '2024-04-08'::timestamp, 10, 'OVERWRITE', 1, false, 'c_decimal', 'ALL');
          |""".stripMargin
      )
      private val partitionnedConfig: TableExtractDataConfig =
        TableExtractDataConfig(
          "public",
          "test_table",
          None,
          List(
            new TableAttribute().copy(
              name = "c_str"
            ),
            new TableAttribute().copy(
              name = "c_short"
            ),
            new TableAttribute().copy(
              name = "c_int"
            ),
            new TableAttribute().copy(
              name = "c_long"
            ),
            new TableAttribute().copy(
              name = "c_decimal"
            ),
            new TableAttribute().copy(
              name = "c_ts"
            ),
            new TableAttribute().copy(
              name = "c_date"
            )
          ),
          false,
          Some(1000),
          new Path(outputFolder.pathAsString),
          None,
          Some(
            PartitionConfig(
              "c_decimal",
              PrimitiveType.decimal,
              Some("abs( hashtext({{col}}) % {{nb_partitions}} )"),
              2
            )
          )
        )
      val allRows = ListBuffer[String]()
      val sinkResult = extractDataJob.extractTableData(
        ExtractJdbcDataConfig(
          JDBCSchema().copy(schema = "public"),
          new Path(outputFolder.pathAsString),
          0,
          2,
          None,
          Some(false),
          None,
          false,
          false,
          Nil,
          Nil,
          FileFormat(),
          jdbcConnection,
          jdbcConnection
        ),
        partitionnedConfig,
        "test",
        columns,
        (_, resultSet, _) => {
          val resultSetRows = resultSet
            .asIteratorOf(rowAsString)
            .toList
          allRows.appendAll(resultSetRows)
          Success(resultSetRows.length)
        }
      )
      allRows should contain theSameElementsAs
      List(
        "A\", 0, 1, 2, 3.4, 2003-02-01 00:00:00.0, 2003-02-02",
        "BÉ, 5, 6, 7, 8.9, 2003-12-24 00:00:00.0, 2003-12-25"
      )
      sinkResult.isSuccess shouldBe true
      st.executeQuery(
        "select domain, schema, last_ts, last_date, last_long, last_decimal, mode, count, success, message, step from audit.sl_last_export"
      ).asIteratorOf(rowAsString)
        .toList should contain theSameElementsAs
      List(
        "public, test_table, null, null, null, 8.9, OVERWRITE, 1, true, c_decimal, 1",
        "public, test_table, null, null, null, 8.9, OVERWRITE, 1, true, c_decimal, 0",
        "public, test_table, null, null, null, 8.9, OVERWRITE, 2, true, c_decimal, ALL",
        "public, test_table, null, null, null, 8.9, OVERWRITE, 0, false, c_decimal, 1",
        "public, test_table, null, null, null, 8.9, OVERWRITE, 1, true, c_decimal, 0",
        "public, test_table, null, null, null, 8.9, OVERWRITE, 1, false, c_decimal, ALL"
      )
    }
  }

  it should "extract full data in incremental mode when recovering on first failed extraction for timestamp partition" in {
    new WithSettings() {
      private val extractDataJob = new ExtractDataJob(settings.schemaHandler())
      val jdbcConnection = settings.appConfig.connections("test-pg")
      val conn = DriverManager.getConnection(
        jdbcConnection.options("url"),
        jdbcConnection.options("user"),
        jdbcConnection.options("password")
      )
      val st = conn.createStatement()
      st.execute(initSQL)
      private val outputFolder: File = better.files.File(starlakeTestRoot + "/data")
      outputFolder.createDirectory()
      implicit val extractExecutionContext: ExtractExecutionContext =
        new ExtractExecutionContext(None)
      val columns = extractDataJob.initExportAuditTable(settings.appConfig.connections("test-pg"))
      st.execute(
        """insert into audit.sl_last_export ("domain", "schema", "last_ts", "start_ts", "end_ts", "duration", "mode", "count", "success", "message", "step")
          |values
          |('public', 'test_table', '2003-12-24'::timestamp,  '2024-04-08'::timestamp, '2024-04-08'::timestamp, 10, 'OVERWRITE', 1, true, 'c_ts', '0'),
          |('public', 'test_table', '2003-12-24'::timestamp,  '2024-04-08'::timestamp, '2024-04-08'::timestamp, 10, 'OVERWRITE', 0, false, 'c_ts', '1'),
          |('public', 'test_table', '2003-12-24'::timestamp,  '2024-04-08'::timestamp, '2024-04-08'::timestamp, 10, 'OVERWRITE', 1, false, 'c_ts', 'ALL');
          |""".stripMargin
      )
      private val partitionnedConfig: TableExtractDataConfig =
        TableExtractDataConfig(
          "public",
          "test_table",
          None,
          List(
            new TableAttribute().copy(
              name = "c_str"
            ),
            new TableAttribute().copy(
              name = "c_short"
            ),
            new TableAttribute().copy(
              name = "c_int"
            ),
            new TableAttribute().copy(
              name = "c_long"
            ),
            new TableAttribute().copy(
              name = "c_decimal"
            ),
            new TableAttribute().copy(
              name = "c_ts"
            ),
            new TableAttribute().copy(
              name = "c_date"
            )
          ),
          false,
          Some(1000),
          new Path(outputFolder.pathAsString),
          None,
          Some(
            PartitionConfig(
              "c_ts",
              PrimitiveType.timestamp,
              Some("abs( hashtext({{col}}) % {{nb_partitions}} )"),
              2
            )
          )
        )
      val allRows = ListBuffer[String]()
      val sinkResult = extractDataJob.extractTableData(
        ExtractJdbcDataConfig(
          JDBCSchema().copy(schema = "public"),
          new Path(outputFolder.pathAsString),
          0,
          2,
          None,
          Some(false),
          None,
          false,
          false,
          Nil,
          Nil,
          FileFormat(),
          jdbcConnection,
          jdbcConnection
        ),
        partitionnedConfig,
        "test",
        columns,
        (_, resultSet, _) => {
          val resultSetRows = resultSet
            .asIteratorOf(rowAsString)
            .toList
          allRows.appendAll(resultSetRows)
          Success(resultSetRows.length)
        }
      )
      allRows should contain theSameElementsAs
      List(
        "A\", 0, 1, 2, 3.4, 2003-02-01 00:00:00.0, 2003-02-02",
        "BÉ, 5, 6, 7, 8.9, 2003-12-24 00:00:00.0, 2003-12-25"
      )
      sinkResult.isSuccess shouldBe true
      st.executeQuery(
        "select domain, schema, last_ts, last_date, last_long, last_decimal, mode, count, success, message, step from audit.sl_last_export"
      ).asIteratorOf(rowAsString)
        .toList should contain theSameElementsAs
      List(
        "public, test_table, 2003-12-24 00:00:00.0, null, null, null, OVERWRITE, 1, true, c_ts, 1",
        "public, test_table, 2003-12-24 00:00:00.0, null, null, null, OVERWRITE, 1, true, c_ts, 0",
        "public, test_table, 2003-12-24 00:00:00.0, null, null, null, OVERWRITE, 2, true, c_ts, ALL",
        "public, test_table, 2003-12-24 00:00:00.0, null, null, null, OVERWRITE, 0, false, c_ts, 1",
        "public, test_table, 2003-12-24 00:00:00.0, null, null, null, OVERWRITE, 1, true, c_ts, 0",
        "public, test_table, 2003-12-24 00:00:00.0, null, null, null, OVERWRITE, 1, false, c_ts, ALL"
      )
    }
  }

  it should "extract full data in incremental mode when recovering on first failed extraction for date partition" in {
    new WithSettings() {
      private val extractDataJob = new ExtractDataJob(settings.schemaHandler())
      val jdbcConnection = settings.appConfig.connections("test-pg")
      val conn = DriverManager.getConnection(
        jdbcConnection.options("url"),
        jdbcConnection.options("user"),
        jdbcConnection.options("password")
      )
      val st = conn.createStatement()
      st.execute(initSQL)
      private val outputFolder: File = better.files.File(starlakeTestRoot + "/data")
      outputFolder.createDirectory()
      implicit val extractExecutionContext: ExtractExecutionContext =
        new ExtractExecutionContext(None)
      val columns = extractDataJob.initExportAuditTable(settings.appConfig.connections("test-pg"))
      st.execute(
        """insert into audit.sl_last_export ("domain", "schema", "last_date", "start_ts", "end_ts", "duration", "mode", "count", "success", "message", "step")
          |values
          |('public', 'test_table', '2003-12-25'::date,  '2024-04-08'::timestamp, '2024-04-08'::timestamp, 10, 'OVERWRITE', 1, true, 'c_date', '0'),
          |('public', 'test_table', '2003-12-25'::date,  '2024-04-08'::timestamp, '2024-04-08'::timestamp, 10, 'OVERWRITE', 0, false, 'c_date', '1'),
          |('public', 'test_table', '2003-12-25'::date,  '2024-04-08'::timestamp, '2024-04-08'::timestamp, 10, 'OVERWRITE', 1, false, 'c_date', 'ALL');
          |""".stripMargin
      )
      private val partitionnedConfig: TableExtractDataConfig =
        TableExtractDataConfig(
          "public",
          "test_table",
          None,
          List(
            new TableAttribute().copy(
              name = "c_str"
            ),
            new TableAttribute().copy(
              name = "c_short"
            ),
            new TableAttribute().copy(
              name = "c_int"
            ),
            new TableAttribute().copy(
              name = "c_long"
            ),
            new TableAttribute().copy(
              name = "c_decimal"
            ),
            new TableAttribute().copy(
              name = "c_ts"
            ),
            new TableAttribute().copy(
              name = "c_date"
            )
          ),
          false,
          Some(1000),
          new Path(outputFolder.pathAsString),
          None,
          Some(
            PartitionConfig(
              "c_date",
              PrimitiveType.date,
              Some("abs( hashtext({{col}}) % {{nb_partitions}} )"),
              2
            )
          )
        )
      val allRows = ListBuffer[String]()
      val sinkResult = extractDataJob.extractTableData(
        ExtractJdbcDataConfig(
          JDBCSchema().copy(schema = "public"),
          new Path(outputFolder.pathAsString),
          0,
          2,
          None,
          Some(false),
          None,
          false,
          false,
          Nil,
          Nil,
          FileFormat(),
          jdbcConnection,
          jdbcConnection
        ),
        partitionnedConfig,
        "test",
        columns,
        (_, resultSet, _) => {
          val resultSetRows = resultSet
            .asIteratorOf(rowAsString)
            .toList
          allRows.appendAll(resultSetRows)
          Success(resultSetRows.length)
        }
      )
      allRows should contain theSameElementsAs
      List(
        "A\", 0, 1, 2, 3.4, 2003-02-01 00:00:00.0, 2003-02-02",
        "BÉ, 5, 6, 7, 8.9, 2003-12-24 00:00:00.0, 2003-12-25"
      )
      sinkResult.isSuccess shouldBe true
      st.executeQuery(
        "select domain, schema, last_ts, last_date, last_long, last_decimal, mode, count, success, message, step from audit.sl_last_export"
      ).asIteratorOf(rowAsString)
        .toList should contain theSameElementsAs
      List(
        "public, test_table, null, 2003-12-25, null, null, OVERWRITE, 1, true, c_date, 1",
        "public, test_table, null, 2003-12-25, null, null, OVERWRITE, 1, true, c_date, 0",
        "public, test_table, null, 2003-12-25, null, null, OVERWRITE, 2, true, c_date, ALL",
        "public, test_table, null, 2003-12-25, null, null, OVERWRITE, 0, false, c_date, 1",
        "public, test_table, null, 2003-12-25, null, null, OVERWRITE, 1, true, c_date, 0",
        "public, test_table, null, 2003-12-25, null, null, OVERWRITE, 1, false, c_date, ALL"
      )
    }
  }

  it should "extract incrementally from highest int partition" in {
    new WithSettings() {
      private val extractDataJob = new ExtractDataJob(settings.schemaHandler())
      val jdbcConnection = settings.appConfig.connections("test-pg")
      val conn = DriverManager.getConnection(
        jdbcConnection.options("url"),
        jdbcConnection.options("user"),
        jdbcConnection.options("password")
      )
      val st = conn.createStatement()
      st.execute(initSQL)
      private val outputFolder: File = better.files.File(starlakeTestRoot + "/data")
      outputFolder.createDirectory()
      implicit val extractExecutionContext: ExtractExecutionContext =
        new ExtractExecutionContext(None)
      val columns = extractDataJob.initExportAuditTable(settings.appConfig.connections("test-pg"))
      st.execute(
        """insert into audit.sl_last_export ("domain", "schema", "last_long", "start_ts", "end_ts", "duration", "mode", "count", "success", "message", "step")
          |values
          |('public', 'test_table', 1,  '2024-04-08'::timestamp, '2024-04-08'::timestamp, 10, 'OVERWRITE', 1, true, 'c_int', '0'),
          |('public', 'test_table', 1,  '2024-04-08'::timestamp, '2024-04-08'::timestamp, 10, 'OVERWRITE', 0, true, 'c_int', '1'),
          |('public', 'test_table', 1,  '2024-04-08'::timestamp, '2024-04-08'::timestamp, 10, 'OVERWRITE', 1, true, 'c_int', 'ALL');
          |""".stripMargin
      )
      private val partitionnedConfig: TableExtractDataConfig =
        TableExtractDataConfig(
          "public",
          "test_table",
          None,
          List(
            new TableAttribute().copy(
              name = "c_str"
            ),
            new TableAttribute().copy(
              name = "c_short"
            ),
            new TableAttribute().copy(
              name = "c_int"
            ),
            new TableAttribute().copy(
              name = "c_long"
            ),
            new TableAttribute().copy(
              name = "c_decimal"
            ),
            new TableAttribute().copy(
              name = "c_ts"
            ),
            new TableAttribute().copy(
              name = "c_date"
            )
          ),
          false,
          Some(1000),
          new Path(outputFolder.pathAsString),
          None,
          Some(
            PartitionConfig(
              "c_int",
              PrimitiveType.int,
              Some("abs( hashtext({{col}}) % {{nb_partitions}} )"),
              2
            )
          )
        )
      val allRows = ListBuffer[String]()
      val sinkResult = extractDataJob.extractTableData(
        ExtractJdbcDataConfig(
          JDBCSchema().copy(schema = "public"),
          new Path(outputFolder.pathAsString),
          0,
          2,
          None,
          Some(false),
          None,
          false,
          false,
          Nil,
          Nil,
          FileFormat(),
          jdbcConnection,
          jdbcConnection
        ),
        partitionnedConfig,
        "test",
        columns,
        (_, resultSet, _) => {
          val resultSetRows = resultSet
            .asIteratorOf(rowAsString)
            .toList
          allRows.appendAll(resultSetRows)
          Success(resultSetRows.length)
        }
      )
      allRows should contain theSameElementsAs
      List(
        "BÉ, 5, 6, 7, 8.9, 2003-12-24 00:00:00.0, 2003-12-25"
      )
      sinkResult.isSuccess shouldBe true
      st.executeQuery(
        "select domain, schema, last_ts, last_date, last_long, last_decimal, mode, count, success, message, step from audit.sl_last_export"
      ).asIteratorOf(rowAsString)
        .toList should contain theSameElementsAs
      List(
        "public, test_table, null, null, 6, null, OVERWRITE, 0, true, c_int, 1",
        "public, test_table, null, null, 6, null, OVERWRITE, 1, true, c_int, 0",
        "public, test_table, null, null, 6, null, OVERWRITE, 1, true, c_int, ALL",
        "public, test_table, null, null, 1, null, OVERWRITE, 0, true, c_int, 1",
        "public, test_table, null, null, 1, null, OVERWRITE, 1, true, c_int, 0",
        "public, test_table, null, null, 1, null, OVERWRITE, 1, true, c_int, ALL"
      )
    }
  }

  it should "extract incrementally from highest decimal partition" in {
    new WithSettings() {
      private val extractDataJob = new ExtractDataJob(settings.schemaHandler())
      val jdbcConnection = settings.appConfig.connections("test-pg")
      val conn = DriverManager.getConnection(
        jdbcConnection.options("url"),
        jdbcConnection.options("user"),
        jdbcConnection.options("password")
      )
      val st = conn.createStatement()
      st.execute(initSQL)
      private val outputFolder: File = better.files.File(starlakeTestRoot + "/data")
      outputFolder.createDirectory()
      implicit val extractExecutionContext: ExtractExecutionContext =
        new ExtractExecutionContext(None)
      val columns = extractDataJob.initExportAuditTable(settings.appConfig.connections("test-pg"))
      st.execute(
        """insert into audit.sl_last_export ("domain", "schema", "last_decimal", "start_ts", "end_ts", "duration", "mode", "count", "success", "message", "step")
          |values
          |('public', 'test_table', 3.4,  '2024-04-08'::timestamp, '2024-04-08'::timestamp, 10, 'OVERWRITE', 1, true, 'c_decimal', '0'),
          |('public', 'test_table', 3.4,  '2024-04-08'::timestamp, '2024-04-08'::timestamp, 10, 'OVERWRITE', 0, true, 'c_decimal', '1'),
          |('public', 'test_table', 3.4,  '2024-04-08'::timestamp, '2024-04-08'::timestamp, 10, 'OVERWRITE', 1, true, 'c_decimal', 'ALL');
          |""".stripMargin
      )
      private val partitionnedConfig: TableExtractDataConfig =
        TableExtractDataConfig(
          "public",
          "test_table",
          None,
          List(
            new TableAttribute().copy(
              name = "c_str"
            ),
            new TableAttribute().copy(
              name = "c_short"
            ),
            new TableAttribute().copy(
              name = "c_int"
            ),
            new TableAttribute().copy(
              name = "c_long"
            ),
            new TableAttribute().copy(
              name = "c_decimal"
            ),
            new TableAttribute().copy(
              name = "c_ts"
            ),
            new TableAttribute().copy(
              name = "c_date"
            )
          ),
          false,
          Some(1000),
          new Path(outputFolder.pathAsString),
          None,
          Some(
            PartitionConfig(
              "c_decimal",
              PrimitiveType.decimal,
              Some("abs( hashtext({{col}}) % {{nb_partitions}} )"),
              2
            )
          )
        )
      val allRows = ListBuffer[String]()
      val sinkResult = extractDataJob.extractTableData(
        ExtractJdbcDataConfig(
          JDBCSchema().copy(schema = "public"),
          new Path(outputFolder.pathAsString),
          0,
          2,
          None,
          Some(false),
          None,
          false,
          false,
          Nil,
          Nil,
          FileFormat(),
          jdbcConnection,
          jdbcConnection
        ),
        partitionnedConfig,
        "test",
        columns,
        (_, resultSet, _) => {
          val resultSetRows = resultSet
            .asIteratorOf(rowAsString)
            .toList
          allRows.appendAll(resultSetRows)
          Success(resultSetRows.length)
        }
      )
      allRows should contain theSameElementsAs
      List(
        "BÉ, 5, 6, 7, 8.9, 2003-12-24 00:00:00.0, 2003-12-25"
      )
      sinkResult.isSuccess shouldBe true
      st.executeQuery(
        "select domain, schema, last_ts, last_date, last_long, last_decimal, mode, count, success, message, step from audit.sl_last_export"
      ).asIteratorOf(rowAsString)
        .toList should contain theSameElementsAs
      List(
        "public, test_table, null, null, null, 8.9, OVERWRITE, 0, true, c_decimal, 1",
        "public, test_table, null, null, null, 8.9, OVERWRITE, 1, true, c_decimal, 0",
        "public, test_table, null, null, null, 8.9, OVERWRITE, 1, true, c_decimal, ALL",
        "public, test_table, null, null, null, 3.4, OVERWRITE, 0, true, c_decimal, 1",
        "public, test_table, null, null, null, 3.4, OVERWRITE, 1, true, c_decimal, 0",
        "public, test_table, null, null, null, 3.4, OVERWRITE, 1, true, c_decimal, ALL"
      )
    }
  }

  it should "extract incrementally from highest timestamp partition" in {
    new WithSettings() {
      private val extractDataJob = new ExtractDataJob(settings.schemaHandler())
      val jdbcConnection = settings.appConfig.connections("test-pg")
      val conn = DriverManager.getConnection(
        jdbcConnection.options("url"),
        jdbcConnection.options("user"),
        jdbcConnection.options("password")
      )
      val st = conn.createStatement()
      st.execute(initSQL)
      private val outputFolder: File = better.files.File(starlakeTestRoot + "/data")
      outputFolder.createDirectory()
      implicit val extractExecutionContext: ExtractExecutionContext =
        new ExtractExecutionContext(None)
      val columns = extractDataJob.initExportAuditTable(settings.appConfig.connections("test-pg"))
      st.execute(
        """insert into audit.sl_last_export ("domain", "schema", "last_ts", "start_ts", "end_ts", "duration", "mode", "count", "success", "message", "step")
          |values
          |('public', 'test_table', '2003-02-01'::timestamp,  '2024-04-08'::timestamp, '2024-04-08'::timestamp, 10, 'OVERWRITE', 1, true, 'c_ts', '0'),
          |('public', 'test_table', '2003-02-01'::timestamp,  '2024-04-08'::timestamp, '2024-04-08'::timestamp, 10, 'OVERWRITE', 0, true, 'c_ts', '1'),
          |('public', 'test_table', '2003-02-01'::timestamp,  '2024-04-08'::timestamp, '2024-04-08'::timestamp, 10, 'OVERWRITE', 1, true, 'c_ts', 'ALL');
          |""".stripMargin
      )
      private val partitionnedConfig: TableExtractDataConfig =
        TableExtractDataConfig(
          "public",
          "test_table",
          None,
          List(
            new TableAttribute().copy(
              name = "c_str"
            ),
            new TableAttribute().copy(
              name = "c_short"
            ),
            new TableAttribute().copy(
              name = "c_int"
            ),
            new TableAttribute().copy(
              name = "c_long"
            ),
            new TableAttribute().copy(
              name = "c_decimal"
            ),
            new TableAttribute().copy(
              name = "c_ts"
            ),
            new TableAttribute().copy(
              name = "c_date"
            )
          ),
          false,
          Some(1000),
          new Path(outputFolder.pathAsString),
          None,
          Some(
            PartitionConfig(
              "c_ts",
              PrimitiveType.timestamp,
              Some("abs( hashtext({{col}}) % {{nb_partitions}} )"),
              2
            )
          )
        )
      val allRows = ListBuffer[String]()
      val sinkResult = extractDataJob.extractTableData(
        ExtractJdbcDataConfig(
          JDBCSchema().copy(schema = "public"),
          new Path(outputFolder.pathAsString),
          0,
          2,
          None,
          Some(false),
          None,
          false,
          false,
          Nil,
          Nil,
          FileFormat(),
          jdbcConnection,
          jdbcConnection
        ),
        partitionnedConfig,
        "test",
        columns,
        (_, resultSet, _) => {
          val resultSetRows = resultSet
            .asIteratorOf(rowAsString)
            .toList
          allRows.appendAll(resultSetRows)
          Success(resultSetRows.length)
        }
      )
      allRows should contain theSameElementsAs
      List(
        "BÉ, 5, 6, 7, 8.9, 2003-12-24 00:00:00.0, 2003-12-25"
      )
      sinkResult.isSuccess shouldBe true
      st.executeQuery(
        "select domain, schema, last_ts, last_date, last_long, last_decimal, mode, count, success, message, step from audit.sl_last_export"
      ).asIteratorOf(rowAsString)
        .toList should contain theSameElementsAs
      List(
        "public, test_table, 2003-12-24 00:00:00.0, null, null, null, OVERWRITE, 0, true, c_ts, 1",
        "public, test_table, 2003-12-24 00:00:00.0, null, null, null, OVERWRITE, 1, true, c_ts, 0",
        "public, test_table, 2003-12-24 00:00:00.0, null, null, null, OVERWRITE, 1, true, c_ts, ALL",
        "public, test_table, 2003-02-01 00:00:00.0, null, null, null, OVERWRITE, 0, true, c_ts, 1",
        "public, test_table, 2003-02-01 00:00:00.0, null, null, null, OVERWRITE, 1, true, c_ts, 0",
        "public, test_table, 2003-02-01 00:00:00.0, null, null, null, OVERWRITE, 1, true, c_ts, ALL"
      )
    }
  }

  it should "extract incrementally from highest date partition" in {
    new WithSettings() {
      private val extractDataJob = new ExtractDataJob(settings.schemaHandler())
      val jdbcConnection = settings.appConfig.connections("test-pg")
      val conn = DriverManager.getConnection(
        jdbcConnection.options("url"),
        jdbcConnection.options("user"),
        jdbcConnection.options("password")
      )
      val st = conn.createStatement()
      st.execute(initSQL)
      private val outputFolder: File = better.files.File(starlakeTestRoot + "/data")
      outputFolder.createDirectory()
      implicit val extractExecutionContext: ExtractExecutionContext =
        new ExtractExecutionContext(None)
      val columns = extractDataJob.initExportAuditTable(settings.appConfig.connections("test-pg"))
      st.execute(
        """insert into audit.sl_last_export ("domain", "schema", "last_date", "start_ts", "end_ts", "duration", "mode", "count", "success", "message", "step")
          |values
          |('public', 'test_table', '2003-02-02'::date,  '2024-04-08'::timestamp, '2024-04-08'::timestamp, 10, 'OVERWRITE', 1, true, 'c_date', '0'),
          |('public', 'test_table', '2003-02-02'::date,  '2024-04-08'::timestamp, '2024-04-08'::timestamp, 10, 'OVERWRITE', 0, true, 'c_date', '1'),
          |('public', 'test_table', '2003-02-02'::date,  '2024-04-08'::timestamp, '2024-04-08'::timestamp, 10, 'OVERWRITE', 1, true, 'c_date', 'ALL');
          |""".stripMargin
      )
      private val partitionnedConfig: TableExtractDataConfig =
        TableExtractDataConfig(
          "public",
          "test_table",
          None,
          List(
            new TableAttribute().copy(
              name = "c_str"
            ),
            new TableAttribute().copy(
              name = "c_short"
            ),
            new TableAttribute().copy(
              name = "c_int"
            ),
            new TableAttribute().copy(
              name = "c_long"
            ),
            new TableAttribute().copy(
              name = "c_decimal"
            ),
            new TableAttribute().copy(
              name = "c_ts"
            ),
            new TableAttribute().copy(
              name = "c_date"
            )
          ),
          false,
          Some(1000),
          new Path(outputFolder.pathAsString),
          None,
          Some(
            PartitionConfig(
              "c_date",
              PrimitiveType.date,
              Some("abs( hashtext({{col}}) % {{nb_partitions}} )"),
              2
            )
          )
        )
      val allRows = ListBuffer[String]()
      val sinkResult = extractDataJob.extractTableData(
        ExtractJdbcDataConfig(
          JDBCSchema().copy(schema = "public"),
          new Path(outputFolder.pathAsString),
          0,
          2,
          None,
          Some(false),
          None,
          false,
          false,
          Nil,
          Nil,
          FileFormat(),
          jdbcConnection,
          jdbcConnection
        ),
        partitionnedConfig,
        "test",
        columns,
        (_, resultSet, _) => {
          val resultSetRows = resultSet
            .asIteratorOf(rowAsString)
            .toList
          allRows.appendAll(resultSetRows)
          Success(resultSetRows.length)
        }
      )
      allRows should contain theSameElementsAs
      List(
        "BÉ, 5, 6, 7, 8.9, 2003-12-24 00:00:00.0, 2003-12-25"
      )
      sinkResult.isSuccess shouldBe true
      st.executeQuery(
        "select domain, schema, last_ts, last_date, last_long, last_decimal, mode, count, success, message, step from audit.sl_last_export"
      ).asIteratorOf(rowAsString)
        .toList should contain theSameElementsAs
      List(
        "public, test_table, null, 2003-12-25, null, null, OVERWRITE, 0, true, c_date, 1",
        "public, test_table, null, 2003-12-25, null, null, OVERWRITE, 1, true, c_date, 0",
        "public, test_table, null, 2003-12-25, null, null, OVERWRITE, 1, true, c_date, ALL",
        "public, test_table, null, 2003-02-02, null, null, OVERWRITE, 0, true, c_date, 1",
        "public, test_table, null, 2003-02-02, null, null, OVERWRITE, 1, true, c_date, 0",
        "public, test_table, null, 2003-02-02, null, null, OVERWRITE, 1, true, c_date, ALL"
      )
    }
  }

  it should "extract fully if forced, ignoring highest int partition" in {
    new WithSettings() {
      private val extractDataJob = new ExtractDataJob(settings.schemaHandler())
      val jdbcConnection = settings.appConfig.connections("test-pg")
      val conn = DriverManager.getConnection(
        jdbcConnection.options("url"),
        jdbcConnection.options("user"),
        jdbcConnection.options("password")
      )
      val st = conn.createStatement()
      st.execute(initSQL)
      private val outputFolder: File = better.files.File(starlakeTestRoot + "/data")
      outputFolder.createDirectory()
      implicit val extractExecutionContext: ExtractExecutionContext =
        new ExtractExecutionContext(None)
      val columns = extractDataJob.initExportAuditTable(settings.appConfig.connections("test-pg"))
      st.execute(
        """insert into audit.sl_last_export ("domain", "schema", "last_long", "start_ts", "end_ts", "duration", "mode", "count", "success", "message", "step")
          |values
          |('public', 'test_table', 1,  '2024-04-08'::timestamp, '2024-04-08'::timestamp, 10, 'OVERWRITE', 1, true, 'c_int', '0'),
          |('public', 'test_table', 1,  '2024-04-08'::timestamp, '2024-04-08'::timestamp, 10, 'OVERWRITE', 0, true, 'c_int', '1'),
          |('public', 'test_table', 1,  '2024-04-08'::timestamp, '2024-04-08'::timestamp, 10, 'OVERWRITE', 1, true, 'c_int', 'ALL');
          |""".stripMargin
      )
      private val partitionnedConfig: TableExtractDataConfig =
        TableExtractDataConfig(
          "public",
          "test_table",
          None,
          List(
            new TableAttribute().copy(
              name = "c_str"
            ),
            new TableAttribute().copy(
              name = "c_short"
            ),
            new TableAttribute().copy(
              name = "c_int"
            ),
            new TableAttribute().copy(
              name = "c_long"
            ),
            new TableAttribute().copy(
              name = "c_decimal"
            ),
            new TableAttribute().copy(
              name = "c_ts"
            ),
            new TableAttribute().copy(
              name = "c_date"
            )
          ),
          true,
          Some(1000),
          new Path(outputFolder.pathAsString),
          None,
          Some(
            PartitionConfig(
              "c_int",
              PrimitiveType.int,
              Some("abs( hashtext({{col}}) % {{nb_partitions}} )"),
              2
            )
          )
        )
      val allRows = ListBuffer[String]()
      val sinkResult = extractDataJob.extractTableData(
        ExtractJdbcDataConfig(
          JDBCSchema().copy(schema = "public"),
          new Path(outputFolder.pathAsString),
          0,
          2,
          None,
          Some(true),
          None,
          false,
          false,
          Nil,
          Nil,
          FileFormat(),
          jdbcConnection,
          jdbcConnection
        ),
        partitionnedConfig,
        "test",
        columns,
        (_, resultSet, _) => {
          val resultSetRows = resultSet
            .asIteratorOf(rowAsString)
            .toList
          allRows.appendAll(resultSetRows)
          Success(resultSetRows.length)
        }
      )
      allRows should contain theSameElementsAs
      List(
        "A\", 0, 1, 2, 3.4, 2003-02-01 00:00:00.0, 2003-02-02",
        "BÉ, 5, 6, 7, 8.9, 2003-12-24 00:00:00.0, 2003-12-25"
      )
      sinkResult.isSuccess shouldBe true
      st.executeQuery(
        "select domain, schema, last_ts, last_date, last_long, last_decimal, mode, count, success, message, step from audit.sl_last_export"
      ).asIteratorOf(rowAsString)
        .toList should contain theSameElementsAs
      List(
        "public, test_table, null, null, 6, null, OVERWRITE, 1, true, c_int, 1",
        "public, test_table, null, null, 6, null, OVERWRITE, 1, true, c_int, 0",
        "public, test_table, null, null, 6, null, OVERWRITE, 2, true, c_int, ALL",
        "public, test_table, null, null, 1, null, OVERWRITE, 0, true, c_int, 1",
        "public, test_table, null, null, 1, null, OVERWRITE, 1, true, c_int, 0",
        "public, test_table, null, null, 1, null, OVERWRITE, 1, true, c_int, ALL"
      )
    }
  }

  it should "extract fully if forced, ignoring highest decimal partition" in {
    new WithSettings() {
      private val extractDataJob = new ExtractDataJob(settings.schemaHandler())
      val jdbcConnection = settings.appConfig.connections("test-pg")
      val conn = DriverManager.getConnection(
        jdbcConnection.options("url"),
        jdbcConnection.options("user"),
        jdbcConnection.options("password")
      )
      val st = conn.createStatement()
      st.execute(initSQL)
      private val outputFolder: File = better.files.File(starlakeTestRoot + "/data")
      outputFolder.createDirectory()
      implicit val extractExecutionContext: ExtractExecutionContext =
        new ExtractExecutionContext(None)
      val columns = extractDataJob.initExportAuditTable(settings.appConfig.connections("test-pg"))
      st.execute(
        """insert into audit.sl_last_export ("domain", "schema", "last_decimal", "start_ts", "end_ts", "duration", "mode", "count", "success", "message", "step")
          |values
          |('public', 'test_table', 3.4,  '2024-04-08'::timestamp, '2024-04-08'::timestamp, 10, 'OVERWRITE', 1, true, 'c_decimal', '0'),
          |('public', 'test_table', 3.4,  '2024-04-08'::timestamp, '2024-04-08'::timestamp, 10, 'OVERWRITE', 0, true, 'c_decimal', '1'),
          |('public', 'test_table', 3.4,  '2024-04-08'::timestamp, '2024-04-08'::timestamp, 10, 'OVERWRITE', 1, true, 'c_decimal', 'ALL');
          |""".stripMargin
      )
      private val partitionnedConfig: TableExtractDataConfig =
        TableExtractDataConfig(
          "public",
          "test_table",
          None,
          List(
            new TableAttribute().copy(
              name = "c_str"
            ),
            new TableAttribute().copy(
              name = "c_short"
            ),
            new TableAttribute().copy(
              name = "c_int"
            ),
            new TableAttribute().copy(
              name = "c_long"
            ),
            new TableAttribute().copy(
              name = "c_decimal"
            ),
            new TableAttribute().copy(
              name = "c_ts"
            ),
            new TableAttribute().copy(
              name = "c_date"
            )
          ),
          true,
          Some(1000),
          new Path(outputFolder.pathAsString),
          None,
          Some(
            PartitionConfig(
              "c_decimal",
              PrimitiveType.decimal,
              Some("abs( hashtext({{col}}) % {{nb_partitions}} )"),
              2
            )
          )
        )
      val allRows = ListBuffer[String]()
      val sinkResult = extractDataJob.extractTableData(
        ExtractJdbcDataConfig(
          JDBCSchema().copy(schema = "public"),
          new Path(outputFolder.pathAsString),
          0,
          2,
          None,
          Some(true),
          None,
          false,
          false,
          Nil,
          Nil,
          FileFormat(),
          jdbcConnection,
          jdbcConnection
        ),
        partitionnedConfig,
        "test",
        columns,
        (_, resultSet, _) => {
          val resultSetRows = resultSet
            .asIteratorOf(rowAsString)
            .toList
          allRows.appendAll(resultSetRows)
          Success(resultSetRows.length)
        }
      )
      allRows should contain theSameElementsAs
      List(
        "A\", 0, 1, 2, 3.4, 2003-02-01 00:00:00.0, 2003-02-02",
        "BÉ, 5, 6, 7, 8.9, 2003-12-24 00:00:00.0, 2003-12-25"
      )
      sinkResult.isSuccess shouldBe true
      st.executeQuery(
        "select domain, schema, last_ts, last_date, last_long, last_decimal, mode, count, success, message, step from audit.sl_last_export"
      ).asIteratorOf(rowAsString)
        .toList should contain theSameElementsAs
      List(
        "public, test_table, null, null, null, 8.9, OVERWRITE, 1, true, c_decimal, 1",
        "public, test_table, null, null, null, 8.9, OVERWRITE, 1, true, c_decimal, 0",
        "public, test_table, null, null, null, 8.9, OVERWRITE, 2, true, c_decimal, ALL",
        "public, test_table, null, null, null, 3.4, OVERWRITE, 0, true, c_decimal, 1",
        "public, test_table, null, null, null, 3.4, OVERWRITE, 1, true, c_decimal, 0",
        "public, test_table, null, null, null, 3.4, OVERWRITE, 1, true, c_decimal, ALL"
      )
    }
  }

  it should "extract fully if forced, ignoring highest timestamp partition" in {
    new WithSettings() {
      private val extractDataJob = new ExtractDataJob(settings.schemaHandler())
      val jdbcConnection = settings.appConfig.connections("test-pg")
      val conn = DriverManager.getConnection(
        jdbcConnection.options("url"),
        jdbcConnection.options("user"),
        jdbcConnection.options("password")
      )
      val st = conn.createStatement()
      st.execute(initSQL)
      private val outputFolder: File = better.files.File(starlakeTestRoot + "/data")
      outputFolder.createDirectory()
      implicit val extractExecutionContext: ExtractExecutionContext =
        new ExtractExecutionContext(None)
      val columns = extractDataJob.initExportAuditTable(settings.appConfig.connections("test-pg"))
      st.execute(
        """insert into audit.sl_last_export ("domain", "schema", "last_ts", "start_ts", "end_ts", "duration", "mode", "count", "success", "message", "step")
          |values
          |('public', 'test_table', '2003-02-01'::timestamp,  '2024-04-08'::timestamp, '2024-04-08'::timestamp, 10, 'OVERWRITE', 1, true, 'c_ts', '0'),
          |('public', 'test_table', '2003-02-01'::timestamp,  '2024-04-08'::timestamp, '2024-04-08'::timestamp, 10, 'OVERWRITE', 0, true, 'c_ts', '1'),
          |('public', 'test_table', '2003-02-01'::timestamp,  '2024-04-08'::timestamp, '2024-04-08'::timestamp, 10, 'OVERWRITE', 1, true, 'c_ts', 'ALL');
          |""".stripMargin
      )
      private val partitionnedConfig: TableExtractDataConfig =
        TableExtractDataConfig(
          "public",
          "test_table",
          None,
          List(
            new TableAttribute().copy(
              name = "c_str"
            ),
            new TableAttribute().copy(
              name = "c_short"
            ),
            new TableAttribute().copy(
              name = "c_int"
            ),
            new TableAttribute().copy(
              name = "c_long"
            ),
            new TableAttribute().copy(
              name = "c_decimal"
            ),
            new TableAttribute().copy(
              name = "c_ts"
            ),
            new TableAttribute().copy(
              name = "c_date"
            )
          ),
          true,
          Some(1000),
          new Path(outputFolder.pathAsString),
          None,
          Some(
            PartitionConfig(
              "c_ts",
              PrimitiveType.timestamp,
              Some("abs( hashtext({{col}}) % {{nb_partitions}} )"),
              2
            )
          )
        )
      val allRows = ListBuffer[String]()
      val sinkResult = extractDataJob.extractTableData(
        ExtractJdbcDataConfig(
          JDBCSchema().copy(schema = "public"),
          new Path(outputFolder.pathAsString),
          0,
          2,
          None,
          Some(true),
          None,
          false,
          false,
          Nil,
          Nil,
          FileFormat(),
          jdbcConnection,
          jdbcConnection
        ),
        partitionnedConfig,
        "test",
        columns,
        (_, resultSet, _) => {
          val resultSetRows = resultSet
            .asIteratorOf(rowAsString)
            .toList
          allRows.appendAll(resultSetRows)
          Success(resultSetRows.length)
        }
      )
      allRows should contain theSameElementsAs
      List(
        "A\", 0, 1, 2, 3.4, 2003-02-01 00:00:00.0, 2003-02-02",
        "BÉ, 5, 6, 7, 8.9, 2003-12-24 00:00:00.0, 2003-12-25"
      )
      sinkResult.isSuccess shouldBe true
      st.executeQuery(
        "select domain, schema, last_ts, last_date, last_long, last_decimal, mode, count, success, message, step from audit.sl_last_export"
      ).asIteratorOf(rowAsString)
        .toList should contain theSameElementsAs
      List(
        "public, test_table, 2003-12-24 00:00:00.0, null, null, null, OVERWRITE, 1, true, c_ts, 1",
        "public, test_table, 2003-12-24 00:00:00.0, null, null, null, OVERWRITE, 1, true, c_ts, 0",
        "public, test_table, 2003-12-24 00:00:00.0, null, null, null, OVERWRITE, 2, true, c_ts, ALL",
        "public, test_table, 2003-02-01 00:00:00.0, null, null, null, OVERWRITE, 0, true, c_ts, 1",
        "public, test_table, 2003-02-01 00:00:00.0, null, null, null, OVERWRITE, 1, true, c_ts, 0",
        "public, test_table, 2003-02-01 00:00:00.0, null, null, null, OVERWRITE, 1, true, c_ts, ALL"
      )
    }
  }

  it should "extract fully if forced, ignoring highest date partition" in {
    new WithSettings() {
      private val extractDataJob = new ExtractDataJob(settings.schemaHandler())
      val jdbcConnection = settings.appConfig.connections("test-pg")
      val conn = DriverManager.getConnection(
        jdbcConnection.options("url"),
        jdbcConnection.options("user"),
        jdbcConnection.options("password")
      )
      val st = conn.createStatement()
      st.execute(initSQL)
      private val outputFolder: File = better.files.File(starlakeTestRoot + "/data")
      outputFolder.createDirectory()
      implicit val extractExecutionContext: ExtractExecutionContext =
        new ExtractExecutionContext(None)
      val columns = extractDataJob.initExportAuditTable(settings.appConfig.connections("test-pg"))
      st.execute(
        """insert into audit.sl_last_export ("domain", "schema", "last_date", "start_ts", "end_ts", "duration", "mode", "count", "success", "message", "step")
          |values
          |('public', 'test_table', '2003-02-02'::date,  '2024-04-08'::timestamp, '2024-04-08'::timestamp, 10, 'OVERWRITE', 1, true, 'c_date', '0'),
          |('public', 'test_table', '2003-02-02'::date,  '2024-04-08'::timestamp, '2024-04-08'::timestamp, 10, 'OVERWRITE', 0, true, 'c_date', '1'),
          |('public', 'test_table', '2003-02-02'::date,  '2024-04-08'::timestamp, '2024-04-08'::timestamp, 10, 'OVERWRITE', 1, true, 'c_date', 'ALL');
          |""".stripMargin
      )
      private val partitionnedConfig: TableExtractDataConfig =
        TableExtractDataConfig(
          "public",
          "test_table",
          None,
          List(
            new TableAttribute().copy(
              name = "c_str"
            ),
            new TableAttribute().copy(
              name = "c_short"
            ),
            new TableAttribute().copy(
              name = "c_int"
            ),
            new TableAttribute().copy(
              name = "c_long"
            ),
            new TableAttribute().copy(
              name = "c_decimal"
            ),
            new TableAttribute().copy(
              name = "c_ts"
            ),
            new TableAttribute().copy(
              name = "c_date"
            )
          ),
          true,
          Some(1000),
          new Path(outputFolder.pathAsString),
          None,
          Some(
            PartitionConfig(
              "c_date",
              PrimitiveType.date,
              Some("abs( hashtext({{col}}) % {{nb_partitions}} )"),
              2
            )
          )
        )
      val allRows = ListBuffer[String]()
      val sinkResult = extractDataJob.extractTableData(
        ExtractJdbcDataConfig(
          JDBCSchema().copy(schema = "public"),
          new Path(outputFolder.pathAsString),
          0,
          2,
          None,
          Some(true),
          None,
          false,
          false,
          Nil,
          Nil,
          FileFormat(),
          jdbcConnection,
          jdbcConnection
        ),
        partitionnedConfig,
        "test",
        columns,
        (_, resultSet, _) => {
          val resultSetRows = resultSet
            .asIteratorOf(rowAsString)
            .toList
          allRows.appendAll(resultSetRows)
          Success(resultSetRows.length)
        }
      )
      allRows should contain theSameElementsAs
      List(
        "A\", 0, 1, 2, 3.4, 2003-02-01 00:00:00.0, 2003-02-02",
        "BÉ, 5, 6, 7, 8.9, 2003-12-24 00:00:00.0, 2003-12-25"
      )
      sinkResult.isSuccess shouldBe true
      st.executeQuery(
        "select domain, schema, last_ts, last_date, last_long, last_decimal, mode, count, success, message, step from audit.sl_last_export"
      ).asIteratorOf(rowAsString)
        .toList should contain theSameElementsAs
      List(
        "public, test_table, null, 2003-12-25, null, null, OVERWRITE, 1, true, c_date, 1",
        "public, test_table, null, 2003-12-25, null, null, OVERWRITE, 1, true, c_date, 0",
        "public, test_table, null, 2003-12-25, null, null, OVERWRITE, 2, true, c_date, ALL",
        "public, test_table, null, 2003-02-02, null, null, OVERWRITE, 0, true, c_date, 1",
        "public, test_table, null, 2003-02-02, null, null, OVERWRITE, 1, true, c_date, 0",
        "public, test_table, null, 2003-02-02, null, null, OVERWRITE, 1, true, c_date, ALL"
      )
    }
  }

  "cleanTableFiles" should "delete all files" in {
    new WithSettings() {
      private val extractDataJob = new ExtractDataJob(settings.schemaHandler())
      val jdbcConnection = settings.appConfig.connections("test-pg")
      val conn = DriverManager.getConnection(
        jdbcConnection.options("url"),
        jdbcConnection.options("user"),
        jdbcConnection.options("password")
      )
      val st = conn.createStatement()
      st.execute(initSQL)
      private val outputFolder: File = better.files.File(starlakeTestRoot + "/data")
      outputFolder.createDirectory()
      private val domainFolder: File = outputFolder / "public"
      domainFolder.createDirectory()
      (domainFolder / "test_table-20240102143002.csv").createFile()
      (domainFolder / "test_table-20240102143002-1.json").createFile()
      (domainFolder / "test_table-invalidpattern.json").createFile()
      (domainFolder / "other_table-20240102143002.json").createFile()
      extractDataJob.cleanTableFiles(
        storageHandler,
        new Path(domainFolder.pathAsString),
        "test_table"
      )
      domainFolder.list.map(_.name).toList should contain theSameElementsAs List(
        "test_table-invalidpattern.json",
        "other_table-20240102143002.json"
      )
    }
  }

  "getCurrentTableDefinition" should "retrieve the correct configuration" in {
    new WithSettings() {
      private val extractDataJob = new ExtractDataJob(settings.schemaHandler())
      // specific configuration
      extractDataJob.resolveTableDefinition(
        JDBCSchema(tables =
          List(
            new JDBCTable().copy(name = "T1", partitionColumn = Some("SpecificT1")),
            new JDBCTable().copy(name = "T2", partitionColumn = Some("SpecificT2")),
            new JDBCTable().copy(name = "*", fullExport = Some(true))
          )
        ),
        "T1"
      ) shouldBe Some(new JDBCTable().copy(name = "T1", partitionColumn = Some("SpecificT1")))

      // ignore case
      extractDataJob.resolveTableDefinition(
        JDBCSchema(tables =
          List(
            new JDBCTable().copy(name = "T1", partitionColumn = Some("SpecificT1")),
            new JDBCTable().copy(name = "T2", partitionColumn = Some("SpecificT2")),
            new JDBCTable().copy(name = "*", fullExport = Some(true))
          )
        ),
        "t2"
      ) shouldBe Some(new JDBCTable().copy(name = "T2", partitionColumn = Some("SpecificT2")))

      // fallback to * config
      extractDataJob.resolveTableDefinition(
        JDBCSchema(tables =
          List(
            new JDBCTable().copy(name = "T1", partitionColumn = Some("SpecificT1")),
            new JDBCTable().copy(name = "T2", partitionColumn = Some("SpecificT2")),
            new JDBCTable().copy(name = "*", fullExport = Some(true))
          )
        ),
        "t3"
      ) shouldBe Some(new JDBCTable().copy(name = "*", fullExport = Some(true)))

      // no config
      extractDataJob.resolveTableDefinition(
        JDBCSchema(tables =
          List(
            new JDBCTable().copy(name = "T1", partitionColumn = Some("SpecificT1")),
            new JDBCTable().copy(name = "T2", partitionColumn = Some("SpecificT2"))
          )
        ),
        "t3"
      ) shouldBe None
    }
  }

  "isExtractionNeeded" should "check predicate if given" in {
    new WithSettings() {
      private val extractDataJob = new ExtractDataJob(settings.schemaHandler())
      val jdbcConnection = settings.appConfig.connections("test-pg")
      val conn = DriverManager.getConnection(
        jdbcConnection.options("url"),
        jdbcConnection.options("user"),
        jdbcConnection.options("password")
      )
      val st = conn.createStatement()
      private val outputFolder: File = better.files.File(starlakeTestRoot + "/data")
      outputFolder.createDirectory()
      val columns = ParUtils.withExecutor() { implicit ec =>
        implicit val extractExecutionContext =
          new ExtractExecutionContext(ec)
        extractDataJob.initExportAuditTable(settings.appConfig.connections("test-pg"))
      }
      st.execute(
        """insert into audit.sl_last_export ("domain", "schema", "last_date", "start_ts", "end_ts", "duration", "mode", "count", "success", "message", "step")
          |values
          |('public', 'stale_table', '2003-02-02'::date,  '2024-04-07'::timestamp, '2024-04-08'::timestamp, 10, 'OVERWRITE', 1, true, 'c_date', '0'),
          |('public', 'stale_table', '2003-02-02'::date,  '2024-04-07'::timestamp, '2024-04-08'::timestamp, 10, 'OVERWRITE', 1, true, 'c_date', 'ALL'),
          |('public', 'refreshed_table', '2003-02-02'::date,  '2024-04-08'::timestamp, '2024-04-08'::timestamp, 10, 'OVERWRITE', 0, true, 'c_date', '0'),
          |('public', 'refreshed_table', '2003-02-02'::date,  '2024-04-08'::timestamp, '2024-04-08'::timestamp, 10, 'OVERWRITE', 0, true, 'c_date', 'ALL'),
          |('public', 'unsucessful_table', '2003-02-02'::date,  '2024-04-08'::timestamp, '2024-04-08'::timestamp, 10, 'OVERWRITE', 0, true, 'c_date', '1'),
          |('public', 'unsucessful_table', '2003-02-02'::date,  '2024-04-08'::timestamp, '2024-04-08'::timestamp, 10, 'OVERWRITE', 0, true, 'c_date', '0'),
          |('public', 'unsucessful_table', '2003-02-02'::date,  '2024-04-08'::timestamp, '2024-04-08'::timestamp, 10, 'OVERWRITE', 0, false, 'c_date', 'ALL');
          |""".stripMargin
      )
      private val partitionnedConfig: TableExtractDataConfig =
        TableExtractDataConfig(
          "public",
          "test_table",
          None,
          List(
            new TableAttribute().copy(
              name = "c_str"
            ),
            new TableAttribute().copy(
              name = "c_short"
            ),
            new TableAttribute().copy(
              name = "c_int"
            ),
            new TableAttribute().copy(
              name = "c_long"
            ),
            new TableAttribute().copy(
              name = "c_decimal"
            ),
            new TableAttribute().copy(
              name = "c_ts"
            ),
            new TableAttribute().copy(
              name = "c_date"
            )
          ),
          true,
          Some(1000),
          new Path(outputFolder.pathAsString),
          None,
          Some(
            PartitionConfig(
              "c_date",
              PrimitiveType.date,
              Some("abs( hashtext({{col}}) % {{nb_partitions}} )"),
              2
            )
          )
        )
      private val extractDataConfig: ExtractJdbcDataConfig = ExtractJdbcDataConfig(
        JDBCSchema().copy(schema = "public"),
        new Path(outputFolder.pathAsString),
        0,
        2,
        None,
        Some(true),
        None,
        false,
        false,
        Nil,
        Nil,
        FileFormat(),
        jdbcConnection,
        jdbcConnection
      )

      val localDateTime = LocalDateTime
        .of(2024, 4, 8, 0, 0)
        .atZone(ZoneId.systemDefault())
        .toInstant
        .toEpochMilli
      // stale case: extracted before but not fresh enough
      ParUtils.withExecutor() { ec =>
        implicit val extractEC = new ExtractExecutionContext(ec)
        extractDataJob.isExtractionNeeded(
          extractDataConfig.copy(extractionPredicate =
            Some(
              _ < localDateTime
            )
          ),
          partitionnedConfig.copy(table = "stale_table"),
          columns
        ) shouldBe true
        // refreshed case : retry all tables on failure. Some tables may have already been extracted
        extractDataJob.isExtractionNeeded(
          extractDataConfig.copy(extractionPredicate =
            Some(
              _ < localDateTime
            )
          ),
          partitionnedConfig.copy(table = "refreshed_table"),
          columns
        ) shouldBe false

        // no predicate case: must be extracted by default
        extractDataJob.isExtractionNeeded(
          extractDataConfig,
          partitionnedConfig.copy(table = "refreshed_table"),
          columns
        ) shouldBe true

        // table with only unsuccessful extract
        extractDataJob.isExtractionNeeded(
          extractDataConfig.copy(extractionPredicate =
            Some(
              _ < LocalDateTime
                .of(2023, 4, 8, 0, 0)
                .toInstant(OffsetDateTime.now(ZoneId.systemDefault()).getOffset)
                .toEpochMilli
            )
          ),
          partitionnedConfig.copy(table = "unsucessful_table"),
          columns
        ) shouldBe true

        // non-existent table: first time it is extracted
        extractDataJob.isExtractionNeeded(
          extractDataConfig.copy(extractionPredicate =
            Some(
              _ < LocalDateTime
                .of(2023, 4, 8, 0, 0)
                .toInstant(OffsetDateTime.now(ZoneId.systemDefault()).getOffset)
                .toEpochMilli
            )
          ),
          partitionnedConfig.copy(table = "non_existant_table"),
          columns
        ) shouldBe true
      }
    }
  }

  "isTableFullExport" should "follow precedence CLI, table, schema, default" in {
    new WithSettings() {
      private val extractDataJob = new ExtractDataJob(settings.schemaHandler())
      // specific configuration
      val jdbcConnection = settings.appConfig.connections("test-pg")

      // CLI case which can be only incremental
      extractDataJob.isTableFullExport(
        ExtractJdbcDataConfig(
          jdbcSchema = new JDBCSchema().copy(fullExport = Some(true)),
          baseOutputDir = new Path("."),
          limit = 1,
          numPartitions = 1,
          parallelism = None,
          cliFullExport = Some(false),
          extractionPredicate = None,
          ignoreExtractionFailure = true,
          cleanOnExtract = true,
          includeTables = Nil,
          excludeTables = Nil,
          outputFormat = FileFormat(),
          data = jdbcConnection,
          audit = jdbcConnection
        ),
        Some(new JDBCTable().copy(fullExport = Some(true)))
      ) shouldBe false

      // table case
      extractDataJob.isTableFullExport(
        ExtractJdbcDataConfig(
          jdbcSchema = new JDBCSchema().copy(fullExport = Some(true)),
          baseOutputDir = new Path("."),
          limit = 1,
          numPartitions = 1,
          parallelism = None,
          cliFullExport = None,
          extractionPredicate = None,
          ignoreExtractionFailure = true,
          cleanOnExtract = true,
          includeTables = Nil,
          excludeTables = Nil,
          outputFormat = FileFormat(),
          data = jdbcConnection,
          audit = jdbcConnection
        ),
        Some(new JDBCTable().copy(fullExport = Some(false)))
      ) shouldBe false

      // schema case
      extractDataJob.isTableFullExport(
        ExtractJdbcDataConfig(
          jdbcSchema = new JDBCSchema().copy(fullExport = Some(false)),
          baseOutputDir = new Path("."),
          limit = 1,
          numPartitions = 1,
          parallelism = None,
          cliFullExport = None,
          extractionPredicate = None,
          ignoreExtractionFailure = true,
          cleanOnExtract = true,
          includeTables = Nil,
          excludeTables = Nil,
          outputFormat = FileFormat(),
          data = jdbcConnection,
          audit = jdbcConnection
        ),
        Some(new JDBCTable().copy(fullExport = None))
      ) shouldBe false

      // default case which is true
      extractDataJob.isTableFullExport(
        ExtractJdbcDataConfig(
          jdbcSchema = new JDBCSchema().copy(fullExport = None),
          baseOutputDir = new Path("."),
          limit = 1,
          numPartitions = 1,
          parallelism = None,
          cliFullExport = None,
          extractionPredicate = None,
          ignoreExtractionFailure = true,
          cleanOnExtract = true,
          includeTables = Nil,
          excludeTables = Nil,
          outputFormat = FileFormat(),
          data = jdbcConnection,
          audit = jdbcConnection
        ),
        Some(new JDBCTable().copy(fullExport = None))
      ) shouldBe true
    }
  }

  "computeTableFetchSize" should "follow precedence table, schema" in {
    new WithSettings() {
      private val extractDataJob = new ExtractDataJob(settings.schemaHandler())
      // specific configuration
      val jdbcConnection = settings.appConfig.connections("test-pg")

      // table case
      extractDataJob.computeTableFetchSize(
        ExtractJdbcDataConfig(
          jdbcSchema = new JDBCSchema().copy(fetchSize = Some(100)),
          baseOutputDir = new Path("."),
          limit = 1,
          numPartitions = 1,
          parallelism = None,
          cliFullExport = None,
          extractionPredicate = None,
          ignoreExtractionFailure = true,
          cleanOnExtract = true,
          includeTables = Nil,
          excludeTables = Nil,
          outputFormat = FileFormat(),
          data = jdbcConnection,
          audit = jdbcConnection
        ),
        Some(new JDBCTable().copy(fetchSize = Some(200)))
      ) shouldBe Some(200)

      // schema case
      extractDataJob.computeTableFetchSize(
        ExtractJdbcDataConfig(
          jdbcSchema = new JDBCSchema().copy(fetchSize = Some(100)),
          baseOutputDir = new Path("."),
          limit = 1,
          numPartitions = 1,
          parallelism = None,
          cliFullExport = None,
          extractionPredicate = None,
          ignoreExtractionFailure = true,
          cleanOnExtract = true,
          includeTables = Nil,
          excludeTables = Nil,
          outputFormat = FileFormat(),
          data = jdbcConnection,
          audit = jdbcConnection
        ),
        Some(new JDBCTable().copy(fetchSize = None))
      ) shouldBe Some(100)

      // default case which is true
      extractDataJob.computeTableFetchSize(
        ExtractJdbcDataConfig(
          jdbcSchema = new JDBCSchema().copy(fetchSize = None),
          baseOutputDir = new Path("."),
          limit = 1,
          numPartitions = 1,
          parallelism = None,
          cliFullExport = None,
          extractionPredicate = None,
          ignoreExtractionFailure = true,
          cleanOnExtract = true,
          includeTables = Nil,
          excludeTables = Nil,
          outputFormat = FileFormat(),
          data = jdbcConnection,
          audit = jdbcConnection
        ),
        Some(new JDBCTable().copy(fetchSize = None))
      ) shouldBe None
    }
  }
}
