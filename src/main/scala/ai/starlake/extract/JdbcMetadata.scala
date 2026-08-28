package ai.starlake.extract
import ai.starlake.config.Settings
import ai.starlake.config.Settings.{ConnectionInfo, JdbcEngine}
import ai.starlake.extract.JdbcDbUtils.{
  formatRemarksSQL,
  ColumnName,
  Columns,
  PrimaryKeys,
  TableName,
  TableRemarks
}
import ai.starlake.schema.model.{JDBCSchema, TableAttribute}
import com.typesafe.scalalogging.LazyLogging

import java.sql.{DatabaseMetaData, ResultSetMetaData}
import java.sql.Types.{
  BIGINT,
  BINARY,
  BIT,
  BOOLEAN,
  CHAR,
  DATE,
  DECIMAL,
  DOUBLE,
  FLOAT,
  INTEGER,
  LONGVARCHAR,
  NUMERIC,
  REAL,
  SMALLINT,
  TIMESTAMP,
  TIMESTAMP_WITH_TIMEZONE,
  TINYINT,
  VARBINARY,
  VARCHAR
}
import scala.collection.mutable
import scala.util.{Failure, Success, Try, Using}

sealed trait JdbcColumnMetadata extends LazyLogging {

  def keepOriginalName: Boolean

  def skipRemarks: Boolean

  def jdbcEngine: Option[JdbcEngine]

  def jdbcSchema: JDBCSchema

  def tableName: TableName

  def primaryKeys: PrimaryKeys

  /** @return
    *   a map of foreign key name and its pk composite name
    */
  def foreignKeys: Map[ColumnName, ColumnName]

  /** @return
    *   a list of attributes representing resource's columns
    */
  def columns: Columns

  // java.sql.Types
  private val sqlTypes = Map(
    "BIT"                     -> -7,
    "TINYINT"                 -> -6,
    "SMALLINT"                -> 5,
    "INTEGER"                 -> 4,
    "BIGINT"                  -> -5,
    "FLOAT"                   -> 6,
    "REAL"                    -> 7,
    "DOUBLE"                  -> 8,
    "NUMERIC"                 -> 2,
    "DECIMAL"                 -> 3,
    "CHAR"                    -> 1,
    "VARCHAR"                 -> 12,
    "LONGVARCHAR"             -> -1,
    "DATE"                    -> 91,
    "TIME"                    -> 92,
    "TIMESTAMP"               -> 93,
    "BINARY"                  -> -2,
    "VARBINARY"               -> -3,
    "LONGVARBINARY"           -> -4,
    "NULL"                    -> 0,
    "OTHER"                   -> 1111,
    "BOOLEAN"                 -> 16,
    "NVARCHAR"                -> -9,
    "NCHAR"                   -> -15,
    "LONGNVARCHAR"            -> -16,
    "TIME_WITH_TIMEZONE"      -> 2013,
    "TIMESTAMP_WITH_TIMEZONE" -> 2014
  )

  private val reverseSqlTypes = sqlTypes map (_.swap)

  /** Map jdbc type to spark type (aka Starflow primitive type)
    */
  protected def sparkType(
    jdbcType: Int,
    tableName: String,
    colName: String,
    colTypename: String
  ): String = {
    val sqlType = reverseSqlTypes.getOrElse(jdbcType, colTypename)
    jdbcType match {
      case VARCHAR | CHAR | LONGVARCHAR          => "string"
      case BIT | BOOLEAN                         => "boolean"
      case DOUBLE | FLOAT | REAL                 => "double"
      case NUMERIC | DECIMAL                     => "decimal"
      case TINYINT | SMALLINT | INTEGER | BIGINT => "long"
      case DATE                                  => "date"
      case TIMESTAMP                             => "timestamp"
      case TIMESTAMP_WITH_TIMEZONE =>
        logger.warn(s"forced conversion for $tableName.$colName from $sqlType to timestamp")
        "timestamp"
      case VARBINARY | BINARY =>
        logger.warn(s"forced conversion for $tableName.$colName from $sqlType to string")
        "string"
      case 2000 => // HUGEINT from DuckDB
        logger.warn(s"forced conversion for $tableName.$colName from $sqlType (2000) to long")
        "long"
      case _ =>
        logger.warn(
          s"""Extracting user defined type $colTypename of type $sqlType ($jdbcType) in $tableName.$colName  as string"""
        )
        // colTypename
        "string"
    }
  }

  /** Get column comments for a database table
    *
    * @param jdbcSchema
    * @param connectionOptions
    * @param table
    * @param settings
    * @return
    */
  protected def extractColumnRemarks(
    jdbcSchema: JDBCSchema,
    connection: java.sql.Connection,
    table: String,
    jdbcEngine: Option[JdbcEngine]
  )(implicit
    settings: Settings
  ): Option[Map[ColumnName, TableRemarks]] = {
    val columnRemarks = jdbcSchema.columnRemarks.orElse(jdbcEngine.flatMap(_.columnRemarks))
    columnRemarks.map { remarks =>
      val sql = formatRemarksSQL(jdbcSchema, table, remarks)
      logger.debug(s"Extracting column remarks using $sql")
      Using.resource(connection.createStatement()) { statement =>
        Using.resource(statement.executeQuery(sql)) { rs =>
          val res = mutable.Map.empty[String, String]
          while (rs.next()) {
            res.put(rs.getString(1), rs.getString(2))
          }
          res.toMap
        }
      }
    }
  }

  // remove duplicates
  // see https://stackoverflow.com/questions/1601203/jdbc-databasemetadata-getcolumns-returns-duplicate-columns
  protected def removeDuplicatesColumns(list: List[TableAttribute]): List[TableAttribute] =
    list.foldLeft(List.empty[TableAttribute]) { (partialResult, element) =>
      if (partialResult.exists(_.name == element.name)) partialResult
      else partialResult :+ element
    }

  protected def computeFinalColumnName(tableName: String, columnName: String) = {
    if (keepOriginalName) {
      columnName
    } else {
      val pkTableColumnsOpt = jdbcSchema.tables
        .find(_.name.equalsIgnoreCase(tableName))
        .map(_.columns)
      pkTableColumnsOpt
        .flatMap(
          _.find(_.name.equalsIgnoreCase(columnName)).flatMap(_.rename)
        )
        .getOrElse(columnName)
    }
  }
}

final class JdbcColumnDatabaseMetadata(
  connectionSettings: ConnectionInfo,
  databaseMetaData: DatabaseMetaData,
  val jdbcSchema: JDBCSchema,
  schemaName: String,
  val tableName: String,
  val keepOriginalName: Boolean,
  val skipRemarks: Boolean,
  val jdbcEngine: Option[JdbcEngine]
)(implicit settings: Settings)
    extends JdbcColumnMetadata {

  override lazy val primaryKeys: PrimaryKeys = {
    Try {
      Using
        .resource(
          connectionSettings match {
            case d if d.isMySQLOrMariaDb() =>
              databaseMetaData.getPrimaryKeys(
                schemaName,
                None.orNull,
                tableName
              )
            case _ =>
              databaseMetaData.getPrimaryKeys(
                jdbcSchema.catalog.orNull,
                schemaName,
                tableName
              )
          }
        ) { primaryKeysResultSet =>
          new Iterator[String] {
            def hasNext: Boolean = primaryKeysResultSet.next()

            def next(): String = {
              val pkColumnName = primaryKeysResultSet.getString("COLUMN_NAME")
              computeFinalColumnName(tableName, pkColumnName)
            }
          }.toList
        }
    } match {
      case Failure(exception) =>
        logger.warn(
          s"Could not extract primary keys for table $tableName",
          exception
        )
        Nil
      case Success(value) => value
    }
  }

  /** @return
    *   a map of foreign key name and its pk composite name
    */
  override lazy val foreignKeys: Map[String, String] = {
    Try {
      Using.resource(connectionSettings match {
        case d if d.isMySQLOrMariaDb() =>
          databaseMetaData.getImportedKeys(
            schemaName,
            None.orNull,
            tableName
          )
        case _ =>
          databaseMetaData.getImportedKeys(
            jdbcSchema.catalog.orNull,
            schemaName,
            tableName
          )
      }) { foreignKeysResultSet =>
        new Iterator[(String, String)] {

          def hasNext: Boolean = foreignKeysResultSet.next()

          def next(): (String, String) = {
            val pkSchemaName = foreignKeysResultSet.getString("PKTABLE_SCHEM")
            val pkTableName = foreignKeysResultSet.getString("PKTABLE_NAME")
            val pkColumnName = foreignKeysResultSet.getString("PKCOLUMN_NAME")
            val pkFinalColumnName = computeFinalColumnName(pkTableName, pkColumnName)
            val fkColumnName =
              foreignKeysResultSet.getString("FKCOLUMN_NAME").toUpperCase

            val pkCompositeName =
              if (pkSchemaName == null) s"$pkTableName.$pkFinalColumnName"
              else s"$pkSchemaName.$pkTableName.$pkFinalColumnName"

            fkColumnName -> pkCompositeName
          }
        }.toMap
      }
    } match {
      case Failure(exception) =>
        logger.warn(s"Could not extract foreign keys for table $tableName")
        Map.empty[String, String]
      case Success(value) => value
    }
  }

  override lazy val columns: List[TableAttribute] = {
    Using.resource(
      connectionSettings match {
        case d if d.isMySQLOrMariaDb() =>
          databaseMetaData.getColumns(
            schemaName,
            None.orNull,
            tableName,
            None.orNull
          )
        case _ =>
          databaseMetaData.getColumns(
            jdbcSchema.catalog.orNull,
            schemaName,
            tableName,
            None.orNull
          )
      }
    ) { columnsResultSet =>
      val remarks: Map[String, String] = (
        if (skipRemarks) None
        else
          extractColumnRemarks(jdbcSchema, databaseMetaData.getConnection, tableName, jdbcEngine)
      ).getOrElse(Map.empty)

      val attrs =
        new Iterator[TableAttribute] {
          def hasNext: Boolean = columnsResultSet.next()

          def next(): TableAttribute = {
            val colName = columnsResultSet.getString("COLUMN_NAME")
            val finalColName =
              jdbcSchema.tables
                .find(_.name.equalsIgnoreCase(tableName))
                .map(_.columns)
                .flatMap(_.find(_.name.equalsIgnoreCase(colName)))
                .flatMap(_.rename)
                .getOrElse(colName)
            logger.debug(
              s"COLUMN_NAME=$tableName.$colName and its extracted name is $finalColName"
            )
            val colType = columnsResultSet.getInt("DATA_TYPE")
            val colTypename = columnsResultSet.getString("TYPE_NAME")
            val colRemarks =
              remarks.getOrElse(colName, columnsResultSet.getString("REMARKS"))
            val colRequired = columnsResultSet.getString("IS_NULLABLE").equals("NO")
            val foreignKey = foreignKeys.get(colName.toUpperCase)
            // val columnPosition = columnsResultSet.getInt("ORDINAL_POSITION")
            TableAttribute(
              name = if (keepOriginalName) colName else finalColName,
              rename = if (keepOriginalName) Some(finalColName) else None,
              `type` = sparkType(
                colType,
                tableName,
                colName,
                colTypename
              ),
              required = Some(colRequired),
              comment = Option(colRemarks),
              foreignKey = foreignKey
            )
          }
        }.toList
      removeDuplicatesColumns(attrs)
    }
  }
}

class ResultSetColumnMetadata(
  queryMetadata: ResultSetMetaData,
  val jdbcSchema: JDBCSchema,
  val tableName: String,
  val keepOriginalName: Boolean,
  val skipRemarks: Boolean,
  val jdbcEngine: Option[JdbcEngine]
) extends JdbcColumnMetadata {

  override val primaryKeys: List[String] = Nil

  /** @return
    *   a map of foreign key name and its pk composite name
    */
  override val foreignKeys: Map[String, String] = Map.empty

  /** @return
    *   a list of attributes representing resource's columns
    */
  override lazy val columns: List[TableAttribute] = {
    (1 to queryMetadata.getColumnCount).map { i =>
      val colName = queryMetadata.getColumnName(i)
      val finalColName =
        jdbcSchema.tables
          .find(_.name.equalsIgnoreCase(tableName))
          .map(_.columns)
          .flatMap(_.find(_.name.equalsIgnoreCase(colName)))
          .flatMap(_.rename)
          .getOrElse(colName)
      logger.debug(
        s"COLUMN_NAME=$tableName.$colName and its extracted name is $finalColName"
      )
      val colType = queryMetadata.getColumnType(i)
      val colTypename = queryMetadata.getColumnTypeName(i)
      TableAttribute(
        name = if (keepOriginalName) colName else finalColName,
        rename = if (keepOriginalName) Some(finalColName) else None,
        `type` = sparkType(
          colType,
          tableName,
          colName,
          colTypename
        )
      )
    }.toList
  }
}
