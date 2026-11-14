package ai.starlake.sql

import ai.starlake.extract.JdbcDbUtils
import ai.starlake.extract.JdbcDbUtils.StarlakeConnectionPool
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.catalyst.analysis.{IndexAlreadyExistsException, NoSuchIndexException}
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.connector.expressions.NamedReference
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JdbcUtils}
import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcDialects, JdbcSQLQueryBuilder, JdbcType}
import org.apache.spark.sql.types.*

import java.sql.{Connection, SQLException, Types}
import java.util
import java.util.Locale
import scala.collection.mutable.ArrayBuilder
import scala.jdk.CollectionConverters.*

private object StarlakeSnowflakeDialect extends JdbcDialect with SQLConfHelper {
  override def canHandle(url: String): Boolean = url.toLowerCase.startsWith("jdbc:snowflake:")
  // override def quoteIdentifier(column: String): String = column
  override def getJDBCType(dt: DataType): Option[JdbcType] = dt match {
    case BooleanType => Some(JdbcType("BOOLEAN", java.sql.Types.BOOLEAN))
    case TimestampType =>
      Some(JdbcType(sys.env.getOrElse("SF_TIMEZONE", "TIMESTAMP"), java.sql.Types.TIMESTAMP))
    case _ => JdbcDbUtils.getCommonJDBCType(dt)._1
  }
}

private object StarlakeDuckDbDialect extends JdbcDialect with SQLConfHelper {
  override def createConnectionFactory(options: JDBCOptions): Int => Connection = {
    // Without the options of the Spark data source (query, fetchsize...), which DuckDB
    // refuses as unknown settings (#1897)
    val connectionOptions =
      options.asConnectionProperties.asScala.toMap +
      ("url" -> options.url) + ("driver" -> options.driverClass)
    (partitionId: Int) => {
      try {
        StarlakeConnectionPool.getConnection(None, connectionOptions)
      } catch {
        case e: Throwable =>
          throw new Exception(
            s"Error while creating connection for partition $partitionId",
            e
          )
      }
    }
  }

  override def canHandle(url: String): Boolean = url.toLowerCase.startsWith("jdbc:duckdb:")
  // override def quoteIdentifier(column: String): String = column
  override def getJDBCType(dt: DataType): Option[JdbcType] = dt match {
    case BooleanType => Some(JdbcType("BOOLEAN", java.sql.Types.BOOLEAN))
    case _           => JdbcDbUtils.getCommonJDBCType(dt)._1
  }
  override def getTableExistsQuery(table: String): String = {
    s"SELECT count(*) as cnt FROM $table WHERE 1=0"
  }
  override def getCatalystType(
    sqlType: Int,
    typeName: String,
    size: Int,
    md: MetadataBuilder
  ): Option[DataType] = {
    typeName.toLowerCase() match {
      case "hugeint" => Some(LongType)
      case _ =>
        if (sqlType == Types.TIMESTAMP_WITH_TIMEZONE) {
          Some(TimestampType)
        } else
          super.getCatalystType(sqlType, typeName, size, md)
    }
  }
}

object StarlakeJdbcDialects {
  val dialects = List(StarlakeSnowflakeDialect, StarlakeDuckDbDialect, MariaDbDialect)
  def registerDialects() =
    dialects.foreach { dialect =>
      JdbcDialects.registerDialect(dialect)
    }
}

private case object MariaDbDialect extends JdbcDialect with SQLConfHelper {

  override def canHandle(url: String): Boolean =
    url.toLowerCase(Locale.ROOT).startsWith("jdbc:mariadb") ||
    url.toLowerCase(Locale.ROOT).startsWith("jdbc:mysql")

  private val distinctUnsupportedAggregateFunctions =
    Set("VAR_POP", "VAR_SAMP", "STDDEV_POP", "STDDEV_SAMP")

  // See https://dev.mysql.com/doc/refman/8.0/en/aggregate-functions.html
  private val supportedAggregateFunctions =
    Set("MAX", "MIN", "SUM", "COUNT", "AVG") ++ distinctUnsupportedAggregateFunctions
  private val supportedFunctions = supportedAggregateFunctions

  override def isSupportedFunction(funcName: String): Boolean =
    supportedFunctions.contains(funcName)

  override def getCatalystType(
    sqlType: Int,
    typeName: String,
    size: Int,
    md: MetadataBuilder
  ): Option[DataType] = {
    if (sqlType == Types.VARBINARY && typeName.equals("BIT") && size != 1) {
      // MariaDB connector behaviour
      // This could instead be a BinaryType if we'd rather return bit-vectors of up to 64 bits as
      // byte arrays instead of longs.
      md.putLong("binarylong", 1)
      Option(LongType)
    } else if (sqlType == Types.BIT && size > 1) {
      // MySQL connector behaviour
      md.putLong("binarylong", 1)
      Option(LongType)
    } else if (sqlType == Types.BIT && typeName.equals("TINYINT")) {
      Option(BooleanType)
    } else if ("TINYTEXT".equalsIgnoreCase(typeName)) {
      // TINYTEXT is Types.VARCHAR(63) from mysql jdbc, but keep it AS-IS for historical reason
      Some(StringType)
    } else if (sqlType == Types.VARCHAR && typeName.equals("JSON")) {
      // Some MySQL JDBC drivers converts JSON type into Types.VARCHAR with a precision of -1.
      // Explicitly converts it into StringType here.
      Some(StringType)
    } else if (sqlType == Types.TINYINT) {
      if (md.build().getBoolean("isSigned")) {
        Some(ByteType)
      } else {
        Some(ShortType)
      }
    } else None
  }

  override def quoteIdentifier(colName: String): String = {
    s"`$colName`"
  }

  override def schemasExists(conn: Connection, options: JDBCOptions, schema: String): Boolean = {
    listSchemas(conn, options).exists(_.head == schema)
  }

  override def listSchemas(conn: Connection, options: JDBCOptions): Array[Array[String]] = {
    val schemaBuilder = ArrayBuilder.make[Array[String]]
    try {
      JdbcUtils.executeQuery(conn, options, "SHOW SCHEMAS") { rs =>
        while (rs.next()) {
          schemaBuilder += Array(rs.getString("Database"))
        }
      }
    } catch {
      case _: Exception =>
        logWarning("Cannot show schemas.")
    }
    schemaBuilder.result()
  }

  override def isCascadingTruncateTable(): Option[Boolean] = Some(false)

  // See https://dev.mysql.com/doc/refman/8.0/en/alter-table.html
  override def getUpdateColumnTypeQuery(
    tableName: String,
    columnName: String,
    newDataType: String
  ): String = {
    s"ALTER TABLE $tableName MODIFY COLUMN ${quoteIdentifier(columnName)} $newDataType"
  }

  // See https://dev.mysql.com/doc/refman/8.0/en/alter-table.html
  override def getTableCommentQuery(table: String, comment: String): String = {
    s"ALTER TABLE $table COMMENT = '$comment'"
  }

  override def getJDBCType(dt: DataType): Option[JdbcType] = dt match {
    // See SPARK-35446: MySQL treats REAL as a synonym to DOUBLE by default
    // We override getJDBCType so that FloatType is mapped to FLOAT instead
    case FloatType  => Option(JdbcType("FLOAT", java.sql.Types.FLOAT))
    case StringType => Option(JdbcType("LONGTEXT", java.sql.Types.LONGVARCHAR))
    case ByteType   => Option(JdbcType("TINYINT", java.sql.Types.TINYINT))
    case _          => JdbcUtils.getCommonJDBCType(dt)
  }

  // CREATE INDEX syntax
  // https://dev.mysql.com/doc/refman/8.0/en/create-index.html
  override def createIndex(
    indexName: String,
    tableIdent: Identifier,
    columns: Array[NamedReference],
    columnsProperties: util.Map[NamedReference, util.Map[String, String]],
    properties: util.Map[String, String]
  ): String = {
    val columnList = columns.map(col => quoteIdentifier(col.fieldNames.head))
    val (indexType, indexPropertyList) = JdbcUtils.processIndexProperties(properties, "mysql")

    // columnsProperties doesn't apply to MySQL so it is ignored
    s"CREATE INDEX ${quoteIdentifier(indexName)} $indexType ON" +
    s" ${quoteIdentifier(tableIdent.name())} (${columnList.mkString(", ")})" +
    s" ${indexPropertyList.mkString(" ")}"
  }

  // SHOW INDEX syntax
  // https://dev.mysql.com/doc/refman/8.0/en/show-index.html
  override def indexExists(
    conn: Connection,
    indexName: String,
    tableIdent: Identifier,
    options: JDBCOptions
  ): Boolean = {
    val sql = s"SHOW INDEXES FROM ${quoteIdentifier(tableIdent.name())} " +
      s"WHERE key_name = '$indexName'"
    JdbcUtils.checkIfIndexExists(conn, sql, options)
  }

  override def dropIndex(indexName: String, tableIdent: Identifier): String = {
    s"DROP INDEX ${quoteIdentifier(indexName)} ON ${tableIdent.name()}"
  }

  override def classifyException(message: String, e: Throwable): AnalysisException = {
    e match {
      case sqlException: SQLException =>
        sqlException.getErrorCode match {
          // ER_DUP_KEYNAME
          case 1061 =>
            // The message is: Failed to create index indexName in tableName
            val regex = "(?s)Failed to create index (.*) in (.*)".r
            regex.findFirstMatchIn(message) match {
              case Some(m) =>
                throw new IndexAlreadyExistsException(
                  indexName = m.group(1),
                  tableName = m.group(2),
                  cause = Some(e)
                )
              case None => super.classifyException(message, e)
            }
          case 1091 =>
            // The message is: Failed to drop index indexName in tableName
            val regex = "(?s)Failed to drop index (.*) in (.*)".r
            regex.findFirstMatchIn(message) match {
              case Some(m) =>
                throw new NoSuchIndexException(m.group(1), m.group(2), cause = Some(e))
              case None => super.classifyException(message, e)
            }
          case _ => super.classifyException(message, e)
        }
      case unsupported: UnsupportedOperationException => throw unsupported
      case _                                          => super.classifyException(message, e)
    }
  }

  class MySQLSQLQueryBuilder(dialect: JdbcDialect, options: JDBCOptions)
      extends JdbcSQLQueryBuilder(dialect, options) {

    override def build(): String = {
      val limitOrOffsetStmt = if (limit > 0) {
        if (offset > 0) {
          s"LIMIT $offset, $limit"
        } else {
          dialect.getLimitClause(limit)
        }
      } else if (offset > 0) {
        // MySQL doesn't support OFFSET without LIMIT. According to the suggestion of MySQL
        // official website, in order to retrieve all rows from a certain offset up to the end of
        // the result set, you can use some large number for the second parameter. Please refer:
        // https://dev.mysql.com/doc/refman/8.0/en/select.html
        s"LIMIT $offset, 18446744073709551615"
      } else {
        ""
      }

      options.prepareQuery +
      s"SELECT $columnList FROM ${options.tableOrQuery} $tableSampleClause" +
      s" $whereClause $groupByClause $orderByClause $limitOrOffsetStmt"
    }
  }

  override def getJdbcSQLQueryBuilder(options: JDBCOptions): JdbcSQLQueryBuilder =
    new MySQLSQLQueryBuilder(this, options)

  override def supportsLimit: Boolean = true

  override def supportsOffset: Boolean = true
}
