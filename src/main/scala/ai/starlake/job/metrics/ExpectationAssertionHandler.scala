package ai.starlake.job.metrics

import ai.starlake.config.Settings
import ai.starlake.extract.JdbcDbUtils
import ai.starlake.job.sink.bigquery.BigQueryNativeJob
import ai.starlake.utils.SparkUtils
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.SparkSession

import scala.util.{Failure, Success, Try, Using}

trait ExpectationAssertionHandler extends LazyLogging {
  def handle(sql: String)(implicit settings: Settings): Int
}

object ExpectationAssertionHandler {

  /** The result of an expectation query, an integer value: a count of offending rows for example
    * (#1879). A value beyond the range of an Int is kept at its bound, so that a count never wraps
    * to 0, which means success. A NULL, a fraction or a value that is not a number fails.
    */
  def result(value: Any): Int = {
    val integral: BigInt = value match {
      case number @ (_: java.lang.Integer | _: java.lang.Long | _: java.lang.Short |
          _: java.lang.Byte) =>
        BigInt(number.asInstanceOf[Number].longValue())
      case number: java.math.BigInteger => BigInt(number)
      // NUMERIC, DECIMAL and NUMBER results, and the values of BigQuery
      case number: java.math.BigDecimal
          if number.signum == 0 || number.stripTrailingZeros.scale <= 0 =>
        BigInt(number.toBigIntegerExact)
      case number @ (_: java.lang.Double | _: java.lang.Float)
          if number.asInstanceOf[Number].doubleValue().isWhole =>
        BigDecimal(number.asInstanceOf[Number].doubleValue()).toBigInt
    }
    integral.max(BigInt(Int.MinValue)).min(BigInt(Int.MaxValue)).toInt
  }
}

class SparkExpectationAssertionHandler(session: SparkSession) extends ExpectationAssertionHandler {
  def handle(sql: String)(implicit settings: Settings): Int = {
    val rows = SparkUtils.sql(session, sql).collect()
    if (rows.length == 1)
      // COUNT(*) is a BIGINT
      Try(ExpectationAssertionHandler.result(rows.head.get(0)))
        .getOrElse(Integer.MIN_VALUE)
    else // More than one line, this is a mistake
      Integer.MIN_VALUE
  }
}

class BigQueryExpectationAssertionHandler(runner: BigQueryNativeJob)
    extends ExpectationAssertionHandler {
  override def handle(sql: String)(implicit
    settings: Settings
  ): Int = {
    runner.runBigQueryJob(Some(sql)) match {
      case Success(result) =>
        result.tableResult
          .map { tableResult =>
            val row = tableResult.iterateAll().iterator().next()
            // A number is a string, which a NULL is not
            Try(
              ExpectationAssertionHandler.result(
                new java.math.BigDecimal(row.get(0).getStringValue)
              )
            ).getOrElse(Integer.MIN_VALUE)
          }
          .getOrElse(
            throw new Exception("Query did not return result object. Should never happen !!!")
          )
      case Failure(e) =>
        throw e
    }
  }
}

class JdbcExpectationAssertionHandler(jdbcProperties: Map[String, String])
    extends ExpectationAssertionHandler {
  override def handle(sql: String)(implicit
    settings: Settings
  ): Int = {
    JdbcDbUtils.withJDBCConnection(settings.schemaHandler().dataBranch(), jdbcProperties) {
      connection =>
        Using.resource(connection.createStatement()) { statement =>
          Using.resource(statement.executeQuery(sql)) { rs =>
            if (rs != null && rs.next()) {
              val returnValue =
                Try(ExpectationAssertionHandler.result(rs.getObject(1)))
                  .getOrElse(Integer.MIN_VALUE)
              if (rs.next()) // More than one line this is a mistake
                Integer.MIN_VALUE
              else
                returnValue
            } else
              Integer.MIN_VALUE
          }
        }
    }
  }
}

class SQLConnectionExpectationAssertionHandler(connection: java.sql.Connection)
    extends ExpectationAssertionHandler {
  override def handle(sql: String)(implicit
    settings: Settings
  ): Int = {
    Using.resource(connection.createStatement()) { statement =>
      Using.resource(statement.executeQuery(sql)) { rs =>
        if (rs != null && rs.next()) {
          val returnValue =
            Try(ExpectationAssertionHandler.result(rs.getObject(1))).getOrElse(Integer.MIN_VALUE)
          if (rs.next()) // More than one line this is a mistake
            Integer.MIN_VALUE
          else
            returnValue
        } else
          Integer.MIN_VALUE
      }
    }
  }
}
