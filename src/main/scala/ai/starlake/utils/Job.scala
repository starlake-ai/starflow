package ai.starlake.utils

import ai.starlake.config.{Settings, SparkEnv, UdfRegistration}
import com.google.gson.Gson
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkConf
import org.apache.spark.sql.*

import java.io.{ByteArrayOutputStream, PrintStream}
import scala.jdk.CollectionConverters.*
import scala.util.Try

case class IngestionCounters(
  inputCount: Long,
  acceptedCount: Long,
  rejectedCount: Long,
  paths: List[String],
  jobid: String
) {
  def ignore: Boolean = inputCount == -1 && rejectedCount == -1 && acceptedCount == -1

  override def toString() = {
    if (ignore) {
      s"""Native Load summary:
         |Data successfully loaded from ${paths.mkString("\n\t")}
         |""".stripMargin
    } else {
      s"""Load summary:
         |Input records: $inputCount
         |Accepted: $acceptedCount
         |Rejected: $rejectedCount
         |Data successfully loaded from ${paths.mkString("\n\t")}
         |""".stripMargin
    }
  }
}

object IngestionCounters {
  def aggregate(
    counters: List[IngestionCounters],
    applicationId: String
  ): Option[IngestionCounters] =
    counters match {
      case Nil => None
      case _ =>
        Some(counters.reduce { (a, b) =>
          IngestionCounters(
            inputCount = a.inputCount + b.inputCount,
            acceptedCount = a.acceptedCount + b.acceptedCount,
            rejectedCount = a.rejectedCount + b.rejectedCount,
            paths = a.paths ++ b.paths,
            jobid = applicationId + "," + b.jobid
          )
        })
    }
}

trait JobResult {
  def sqlSchema(): List[(String, String)] = ???
  def asList(): List[List[(String, Any)]] = Nil
  def prettyPrint(format: String, dryRun: Boolean = false): String = ""
  def prettyPrint(
    format: String,
    headers: List[String],
    values: List[List[String]]
  ): String = {
    val baos = new ByteArrayOutputStream()
    val printStream = new PrintStream(baos)

    format match {
      case "csv" =>
        (headers :: values).foreach { row =>
          printStream.println(row.mkString(","))
        }

      case "table" =>
        headers :: values match {
          case Nil =>
            printStream.println("Result is empty.")
          case _ =>
            printStream.println(TableFormatter.format(headers :: values))
        }

      case "json" =>
        val res = values.foreach { value =>
          val list = headers.zip(value).map(it => Array(it._1, it._2)).asJava
          val json = new Gson().toJson(list)
          printStream.println(json)
        }

      case "json-array" =>
        val res = values.map { value =>
          val list = headers.zip(value).map(it => Array(it._1, it._2)).asJava
          list
        }
        val json = new Gson().toJson(res.asJava)
        printStream.println(json)
    }
    baos.toString
  }
}

/** A result that decides the process exit code itself. Used by commands such as `impact` whose
  * successful run can still mean "fail the CI step" (1) or "could not analyze" (2).
  */
trait ExitCodeJobResult extends JobResult {
  def exitCode: Int
}
/*
          val values = rows.iterateAll().asScala.toList.map { row =>
            val fields = row
              .iterator()
              .asScala
              .toList
            asMap(fields, headers)

 */
case class SparkJobResult(
  dataframe: Option[DataFrame],
  counters: Option[IngestionCounters]
) extends JobResult {

  override def sqlSchema(): List[(String, String)] = {
    dataframe
      .map { dataFrame =>
        dataFrame.schema.fields.map(field => field.name -> field.dataType.typeName).toList
      }
      .getOrElse(Nil)
  }
  override def asList(): List[List[(String, Any)]] = {
    dataframe
      .map { dataFrame =>
        val headers = dataFrame.schema.fields.map(_.name).toList
        val dataAsList = dataFrame
          .collect()
          .map { row =>
            val fields = row.toSeq.map(Option(_).map(_.toString).getOrElse("NULL")).toList
            headers.zip(fields)
          }
          .toList
        dataAsList
      }
      .getOrElse(Nil)
  }
  override def prettyPrint(format: String, dryRun: Boolean = false): String = {
    dataframe
      .map { dataFrame =>
        val dataAsList = dataFrame
          .collect()
          .map(_.toSeq.map(Option(_).map(_.toString).getOrElse("NULL")).toList)
          .toList
        val headers = dataFrame.schema.fields.map(_.name).toList
        prettyPrint(format, headers, dataAsList)
      }
      .getOrElse("")
  }

}

case class JdbcJobResult(headers: List[(String, String)], rows: List[List[String]] = Nil)
    extends JobResult {
  override def sqlSchema(): List[(String, String)] = headers
  override def prettyPrint(format: String, dryRun: Boolean = false): String = {
    prettyPrint(format, headers.map(_._1), rows)
  }

  override def asList(): List[List[(String, Any)]] = {
    rows.map { value => headers.map(_._1).zip(value) }

  }

  def show(format: String): Unit = {
    val result = prettyPrint(format, headers.map(_._1), rows)
    println(result)
  }
}
object JobResult {
  def empty: JobResult = EmptyJobResult
}

case class DagGenerateJobResult(dagFiles: List[Path]) extends JobResult

case object EmptyJobResult extends JobResult
case object FailedJobResult extends JobResult
case class PreLoadJobResult(domain: String, tables: Map[String, Int]) extends JobResult {
  val headers: List[String] = List("domain", "table", "loadable")
  val rows: List[List[String]] = tables.map { case (k, v) => List(domain, k, v.toString) }.toList
  override def asList(): List[List[(String, Any)]] = {
    rows.map { value => headers.zip(value) }
  }
  override def prettyPrint(format: String, dryRun: Boolean = false): String = {
    prettyPrint(format, headers, rows)
  }
  def show(format: String): Unit = {
    val result = prettyPrint(format, headers, rows)
    println(result)
  }
  val full: Boolean = tables.values.forall(_ > 0)
  val loadable: Boolean = tables.values.exists(_ > 0)
  val partial: Boolean = !full && loadable
  val empty: Boolean = !full && !loadable
}

/** All Spark Job extend this trait. Build Spark session using spark variables from
  * application.conf.
  */

trait JobBase extends LazyLogging with DatasetLogging {
  def name: String
  implicit def settings: Settings

  val appName =
    Option(System.getenv("SL_JOB_ID"))
      .orElse(settings.appConfig.jobIdEnvName.flatMap(e => Option(System.getenv(e))))
      .getOrElse(s"$name-${System.currentTimeMillis()}")

  def applicationId(): String = appName

  /** Just to force any job to implement its entry point using within the "run" method
    *
    * @return
    *   : Spark Dataframe for Spark Jobs None otherwise
    */
  def run(): Try[JobResult]

}

/** All Spark Job extend this trait. Build Spark session using spark variables from
  * application.conf. Make sure all variables are lazy since we do not want to build a spark session
  * for any of the other services
  */
trait SparkJob extends JobBase {

  protected def withExtraSparkConf(sourceConfig: SparkConf): SparkConf = {
    // During Job execution, schema update are done on the table before data is written
    // These two options below are thus disabled.
    // We disable them because even though the user asked for WRITE_APPEND
    // On merge, we write in WRITE_TRUNCATE mode.
    // Moreover, since we handle schema validaty through the YAML file, we manage these settings automatically
    sourceConfig.remove("spark.datasource.bigquery.allowFieldAddition")
    sourceConfig.remove("spark.datasource.bigquery.allowFieldRelaxation")

    settings.storageHandler().extraConf(settings.appConfig.getDefaultConnection().options).foreach {
      case (k, v) =>
        sourceConfig.set("spark.hadoop." + k, v)
    }

    val thisConf = sourceConfig.setAppName(appName).set("spark.app.id", appName)
    logger.whenDebugEnabled {
      logger.debug(thisConf.toDebugString)
    }
    thisConf
  }

  private lazy val sparkEnv: SparkEnv = SparkEnv.get(name, withExtraSparkConf, settings)

  def getTableLocation(domain: String, schema: String): String = {
    getTableLocation(s"$domain.$schema")
  }

  def getTableLocation(fullTableName: String): String = {
    import session.implicits.*
    val rows = session
      .sql(s"desc formatted $fullTableName")
      .toDF()
      .filter(Symbol("col_name") === "Location")
      .collect()
    rows.headOption
      .map(_(1).toString)
      .getOrElse(
        throw new NoSuchElementException(s"Location not found for table $fullTableName")
      )
  }

  protected def registerUdf(udf: String): Unit = {
    val udfInstance: UdfRegistration =
      Class
        .forName(udf)
        .getDeclaredConstructor()
        .newInstance()
        .asInstanceOf[UdfRegistration]
    udfInstance.register(sparkEnv.session)
  }

  lazy val session: SparkSession = {
    val udfs = settings.appConfig.getEffectiveUdfs()
    udfs.foreach(registerUdf)
    sparkEnv.session
  }

}
