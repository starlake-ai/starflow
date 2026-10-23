package ai.starlake.job.ingest

import ai.starlake.TestHelper
import ai.starlake.extract.JdbcDbUtils
import org.apache.hadoop.fs.Path

import java.sql.Timestamp
import java.time.Instant
import java.util.UUID
import scala.util.Using

/** The rejected records of the native loads, written without Spark to a JDBC audit sink (#1856) */
class RejectedRecordsSinkSpec extends TestHelper {
  new WithSettings() {
    "sinkRejectedRecords" should "write at most audit.maxErrors records to a JDBC audit sink, without Spark" in {
      val schemaHandler = settings.schemaHandler()
      val jobid = s"native-${UUID.randomUUID()}"
      val maxErrors = settings.appConfig.audit.maxErrors
      // Quotes, line breaks and braces need no escaping
      val error = "ERR -> \n`a`  can't be null\n\nFILE -> {{f}} 'x'"
      val records = (1 to maxErrors + 5).toList.map { i =>
        RejectedRecord(jobid, Timestamp.from(Instant.now()), "d", s"t", s"$error $i", "p1,p2")
      }
      IngestionUtil
        .sinkRejectedRecords(
          jobid,
          records,
          List(new Path("p1"), new Path("p2")),
          None,
          None,
          throw new IllegalStateException("Spark is not needed")
        )(settings, storageHandler, schemaHandler)
        .get
      val (count, sample) =
        JdbcDbUtils.withJDBCConnection(
          None,
          settings.appConfig.connections(settings.appConfig.audit.getConnectionRef()).options
        ) { connection =>
          Using.resource(connection.createStatement()) { statement =>
            Using.resource(
              statement.executeQuery(
                s"SELECT COUNT(*), MIN(error), MIN(path) FROM audit.rejected WHERE jobid = '$jobid'"
              )
            ) { result =>
              result.next()
              (result.getLong(1), (result.getString(2), result.getString(3)))
            }
          }
        }
      count shouldBe maxErrors
      sample._1 should startWith(error)
      sample._2 shouldBe "p1,p2"
    }
  }
}
