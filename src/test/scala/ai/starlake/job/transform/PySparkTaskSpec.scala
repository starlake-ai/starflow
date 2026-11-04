package ai.starlake.job.transform

import ai.starlake.TestHelper
import ai.starlake.config.{DatasetArea, Settings}
import ai.starlake.workflow.IngestionWorkflow
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession

import scala.sys.process._
import scala.util.Try

/** Runs real python tasks, in the JVM of the tests. A script using Spark creates a SparkContext in
  * this JVM, so that it can only run if no other one exists: see the last test.
  */
class PySparkTaskSpec extends TestHelper {

  private def pythonFinds(module: String): Boolean =
    Try(Seq("python3", "-c", s"import $module").!(ProcessLogger(_ => ())) == 0).getOrElse(false)

  private def deliver(tasks: (String, String)*)(implicit settings: Settings): Unit = {
    val storage = settings.storageHandler()
    val job = new Path(DatasetArea.transform, "py")
    storage.mkdirs(job)
    storage.write("version: 1\ntransform: {}\n", new Path(job, "_config.sl.yml"))
    tasks.foreach { case (name, script) =>
      storage.write(script, new Path(job, s"$name.py"))
      storage.write(
        """version: 1
          |task:
          |  sink:
          |    format: csv
          |""".stripMargin,
        new Path(job, s"$name.sl.yml")
      )
    }
  }

  private def result(name: String)(implicit settings: Settings): Option[List[String]] =
    new IngestionWorkflow(settings.storageHandler(), settings.schemaHandler(reload = true))
      .buildTask(TransformConfig(name = s"py.$name", scheduledDate = None))
      .asInstanceOf[SparkAutoTask]
      .runSparkOnAny()
      .get
      .dataframe
      .map(_.collect().map(_.getString(0)).toList)

  "A python task that does not register SL_THIS" should "not get the view of an earlier task" in {
    assume(pythonFinds("sys"), "needs python3")
    new WithSettings() {
      deliver("noview" -> "print('this script does not use Spark')\n")
      // What an earlier python task leaves behind
      sparkSession.sql("SELECT 'from-earlier' AS id").createOrReplaceTempView("SL_THIS")
      result("noview") shouldBe None
    }
  }

  // Skipped once a test created a Spark session: run it alone, with pyspark on the PYTHONPATH
  "A first python task" should "get the view its script registers" in {
    assume(pythonFinds("pyspark"), "needs python3 with pyspark")
    assume(SparkSession.getDefaultSession.isEmpty, "needs a JVM without a Spark session")
    // Set by spark-submit in production: this is how a script finds them
    System.setProperty("spark.master", "local[*]")
    System.setProperty("spark.app.name", "PySparkTaskSpec")
    new WithSettings() {
      deliver(
        "view" ->
        """from pyspark.sql import SparkSession
          |spark = SparkSession.builder.getOrCreate()
          |spark.createDataFrame([["from-script"]], ["id"]).createOrReplaceTempView("SL_THIS")
          |""".stripMargin
      )
      result("view") shouldBe Some(List("from-script"))
    }
  }
}
