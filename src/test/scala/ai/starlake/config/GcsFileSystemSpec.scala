package ai.starlake.config

import com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem
import com.typesafe.config.ConfigFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** Hadoop 3.5 maps gs:// in its core-default.xml to its own hadoop-gcp module
  * (org.apache.hadoop.fs.gs.GoogleHadoopFileSystem), which we do not ship. That mapping wins over
  * the FileSystem service of the gcs-connector, so every gs:// path failed with
  * ClassNotFoundException until the defaults named the connector's class again.
  */
class GcsFileSystemSpec extends AnyFlatSpec with Matchers {

  private val settings: Settings = {
    val root = java.nio.file.Files.createTempDirectory("sl-gcs-fs").toString
    Settings(
      ConfigFactory.load(ConfigFactory.parseString(s"""SL_ROOT="$root"""")),
      None,
      None,
      None,
      None
    )
  }

  "the Hadoop conf of a Spark session" should "resolve gs:// to the gcs-connector" in {
    // What Spark does with spark.hadoop.* when it builds a session's Hadoop conf
    val conf = new Configuration()
    settings.jobConf.getAll.collect {
      case (key, value) if key.startsWith("spark.hadoop.") =>
        conf.set(key.stripPrefix("spark.hadoop."), value)
    }
    FileSystem.getFileSystemClass("gs", conf) shouldBe classOf[GoogleHadoopFileSystem]
  }

  "the Hadoop conf of the storage handler" should "resolve gs:// to the gcs-connector" in {
    val conf = new Configuration()
    settings.appConfig.hadoop.foreach { case (k, v) => conf.set(k, v) }
    FileSystem.getFileSystemClass("gs", conf) shouldBe classOf[GoogleHadoopFileSystem]
  }

  "a bare Hadoop 3.5 conf" should "not resolve gs:// to the gcs-connector (why the override exists)" in {
    an[Exception] should be thrownBy FileSystem.getFileSystemClass("gs", new Configuration())
  }
}
