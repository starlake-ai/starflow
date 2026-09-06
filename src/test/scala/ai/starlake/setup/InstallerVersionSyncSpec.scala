package ai.starlake.setup

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.io.Source
import scala.util.Using

/** The installer (Setup.java and .versions) pins its own copies of the dependency versions the
  * build declares in project/Versions.scala. Nothing propagated them automatically, so a version
  * bump that only touched Versions.scala shipped an installer that kept provisioning the old
  * artifacts: v1.8.4 released spark-4.1-bigquery 0.45.0 in the assembly while `starlake upgrade`
  * still installed 0.44.2-preview. This spec is the missing guard: every mapped pin must match.
  */
class InstallerVersionSyncSpec extends AnyFlatSpec with Matchers {

  private def read(path: String): String =
    Using.resource(Source.fromFile(path))(_.mkString)

  private val versionsScala = read("project/Versions.scala")
  private val dependenciesScala = read("project/Dependencies.scala")
  private val setupJava = read("src/main/java/Setup.java")
  private val dotVersions = read(".versions")

  private def sbtVersion(name: String): String =
    s"""val $name(?:: String)? =\\s*"([^"]+)"""".r
      .findFirstMatchIn(versionsScala)
      .map(_.group(1))
      .getOrElse(fail(s"$name not found in project/Versions.scala"))

  private def setupPin(name: String): String =
    s"""String $name = getEnv\\("$name"\\)\\.orElse\\("([^"]+)"\\)""".r
      .findFirstMatchIn(setupJava)
      .map(_.group(1))
      .getOrElse(fail(s"$name not found in src/main/java/Setup.java"))

  private def dotVersion(key: String): String =
    s"""(?m)^$key=(.+)$$""".r
      .findFirstMatchIn(dotVersions)
      .map(_.group(1).trim)
      .getOrElse(fail(s"$key not found in .versions"))

  private val postgresqlBuild =
    """"org.postgresql" % "postgresql" % "([^"]+)"""".r
      .findFirstMatchIn(dependenciesScala)
      .map(_.group(1))
      .getOrElse(fail("postgresql not found in project/Dependencies.scala"))

  behavior of "Setup.java installer pins"

  private val setupMappings: Seq[(String, String)] = Seq(
    "SPARK_VERSION"           -> "spark4",
    "SPARK_BQ_VERSION"        -> "sparkBigquery",
    "SNOWFLAKE_JDBC_VERSION"  -> "snowflakeJDBC",
    "SPARK_SNOWFLAKE_VERSION" -> "snowflakeSpark",
    "DUCKDB_VERSION"          -> "duckdb",
    "CONFLUENT_VERSION"       -> "confluentVersion",
    "SPARK_REDSHIFT_VERSION"  -> "sparkRedshift",
    "REDSHIFT_JDBC_VERSION"   -> "redshiftJDBC",
    "HADOOP_AWS_VERSION"      -> "hadoop",
    "HADOOP_AZURE_VERSION"    -> "hadoop"
  )

  setupMappings.foreach { case (pin, sbtName) =>
    it should s"pin $pin to Versions.$sbtName" in {
      setupPin(pin) shouldBe sbtVersion(sbtName)
    }
  }

  it should "pin POSTGRESQL_VERSION to the build's postgresql dependency" in {
    setupPin("POSTGRESQL_VERSION") shouldBe postgresqlBuild
  }

  behavior of ".versions docker pins"

  private val dotMappings: Seq[(String, String)] = Seq(
    "SPARK_VERSION"           -> "spark4",
    "SPARK_BQ_VERSION"        -> "sparkBigquery",
    "SNOWFLAKE_JDBC_VERSION"  -> "snowflakeJDBC",
    "SPARK_SNOWFLAKE_VERSION" -> "snowflakeSpark",
    "AWS_JAVA_SDK_V2_VERSION" -> "awsSdkBundle",
    "HADOOP_AWS_VERSION"      -> "hadoop",
    "HADOOP_AZURE_VERSION"    -> "hadoop"
  )

  dotMappings.foreach { case (key, sbtName) =>
    it should s"pin $key to Versions.$sbtName" in {
      dotVersion(key) shouldBe sbtVersion(sbtName)
    }
  }

  it should "pin POSTGRESQL_VERSION to the build's postgresql dependency" in {
    dotVersion("POSTGRESQL_VERSION") shouldBe postgresqlBuild
  }
}
