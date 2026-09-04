package ai.starlake.setup

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class DependencySyncSpec extends AnyFlatSpec with Matchers {

  private def prefix(url: String): String =
    DependencySync.derivePrefix(url, url.substring(url.lastIndexOf('/') + 1))

  "derivePrefix" should "strip the version taken from the second-to-last url segment" in {
    prefix(
      "https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.11/postgresql-42.7.11.jar"
    ) shouldBe "postgresql-"
  }

  it should "handle an artifact whose file name differs from its label" in {
    prefix(
      "https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.29.52/bundle-2.29.52.jar"
    ) shouldBe "bundle-"
  }

  it should "strip a leading v from a github release tag segment" in {
    prefix(
      "https://github.com/starlake-ai/spark-redshift/releases/download/v7.0.0/spark-redshift_2.13-7.0.0.jar"
    ) shouldBe "spark-redshift_2.13-"
  }

  it should "not over-truncate a file name that embeds a version-looking token" in {
    prefix(
      "https://repo1.maven.org/maven2/com/google/cloud/spark/spark-4.1-bigquery/0.44.2-preview/spark-4.1-bigquery-0.44.2-preview.jar"
    ) shouldBe "spark-4.1-bigquery-"
  }

  it should "fall back to the first digit-led segment when the url carries no version segment" in {
    prefix(
      "https://raw.githubusercontent.com/starlake-ai/starflow/master/distrib/python-libs/starlake_airflow-0.6.11-py3-none-any.whl"
    ) shouldBe "starlake_airflow-"
  }

  it should "fall back to the whole name when there is no version anywhere" in {
    prefix(
      "https://raw.githubusercontent.com/cdarlint/winutils/master/hadoop-3.3.6/bin/winutils.exe"
    ) shouldBe "winutils.exe"
  }

  // Every distinct URL shape the installer downloads from, with the prefix each one must
  // yield. Versions here are illustrative and deliberately NOT tied to the real pinned
  // values: what is asserted is the artifact-id SHAPE, which is what ownership depends on,
  // so a routine version bump must never break this table. Add a row when you add a
  // dependency with a new naming shape.
  private val goldenPrefixes: List[(String, String)] = List(
    "https://repo1.maven.org/maven2/com/google/cloud/spark/spark-4.1-bigquery/9.9.9-preview/spark-4.1-bigquery-9.9.9-preview.jar" -> "spark-4.1-bigquery-",
    "https://repo1.maven.org/maven2/io/delta/delta-spark_4.1_2.13/9.9.9/delta-spark_4.1_2.13-9.9.9.jar" -> "delta-spark_4.1_2.13-",
    "https://repo1.maven.org/maven2/io/delta/delta-storage/9.9.9/delta-storage-9.9.9.jar" -> "delta-storage-",
    "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-4.1_2.13/9.9.9/iceberg-spark-runtime-4.1_2.13-9.9.9.jar" -> "iceberg-spark-runtime-4.1_2.13-",
    "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-azure/9.9.9/hadoop-azure-9.9.9.jar" -> "hadoop-azure-",
    "https://repo1.maven.org/maven2/com/microsoft/azure/azure-storage/9.9.9/azure-storage-9.9.9.jar" -> "azure-storage-",
    "https://repo1.maven.org/maven2/org/eclipse/jetty/jetty-server/9.9.9.v99999999/jetty-server-9.9.9.v99999999.jar" -> "jetty-server-",
    "https://repo1.maven.org/maven2/net/snowflake/snowflake-jdbc/9.9.9/snowflake-jdbc-9.9.9.jar" -> "snowflake-jdbc-",
    "https://repo1.maven.org/maven2/net/snowflake/spark-snowflake_2.13/9.9.9-spark_4.1/spark-snowflake_2.13-9.9.9-spark_4.1.jar" -> "spark-snowflake_2.13-",
    "https://repo1.maven.org/maven2/org/postgresql/postgresql/9.9.9/postgresql-9.9.9.jar" -> "postgresql-",
    "https://repo1.maven.org/maven2/org/duckdb/duckdb_jdbc/9.9.9.9/duckdb_jdbc-9.9.9.9.jar" -> "duckdb_jdbc-",
    "https://repo1.maven.org/maven2/org/apache/arrow/flight-sql-jdbc-driver/9.9.9/flight-sql-jdbc-driver-9.9.9.jar" -> "flight-sql-jdbc-driver-",
    "https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/9.9.9/bundle-9.9.9.jar" -> "bundle-",
    "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/9.9.9/hadoop-aws-9.9.9.jar" -> "hadoop-aws-",
    "https://repo1.maven.org/maven2/com/amazon/redshift/redshift-jdbc42/9.9.9/redshift-jdbc42-9.9.9.jar" -> "redshift-jdbc42-",
    "https://github.com/starlake-ai/spark-redshift/releases/download/v9.9.9/spark-redshift_2.13-9.9.9.jar" -> "spark-redshift_2.13-",
    "https://github.com/starlake-ai/starflow/releases/download/v9.9.9/starlake-core_2.13-9.9.9-assembly.jar" -> "starlake-core_2.13-",
    "https://packages.confluent.io/maven/io/confluent/kafka-schema-registry-client/9.9.9/kafka-schema-registry-client-9.9.9.jar" -> "kafka-schema-registry-client-",
    "https://packages.confluent.io/maven/io/confluent/kafka-avro-serializer/9.9.9/kafka-avro-serializer-9.9.9.jar" -> "kafka-avro-serializer-",
    "https://repo1.maven.org/maven2/org/mariadb/jdbc/mariadb-java-client/9.9.9/mariadb-java-client-9.9.9.jar" -> "mariadb-java-client-",
    "https://repo1.maven.org/maven2/com/clickhouse/clickhouse-jdbc/9.9.9/clickhouse-jdbc-9.9.9-all.jar" -> "clickhouse-jdbc-",
    "https://repo1.maven.org/maven2/io/trino/trino-jdbc/999/trino-jdbc-999.jar" -> "trino-jdbc-",
    "https://raw.githubusercontent.com/starlake-ai/starflow/master/distrib/python-libs/starlake_airflow-9.9.9-py3-none-any.whl" -> "starlake_airflow-",
    "https://raw.githubusercontent.com/starlake-ai/starflow/master/distrib/python-libs/starlake_dagster-9.9.9-py3-none-any.whl" -> "starlake_dagster-",
    "https://raw.githubusercontent.com/starlake-ai/starflow/master/distrib/python-libs/starlake_orchestration-9.9.9-py3-none-any.whl" -> "starlake_orchestration-",
    "https://raw.githubusercontent.com/starlake-ai/starflow/master/distrib/python-libs/starlake_snowflake-9.9.9-py3-none-any.whl" -> "starlake_snowflake-"
  )

  it should "derive the documented prefix for every dependency naming shape" in {
    goldenPrefixes.foreach { case (url, expected) =>
      withClue(s"url=$url: ") { prefix(url) shouldBe expected }
    }
  }

  "SyncPlan.render" should "collapse to one line when there is nothing to do" in {
    val plan = new SyncPlan()
    plan.addUpToDate(new java.io.File("/tmp/a.jar"))
    plan.addUpToDate(new java.io.File("/tmp/b.jar"))
    plan.isEmpty shouldBe true
    plan.render("Dependency plan") shouldBe
    "All 2 dependencies up to date, nothing to download."
  }

  it should "list only what changes, with sizes and reasons" in {
    val plan = new SyncPlan()
    val artifact = new Artifact(
      "Postgres",
      "postgresql-42.7.11.jar",
      "https://example.invalid/postgresql-42.7.11.jar",
      java.util.List.of("postgresql-"),
      true
    )
    plan.addUpToDate(new java.io.File("/tmp/kept.jar"))
    plan.add(
      new SyncPlan.Download(artifact, new java.io.File("/tmp/postgresql-42.7.11.jar"), 1048576L)
    )
    plan.add(new SyncPlan.Deletion(new java.io.File("/tmp/postgresql-42.7.10.jar"), "superseded"))
    plan.isEmpty shouldBe false
    plan.bytesToDownload shouldBe 1048576L

    val rendered = plan.render("Dependency plan")
    rendered should include("Dependency plan")
    rendered should include("= 1 up to date")
    rendered should include("+ 1 to download (1 MB)")
    rendered should include("postgresql-42.7.11.jar")
    rendered should include("- 1 to remove")
    rendered should include("postgresql-42.7.10.jar")
    rendered should include("(superseded)")
    rendered should not include "kept.jar"
  }

  it should "report an unknown download size rather than a bogus zero" in {
    val plan = new SyncPlan()
    val artifact =
      new Artifact(
        "Core",
        "core.jar",
        "https://example.invalid/core.jar",
        java.util.List.of("core"),
        true
      )
    plan.add(new SyncPlan.Download(artifact, new java.io.File("/tmp/core.jar"), -1L))
    plan.bytesToDownload shouldBe 0L
    plan.render("Dependency plan") should include("(unknown size)")
  }
}
