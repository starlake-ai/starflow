/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *    http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 *
 */

import sbt.{ExclusionRule, Resolvers => _, _}

object Dependencies {

  def scalaReflection(scalaVersion: String): Seq[ModuleID] =
    Seq(
      "org.scala-lang" % "scala-reflect" % scalaVersion,
      "org.scala-lang" % "scala-compiler" % scalaVersion
    )

  // Exclusions

  val jacksonExclusions = Seq(
    ExclusionRule(organization = "com.fasterxml.jackson.core"),
    ExclusionRule(organization = "com.fasterxml.jackson.databind"),
    ExclusionRule(organization = "com.fasterxml.jackson.jaxrs"),
    ExclusionRule(organization = "com.fasterxml.jackson.module"),
    ExclusionRule(
      organization = "com.fasterxml.jackson.dataformat",
      name = "jackson-dataformat-yaml"
    ),
    ExclusionRule(organization = "com.fasterxml.jackson.datatype", name = "jackson-datatype-jsr310")
  )

  val jnaExclusions = Seq(ExclusionRule(organization = "net.java.dev.jna"))

  val sparkExclusions = Seq(
    ExclusionRule(organization = "org.apache.spark")
  )

  // Provided

  val jacksonForSpark3 = Seq(
    "com.fasterxml.jackson.core" % "jackson-core" % Versions.jacksonForSpark3 % "provided",
    "com.fasterxml.jackson.core" % "jackson-annotations" % Versions.jacksonForSpark3 % "provided",
    "com.fasterxml.jackson.core" % "jackson-databind" % Versions.jacksonForSpark3 % "provided",
    "com.fasterxml.jackson.module" %% "jackson-module-scala" % Versions.jacksonForSpark3 % "provided",
    "com.fasterxml.jackson.dataformat" % "jackson-dataformat-yaml" % Versions.jacksonForSpark3 % "provided",
    "com.fasterxml.jackson.datatype" % "jackson-datatype-jsr310" % Versions.jacksonForSpark3 % "provided"
  )

  val spark3 = Seq(
    "org.apache.spark" %% "spark-core" % Versions.spark3 % "provided" exclude (
      "com.google.guava",
      "guava"
    ) excludeAll (jacksonExclusions: _*),
    "org.apache.spark" %% "spark-sql" % Versions.spark3 % "provided" exclude (
      "com.google.guava",
      "guava"
    ) excludeAll (jacksonExclusions: _*),
    "org.apache.spark" %% "spark-hive" % Versions.spark3 % "provided" exclude (
      "com.google.guava",
      "guava"
    ) excludeAll (jacksonExclusions: _*),
    "org.apache.spark" %% "spark-mllib" % Versions.spark3 % "provided" exclude (
      "com.google.guava",
      "guava"
    ) excludeAll (jacksonExclusions: _*),
    "com.databricks" %% "spark-xml" % Versions.sparkXML excludeAll (jacksonExclusions: _*),
    "org.apache.spark" %% "spark-sql-kafka-0-10" % Versions.spark3 excludeAll (jacksonExclusions: _*),
    "org.apache.spark" %% "spark-avro" % Versions.spark3 excludeAll (jacksonExclusions: _*),
    "io.delta" %% "delta-spark" % Versions.deltaSpark3d0 % "provided" exclude (
      "com.google.guava",
      "guava"
    ) excludeAll (jacksonExclusions: _*)
  )

  val hadoop = Seq(
    "org.apache.hadoop" % "hadoop-common" % Versions.hadoop % "provided" excludeAll (jacksonExclusions: _*) exclude (
      "com.google.guava",
      "guava"
    ),
    "org.apache.hadoop" % "hadoop-hdfs" % Versions.hadoop % "provided" excludeAll (jacksonExclusions: _*) exclude (
      "com.google.guava",
      "guava"
    ),
    "org.apache.hadoop" % "hadoop-yarn-client" % Versions.hadoop % "provided" excludeAll (jacksonExclusions: _*) exclude (
      "com.google.guava",
      "guava"
    ),
    "org.apache.hadoop" % "hadoop-mapreduce-client-app" % Versions.hadoop % "provided" excludeAll (jacksonExclusions: _*) exclude (
      "com.google.guava",
      "guava"
    ),
    "org.apache.hadoop" % "hadoop-client" % Versions.hadoop % "provided" excludeAll (jacksonExclusions: _*) exclude (
      "com.google.guava",
      "guava"
    )
  )

  val azure = Seq(
    "org.apache.hadoop" % "hadoop-azure" % "3.4.2" % "provided" excludeAll (jacksonExclusions: _*) exclude (
      "com.google.guava",
      "guava"
    ),
    "com.microsoft.azure" % "azure-storage" % "8.6.6" % "provided" excludeAll (jacksonExclusions: _*) exclude (
      "com.google.guava",
      "guava"
    )
  )

  val snowflake = Seq(
    "net.snowflake" % "snowflake-jdbc" % Versions.snowflakeJDBC % "provided" excludeAll (jacksonExclusions: _*),
    "net.snowflake" %% "spark-snowflake" % Versions.snowflakeSpark % "provided" excludeAll (jacksonExclusions: _*)
  )

  val redshift = Seq(
    "com.amazon.redshift" % "redshift-jdbc42" % Versions.redshiftJDBC % "provided" excludeAll (jacksonExclusions: _*),
    "org.apache.hadoop" % "hadoop-aws" % "3.3.6" % "provided" excludeAll (jacksonExclusions: _*),
    "com.amazonaws" % "aws-java-sdk-bundle" % "1.12.794" % "provided" excludeAll (jacksonExclusions: _*)
  )

  val scalaTest = Seq(
    "org.scalatest" %% "scalatest" % Versions.scalatest % Test excludeAll (jacksonExclusions: _*),
    "org.scalatestplus" %% "scalacheck-1-17" % Versions.scalacheckForScalatest % Test excludeAll (jacksonExclusions: _*)
  )

  val h2 = Seq(
    "com.h2database" % "h2" % Versions.h2 % Test
  )

  // Included

  val swaggerParser = Seq(
    "io.swagger.parser.v3" % "swagger-parser" % Versions.swaggerParser excludeAll (jacksonExclusions: _*)
  )

  val betterfiles = Seq(
    "com.github.pathikrit" %% "better-files" % Versions.betterFiles
  )

  val logging = Seq(
    "com.typesafe" % "config" % Versions.typesafeConfig,
    "com.typesafe.scala-logging" %% "scala-logging" % Versions.scalaLogging
  )

  val pureConfig =
    Seq(
      "com.github.pureconfig" %% "pureconfig" % Versions.pureConfig
    )

  val gcsConnectorShadedJar =
    s"${Resolvers.googleCloudBigDataMavenRepo}/gcs-connector/${Versions.gcsConnector}/gcs-connector-${Versions.gcsConnector}-shaded.jar"

  val gcpBigQueryConnectorShadedJar =
    s"${Resolvers.googleCloudBigDataMavenRepo}/bigquery-connector/${Versions.bigqueryConnector}/bigquery-connector-${Versions.bigqueryConnector}-shaded.jar"

  val gcp = Seq(
    "com.google.cloud.bigdataoss" % "gcs-connector" % Versions.gcsConnector from gcsConnectorShadedJar exclude ("javax.jms", "jms") exclude ("com.sun.jdmk", "jmxtools") exclude ("com.sun.jmx", "jmxri") excludeAll (jacksonExclusions: _*) classifier "shaded",
    "com.google.cloud.bigdataoss" % "bigquery-connector" % Versions.bigqueryConnector /*from gcpBigQueryConnectorShadedJar*/ exclude ("javax.jms", "jms") exclude ("com.sun.jdmk", "jmxtools") exclude ("com.sun.jmx", "jmxri") excludeAll (jacksonExclusions: _*) /*classifier "shaded" */,
    "com.google.cloud" % "google-cloud-bigquery" % Versions.bigquery exclude ("javax.jms", "jms") exclude ("com.sun.jdmk", "jmxtools") exclude ("com.sun.jmx", "jmxri") excludeAll (jacksonExclusions: _*),
    // see https://github.com/GoogleCloudDataproc/spark-bigquery-connector/issues/36
    // Add the jar file to spark dependencies
    "com.google.cloud.spark" %% "spark-bigquery-with-dependencies" % Versions.sparkBigqueryWithDependencies % "provided" excludeAll (jacksonExclusions: _*),
    "com.google.cloud" % "google-cloud-datacatalog" % Versions.gcpDataCatalog excludeAll (jacksonExclusions: _*),
    "com.google.cloud" % "google-cloud-logging" % Versions.gcpCloudLogging
  )

  val esSpark212 = Seq(
    "org.elasticsearch" %% "elasticsearch-spark-30" % Versions.esSpark % "provided" exclude (
      "com.google.guava",
      "guava"
    ) excludeAll ((sparkExclusions ++ jacksonExclusions): _*),
    "com.dimafeng" %% "testcontainers-scala-elasticsearch" % Versions.testContainers % Test excludeAll (jnaExclusions: _*)
  )

  val scopt = Seq(
    "com.github.scopt" %% "scopt" % Versions.scopt
  )

  val excelClientApi = Seq(
    "org.apache.poi" % "poi-ooxml" % Versions.poi
  )

  val scalate = Seq.empty

  val kafkaClients = Seq(
    "org.apache.kafka" % "kafka-clients" % Versions.kafkaClients excludeAll (jacksonExclusions: _*),
    "io.confluent" % "kafka-schema-registry-client" % Versions.confluentVersion % "provided" excludeAll (jacksonExclusions: _*),
    "io.confluent" % "kafka-avro-serializer" % Versions.confluentVersion % "provided" excludeAll (jacksonExclusions: _*),
    "com.dimafeng" %% "testcontainers-scala-scalatest" % Versions.testContainers % Test excludeAll (jnaExclusions: _*),
    "com.dimafeng" %% "testcontainers-scala-kafka" % Versions.testContainers % Test excludeAll (jnaExclusions: _*)
  )

  val jna_apple_arm_testcontainers = Seq(
    "net.java.dev.jna" % "jna" % "5.12.1"
  )

  val pgGcp = Seq(
    "com.google.cloud.sql" % "postgres-socket-factory" % "1.28.2" % Test,
    "com.dimafeng" %% "testcontainers-scala-postgresql" % Versions.testContainers % Test excludeAll (jnaExclusions: _*),
    "org.postgresql" % "postgresql" % "42.7.10" % "provided"
  )

  val mariadb = Seq(
    "com.dimafeng" %% "testcontainers-scala-mariadb" % Versions.testContainers % Test excludeAll (jnaExclusions: _*),
    "org.mariadb.jdbc" % "mariadb-java-client" % "3.5.5" % Test,
    "com.mysql" % "mysql-connector-j" % "9.4.0" % Test
  )

  val jinja = Seq(
    "com.hubspot.jinjava" % "jinjava" % Versions.jinja excludeAll (jacksonExclusions: _*) exclude (
      "com.google.guava",
      "guava"
    ) exclude ("org.apache.commons", "commons-lang3")
  )

  // com.manticore-projects.jsqlformatter
  def jSqlTranspiler(isSnapshot: Boolean) = {
    val jSqlTranspilerVersion =
      if (isSnapshot) Versions.jSqlTranspiler
      else Versions.jSqlTranspiler.replace("-SNAPSHOT", "")
    val starlakeJdbcVersion =
      if (isSnapshot) Versions.starlakejdbc
      else Versions.starlakejdbc.replace("-SNAPSHOT", "")
    Seq(
      "com.manticore-projects.jsqlformatter" % "jsqlparser" % Versions.jSqlParser,
      "ai.starlake.jsqltranspiler" % "jsqltranspiler" % jSqlTranspilerVersion exclude (
        "org.apache.commons",
        "commons-io"
      ),
      "ai.starlake.jdbc" % "starlakejdbc" % starlakeJdbcVersion exclude (
        "org.apache.commons",
        "commons-io"
      ),
      "com.manticore-projects.jsqlformatter" % "jsqlformatter" % Versions.jSqlFormatter
    )
  }

  val duckdb = Seq("org.duckdb" % "duckdb_jdbc" % Versions.duckdb)

  val jsonSchemaValidator = Seq(
    "com.networknt" % "json-schema-validator" % Versions.jsonSchemaValidator excludeAll (jacksonExclusions: _*)
  )

  val scala213LibsOnly = Seq(
    "ai.starlake" %% "spark-redshift" % "6.5.0" % Test
  )

  val scalaCompat = Seq(
    "org.scala-lang.modules" %% "scala-collection-compat" % Versions.scalaCompat,
    "org.scala-lang.modules" %% "scala-parallel-collections" % Versions.scalaParallelCollections
  )

  val derbyTestServer = Seq(
    "org.apache.derby" % "derby" % Versions.derbyVersion % Test,
    "org.apache.derby" % "derbyclient" % Versions.derbyVersion % Test,
    "org.apache.derby" % "derbynet" % Versions.derbyVersion % Test
  )

  val cache = Seq(
    "com.github.ben-manes.caffeine" % "caffeine" % "3.2.2"
  )

  val starlakeStreaming = Seq(
    "ai.starlake" %% "starlake-streaming" % "1.3.5" % "provided"
  )

  val templates = Seq(
    "ai.starlake" % "starlake-airflow" % Versions.airflowTemplates,
    "ai.starlake" % "starlake-dagster" % Versions.dagsterTemplates,
    "ai.starlake" % "starlake-orchestration" % Versions.orchestrationTemplates,
    "ai.starlake" % "starlake-snowflake" % Versions.snowflakeTemplates
  )

  def dependencies(isSnapshot: Boolean) =
    jna_apple_arm_testcontainers ++ scalate ++ logging ++ betterfiles ++ snowflake ++ redshift ++ scalaTest ++
    scopt ++ hadoop ++ duckdb ++ gcp ++ azure ++ h2 ++ excelClientApi ++ kafkaClients ++ jinja ++
    pgGcp ++ jsonSchemaValidator ++ mariadb ++ derbyTestServer ++ jSqlTranspiler(
      isSnapshot
    ) ++ cache ++ swaggerParser ++
    starlakeStreaming ++ templates
}
