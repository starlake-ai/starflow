object Versions {
  val spark4 = "4.1.3"
  val deltaSpark = "4.3.1" // artifact id is delta-spark_4.1
  val scalatest = "3.2.19"
  val scalacheckForScalatest = "3.2.19.0"
  // sparkXML / sparkXML2d0 DELETED: Spark 4 has a built-in xml data source
  val springBoot = "2.0.6.RELEASE"
  val typesafeConfig = "1.4.6"
  val scalaLogging = "3.9.6"
  val hive = "3.1.0"
  val log4s = "1.3.3"
  val swaggerParser = "2.1.41"
  val betterFiles = "3.9.2"
  val jacksonForSpark4 = "2.21.2" // exactly what Spark 4.1.3 ships via jackson-bom
  // jackson-annotations dropped patch versioning at jackson-bom 2.20 (kept as "2.21", not "2.21.2");
  // see jackson-bom's own pom comment on <jackson.version.annotations>. No 2.21.2 artifact exists.
  val jacksonAnnotationsForSpark4 = "2.21"
  val pureConfig = "0.17.9"
  // elasticsearch-spark has no Spark 4 build yet (merged upstream 2026-07, not released).
  // Re-enable esSpark212 in build.sbt when elasticsearch-spark-41_2.13 ships.
  val esSpark = "8.16.3"
  // json-schema-validator 2.0.4 is the last Jackson-2 line (3.x uses Jackson 3 = tools.jackson,
  // incompatible with the Spark classpath). Never bump past the 2.x line.
  val jsonSchemaValidator = "2.0.4"
  val scopt = "4.1.0"
  // Capped at 2.68.0, NOT the latest (2.69.0): 2.69.0 hangs TransformIntegration2Spec
  // forever (forked test JVM never completes its handshake back to sbt; new "could not
  // find method serverCertificateRequested" / "Unable to read from client" lines appear
  // right before the hang). Bisected to exactly this one release (2.68.0 passes, 2.69.0
  // hangs) with grpc-api/grpc-netty-shaded/conscrypt-openjdk-uber held at identical
  // versions on both sides (1.82.2 / 2.6.0, pulled in either way via datacatalog 1.101.0),
  // so the regression is inside the google-cloud-bigquery 2.69.0 artifact itself, not a
  // transitive dependency clash. See .superpowers/sdd/recipeb-report.md for the full bisect.
  val bigquery = "2.68.0"
  val gcsConnector = "4.0.4" // new versioning scheme, built against Hadoop 3.4.2
  val hadoop = "3.4.2" // must match Spark 4.1.3's Hadoop line; aws/azure artifacts use this too
  val awsSdkBundle = "2.29.52" // software.amazon.awssdk (v2), pinned by hadoop-project 3.4.2
  val sparkBigquery =
    "0.44.2-preview" // artifact spark-4.1-bigquery, no scala suffix, GA build not yet published for 4.1
  val bigqueryConnector = "hadoop3-1.2.0"
  val h2 = "2.3.232" // Test only
  val poi = "4.1.2"
  val confluentVersion = "7.7.11"
  val kafkaClients = "7.7.5-ce"
  val testContainers = "0.44.0"
  val gcpCloudLogging = "3.36.0"
  val gcpDataCatalog = "1.101.0"
  val jinja = "2.7.4" // forces dependency override on guava
  val snowflakeJDBC = "4.3.3" // spark-snowflake 3.2.x requires >= 4.0.2
  val snowflakeSpark: String = "3.2.1-spark_4.1"
  val duckdb = "1.5.5.1"
  val bigQueue = "0.7.0"
  val redshiftJDBC = "2.2.8"
  val scalaParallelCollections = "1.2.0" // matches Spark 4.1.3
  val derbyVersion =
    "10.15.2.0" // last version compatible with Java 11, see https://db.apache.org/derby/derby_downloads.html
  // jSqlParser must match the version jsqltranspiler's pom pins (its manticore snapshot line)
  val jSqlParser = "5.4.260-SNAPSHOT"
  val jSqlTranspiler = "1.11-SNAPSHOT"
  val starlakejdbc = "0.7"
  val airflowTemplates = "0.6.14"
  val jSqlFormatter = "5.4.1"
  val dagsterTemplates = "0.5.9"
  val orchestrationTemplates = "0.5.6.1"
  val snowflakeTemplates = "0.4.1"
  val starlakeStreaming =
    "1.4.0" // Spark 4 build, resolved from the starlake-streaming GitHub release
  val sparkRedshift =
    "7.0.0" // Spark 4 build of the ai.starlake fork, resolved from its GitHub release
}
