import Dependencies.*
import sbt.Tests.{Group, SubProcess}

import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, ZoneId}

lazy val javacCompilerVersion = "17"


javacOptions ++= Seq(
  "--release", javacCompilerVersion,
  "-Xlint"
)

updateOptions := updateOptions.value.withLatestSnapshots(true)

Test / javaOptions ++= Seq("-Dfile.encoding=UTF-8")

val testJavaOptions = {
  val heapSize = sys.env.get("HEAP_SIZE").getOrElse("4g")
  val extraTestJavaArgs = Seq("-XX:+IgnoreUnrecognizedVMOptions",
    "--add-opens=java.base/java.lang=ALL-UNNAMED",
    "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
    "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
    "--add-opens=java.base/java.io=ALL-UNNAMED",
    "--add-opens=java.base/java.net=ALL-UNNAMED",
    "--add-opens=java.base/java.nio=ALL-UNNAMED",
    "--add-opens=java.base/java.util=ALL-UNNAMED",
    "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
    "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
    "--add-opens=java.base/jdk.internal.ref=ALL-UNNAMED",
    "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
    "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
    "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
    "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED").mkString(" ")
  s"-Xmx$heapSize -Xss4m -XX:ReservedCodeCacheSize=128m -Dfile.encoding=UTF-8 $extraTestJavaArgs"
    .split(" ").toSeq
}

Test / javaOptions ++= testJavaOptions

// lazy val scala212 = "2.12.20"

lazy val scala213 = "2.13.18"
lazy val scala3 = "3.3.1"

lazy val supportedScalaVersions = Seq(scala213)


crossScalaVersions := supportedScalaVersions

organization := "ai.starlake"

organizationName := "starlake"

scalaVersion := scala213 // scala213 scala212

organizationHomepage := Some(url("https://github.com/starlake-ai/starlake"))

resolvers ++= Resolvers.allResolvers

libraryDependencies ++= {
  val versionSpecificLibs = {
    CrossVersion.partialVersion(scalaVersion.value) match {
      case Some((2, 13)) => scalaCompat ++ scala213LibsOnly
      case _ => throw new Exception(s"Invalid Scala Version")
    }
  }
  dependencies(isSnapshot.value) ++ spark4 ++
    jacksonForSpark4 ++ // esSpark212 excluded: no elasticsearch-spark release supports Spark 4 yet
    pureConfig ++ scalaReflection(scalaVersion.value) ++
    versionSpecificLibs
}

dependencyOverrides := Seq(
  // proto-google-iam-v1 (pulled in by google-cloud-bigquery/-bigquerystorage/-datacatalog)
  // ships gencode stamped 4.33.2, and protobuf enforces runtime >= gencode since 4.26
  // ("poison pill" checks), so PolicyTagManagerClient (BigQuery CLS) throws
  // ProtobufRuntimeVersionException under 4.33.0. gcs-connector-4.0.4 and spark-4.1-bigquery
  // fully shade/relocate their own protobuf-java, so raising this override does not affect
  // GCS batch requests (an earlier 4.33.2 regression report was collateral eviction churn,
  // not a protobuf conflict through this path). google-cloud-datacatalog 1.101.0's own gencode
  // is stamped 4.33.6, raising the runtime floor accordingly; 4.35.1 is the latest stable 4.x
  // release satisfying that floor (avoid 4.36.0-RC, a pre-release).
  "com.google.protobuf"                % "protobuf-java"             % "4.35.1",
  "org.scala-lang"                    % "scala-library"             % scalaVersion.value,
  "org.scala-lang"                    % "scala-reflect"             % scalaVersion.value,
  "org.scala-lang"                    % "scala-compiler"            % scalaVersion.value,
  "com.google.guava"                  %  "guava"                    % "33.4.8-jre", // match Spark 4.1.3; jinjava is fine with it
  "com.fasterxml.jackson.dataformat"  % "jackson-dataformat-csv"    % Versions.jacksonForSpark4,
  "com.manticore-projects.jsqlformatter" % "jsqlparser"             % Versions.jSqlParser // avoid MethodTooLargeException during assembly shading
)

name := "starlake-core"


Common.enableStarlakeAliases

enablePlugins(Common.starlakePlugins: _*)

buildInfoPackage := "ai.starlake.buildinfo"



scalacOptions ++= {
  val extractOptions = {
    CrossVersion.partialVersion(scalaVersion.value) match {
      case Some((2, 12)) => Seq("-Xfatal-warnings")
      case Some((2, 13)) =>  Seq()
      case _ => throw new Exception(s"Invalid Scala Version")
    }
  }
  Seq(
    "-deprecation",
    "-feature",
    "-Xmacro-settings:materialize-derivations",
    "-Ywarn-unused:imports",
    "-Xsource:3"  ) ++ extractOptions

}


Common.customSettings

// Builds a far JAR with embedded spark libraries and other provided libs.
// Can be useful for running YAML generation without having a spark distribution
commands += Command.command("assemblyWithSpark") { state =>
  """set assembly / fullClasspath := (Compile / fullClasspath).value""" :: "assembly" :: state
}

assembly / assemblyJarName := s"${name.value}_${scalaBinaryVersion.value}-${version.value}-assembly.jar"

Compile / assembly / artifact := {
  val art: Artifact = (Compile / packageBin / artifact).value
  art.withClassifier(Some("assembly"))
}

// Assembly
addArtifact(Compile / assembly / artifact, assembly)

assembly / assemblyMergeStrategy := {
  case "META-INF/services/org.apache.spark.sql.sources.DataSourceRegister" => MergeStrategy.concat
  case PathList("META-INF", "services", _ @_*) => MergeStrategy.concat
  case PathList("META-INF", _ @_*) => MergeStrategy.discard
  case "reference.conf"            => MergeStrategy.concat
  case _                           => MergeStrategy.first
}

// Required by the Test container framework
Test / fork := true

excludeDependencies ++= Seq(
  ExclusionRule("org.javassist", "javassist")
)

assembly / assemblyExcludedJars := {
  val cp: Classpath = (assembly / fullClasspath).value
  val sortedCp = cp.sortBy(_.data.getName)
  sortedCp.foreach(x => println("->" + x.data.getName))
  sortedCp filter { jar =>
    val jarList = List(
      "netty-buffer-",
      "netty-common-",
      "arrow-memory-core-",
      "arrow-memory-netty-",
      "arrow-format-",
      "arrow-vector-",
      "commons-codec-",
      //"commons-compress-", // Because POI needs it
      "commons-math3-",
      "flatbuffers-java-",
      // "gson-", // because BigQuery needs com.google.gson.JsonParser.parseString(Ljava/lang/String;)
      //"guava-", // BigQuery needs com/google/common/base/MoreObjects (guava-20)
      "hadoop-client-api-",
      "hadoop-client-runtime-",
      "httpclient-",
      "httpcore-",
      "jackson-datatype-jsr310-",
      "java-diff-utils-",
      "jline-3.29.0-jdk8", // pinned: distro also ships jline-2.14.6.jar (legacy Hive shell) under the bare jline- prefix
      "jsr305-",
      "lz4-java-",
      // "protobuf-java-", // BigQuery needs com/google/protobuf/GeneratedMessageV3
      "scala-compiler-",
      "scala-library-",
      "scala-parallel-collections_",
      "scala-parser-combinators_",
      "scala-reflect-",
      "scala-xml_",
      "slf4j-api-",
      "snappy-java-",
      "spark-tags_",
      "threeten-extra-",
      "xz-",
      "zstd-jni-"
    )
    if (jarList.exists(jar.data.getName.startsWith)) {
      println("Exclude ->" + jar.data.getName)
      true
    }
    else
      false
  }
}


assembly / assemblyShadeRules := Seq(
  // poi needs a newer version of commons-compress (> 1.17) than the one shipped with spark 2.4.X
  ShadeRule.rename("org.apache.commons.compress.**" -> "poiShade.commons.compress.@1").inAll,
//  ShadeRule.rename("shapeless.**" -> "shade.@0").inAll,
  //shade it or else writing to bigquery wont work because spark comes with an older version of google common.
  ShadeRule.rename("com.google.common.**" -> "shade.@0").inAll,
  ShadeRule.rename("com.google.gson.**" -> "shade.@0").inAll//,
  //ShadeRule.rename("com.google.protobuf.**" -> "shade.@0").inAll
)


// Disable scaladoc generation

Compile / doc / sources := Seq.empty

//Compile / packageDoc / publishArtifact := false

Compile / packageBin / publishArtifact := true

Compile / packageSrc / publishArtifact := true

// To sync with Maven central, you need to supply the following information:
publishMavenStyle := true

// Open-source license of your choice
licenses := Seq(
  "Apache License, Version 2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0.html")
)

pgpPassphrase := sys.env.get("PGP_PASSPHRASE").map(_.toCharArray)

// Do not disable checksum
publishLocal / checksums := Nil

developers := List(
  Developer(
    id = "hayssams",
    name = "Hayssam Saleh",
    email = "hayssam@saleh.fr",
    url = url("https://github.com/hayssams")
  ),
  Developer(
    id = "tiboun",
    name = "Bounkong Khamphousone",
    email = "bounkong@gmail.com",
    url = url("https://github.com/tiboun")
  ),
  Developer(
    id = "fupelaqu",
    name = "Stéphane Manciot",
    email = "stephane.manciot@gmail.com",
    url = url("https://github.com/fupelaqu")
  ),
  Developer(
    id = "elarib",
    name = "Abdelhamide Elarib",
    email = "elarib.abdelhamide@gmail.com",
    url = url("https://github.com/elarib")
  ),
  Developer(
    id = "cchepelov",
    name = "Cyrille Chepelov",
    email = "cyrille@chepelov.org",
    url = url("https://github.com/cchepelov")
  ),
  Developer(
    id = "AmineSagaama",
    name = "Amine Sagaama",
    email = "amine.sagaama@gmail.com",
    url = url("https://github.com/AmineSagaama")
  ),
  Developer(
    id = "mhdqassir",
    name = "Mohamad Kassir",
    email = "mbkassir@gmail.com",
    url = url("https://github.com/mhdkassir")
  ),
  Developer(
    id = "mmenestret",
    name = "Martin Menestret",
    email = "martinmenestret@gmail.com",
    url = url("https://github.com/mmenestret")
  ),
  Developer(
    id = "pchalcol",
    name = "Patice Chalcol",
    email = "pchalcol@gmail.com",
    url = url("https://github.com/pchalcol")
  ),
  Developer(
    id = "zedach",
    name = "Mourad Dachraoui",
    email = "mourad.dachraoui@gmail.com",
    url = url("https://github.com/zedach")
  ),
  Developer(
    id = "seyguai",
    name = "Nicolas Boussuge",
    email = "nb.seyguai@gmail.com",
    url = url("https://github.com/seyguai")
  )
)

//assembly / logLevel := Level.Debug

val packageSetup = Def.taskKey[Unit]("Package Setup.class")
packageSetup := {
  // depend on compile so the jar never packages stale Setup.class files
  val _ = (Compile / compile).value
  import java.nio.file.Paths
  def zipFile(from: List[java.nio.file.Path], to: java.nio.file.Path): Unit = {
    import java.util.jar.Manifest
    val manifest = new Manifest()
    manifest.getMainAttributes().putValue("Manifest-Version", "1.0")
    manifest.getMainAttributes().putValue("Implementation-Version", version.value)
    manifest.getMainAttributes().putValue("Implementation-Vendor", "starlake")
    manifest.getMainAttributes().putValue("Compiler-Version", javacCompilerVersion)

    IO.jar(from.map(f => f.toFile -> f.toFile.getName()), to.toFile, manifest)

  }
  val scalaMajorVersion = scalaVersion.value.split('.').take(2).mkString(".")
  val setupClass = Paths.get(s"target/scala-$scalaMajorVersion/classes/Setup.class")
  val setupAuthenticatorClass = Paths.get(s"target/scala-$scalaMajorVersion/classes/Setup$$UserPwdAuth.class")
  val setupJarDependencyClass = Paths.get(s"target/scala-$scalaMajorVersion/classes/Setup$$ResourceDependency.class")
  val to = Paths.get("distrib/setup.jar")
  zipFile(
    List(setupClass, setupAuthenticatorClass, setupJarDependencyClass),
    to
  )
}

val syncPythonLibs = Def.taskKey[Unit](
  "Download the orchestration python wheels pinned in Versions.scala into distrib/python-libs and regenerate versions.txt"
)
syncPythonLibs := PythonLibs.sync(baseDirectory.value / "distrib" / "python-libs", streams.value.log)

val checkPythonLibs = Def.taskKey[Unit](
  "Fail when distrib/python-libs is out of sync with the template versions pinned in Versions.scala"
)
checkPythonLibs := PythonLibs.check(baseDirectory.value / "distrib" / "python-libs", streams.value.log)

Compile / mainClass := Some("ai.starlake.job.Main")

// Compile / packageBin := ((Compile / packageBin).dependsOn(packageSetup)).value


Test / parallelExecution := false

// We want each test to run using its own spark context
Test / testGrouping :=  (Test / definedTests).value.map { suite =>
  Group(suite.name, Seq(suite), SubProcess(ForkOptions().withRunJVMOptions(testJavaOptions.toVector)))
}


