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

import com.github.sbt.git.GitPlugin.autoImport._
import com.github.sbt.git.{GitBranchPrompt, GitVersioning}
import org.scalafmt.sbt.ScalafmtPlugin.autoImport.scalafmtOnCompile
import sbt.Keys._
import sbt.{Def, _}
import sbtassembly.AssemblyKeys._
import sbtbuildinfo.{BuildInfoKey, BuildInfoPlugin}
import sbtbuildinfo.BuildInfoPlugin.autoImport.buildInfoKeys

import java.time.{LocalDateTime, ZoneId}
import java.time.format.DateTimeFormatter
import java.time.ZonedDateTime

object Common {

  def enableStarlakeAliases: Seq[Def.Setting[_]] =
    Seq(
      addCommandAlias("cd", "project"), // navigate the projects
      addCommandAlias("cc", ";clean;compile"), // clean and compile
      addCommandAlias("pl", ";clean;publishLocal"), // clean and publish locally
      addCommandAlias("pr", ";clean;publish"), // clean and publish globally
      addCommandAlias(
        "pld",
        ";clean;local:publishLocal;dockerComposeUp"
      ) // clean and publish/launch the docker environment
    ).flatten

  def starlakePlugins: Seq[AutoPlugin] = Seq(
    GitVersioning,
    GitBranchPrompt,
    BuildInfoPlugin
  )

  def gitSettings = Seq(
    git.useGitDescribe := true, {
      val VersionRegex = "v([0-9]+.[0-9]+.[0-9]+)-?(.*)?".r
      git.gitTagToVersionNumber := {
        case VersionRegex(v, "")         => Some(v)
        case VersionRegex(v, "SNAPSHOT") => Some(s"$v-SNAPSHOT")
        case VersionRegex(v, s)          => Some(s"$v-$s-SNAPSHOT")
        case _                           => None
      }
    }
    //    git.gitTagToVersionNumber := { tag: String =>
//      if (tag matches "[0-9]+\\..*") Some(tag)
//      else None
//    }
  )

  def assemlySettings = Seq(
    assembly / test := {},
    compile / mainClass := Some("ai.starlake.job.Main")
  )

  def customSettings: Seq[Def.Setting[_]] =
    Seq(
      Test / testOptions ++= Seq(
        // show full stack traces and test case durations
        Tests.Argument("-oDF"),
        // -v Log "test run started" / "test started" / "test run finished" events on log level "info" instead of "debug".
        // -a Show stack traces a nd exception class name for ExpectationErrors.
        Tests.Argument(TestFrameworks.JUnit, "-v", "-a")
      ),
      Test / parallelExecution := false,
      scalafmtOnCompile := true,
      buildInfoKeys := Seq[BuildInfoKey](name, scalaVersion, sbtVersion),
      buildInfoKeys ++= Seq[BuildInfoKey](
        BuildInfoKey.action("version") {
          val currentVersion = version.value
          if (currentVersion.contains("-SNAPSHOT")) {
            val suffix = git.gitHeadCommit.value
              .map(_.take(8))
              .getOrElse("T" + ZonedDateTime.now().format(DateTimeFormatter.ISO_ZONED_DATE_TIME))
            // val suffix = "T" + ZonedDateTime.now().format(DateTimeFormatter.ISO_OFFSET_DATE_TIME)
            currentVersion + " @ " + suffix
          } else currentVersion
        },
        BuildInfoKey.action("buildTime") {
          val readableFormat: DateTimeFormatter =
            DateTimeFormatter.ofPattern("EEEE, MMMM dd, yyyy 'at' hh:mm:ss a z")
          val now: LocalDateTime = LocalDateTime.now()
          val zonedDateTime = now.atZone(ZoneId.systemDefault())
          zonedDateTime.format(readableFormat)
        } // re-computed each time at compile
      )
    ) ++ gitSettings ++ assemlySettings

}

object Resolvers {

  val typeSafe = "Typesafe repository" at "https://repo.typesafe.com/typesafe/releases/"
  val confluent = "Confluent repository" at "https://packages.confluent.io/maven/"
  val mulesoft =
    "Mulesoft repository" at "https://repository.mulesoft.org/nexus/content/repositories/public/"

  val snapshots = Resolver.sonatypeCentralSnapshots

  // resolves ai.starlake:starlake-streaming from its GitHub Release assets (anonymous; see plan Task 18)
  val starlakeStreamingGithubReleases = Resolver.url(
    "starlake-streaming-github-releases",
    url("https://github.com/starlake-ai/starlake-streaming/releases/download/")
  )(
    Patterns()
      .withIvyPatterns(Vector("v[revision]/ivy-[revision].xml"))
      .withArtifactPatterns(Vector("v[revision]/[artifact]-[revision](-[classifier]).[ext]"))
      .withIsMavenCompatible(false)
  )

  // resolves ai.starlake:spark-redshift from its GitHub Release assets (anonymous; see plan Task 18)
  val sparkRedshiftGithubReleases = Resolver.url(
    "spark-redshift-github-releases",
    url("https://github.com/starlake-ai/spark-redshift/releases/download/")
  )(
    Patterns()
      .withIvyPatterns(Vector("v[revision]/ivy-[revision].xml"))
      .withArtifactPatterns(Vector("v[revision]/[artifact]-[revision](-[classifier]).[ext]"))
      .withIsMavenCompatible(false)
  )

  // Our build of Delta (io.delta) and of the Unity Catalog snapshot it pins (io.unitycatalog),
  // all in one starflow release: modules are told apart by name, not by tag. Only consulted while
  // Versions.deltaSpark is one of our "-starlake.N" builds.
  val deltaGithubRelease = Resolver.url(
    "delta-github-release",
    url(s"https://github.com/starlake-ai/starflow/releases/download/deps-delta-${Versions.deltaSpark}/")
  )(
    Patterns()
      .withIvyPatterns(Vector("[module]-[revision]-ivy.xml"))
      .withArtifactPatterns(Vector("[artifact]-[revision](-[classifier]).[ext]"))
      .withIsMavenCompatible(false)
  )

  val allResolvers =
    Seq(
      Resolver.mavenLocal,
      snapshots,
      typeSafe,
      confluent,
      mulesoft,
      starlakeStreamingGithubReleases,
      sparkRedshiftGithubReleases
    ) ++ (if (Versions.deltaSpark.contains("-starlake.")) Seq(deltaGithubRelease) else Nil)

  val googleCloudBigDataMavenRepo = "https://repo1.maven.org/maven2/com/google/cloud/bigdataoss"

}
