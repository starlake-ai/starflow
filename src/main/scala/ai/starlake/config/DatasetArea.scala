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

package ai.starlake.config

import ai.starlake.core.utils.StringUtils
import ai.starlake.schema.handlers.StorageHandler
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.fs.Path

/** Utilities methods to reference datasets paths Datasets paths are constructed as follows :
  *   - root path defined by the SL_DATASETS env var or datasets application property
  *   - followed by the area name
  *   - followed by the the domain name
  */
object DatasetArea extends LazyLogging {

  def path(domain: String, area: String)(implicit settings: Settings): Path = {
    if (
      area.contains("://") ||
      (settings.appConfig.fileSystem.startsWith("file:") && area.startsWith("/"))
    )
      new Path(area, domain)
    else if (settings.appConfig.datasets.contains("://"))
      new Path(
        s"${settings.appConfig.datasets}/$area/$domain"
      )
    else {
      val ds =
        if (settings.appConfig.datasets.startsWith("/"))
          settings.appConfig.datasets
        else "/" + settings.appConfig.datasets
      val path = s"${settings.appConfig.fileSystem}$ds/$area/$domain"
      new Path(
        path
      )
    }
  }

  def duckdbPath()(implicit settings: Settings): Path = {
    duckdbPath("duckdb.db")
  }
  def duckdbPath(name: String)(implicit settings: Settings): Path = {
    settings.appConfig.duckdbPath match {
      case Some(path) =>
        new Path(path)
      case None =>
        val duckdb = path(name)
        if (duckdb.toString.startsWith("file:")) {
          duckdb
        } else {
          val datasets = new Path(metadata.getParent, "datasets")
          new Path(datasets, name)
        }
    }
  }
  def secureDuckdbPath(dbName: String)(implicit settings: Settings): Path = {
    val result =
      settings.appConfig.duckdbPath match {
        case Some(path) =>
          new Path(path)
        case None =>
          val duckdb = path(dbName)
          if (duckdb.toString.startsWith("file:")) {
            duckdb
          } else {
            val datasets = new Path(metadata.getParent, "datasets")
            new Path(datasets, dbName)
          }
      }
    require(result.toString.indexOf("..") < 0, s"DuckDB path should not contain '..': $result")
    result
  }

  def path(domain: String)(implicit settings: Settings): Path =
    if (settings.appConfig.datasets.contains("://"))
      new Path(
        s"${settings.appConfig.datasets}/$domain"
      )
    else
      new Path(
        s"${settings.appConfig.fileSystem}/${settings.appConfig.datasets}/$domain"
      )

  def path(domainPath: Path, schema: String) = new Path(domainPath, schema)

  def testsResults(domain: String)(implicit settings: Settings): Path =
    path("_tests_results")

  /** datasets waiting to be ingested are stored here
    *
    * @param domain
    *   : Domain Name
    * @return
    *   Absolute path to the pending folder of domain
    */
  def stage(domain: String)(implicit settings: Settings): Path =
    path(domain, settings.appConfig.area.stage)

  def incoming(domain: String)(implicit settings: Settings): Path =
    path(domain, settings.appConfig.area.incoming)

  def incoming(implicit settings: Settings): Path =
    path("dummy", settings.appConfig.area.incoming).getParent

  /** datasets with a file name that could not match any schema file name pattern in the specified
    * domain are marked unresolved by being stored in this folder.
    *
    * @param domain
    *   : Domain name
    * @return
    *   Absolute path to the pending unresolved folder of domain
    */
  def unresolved(domain: String)(implicit settings: Settings): Path =
    path(domain, settings.appConfig.area.unresolved)

  /** Once ingested datasets are archived in this folder.
    *
    * @param domain
    *   : Domain name
    * @return
    *   Absolute path to the archive folder of domain
    */
  def archive(domain: String)(implicit settings: Settings): Path =
    path(domain, settings.appConfig.area.archive)

  /** Datasets of the specified domain currently being ingested are located in this folder
    *
    * @param domain
    *   : Domain name
    * @return
    *   Absolute path to the ingesting folder of domain
    */
  def ingesting(domain: String)(implicit settings: Settings): Path =
    path(domain, settings.appConfig.area.ingesting)

  def `export`(domain: String)(implicit settings: Settings): Path = {
    path(domain, "export")
  }

  def `export`(domain: String, table: String)(implicit settings: Settings): Path = {
    new Path(`export`(domain), table)
  }

  def refs()(implicit settings: Settings): Path = new Path(metadata, "refs.sl.yml")

  def env()(implicit settings: Settings): Path = new Path(metadata, "env.sl.yml")

  def env(name: String)(implicit settings: Settings): Path = new Path(metadata, s"env.$name.sl.yml")

  def metrics(domain: String, schema: String)(implicit settings: Settings): Path =
    substituteDomainAndSchemaInPath(domain, schema, settings.appConfig.metrics.path)

  def audit(domain: String, schema: String)(implicit settings: Settings): Path =
    substituteDomainAndSchemaInPath(domain, schema, settings.appConfig.audit.path)

  def expectations(domain: String, schema: String)(implicit settings: Settings): Path =
    substituteDomainAndSchemaInPath(domain, schema, settings.appConfig.expectations.path)

  def replay(domain: String)(implicit settings: Settings): Path =
    path(domain, settings.appConfig.area.replay)

  def substituteDomainAndSchemaInPath(
    domain: String,
    schema: String,
    path: String
  ): Path = {
    new Path(
      substituteDomainAndSchema(domain, schema, path)
    )
  }

  def substituteDomainAndSchema(domain: String, schema: String, template: String): String = {
    val normalizedDomainName = StringUtils.replaceNonAlphanumericWithUnderscore(domain)
    template
      .replace("{{domain}}", domain)
      .replace("{{normalized_domain}}", normalizedDomainName)
      .replace("{{schema}}", schema)
  }

  def discreteMetrics(domain: String, schema: String)(implicit settings: Settings): Path =
    DatasetArea.metrics(domain, "discrete")

  def continuousMetrics(domain: String, schema: String)(implicit settings: Settings): Path =
    DatasetArea.metrics(domain, "continuous")

  def frequenciesMetrics(domain: String, schema: String)(implicit settings: Settings): Path =
    DatasetArea.metrics(domain, "frequencies")

  def metadata(implicit settings: Settings): Path =
    new Path(s"${settings.appConfig.metadata}")

  def types(implicit settings: Settings): Path =
    new Path(settings.appConfig.types)

  def dags(implicit settings: Settings): Path =
    new Path(settings.appConfig.dags)

  def build(implicit settings: Settings): Path =
    new Path(metadata, ".build")

  def keys(implicit settings: Settings): Path =
    new Path(metadata, ".keys")

  def writeStrategies(implicit settings: Settings): Path =
    new Path(settings.appConfig.writeStrategies)

  def loadStrategies(implicit settings: Settings): Path =
    new Path(settings.appConfig.loadStrategies)

  def expectations(implicit settings: Settings): Path =
    new Path(metadata, "expectations")

  def semantic(implicit settings: Settings): Path =
    new Path(metadata, "semantic")

  def macros(implicit settings: Settings): Path =
    new Path(settings.appConfig.macros)

  def mapping(implicit settings: Settings): Path =
    new Path(metadata, "mapping")

  def tests(implicit settings: Settings): Path =
    new Path(metadata, "tests")

  def loadTests(implicit settings: Settings): Path =
    new Path(tests, "load")

  def transformTests(implicit settings: Settings): Path =
    new Path(tests, "transform")

  def load(implicit settings: Settings): Path =
    new Path(metadata, "load")

  def cache(implicit settings: Settings): Path =
    new Path(metadata, ".cache")

  def external(implicit settings: Settings): Path =
    new Path(metadata, "external")

  def extract(implicit settings: Settings): Path =
    new Path(metadata, "extract")

  def worksheets(implicit settings: Settings): Path =
    new Path(metadata, "worksheets")

  def transform(implicit settings: Settings): Path =
    new Path(metadata, "transform")

  def iamPolicyTags()(implicit settings: Settings): Path =
    new Path(DatasetArea.metadata, "iam-policy-tags.sl.yml")

  def folders(implicit settings: Settings): List[Path] =
    List(
      metadata,
      types,
      load,
      external,
      extract,
      transform,
      expectations,
      mapping,
      incoming,
      build,
      worksheets,
      macros,
      semantic
    )

  /** @param storage
    */
  def initMetadata(
    storage: StorageHandler
  )(implicit settings: Settings): Unit = {
    folders
      .foreach(
        storage.mkdirs
      )

  }

  def initDomains(storage: StorageHandler, domains: Iterable[String])(implicit
    settings: Settings
  ): Unit = {
    domains.foreach { domain =>
      List(stage _, unresolved _, archive _, replay _)
        .map(_(domain))
        .foreach(storage.mkdirs)
    }
  }
}
