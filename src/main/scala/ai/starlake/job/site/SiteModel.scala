package ai.starlake.job.site

import ai.starlake.schema.model.{AllSinks, Freshness, Metadata, WriteStrategy}

import scala.collection.immutable.TreeMap

object NodeId {
  def load(domain: String, table: String): String = s"load:$domain.$table".toLowerCase
  def transform(fullName: String): String = s"transform:$fullName".toLowerCase
  def external(name: String): String = s"external:$name".toLowerCase
  def kindOf(id: String): String = id.takeWhile(_ != ':')
}

case class SiteColumn(
  name: String,
  rename: Option[String] = None,
  `type`: String = "string",
  sqlType: Option[String] = None,
  required: Boolean = false,
  primaryKey: Boolean = false,
  foreignKey: Option[String] = None,
  foreignKeyNode: Option[String] = None,
  privacy: Option[String] = None,
  script: Option[String] = None,
  default: Option[String] = None,
  metricType: Option[String] = None,
  description: Option[String] = None,
  tags: List[String] = Nil,
  columns: List[SiteColumn] = Nil,
  // Some(true) for an attribute core drops from the table (SchemaInfo.exceptIgnoreAttributes),
  // absent otherwise
  ignore: Option[Boolean] = None
)

case class SiteSql(raw: Option[String], compiled: Option[String])
case class SiteExpectation(expect: String, failOnError: Boolean)
case class SiteAcl(role: String, grants: List[String])
case class SiteRls(name: String, predicate: String, grants: List[String], description: String)

case class SiteNode(
  id: String,
  kind: String,
  name: String,
  domain: String,
  table: String,
  database: Option[String] = None,
  connectionRef: Option[String] = None,
  engine: Option[String] = None,
  description: Option[String] = None,
  tags: List[String] = Nil,
  filePath: Option[String] = None,
  columns: List[SiteColumn] = Nil,
  expectations: List[SiteExpectation] = Nil,
  acl: List[SiteAcl] = Nil,
  rls: List[SiteRls] = Nil,
  sink: Option[AllSinks] = None,
  dag: Option[String] = None,
  primaryKey: List[String] = Nil,
  presql: List[String] = Nil,
  postsql: List[String] = Nil,
  writeStrategy: Option[WriteStrategy] = None,
  pattern: Option[String] = None,
  patternSample: Option[String] = None,
  metadata: Option[Metadata] = None,
  sql: Option[SiteSql] = None,
  python: Option[String] = None,
  freshness: Option[Freshness] = None,
  schedule: Option[String] = None,
  warnings: List[String] = Nil
)

case class SiteEdge(from: String, to: String, kind: String)
case class SiteColumnEdge(from: String, to: String, task: String)
case class SiteGroup(name: String, description: Option[String], nodes: List[String])
case class SiteDagGroup(name: String, template: String, comment: String, nodes: List[String])
case class SiteGroups(domains: List[SiteGroup], jobs: List[SiteGroup], dags: List[SiteDagGroup])
case class SiteProject(
  name: String,
  env: String,
  defaultConnection: String,
  overview: Option[String]
)

case class SiteModel(
  schemaVersion: Int,
  generatedAt: String,
  starlakeVersion: String,
  project: SiteProject,
  nodes: TreeMap[String, SiteNode],
  edges: List[SiteEdge],
  columnEdges: List[SiteColumnEdge],
  groups: SiteGroups,
  docs: TreeMap[String, String]
)
