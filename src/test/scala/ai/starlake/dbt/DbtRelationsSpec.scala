package ai.starlake.dbt

import ai.starlake.dbt.manifest._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class DbtRelationsSpec extends AnyFlatSpec with Matchers {
  private def node(
    name: String,
    pkg: String = "shop",
    database: Option[String] = Some("shop"),
    version: Option[String] = None,
    latest: Option[String] = None,
    resourceType: String = "model"
  ): DbtNode =
    DbtFixtures
      .model(name)
      .copy(
        uniqueId = s"$resourceType.$pkg.$name${version.fold("")(v => s".v$v")}",
        resourceType = resourceType,
        packageName = pkg,
        database = database,
        version = version,
        latestVersion = latest
      )

  private def source(name: String, database: Option[String]): DbtSource =
    DbtSource(
      uniqueId = s"source.shop.crm.$name",
      packageName = "shop",
      sourceName = "crm",
      name = name,
      database = database,
      schema = "legacy",
      identifier = s"${name}_id",
      description = "",
      docBlocks = Nil,
      columns = Nil,
      freshness = None,
      loadedAtField = None,
      tags = Nil,
      enabled = true
    )

  private def relations(
    nodes: List[DbtNode],
    disabled: List[DbtNode] = Nil,
    sources: List[DbtSource] = Nil
  ) =
    new DbtRelations(
      DbtFixtures.emptyManifest().copy(nodes = nodes, disabled = disabled, sources = sources)
    )

  "DbtRelations" should "choose the most frequent database as the default" in {
    val r = relations(
      List(
        node("a"),
        node("b"),
        node("c", database = Some("other")),
        node("t", resourceType = "test", database = Some("other")),
        node("u", resourceType = "test", database = Some("other"))
      )
    )
    r.defaultDatabase shouldBe Some("shop")
    relations(Nil).defaultDatabase shouldBe None
    relations(List(node("a", database = None))).defaultDatabase shouldBe None
  }

  it should "resolve the fixture models with their package" in {
    val r = new DbtRelations(DbtFixtures.shopManifest)
    r.defaultDatabase shouldBe Some("shop")
    r.ref("shop", None, "orders", None).map(_.uniqueId) shouldBe Some("model.shop.orders")
    r.ref("shop", Some("shop"), "orders", None).map(_.uniqueId) shouldBe Some("model.shop.orders")
    r.ref("shop", Some("other_pkg"), "orders", None) shouldBe None
    r.ref("shop", None, "missing", None) shouldBe None
    r.ofNode(r.ref("shop", None, "orders", None).get) shouldBe
    DbtRelation(None, "marts", "orders")
  }

  it should "prefer the caller package, then the root package, for an unqualified ref" in {
    val r = relations(List(node("m", pkg = "pkg_a"), node("m", pkg = "pkg_b"), node("m")))
    r.ref("pkg_b", None, "m", None).get.packageName shouldBe "pkg_b"
    r.ref("pkg_a", None, "m", None).get.packageName shouldBe "pkg_a"
    r.ref("elsewhere", None, "m", None).get.packageName shouldBe "shop"
    r.ref("pkg_b", Some("pkg_a"), "m", None).get.packageName shouldBe "pkg_a"
  }

  it should "resolve versioned models" in {
    val v1 = node("m", version = Some("1"), latest = Some("2"))
    val v2 = node("m", version = Some("2"), latest = Some("2"))
    val r = relations(List(v1, v2))
    r.ref("shop", None, "m", Some("1")) shouldBe Some(v1)
    r.ref("shop", None, "m", Some("2")) shouldBe Some(v2)
    r.ref("shop", None, "m", Some("3")) shouldBe None
    r.ref("shop", None, "m", None) shouldBe Some(v2)
    val unversioned = node("m")
    relations(List(v1, v2, unversioned)).ref("shop", None, "m", None) shouldBe Some(v2)
    relations(List(v1, unversioned)).ref("shop", None, "m", None) shouldBe Some(unversioned)
  }

  it should "resolve a ref to a disabled node" in {
    val off = node("off")
    val r = relations(List(node("on")), disabled = List(off))
    r.ref("shop", None, "off", None) shouldBe Some(off)
    r.ref("shop", None, "on", None).map(_.name) shouldBe Some("on")
  }

  it should "not resolve a ref to a test or a source-like node" in {
    relations(List(node("t", resourceType = "test"))).ref("shop", None, "t", None) shouldBe None
  }

  it should "drop the default database and keep another one" in {
    val r = relations(List(node("a"), node("b"), node("c", database = Some("lake"))))
    r.ofNode(node("a")) shouldBe DbtRelation(None, "marts", "a")
    r.ofNode(node("c", database = Some("lake"))) shouldBe DbtRelation(Some("lake"), "marts", "c")
    r.ofNode(node("c", database = Some("lake"))).render shouldBe "lake.marts.c"
    r.ofNode(node("a")).render shouldBe "marts.a"
    r.ofNode(node("n", database = None)) shouldBe DbtRelation(None, "marts", "n")
    val local = source("local", Some("shop"))
    val remote = source("remote", Some("lake"))
    r.ofSource(local) shouldBe DbtRelation(None, "legacy", "local_id")
    r.ofSource(remote) shouldBe DbtRelation(Some("lake"), "legacy", "remote_id")
    r.ofSource(remote).render shouldBe "lake.legacy.remote_id"
  }

  it should "look sources up by source name and table name" in {
    val r = relations(Nil, sources = List(source("accounts", None)))
    r.source("crm", "accounts").map(_.identifier) shouldBe Some("accounts_id")
    r.source("crm", "nope") shouldBe None
    r.source("other", "accounts") shouldBe None
    val fixture = new DbtRelations(DbtFixtures.shopManifest)
    fixture.source("crm", "crm_accounts").map(_.schema) shouldBe Some("legacy")
    fixture.ofSource(fixture.source("crm", "crm_accounts").get) shouldBe
    DbtRelation(None, "legacy", "accounts")
  }
}
