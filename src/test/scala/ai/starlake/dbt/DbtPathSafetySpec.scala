package ai.starlake.dbt

import ai.starlake.TestHelper
import ai.starlake.config.DatasetArea
import ai.starlake.dbt.manifest._
import ai.starlake.dbt.mapping._
import ai.starlake.dbt.report.{EntryStatus, ImportReport}
import ai.starlake.dbt.write.ProjectWriter
import ai.starlake.schema.model.TableAttribute
import org.apache.hadoop.fs.Path

import java.nio.file.{Files, Paths}
import scala.util.Success

/** Names from the manifest are path components: none may leave the project */
class DbtPathSafetySpec extends TestHelper {
  new WithSettings() {
    private val stub = (_: java.nio.file.Path, _: String) =>
      Success(List(TableAttribute("id", "long")))

    private val projectDir = Files.createTempDirectory("dbt-unsafe")
    private val seedFile = "weird\\name.csv"
    Files.createDirectories(projectDir.resolve("seeds"))
    Files.writeString(projectDir.resolve(s"seeds/$seedFile"), "id\n1\n")
    Files.writeString(projectDir.resolve("seeds/ok.csv"), "id\n1\n")

    private def seed(name: String, schema: String, file: String) =
      DbtFixtures
        .model(name)
        .copy(
          uniqueId = s"seed.shop.$name",
          resourceType = "seed",
          schema = schema,
          originalFilePath = s"seeds/$file"
        )

    private def plan(
      models: List[DbtNode] = Nil,
      seeds: List[DbtNode] = Nil,
      docs: List[DbtDoc] = Nil
    ): ImportPlan = {
      val manifest = DbtFixtures
        .emptyManifest()
        .copy(
          nodes = models ++ seeds,
          docs = docs,
          manifestPath = projectDir.resolve("target/manifest.json")
        )
      DbtMapper.plan(
        DbtMappingContext(
          manifest,
          new DbtRelations(manifest),
          DbtFixtures.jinja(),
          None,
          stub
        ),
        None
      )
    }

    private def skipReason(p: ImportPlan, id: String): Option[String] =
      p.report.entries.find(_.dbtId == id).map(_.status).collect { case EntryStatus.Skipped(r) =>
        r
      }

    private def checked(p: ImportPlan): Unit = {
      ProjectWriter.escaping(p) shouldBe Nil
      p.outputs.map(_.path.toString).exists(s => s.contains("/abs") || s.contains("/x/")) shouldBe
      false
      ProjectWriter.write(p, force = true).isSuccess shouldBe true
      Files.exists(Paths.get("/abs")) shouldBe false
      Files.exists(Paths.get(settings.appConfig.root).getParent.resolve("x")) shouldBe false
    }

    "A model with a schema that climbs out" should "be skipped with its reason" in {
      val p = plan(models = List(DbtFixtures.model("m1").copy(schema = "../x")))
      skipReason(p, "model.shop.m1") shouldBe Some(
        """schema "../x" cannot be a folder or file name"""
      )
      p.outputs shouldBe Nil
      checked(p)
    }

    "A model with an absolute alias" should "be skipped with its reason" in {
      val p = plan(models = List(DbtFixtures.model("m2").copy(alias = "/abs")))
      skipReason(p, "model.shop.m2") shouldBe Some(
        """alias "/abs" cannot be a folder or file name"""
      )
      p.outputs shouldBe Nil
      checked(p)
    }

    "A seed" should "be skipped when its schema escapes" in {
      val p = plan(seeds = List(seed("s1", "../x", "ok.csv")))
      skipReason(p, "seed.shop.s1") shouldBe Some(
        """schema "../x" cannot be a folder or file name"""
      )
      p.outputs shouldBe Nil
      checked(p)
    }

    it should "be skipped when its CSV file name is not a file name" in {
      val p = plan(seeds = List(seed("s2", "raw", seedFile)))
      skipReason(p, "seed.shop.s2") shouldBe Some(
        s"""csv file "$seedFile" cannot be a folder or file name"""
      )
      p.outputs shouldBe Nil
    }

    "A doc block with a / in its name" should "be skipped with its reason" in {
      val p = plan(docs = List(DbtDoc("doc.shop.d", "shop", "a/../../../b", "text")))
      skipReason(p, "doc.shop.d") shouldBe Some(
        """name "a/../../../b" cannot be a folder or file name"""
      )
      p.outputs shouldBe Nil
      checked(p)
    }

    "A model, a seed or a doc block with a : in its name" should "be skipped with its reason" in {
      val p = plan(
        models = List(
          DbtFixtures.model("m3").copy(alias = "a:b"),
          DbtFixtures.model("m4").copy(schema = "c:d")
        ),
        seeds = List(seed("s3", "e:f", "ok.csv")),
        docs = List(DbtDoc("doc.shop.d2", "shop", "g:h", "text"))
      )
      skipReason(p, "model.shop.m3") shouldBe Some(
        """alias "a:b" cannot be a folder or file name"""
      )
      skipReason(p, "model.shop.m4") shouldBe Some(
        """schema "c:d" cannot be a folder or file name"""
      )
      skipReason(p, "seed.shop.s3") shouldBe Some(
        """schema "e:f" cannot be a folder or file name"""
      )
      skipReason(p, "doc.shop.d2") shouldBe Some("""name "g:h" cannot be a folder or file name""")
      p.outputs shouldBe Nil
    }

    "A seed whose CSV is outside the dbt project" should "be skipped with its reason" in {
      val outside = Files.createTempDirectory("dbt-outside")
      Files.createDirectories(outside.resolve("seeds"))
      Files.writeString(outside.resolve("seeds/far.csv"), "id\n1\n")
      val absolute = seed("s4", "raw", "ok.csv")
        .copy(originalFilePath = outside.resolve("seeds/far.csv").toString)
      val fromPackage = seed("s5", "raw", "far.csv")
        .copy(packageName = "pkg", rootPath = Some(outside.toString))
      val p = plan(seeds = List(absolute, fromPackage))
      List("seed.shop.s4", "seed.shop.s5").foreach { id =>
        skipReason(p, id).getOrElse(fail(s"$id not skipped")) should include(
          "outside the dbt project"
        )
      }
      p.outputs shouldBe Nil
    }

    "Two models that map to the same file" should "be one converted, the other skipped naming it" in {
      val first = DbtFixtures.model("same").copy(uniqueId = "model.shop.same_db1")
      val second =
        DbtFixtures.model("same").copy(uniqueId = "model.shop.same_db2", database = Some("db2"))
      val p = plan(models = List(first, second))
      p.report.entries.find(_.dbtId == "model.shop.same_db1").map(_.status) shouldBe Some(
        EntryStatus.Converted
      )
      skipReason(p, "model.shop.same_db2").getOrElse(fail("not skipped")) should include(
        "model.shop.same_db1"
      )
      p.outputs.map(_.path).distinct.size shouldBe p.outputs.size
    }

    "safeName" should "accept one path component only" in {
      List("orders", "a.b", "..x", "a b").foreach(n => DbtText.safeName(n) shouldBe Right(n))
      List("", ".", "..", "a/b", "/a", "a\\b", "a\u0000b", "a\nb", "a:b").foreach(n =>
        DbtText.safeName(n) shouldBe Left(n)
      )
    }

    "ProjectWriter" should "refuse a plan that escapes the project and write nothing" in {
      val metadata = DatasetArea.metadata
      val sibling = new Path(metadata.getParent, s"${metadata.getName}2/x.sql")
      val climbing = new Path(metadata, "transform/../../outside.sql")
      val good = new Path(metadata, "transform/ok.sql")
      val p = ImportPlan(
        List(
          FileOutput(good, "select 1"),
          FileOutput(sibling, "select 2"),
          FileOutput(climbing, "select 3")
        ),
        None,
        ImportReport(Nil, Nil, Nil, Nil)
      )
      ProjectWriter.escaping(p) should contain theSameElementsAs List(sibling, climbing)
      val result = ProjectWriter.write(p, force = true)
      result.isFailure shouldBe true
      result.failed.get.getMessage should include(sibling.toString)
      storageHandler.exists(good) shouldBe false
      storageHandler.exists(sibling) shouldBe false
    }
  }
}
