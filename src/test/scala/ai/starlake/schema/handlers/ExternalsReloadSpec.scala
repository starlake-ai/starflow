package ai.starlake.schema.handlers

import ai.starlake.TestHelper
import ai.starlake.config.DatasetArea
import org.apache.hadoop.fs.Path

class ExternalsReloadSpec extends TestHelper {

  private def domainYaml(name: String) =
    s"""version: 1
       |load:
       |  name: "$name"
       |""".stripMargin

  private def tableYaml(name: String, columns: String*) =
    s"""version: 1
       |table:
       |  name: "$name"
       |  pattern: "$name.*"
       |  attributes:
       |""".stripMargin + columns
      .map(c => s"""    - name: "$c"\n      type: "string"\n""")
      .mkString

  private def tables(schemaHandler: SchemaHandler): Map[String, List[String]] =
    schemaHandler
      .externals()
      .flatMap(d => d.tables.map(t => s"${d.name}.${t.name}" -> t.attributes.map(_.name)))
      .toMap

  "externals()" should "follow files added, changed and removed under the externals folder" in {
    new WithSettings() {
      val root = DatasetArea.external
      val ext1 = new Path(root, "ext1")
      storageHandler.write(domainYaml("ext1"), new Path(ext1, "_config.sl.yml"))
      storageHandler.write(tableYaml("a", "x"), new Path(ext1, "a.sl.yml"))

      val schemaHandler = settings.schemaHandler(reload = true)
      schemaHandler.externalsRecheckIntervalMs = 0
      tables(schemaHandler) shouldBe Map("ext1.a" -> List("x"))

      // a new table in an existing domain and a new domain
      storageHandler.write(tableYaml("b", "y"), new Path(ext1, "b.sl.yml"))
      val ext2 = new Path(root, "ext2")
      storageHandler.write(domainYaml("ext2"), new Path(ext2, "_config.sl.yml"))
      storageHandler.write(tableYaml("c", "z"), new Path(ext2, "c.sl.yml"))
      tables(schemaHandler) shouldBe Map(
        "ext1.a" -> List("x"),
        "ext1.b" -> List("y"),
        "ext2.c" -> List("z")
      )

      // a changed table
      storageHandler.write(tableYaml("a", "x", "x2"), new Path(ext1, "a.sl.yml"))
      tables(schemaHandler)("ext1.a") shouldBe List("x", "x2")

      // a removed table and a removed domain
      storageHandler.delete(new Path(ext1, "b.sl.yml"))
      storageHandler.delete(ext2)
      tables(schemaHandler) shouldBe Map("ext1.a" -> List("x", "x2"))

      // an explicit reload keeps every external, not only the files changed since the last load
      schemaHandler.loadExternals()
      tables(schemaHandler) shouldBe Map("ext1.a" -> List("x", "x2"))
    }
  }

  it should "list the externals folder at most once per recheck interval" in {
    new WithSettings() {
      val ext1 = new Path(DatasetArea.external, "ext1")
      storageHandler.write(domainYaml("ext1"), new Path(ext1, "_config.sl.yml"))
      storageHandler.write(tableYaml("a", "x"), new Path(ext1, "a.sl.yml"))

      val schemaHandler = settings.schemaHandler(reload = true)
      schemaHandler.externalsRecheckIntervalMs = 3600000
      tables(schemaHandler) shouldBe Map("ext1.a" -> List("x"))
      storageHandler.write(tableYaml("b", "y"), new Path(ext1, "b.sl.yml"))
      // still within the interval: the cache is trusted
      tables(schemaHandler) shouldBe Map("ext1.a" -> List("x"))
      // an explicit reload always reads the folder
      schemaHandler.loadExternals()
      tables(schemaHandler).keySet shouldBe Set("ext1.a", "ext1.b")
    }
  }
}
