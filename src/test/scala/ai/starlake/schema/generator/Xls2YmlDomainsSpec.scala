package ai.starlake.schema.generator

import ai.starlake.TestHelper
import ai.starlake.config.DatasetArea
import ai.starlake.schema.model.{DomainInfo, Format, FsSink, SchemaInfo}
import ai.starlake.utils.YamlSerde
import better.files.File

import scala.util.{Failure, Success}

class Xls2YmlDomainsSpec extends TestHelper {
  new WithSettings() {
    Xls2Yml.writeDomainsAsYaml(
      File(getClass.getResource("/sample/SomeDomainTemplate.xlsx")).pathAsString
    )
    val outputPath: File = File(DatasetArea.load.toString + "/someDomain/_config.sl.yml")
    val schema1Path: File = File(DatasetArea.load.toString + "/someDomain/SCHEMA1.sl.yml")
    val schema2Path: File = File(DatasetArea.load.toString + "/someDomain/SCHEMA2.sl.yml")

    lazy val result: DomainInfo = YamlSerde
      .deserializeYamlLoadConfig(
        outputPath.contentAsString,
        outputPath.pathAsString,
        isForExtract = false
      ) match {
      case Success(value)     => value
      case Failure(exception) => throw exception
    }

    lazy val schema1: SchemaInfo = YamlSerde
      .deserializeYamlTables(schema1Path.contentAsString, schema1Path.pathAsString)
      .head
      .table

    lazy val schema2: SchemaInfo = YamlSerde
      .deserializeYamlTables(schema2Path.contentAsString, schema2Path.pathAsString)
      .head
      .table

    "Parsing a sample xlsx file" should "generate a yml file" in {
      outputPath.exists shouldBe true
      result.name shouldBe "someDomain"
      result.tags shouldBe Set("aTag")
    }

    it should "take into account the index col of a schema" in {
      val sink = for {
        schema   <- Some(schema1)
        metadata <- schema.metadata
        sink     <- metadata.sink
      } yield sink

      sink.map(_.getSink()) shouldBe Some(
        FsSink(
          Some("spark"),
          None,
          None,
          None,
          Some(List("sl_year", "sl_month", "sl_day", "sl_hour")),
          options = Some(Map.empty)
        )
      )
    }

    "All configured schemas" should "have all declared attributes correctly set" in {
      schema1.metadata.flatMap(_.format) shouldBe Some(Format.POSITION)
      schema1.metadata.flatMap(_.encoding) shouldBe None
      schema1.attributes.size shouldBe 19

      schema1.metadata.flatMap(_.writeStrategy.flatMap(_.timestamp)) shouldBe Some("ATTRIBUTE_1")
      schema1.metadata.flatMap(_.writeStrategy.map(_.key)) shouldBe Some(List("ID1", "ID2"))

      val s1MaybePartitions = schema1.metadata.map(_.getPartitionAttributes()).getOrElse(Nil)
      s1MaybePartitions shouldEqual List("sl_year", "sl_month", "sl_day", "sl_hour")

      schema2.metadata.flatMap(_.format) shouldBe Some(Format.DSV)
      schema2.metadata.flatMap(_.encoding) shouldBe Some("ISO-8859-1")
      schema2.attributes.size shouldBe 19

      val s2MaybePartitions = schema2.metadata.map(_.getPartitionAttributes()).getOrElse(Nil)
      s2MaybePartitions shouldEqual List("RENAME_ATTRIBUTE_8", "RENAME_ATTRIBUTE_9")
    }

    val reader = new XlsDomainReader(
      InputPath(getClass.getResource("/sample/SomeDomainTemplate.xlsx").getPath)
    )
    val domainOpt: Option[DomainInfo] = reader.getDomain()

    "a complex XLS (aka JSON/XML)" should "produce the correct schema" in {
      val complexReader =
        new XlsDomainReader(
          InputPath(
            File(getClass.getResource("/sample/SomeComplexDomainTemplate.xlsx")).pathAsString
          )
        )
      val xlsTable = complexReader.getDomain().get.tables.head
      val yamlPath =
        File(getClass.getResource("/sample/SCHEMA1.sl.yml"))

      val yamlTable = YamlSerde
        .deserializeYamlTables(
          yamlPath.contentAsString,
          yamlPath.pathAsString
        )
        .head
        .table

      xlsTable.attributes.length shouldBe yamlTable.attributes.length

      deepEquals(xlsTable.attributes, yamlTable.attributes)
    }

    "Column Description in schema" should "be present" in {
      domainOpt shouldBe defined
      domainOpt.get.tables.flatMap(_.comment) should have length 1
    }

    private def validCount(domain: DomainInfo, algo: String, count: Int) =
      domain.tables
        .flatMap(_.attributes)
        .filter(_.resolvePrivacy().toString == algo) should have length count

    "a scripted attribute" should "be generated" in {
      domainOpt shouldBe defined
      domainOpt
        .flatMap(_.tables.find(_.name == "SCHEMA1"))
        .flatMap(_.attributes.find(_.name == "ATTRIBUTE_4").flatMap(_.script)) shouldBe Some(
        "current_date()"
      )
    }

    "All SchemaGen Config" should "be known and taken  into account" in {
      val rendered = Xls2YmlCmd.usage()
      val expected =
        """
          |Usage: starlake xls2yml [options]
          |Generate Starflow YAML configuration files from Excel spreadsheets.
          |
          |  --files <value>       List of Excel files describing domains & schemas or jobs
          |  --iamPolicyTagsFile <value>
          |                        If true generate IAM PolicyTags YML
          |  --outputDir <value>  Path for saving the resulting YAML file(s).
          | Starflow domains path is used by default.
          |  --policyFile <value>  Optional File for centralising ACL & RLS definition.
          |  --job <value>         If true generate YML for a Job.
          |  --reportFormat <value> Report format: console, json, html
          |""".stripMargin
      rendered.substring(rendered.indexOf("Usage:")).replaceAll("\\s", "") shouldEqual expected
        .replaceAll("\\s", "")

    }
  }

}
