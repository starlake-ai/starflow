package ai.starlake.semantic

import ai.starlake.TestHelper
import ai.starlake.config.DatasetArea
import ai.starlake.utils.YamlSerde
import org.apache.hadoop.fs.Path

import scala.io.Source
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success}

class SMLExportSpec extends TestHelper {

  private def modelYaml: String = {
    val source =
      Source.fromInputStream(getClass.getResourceAsStream("/semantic/sml/tpch_mini.yaml"))
    try source.mkString
    finally source.close()
  }

  "semantic-export --format sml" should "write an SML repository per model" in {
    new WithSettings() {
      cleanMetadata
      val storage = settings.storageHandler()
      storage.write(modelYaml, new Path(DatasetArea.semantic, "tpch_mini.yaml"))

      new SemanticExportJob(
        SemanticExportConfig(format = "sml", connection = Some("snowflake_prod"))
      ).run() match {
        case Failure(exception) => throw exception
        case Success(_)         =>
      }

      val outDir = new Path(DatasetArea.semantic, "export/sml/tpch_mini")
      storage.read(new Path(outDir, "catalog.yml")) should include("object_type: catalog")
      storage.read(new Path(outDir, "models/tpch_mini.yml")) should include("orders_self")
      storage.read(
        new Path(outDir, "dimensions/orders order_calendar Dimension.yml")
      ) should include("time_unit: quarter")
      storage.read(
        new Path(outDir, "connections/tpch_mini - TPCH.SF1.yml")
      ) should include("as_connection: snowflake_prod")
    }
  }

  it should "keep hierarchies in the Ossie extension and let LookML and TMDL ignore them" in {
    new WithSettings() {
      cleanMetadata
      val storage = settings.storageHandler()
      storage.write(modelYaml, new Path(DatasetArea.semantic, "tpch_mini.yaml"))

      Seq(
        SemanticExportConfig(),
        SemanticExportConfig(format = "lookml", connection = Some("wh")),
        SemanticExportConfig(format = "tmdl", connection = Some("unknown_connection"))
      ).foreach { config =>
        new SemanticExportJob(config).run() match {
          case Failure(exception) => throw exception
          case Success(_)         =>
        }
      }

      val ossie = YamlSerde.mapper.readTree(
        storage.read(new Path(DatasetArea.semantic, "export/ossie/tpch_mini.ossie.yaml"))
      )
      val orders = ossie.get("semantic_model").get(0).get("datasets").get(1)
      orders.get("name").asText() shouldBe "orders"
      val extensionData =
        orders.get("custom_extensions").elements().asScala.map(_.get("data").asText()).mkString
      extensionData should include("\"hierarchies\"")
      extensionData should include("order_calendar")
      storage.exists(
        new Path(DatasetArea.semantic, "export/lookml/tpch_mini/orders.view.lkml")
      ) shouldBe true
      storage.exists(
        new Path(DatasetArea.semantic, "export/tmdl/tpch_mini/tables/orders.tmdl")
      ) shouldBe true
    }
  }
}
