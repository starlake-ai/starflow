package ai.starlake.semantic

import ai.starlake.TestHelper
import ai.starlake.config.DatasetArea
import org.apache.hadoop.fs.Path

import scala.util.{Failure, Success}

class TMDLExportSpec extends TestHelper {

  private val modelYaml =
    """name: ecommerce_analytics
      |description: E-commerce analytics model
      |tables:
      |  - name: orders
      |    base_table:
      |      database: ANALYTICS_DB
      |      schema: ECOMMERCE
      |      table: ORDERS
      |    dimensions:
      |      - name: order_id
      |        expr: ORDER_ID
      |        data_type: NUMBER
      |      - name: customer_id
      |        expr: CUSTOMER_ID
      |        data_type: NUMBER
      |    facts:
      |      - name: order_total
      |        expr: ORDER_TOTAL
      |        data_type: NUMBER
      |    metrics:
      |      - name: avg_order_value
      |        expr: AVG(order_total)
      |    primary_key:
      |      columns: [order_id]
      |  - name: customers
      |    base_table:
      |      database: ANALYTICS_DB
      |      schema: ECOMMERCE
      |      table: CUSTOMERS
      |    dimensions:
      |      - name: customer_id
      |        expr: CUSTOMER_ID
      |        data_type: NUMBER
      |    primary_key:
      |      columns: [customer_id]
      |relationships:
      |  - name: orders_to_customers
      |    left_table: orders
      |    right_table: customers
      |    relationship_columns:
      |      - left_column: customer_id
      |        right_column: customer_id
      |""".stripMargin

  "semantic-export --format tmdl" should "write a TMDL folder per model" in {
    new WithSettings() {
      cleanMetadata
      val storage = settings.storageHandler()
      storage.write(modelYaml, new Path(DatasetArea.semantic, "ecommerce.yaml"))

      new SemanticExportJob(
        SemanticExportConfig(format = "tmdl", connection = Some("unknown_connection"))
      ).run() match {
        case Failure(exception) => throw exception
        case Success(_)         =>
      }

      val outDir = new Path(DatasetArea.semantic, "export/tmdl/ecommerce_analytics")
      storage.read(new Path(outDir, "database.tmdl")) should include(
        "database ecommerce_analytics"
      )
      storage.read(new Path(outDir, "model.tmdl")) should include("model Model")

      val orders = storage.read(new Path(outDir, "tables/orders.tmdl"))
      orders should include("table orders")
      orders should include("\t\tisKey")
      orders should include("\tmeasure avg_order_value = AVERAGE('orders'[order_total])")
      // unknown connection name falls back to the generic source
      orders should include("// TODO Starflow: set the connector for your warehouse")
      orders should include("FROM ANALYTICS_DB.ECOMMERCE.ORDERS")

      storage.read(new Path(outDir, "relationships.tmdl")) should include(
        "relationship orders_to_customers"
      )
      storage.read(new Path(outDir, "tables/customers.tmdl")) should include("table customers")
    }
  }

  it should "leave the ossie and lookml exports untouched" in {
    new WithSettings() {
      cleanMetadata
      val storage = settings.storageHandler()
      storage.write(modelYaml, new Path(DatasetArea.semantic, "ecommerce.yaml"))

      new SemanticExportJob(SemanticExportConfig()).run() match {
        case Failure(exception) => throw exception
        case Success(_)         =>
      }
      storage.exists(
        new Path(DatasetArea.semantic, "export/ossie/ecommerce_analytics.ossie.yaml")
      ) shouldBe true

      new SemanticExportJob(
        SemanticExportConfig(format = "lookml", connection = Some("wh"))
      ).run() match {
        case Failure(exception) => throw exception
        case Success(_)         =>
      }
      storage.exists(
        new Path(DatasetArea.semantic, "export/lookml/ecommerce_analytics/orders.view.lkml")
      ) shouldBe true
    }
  }
}
