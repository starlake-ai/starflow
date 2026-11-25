package ai.starlake.dbt

import ai.starlake.TestHelper

class DbtGraphSpec extends TestHelper {
  "graph" should "be the manifest's nodes and sources, without a warehouse" in {
    val manifest = DbtFixtures.shopManifest
    val jinja = DbtFixtures.jinja(manifest)
    val node = DbtFixtures.node(manifest, "stg_orders")
    val models = manifest.nodes.count(_.resourceType == "model")
    val r = jinja.render(
      node,
      """{{ graph.nodes.values() | selectattr('resource_type', 'equalto', 'model') | list | length }}|{{ graph.sources['source.shop.shop_raw.raw_customers']['identifier'] }}|{{ graph.nodes | length }}""",
      incremental = false
    )
    r.sql.get.trim shouldBe s"$models|raw_customers|${manifest.nodes.size}"
  }
}
