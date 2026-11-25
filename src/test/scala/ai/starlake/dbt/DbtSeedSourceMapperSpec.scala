package ai.starlake.dbt

import ai.starlake.TestHelper
import ai.starlake.config.DatasetArea
import ai.starlake.dbt.manifest.DbtRelations
import ai.starlake.dbt.mapping._
import ai.starlake.schema.model._
import ai.starlake.utils.YamlSerde
import org.apache.hadoop.fs.Path

import java.nio.file.Files
import scala.util.Success

class DbtSeedSourceMapperSpec extends TestHelper {
  new WithSettings() {
    private val manifest = DbtFixtures.shopManifest

    /** Every column of the header, as a string: Starlake's inference itself is not under test */
    private val stubInference
      : (java.nio.file.Path, String) => scala.util.Try[List[TableAttribute]] =
      (csv, _) =>
        Success(
          Files.readAllLines(csv).get(0).split(",").toList.map(c => TableAttribute(c, "string"))
        )

    private val ctx = DbtMappingContext(
      manifest,
      new DbtRelations(manifest),
      DbtFixtures.jinja(),
      None,
      stubInference
    )

    private lazy val seeds =
      manifest.nodesOfType("seed").map(DbtSeedMapper.map(_, ctx)).collect { case Right(s) => s }
    private lazy val sources =
      manifest.sources.map(DbtSourceMapper.map(_, ctx)).collect { case Right(s) => s }
    private lazy val outputs =
      DbtSeedMapper.outputs(seeds, None) ++ DbtSourceMapper.outputs(sources)

    private def file(suffix: String): FileOutput =
      outputs
        .collect { case f: FileOutput if f.path.toString.endsWith(suffix) => f }
        .headOption
        .getOrElse(
          fail(s"no output ending with $suffix in ${outputs.map(_.path)}")
        )

    private def table(area: String, domain: String, name: String): SchemaInfo = {
      val f = file(s"metadata/$area/$domain/$name.sl.yml")
      YamlSerde.deserializeYamlTables(f.content, f.path.toString).head.table
    }

    "A seed" should "be a DSV load table with its column_types, its CSV in the incoming area" in {
      val t = table("load", "raw", "raw_orders")
      t.metadata.flatMap(_.format) shouldBe Some(Format.DSV)
      t.metadata.flatMap(_.withHeader) shouldBe Some(true)
      t.metadata.flatMap(_.separator) shouldBe Some(",")
      t.metadata.flatMap(_.writeStrategy).flatMap(_.`type`) shouldBe Some(
        WriteStrategyType.OVERWRITE
      )
      t.attributes.find(_.name == "amount_cents").map(_.`type`) shouldBe Some("integer")
      t.pattern.matcher("raw_orders.csv").matches() shouldBe true
      outputs
        .collect { case c: CopyOutput => c.path.toString }
        .exists(_.endsWith("incoming/raw/raw_orders.csv")) shouldBe true
      file("metadata/load/raw/_config.sl.yml").content should include(
        "{{SL_DATASETS}}/incoming/raw"
      )
    }

    "A source" should "be an external table with its columns and freshness" in {
      val t = table("external", "legacy", "accounts")
      t.attributes.map(a => a.name -> a.`type`) shouldBe List(
        "account_id" -> "integer",
        "synced_at"  -> "timestamp"
      )
      t.metadata.flatMap(_.freshness) shouldBe Some(Freshness(Some("12 hours"), Some("1 day")))
      sources
        .find(_.entry.dbtId == "source.shop.crm.crm_accounts")
        .get
        .entry
        .notes
        .exists(_.contains("loaded_at_field synced_at")) shouldBe true
    }

    it should "not be written when it is a seed's table" in {
      outputs.exists(_.path.toString.contains("metadata/external/raw/")) shouldBe false
      sources.find(_.entry.dbtId == "source.shop.shop_raw.raw_customers").get.seedId shouldBe Some(
        "seed.shop.raw_customers"
      )
    }

    "Freshness periods" should "be durations that Freshness reads" in {
      DbtSourceMapper.duration(ai.starlake.dbt.manifest.DbtPeriod(12, "hour")) shouldBe "12 hours"
      DbtSourceMapper.duration(ai.starlake.dbt.manifest.DbtPeriod(1, "day")) shouldBe "1 day"
    }

    "A source column data_type with no Starlake type" should "be noted as kept as string" in {
      val source = manifest.sources
        .find(_.uniqueId == "source.shop.crm.crm_accounts")
        .get
        .copy(columns =
          List(ai.starlake.dbt.manifest.DbtColumn("c", "", Some("no_such_type"), Nil))
        )
      val mapped = DbtSourceMapper.map(source, ctx).toOption.get
      mapped.table.get.attributes.map(a => a.name -> a.`type`) shouldBe List("c" -> "string")
      mapped.entry.notes should contain(
        "column c: data_type no_such_type has no Starlake type, kept as string"
      )
    }
  }

  new WithSettings() {
    private val manifest = DbtFixtures.shopManifest
    private val ctx = DbtMappingContext(
      manifest,
      new DbtRelations(manifest),
      DbtFixtures.jinja(),
      None,
      (_: java.nio.file.Path, _: String) => Success(List(TableAttribute("id", "long")))
    )
    private val userLoad = "version: 1\nload:\n  name: raw\n  metadata:\n    format: JSON\n"
    private val userExternal = "version: 1\nload:\n  name: legacy\n"

    private def outputs =
      DbtSeedMapper.outputs(
        manifest.nodesOfType("seed").map(DbtSeedMapper.map(_, ctx)).collect { case Right(s) => s },
        None
      ) ++
      DbtSourceMapper.outputs(
        manifest.sources.map(DbtSourceMapper.map(_, ctx)).collect { case Right(s) => s }
      )

    "An existing domain _config.sl.yml" should "be kept, the seed tables carrying their metadata" in {
      // in the test: the settings of the spec share one project folder
      storageHandler.write(userLoad, new Path(DatasetArea.load, "raw/_config.sl.yml"))
      storageHandler.write(userExternal, new Path(DatasetArea.external, "legacy/_config.sl.yml"))
      val outputs = this.outputs
      val paths = outputs.map(_.path.toString)
      paths.exists(_.endsWith("metadata/load/raw/_config.sl.yml")) shouldBe false
      paths.exists(_.endsWith("metadata/external/legacy/_config.sl.yml")) shouldBe false
      val f = outputs.collect {
        case f: FileOutput if f.path.toString.endsWith("load/raw/raw_orders.sl.yml") => f
      }.head
      val metadata = YamlSerde.deserializeYamlTables(f.content, f.path.toString).head.table.metadata
      metadata.flatMap(_.directory) shouldBe Some("{{SL_DATASETS}}/incoming/raw")
      metadata.flatMap(_.format) shouldBe Some(Format.DSV)
      metadata.flatMap(_.withHeader) shouldBe Some(true)
      metadata.flatMap(_.separator) shouldBe Some(",")
      metadata.flatMap(_.writeStrategy).flatMap(_.`type`) shouldBe Some(
        WriteStrategyType.OVERWRITE
      )
      storageHandler.read(new Path(DatasetArea.load, "raw/_config.sl.yml")) shouldBe userLoad
    }
  }
}
