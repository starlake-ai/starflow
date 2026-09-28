package ai.starlake.semantic

import ai.starlake.utils.YamlSerde
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SMLGoldenSpec extends AnyFlatSpec with Matchers {

  "SMLConverter" should "export tpch_mini exactly as the checked-in SML repository" in {
    val model =
      YamlSerde.mapper.readTree(getClass.getResourceAsStream("/semantic/sml/tpch_mini.yaml"))
    val files = SMLConverter.convert("tpch_mini", model, "snowflake_prod")
    files.size shouldBe 19
    files.foreach { case (path, content) =>
      withClue(s"$path: ") {
        val expected = getClass.getResourceAsStream(s"/semantic/sml/expected/tpch_mini/$path")
        expected should not be null
        SMLConverter.yaml.readTree(content) shouldBe SMLConverter.yaml.readTree(expected)
      }
    }
  }
}
