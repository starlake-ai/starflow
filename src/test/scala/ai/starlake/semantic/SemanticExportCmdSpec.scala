package ai.starlake.semantic

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SemanticExportCmdSpec extends AnyFlatSpec with Matchers {

  "semantic-export parser" should "accept the lookml format and connection flag" in {
    val config = SemanticExportCmd.parse(
      Seq("--format", "lookml", "--connection", "analytics_wh", "--model", "ecommerce")
    )
    config shouldBe Some(
      SemanticExportConfig(
        format = "lookml",
        model = Some("ecommerce"),
        connection = Some("analytics_wh")
      )
    )
  }

  it should "keep accepting the ossie format and reject unknown formats" in {
    SemanticExportCmd.parse(Seq("--format", "ossie")) shouldBe Some(
      SemanticExportConfig(format = "ossie")
    )
    SemanticExportCmd.parse(Seq("--format", "tableau")) shouldBe None
  }

  it should "accept the tmdl format" in {
    SemanticExportCmd.parse(Seq("--format", "tmdl", "--connection", "pg_conn")) shouldBe Some(
      SemanticExportConfig(format = "tmdl", connection = Some("pg_conn"))
    )
  }

  it should "accept the sml format" in {
    SemanticExportCmd.parse(Seq("--format", "sml", "--connection", "snowflake_prod")) shouldBe Some(
      SemanticExportConfig(format = "sml", connection = Some("snowflake_prod"))
    )
  }
}
