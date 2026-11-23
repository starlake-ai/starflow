package ai.starlake.job.site

import ai.starlake.integration.IntegrationTestBase
import ai.starlake.job.Main
import better.files.File
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode

class SiteHandlerSpec extends IntegrationTestBase with SiteFixture {

  "site command" should "generate sl-model.json and index.html" in withFixture { _ =>
    val out = fixtureRoot / "site-out"
    new Main().run(Array("site", "--outputDir", out.pathAsString, "--clean")) shouldBe true
    (out / "sl-model.json").exists shouldBe true
    (out / "index.html").contentAsString should include("""id="sl-model"""")
    (out / "load").exists shouldBe false
  }

  "SiteCmd" should "reject removed options" in {
    SiteCmd.parse(Seq("--template", "standalone")) shouldBe None
    SiteCmd.parse(Seq("--json")) shouldBe None
  }

  "sl-model.json" should "match the golden file" in withFixture { implicit settings =>
    val out = File.newTemporaryDirectory("sl-site-golden-")
    new SiteHandler(SiteConfig(out, Some(true)), settings.schemaHandler()).run().get
    val mapper = new ObjectMapper()
    val actual = mapper.readTree((out / "sl-model.json").contentAsString).asInstanceOf[ObjectNode]
    actual.put("generatedAt", "<normalized>")
    actual.put("starlakeVersion", "<normalized>")
    actual.get("project").asInstanceOf[ObjectNode].put("name", "<normalized>")
    // Jackson's pretty printer uses the platform line separator; compare with \n only.
    val pretty =
      mapper.writerWithDefaultPrettyPrinter().writeValueAsString(actual).replace("\r\n", "\n")
    val golden = File("src/test/resources/site/expected-sl-model.json")
    if (sys.env.get("SL_UPDATE_GOLDEN").contains("true")) golden.overwrite(pretty)
    pretty shouldBe golden.contentAsString.replace("\r\n", "\n")
  }
}
