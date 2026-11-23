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

  "SiteCmd" should "parse --serve and --port" in {
    val c = SiteCmd.parse(Seq("--serve", "--port", "9090")).get
    c.serve shouldBe true
    c.port shouldBe 9090
    SiteCmd.parse(Seq()).get.serve shouldBe false
    SiteCmd.parse(Seq()).get.port shouldBe 8080
  }

  it should "reject a --port outside 1..65535" in {
    SiteCmd.parse(Seq("--serve", "--port", "0")) shouldBe None
    SiteCmd.parse(Seq("--serve", "--port", "65536")) shouldBe None
    SiteCmd.parse(Seq("--serve", "--port", "-1")) shouldBe None
    SiteCmd.parse(Seq("--port", "65535")).get.port shouldBe 65535
    SiteCmd.parse(Seq("--port", "1")).get.port shouldBe 1
  }

  it should "reject removed options" in {
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

  "site --serve" should "fail quickly when the port is busy" in withFixture { implicit settings =>
    val busy = new java.net.ServerSocket(0, 50, java.net.InetAddress.getByName("127.0.0.1"))
    try {
      val out = File.newTemporaryDirectory("sl-site-busy-")
      val cfg = SiteConfig(out, Some(true), serve = true, port = busy.getLocalPort)
      val start = System.nanoTime()
      new SiteHandler(cfg, settings.schemaHandler()).run().isFailure shouldBe true
      ((System.nanoTime() - start) / 1000000000L) should be < 60L
    } finally busy.close()
  }

  it should "release the port and remove its hook when interrupted" in withFixture {
    implicit settings =>
      val probe = new java.net.ServerSocket(0)
      val port = probe.getLocalPort
      probe.close()
      val out = File.newTemporaryDirectory("sl-site-serve-")
      val handler =
        new SiteHandler(
          SiteConfig(out, Some(true), serve = true, port = port),
          settings.schemaHandler()
        )
      @volatile var result: Option[scala.util.Try[Unit]] = None
      val t = new Thread(() => result = Some(handler.run()))
      t.start()
      val deadline = System.currentTimeMillis() + 120000
      while (handler.servingPort == 0 && t.isAlive && System.currentTimeMillis() < deadline)
        Thread.sleep(50)
      handler.servingPort shouldBe port
      t.interrupt()
      t.join(30000)
      t.isAlive shouldBe false
      result.get.isFailure shouldBe true
      val again = new java.net.ServerSocket(port)
      again.close()
      val hook = handler.lastHook.get
      noException should be thrownBy {
        Runtime.getRuntime.addShutdownHook(hook)
        Runtime.getRuntime.removeShutdownHook(hook)
      }
  }
}
