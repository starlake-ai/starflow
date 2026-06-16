package ai.starlake.job.quack

import ai.starlake.TestHelper

class QuackCmdSpec extends TestHelper {
  new WithSettings() {

    "QuackCmd.parse" should "parse 'serve --connection foo --port 9999'" in {
      val cfg = QuackCmd.parse(Seq("serve", "--connection", "foo", "--port", "9999")).get
      cfg.action shouldBe "serve"
      cfg.connectionName shouldBe Some("foo")
      cfg.port shouldBe Some(9999)
    }

    it should "parse 'stop --connection bar'" in {
      val cfg = QuackCmd.parse(Seq("stop", "--connection", "bar")).get
      cfg.action shouldBe "stop"
      cfg.connectionName shouldBe Some("bar")
    }

    "QuackCmd.run" should "fail when --connection is missing on serve" in {
      val result = QuackCmd.run(
        QuackConfig(action = "serve"),
        schemaHandler = settings.schemaHandler()
      )
      result.isFailure shouldBe true
      result.failed.get.getMessage should include("--connection is required")
    }

    it should "fail when connection name is unknown" in {
      val result = QuackCmd.run(
        QuackConfig(action = "serve", connectionName = Some("does-not-exist")),
        schemaHandler = settings.schemaHandler()
      )
      result.isFailure shouldBe true
      result.failed.get.getMessage should include("Connection not found: does-not-exist")
    }

    it should "list as an empty result when no servers are running" in {
      // ensure clean state dir
      val dir = QuackState.stateDir(settings)
      if (dir.exists) dir.list.toList.foreach(_.delete(swallowIOExceptions = true))

      val result =
        QuackCmd.run(QuackConfig(action = "list"), schemaHandler = settings.schemaHandler())
      result.isSuccess shouldBe true
      result.get.asInstanceOf[QuackJobResult].rows shouldBe Nil
    }

    it should "succeed silently on 'stop' for an unknown connection" in {
      val result = QuackCmd.run(
        QuackConfig(action = "stop", connectionName = Some("never-started")),
        schemaHandler = settings.schemaHandler()
      )
      result.isSuccess shouldBe true
      result.get
        .asInstanceOf[QuackJobResult]
        .rows
        .head
        .last should include("no Quack server")
    }
  }
}
